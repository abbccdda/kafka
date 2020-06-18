/*
 * Copyright 2018 Confluent Inc.
 */

package kafka.durability

import kafka.utils.Logging
import org.apache.kafka.common.metrics.{Gauge, MetricConfig, Metrics}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

/**
 * MetricsManager manages the metrics data and exposes the broker level durability metrics.
 * There are 3 broker level metrics today
 *
 * 1. total_messages: Total number of messages audited for the broker's durability audit. This metric is updated at the
 * end of a audit run on the entire broker. This metric is uniquely identified using durability run span counter.
 *
 * 2. external_lost_messages: Total number of messages found to be under durability loss, at the end of a durability audit
 * on the broker. Here we only account those loss which will effect the leader partition (something which will be seen by
 * consumer). This metric is updated at the end of a durability audit on the entire broker. This metric is uniquely
 * identified using durability run span counter.
 *
 * 3. total_lost_messages: Total number of brokers messages found to be under durability loss. This metric is updated on
 * continuous basis throughout the audit runs. The main purpose of this metric is to track increase in loss and raise alert
 * for timely action.
 *
 * @param brokerId id of the broker.
 * @param metrics object against which durability metrics will be registered and exposed.
 */

class MetricsManager(brokerId: String, metrics: Metrics) extends Logging {

  // Various durability metrics attributes.
  private val durabilityMetricGroupName = "DurabilityMetrics"

  // Limit a max value which a Durability run span(DRS) counter can get. DRS counter is used to distinguish between
  // various runs. This is send as tag with every metrics.
  // Typically every DRS will send its counter. We need to make sure being a tag the cardinality is not high. It is assumed
  // that we will not have more then 1 DRS per day and also at any given time we wont query more then 1 year of metrics.
  // Keeping all these in mind and some scope of overlap we are limiting the max cardinality to be 500.
  val DRS_MAX_CARDINALITY = 500

  private val durabilityScoreMetricsTags = new HashMap[String, String]().asJava

  // Maintains the map of TopicPartition and its Stats. These are updated at the end of durability audit on entire
  // partition and is used for computing the broker's durability  metrics. This map will be used to expose future
  // partition metrics.
  private var partitionStatsMap = HashMap[TopicPartition, Stats]()

  // Store the metrics raw data for various broker's partition during the audit runs.
  // Typically the data will be populated via audit jobs, in future when we will support fine level of recovery for audit
  // jobs, these may also be populate via recovery flow.
  private var activePartitionStatsMap = HashMap[TopicPartition, Stats]()

  // Tracks the total_lost_messages.
  @volatile var totalLostMessages: Long = 0

  // Maintains the total messages in last durability audit.
  private var totalMessages: Long = 0

  // Maintains externally visible lost messages count (messages lost from leader partition).
  private var externalLostMessages: Long = 0

  private val currentLossCounterGauge: Gauge[Long] = new Gauge[Long] {
    override def value(config: MetricConfig, now: Long): Long = this.synchronized {totalLostMessages}
  }

  private val totalLostMessagesMetrics = metrics.metricName("total_lost_messages", durabilityMetricGroupName,
    "Broker's durability lost count so far for the existing durability run")

  // Methods for registering and exposing durability metrics.
  private val totalMessagesGauge: Gauge[Long] = new Gauge[Long]() {
    override def value(config: MetricConfig, now: Long): Long = this.synchronized {totalMessages}
  }

  private val totalLostMessagesLeaderGauge: Gauge[Long] = new Gauge[Long]() {
    override def value(config: MetricConfig, now: Long): Long = this.synchronized {externalLostMessages}
  }

  private def totalMessagesMetrics = metrics.metricName("total_messages", durabilityMetricGroupName,
    "Durability metrics for broker ", durabilityScoreMetricsTags)

  private def externalLostMessagesMetrics = metrics.metricName("external_lost_messages", durabilityMetricGroupName,
    "Number of message lost on leader replicas during durability audit", durabilityScoreMetricsTags)

  metrics.addMetric(totalLostMessagesMetrics, currentLossCounterGauge)

  private def resetMetricsWithChangedTag(drsCounter: Int) = {
    metrics.removeMetric(totalMessagesMetrics)
    metrics.removeMetric(externalLostMessagesMetrics)

    durabilityScoreMetricsTags.put("durability_run_counter", drsCounter.toString)

    metrics.addMetric(totalMessagesMetrics, totalMessagesGauge)
    metrics.addMetric(externalLostMessagesMetrics, totalLostMessagesLeaderGauge)
  }

  /**
   * During Audit job span, each time a partition is completed with entire audit, this call is made to report the
   * durability stats for the given partition. Right now we expect this reporting only for loss found in leader partition.
   * This means that even if the partition is not leader but based on its local state and audit db state if the loss
   * is found in leader, it will be accounted. The reason for only leader side loss is to simulate durability impact
   * from consumer side, which will communicate with leader.
   * We do not expect multiple update within same span, if it happens we will log and ignore.
   * For all kinds of loss (including leaders) we also call reportDurabilityLoss.
   *
   * @param id is TopicPartition
   * @param stats is stats at end of audit on partition.
   */
  def updateStats(id: TopicPartition, stats: Stats): Unit = {
    if (activePartitionStatsMap.contains(id)) error(s"Stats for id $id already populated in ${activePartitionStatsMap.get(id)}")
    else activePartitionStatsMap.put(id, stats)
  }

  /**
   * During Audit job span, whenever a new loss is found, it's reported to update totalLostMessages. This will be used to trigger
   * alert in real time (or as soon as possible). This is done for all sorts of loss and is not limited to leader. This
   * counter is broker level.
   *
   * @param count number of new loss found.
   */
  def reportDurabilityLoss(count: Long = 1) = totalLostMessages += count

  /**
   * Called on resetting to new durability run span (auditing entire cluster). At this time, all the leader's stats reported are
   * aggregated to form new broker level metric values.
   * The aggregation is under lock to provide atomic update to both total and loss counter along with switching
   * partitionStatMap.
   *
   * @param counter is the counter for the durability run span which sets the DrsCounter tag.
   */
  def resetDurabilityRunSpan(counter: Int) = {
    this.synchronized {
      totalMessages = 0
      externalLostMessages = 0
      activePartitionStatsMap.foreach { case(_, stats) => totalMessages += stats.total; externalLostMessages += stats.loss }
      partitionStatsMap = activePartitionStatsMap
      activePartitionStatsMap = HashMap[TopicPartition, Stats]()
      resetMetricsWithChangedTag(counter % DRS_MAX_CARDINALITY)
      info(s"Completing new audit run(#$counter) for the broker metrics externalLostMessages: $externalLostMessages, " +
        s"totalLostMessages: $totalLostMessages and totalMessages: $totalMessages with stats from $partitionStatsMap.size partitions")
    }
  }
}

/**
 * Durability Stats of a given partition.
 * @param total refers to total count of messages.
 * @param loss refers to count of messages which falls under durability loss.
 */
case class Stats (total: Long, loss: Long) {
  override def toString: String = {
    "{message_count: " + total + ", loss_count: " + loss +  "}"
  }
}

object MetricsManager {
  // The MetricsManager caller will make sure its instantiated with right initialDrsCounter.
  def apply(brokerId: String, metrics: Metrics): MetricsManager = {
    val metricsManager = new MetricsManager(brokerId, metrics)
    metricsManager
  }
}