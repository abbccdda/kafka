/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tasks.archive

import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.{Gauge, Meter}
import kafka.log.AbstractLog
import kafka.metrics.KafkaMetricsGroup
import kafka.server.ReplicaManager
import kafka.tier.fetcher.CancellationContext
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.{TierTaskWorkingSet, TierTasksConfig}
import kafka.tier.topic.TierTopicAppender
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{ExecutionContext, Future}

/**
  * Tier Archiver uploads segment files, associated indices and other broker status to blob storage.
  * Segments for a topic partition are eligible for upload if `tierEnable` is set in `logConfig`, and
  * the Tier Archiver is running on the broker that is the current leader for the topic partition.
  *
  * An asynchronous status machine is created from each eligible topic partition, where (potentially blocking)
  * status transitions can be executed serially without blocking status transitions for other topic partitions.
  * These status transitions are performed in the provided execution context. Because the number of threads
  * available to use via the execution context is a limited resource, there is a `priority()` method on each
  * status, allowing the derivation of relative priority between status transitions. State transitions are
  * scheduled based on this `priority()` method.
  */
final class TierArchiver(config: TierTasksConfig,
                         replicaManager: ReplicaManager,
                         tierTopicAppender: TierTopicAppender,
                         tierObjectStore: TierObjectStore,
                         ctx: CancellationContext,
                         maxTasks: Int,
                         time: Time = Time.SYSTEM)(implicit ec: ExecutionContext) extends KafkaMetricsGroup with Logging {

  override protected def loggerName: String = classOf[TierArchiver].getName

  private final val TOP_LAGGING_PARTITIONS_COUNT = 5

  removeMetric("BytesPerSec")
  private val byteRate = newMeter("BytesPerSec", "bytes per second", TimeUnit.SECONDS)

  removeMetric("RetriesPerSec")
  private val retryRate = newMeter("RetriesPerSec", "number of retries per second", TimeUnit.SECONDS)

  // Sum of lag of all partitions
  @volatile private var totalLagValue = 0L
  removeMetric("TotalLagValue")
  newGauge("TotalLagValue", new Gauge[Long] {
    def value(): Long = totalLagValue
  })

  // Lag value (in bytes) of the partition with the highest lag
  @volatile private var partitionLagMaxValue = 0L
  removeMetric("PartitionLagMaxValue")
  newGauge("PartitionLagMaxValue", new Gauge[Long] {
    def value(): Long = partitionLagMaxValue
  })

  // Count of partitions seen with lag > 0
  @volatile private var laggingPartitionsCount = 0
  removeMetric("LaggingPartitionsCount")
  newGauge("LaggingPartitionsCount", new Gauge[Int] {
    def value(): Int = laggingPartitionsCount
  })

  private[tasks] val taskQueue = new ArchiverTaskQueue(ctx.subContext(), maxTasks, time, schedulingLag,
    ArchiverMetrics(Some(byteRate), Some(retryRate)))
  private val workingSet = new TierTaskWorkingSet[ArchiveTask](taskQueue, replicaManager, tierTopicAppender,
    tierObjectStore, config.maxRetryBackoffMs, time)

  /**
    * Initiate transitions for tasks and complete transitions if outstanding futures have completed. This method is
    * non-blocking.
    * @return List of outstanding futures
    */
  def doWork(): List[Future[ArchiveTask]] = {
    workingSet.doWork()
  }

  def shutdown(): Unit = {
    ctx.cancel()
    taskQueue.close()
  }

  /**
   * Returns archiver lag information for lagging partitions with their lag. Method is kept visible
   * for testing.
   *
   * @return   a list containing lagging partitions along with their lag value
   */
  def partitionLagInfo: List[(TopicPartition, Long)] = {
    return replicaManager
      .leaderPartitionsIterator
      .flatMap(_.log)
      .filter(_.tierPartitionState.isTieringEnabled)
      .map(log => (log.topicPartition, sizeOfTierableSegments(log)))
      .filter { case (_, size) => size > 0 }
      .toList
      .sortBy { case (_, size) => 0 - size }
  }

  def logPartitionLagInfo(): Unit = {
    val laggingPartitions = partitionLagInfo
    totalLagValue = laggingPartitions.map(_._2).sum
    info("Sum of TierArchiver lag of all partitions: " + totalLagValue)
    laggingPartitionsCount = laggingPartitions.size
    val topLaggingPartitions = laggingPartitions.take(TOP_LAGGING_PARTITIONS_COUNT)
    if (topLaggingPartitions.nonEmpty) {
      info(
          s"$laggingPartitionsCount partitions seen with lag > 0. Partitions with most" +
            s" TierArchiver lag in descending order of lag (TopicPartition, LagInBytes):" +
            s" $topLaggingPartitions")
      partitionLagMaxValue = topLaggingPartitions(0)._2
    }
  }

  private def sizeOfTierableSegments(log: AbstractLog): Long = {
    log.tierableLogSegments.map(_.size.toLong).sum
  }

  /**
    * Tasks are ordered in the following way,
    *
    *   1. AfterUpload > Upload > BeforeLeader > BeforeUpload
    *
    *   2. A partition in the BeforeUpload state with low
    *      lag always comes before a partition in the BeforeUpload
    *      state with high lag.
    *
    * `schedulingLag()` returns an Option[Long] suitable for sorting
    * tasks using a less-than comparison. That is to say, more important tasks
    * will have a lower return value while less important tasks will
    * have a higher return value. If a lag cannot be found, None is returned.
    */
  private def schedulingLag(task: ArchiveTask): Option[Long] = {
    task.state match {
      case _: BeforeLeader => Some(-1)
      case _: BeforeUpload =>
        replicaManager
          .getLog(task.topicPartition)
          .map { log => sizeOfTierableSegments(log) }
      case _: Upload => Some(-2)
      case _: AfterUpload => Some(-3)
    }
  }
}

case class ArchiverMetrics(byteRateOpt: Option[Meter], retryRateOpt: Option[Meter])
