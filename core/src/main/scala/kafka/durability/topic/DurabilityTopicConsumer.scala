/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.topic

import java.time.Duration
import java.util.Properties

import kafka.durability.db.DurabilityDB
import kafka.utils.Logging

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.KafkaThread

/**
 * DurabilityConsumer class. This is responsible for listening for the durability audit events on the internal events
 * partitions. It fetches those events and passes it to the materialization layer(todo) where they are materialized before
 * getting updated to the database.
 *
 * The database also maintains the committed offset of each of the internal partitions. The consumer will call for update
 * of the committed offset at the end of the materialization call for each event. The database will update the committed
 * offset in its in-memory map and periodically database will checkpoint them along with rest of the database state.
 *
 * During restart, the consumer will recover committed offset state from the already initialized database and continue
 * fetching events after seeking accordingly.
 * @param config
 * @param db
 */
class DurabilityTopicConsumer(val config: DurabilityTopicConfig, val db: DurabilityDB) extends Logging with Runnable {
  /**
   * Volatile state 'ready' conveys if consumer is initialized to consume from durability topic partitions.
   */
  @volatile var ready = false

  /**
   * Volatile state doShutdown conveys if a shutdown of the consumer has already been issued.
   */
  @volatile var doShutdown  = false

  /**
   * consumerThread which performs the consumer tasks.
   * Todo: In an event of unexpected exception, we need to provide recovery mechanism of consumerThread.
   */
  private val consumerThread = new KafkaThread("DurabilityTopicConsumer", this, false)

  /**
   * External interface to query the readiness of the consumer thread.
   * @return true if ready.
   */
  def isReady: Boolean = ready

  /**
   * External interface to shutdown the consumer thread. If during shutdown handling an unexpected exception is raised
   * then we will log and ignore the exception and continue with completion of shutdown.
   */
  def shutdown(): Unit= this.synchronized {
    if (ready) {
      doShutdown = true
      consumer.wakeup()
      try {
        consumerThread.join()
      } catch {
        case ex: Exception => error("Shutdown interrupted, logging", ex)
      }
    }
  }

  /**
   * state representing the consumer.
   */
  lazy val consumer: Consumer[Array[Byte], Array[Byte]] = {
    val clientId = DurabilityTopicClient.clientId(config.clusterId, config.brokerId, 0)
    new KafkaConsumer[Array[Byte], Array[Byte]](properties(clientId))
  }

  /**
   * The consumer thread loops until its asked to shutdown. Fetches durability events in each loop and materializes them.
   */
  override def run(): Unit = {
    try {
      while (!doShutdown) processRecords(consumer.poll(Duration.ofMillis(config.pollDurationMs)))
    } catch {
      case e: Exception =>
        if (doShutdown) debug("Exception caught during shutdown", e)
        else error("Exception in TierTopicConsumer", e)
    } finally ready = false
  }

  def partitions(): Set[TopicPartition] = (0 to config.configuredNumPartitions-1)
    .map(x => new TopicPartition(config.topicName, x))
    .toSet

  /**
   * 'startConsumer' recovers the last committed offset for each of the durability topic partition, seeks accordingly
   *  before consuming events for materialization.
   * @param startConsumeThread
   */
  private[topic] def startConsumer(startConsumeThread: Boolean = true): Unit = {
    val tierTopicPartitions = partitions()
    val offsets = db.getDurabilityTopicPartitionOffsets()
    for (topicPartition <- tierTopicPartitions) {
      info("seeking durability consumer for partition " +  topicPartition.partition() + " to offset " +
        offsets(topicPartition.partition()))
      consumer.seek(topicPartition, offsets(topicPartition.partition()))
    }
    // for testing we may end up running this in foreground.
    if (startConsumeThread) consumerThread.start()
    ready = true
  }

  private[topic] def properties(clientId: String) = {
    val properties = new Properties
    config.interBrokerClientConfigs.get.entrySet.forEach(x => properties.put(x.getKey, x.getValue))
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    // Explicitly remove the metrics reporter configuration, as it is not expected to be configured for durability topic clients.
    properties.remove(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG)
    properties
  }

  private[topic] def processRecords(records: ConsumerRecords[Array[Byte], Array[Byte]]): Unit = None

}
