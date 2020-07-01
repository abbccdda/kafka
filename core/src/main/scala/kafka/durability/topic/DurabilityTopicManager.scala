/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.topic

import java.util
import java.util.Properties
import java.util.function.Supplier

import kafka.durability.db.DurabilityDB
import kafka.durability.events.AbstractDurabilityEvent
import kafka.durability.exceptions.DurabilityServiceShuttingDownException
import kafka.utils.Logging
import kafka.zk.AdminZkClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException

import scala.collection.mutable

/**
 * The top level class, responsible for managing all sorts of functionality of the durability topic partitions.
 * This needs to be called after the durability database initialization.
 *
 * The manager also exposes api for sending the events to the durability topic partitions.
 * @param config the durability topic related configuration.
 * @param database the durability database instance.
 * @param zkSupplier admin zookeeper client supplier.
 */
class DurabilityTopicManager (val config: DurabilityTopicConfig,
                              val database: DurabilityDB,
                              val zkSupplier: Supplier[AdminZkClient]) extends Logging {
  /**
   * Backoff duration used while waiting for cluster to initialize.
   */
  private val DURABILITY_CREATION_BACKOFF_MS: Long = 5000L

  /**
   * ConsumerProvider manages the consumer functionality.
   */
  private[topic] lazy val consumerProvider: DurabilityTopicConsumer = new DurabilityTopicConsumer(config, database)

  /**
   * ProducerProvider provides the producer for topic partitions.
   */
  private[topic] lazy val producerProvider: DurabilityTopicProducer = new DurabilityTopicProducer(config)

  /**
   * Internal state to track readiness of the durability topic manager.
   */
  @volatile var ready: Boolean = false

  /**
   * Internal state to track if the manager is going to be shutdown.
   */
  @volatile var isShutdown: Boolean = false

  val queuedEvents: mutable.Queue[AbstractDurabilityEvent]  = mutable.Queue[AbstractDurabilityEvent]()

  /**
   * Externally visible interface to initialize and start producer consumer operations.
   */
  def start(): Unit = {
    try {
      while (!ready && !isShutdown) {
        if (!tryBecomeReady()) {
          warn("Failed to become ready.")
          Thread.sleep(DURABILITY_CREATION_BACKOFF_MS)
        }
      }
    } catch {
      case (e: Exception) => {
        if (isShutdown) debug("Ignoring exception caught during shutdown", e)
        else error("Caught exception while starting DurabilityTopicManager", e)
      }
    }
  }

  /**
   * Externally exposed interface to check state of topic manager.
   * @return
   */
  def isReady(): Boolean = ready && consumerProvider.isReady

  /**
   * Starts the producer and consumer functionality after making sure durability topic and partitions are up and accessible.
   * @return true if successfully started . In case cluster is not yet ready the method will return false and the caller
   *         will retry.
   */
  private def tryBecomeReady(): Boolean = {
    if (config.interBrokerClientConfigs.get.isEmpty) {
      info("Could not resolve bootstrap server. Will retry.")
      return false
    }
    // ensure durability topic is created; create one if not
    try ensureTopic(zkSupplier.get)
    catch {
      case e: Exception =>
        info("Caught exception when ensuring durability topic is created. Will retry.", e)
        return false
    }

    startConsumer()
    true
  }

  /**
   * Externally visible interface to shutdown the topic manger.
   */
  def shutdown(): Unit = {
    isShutdown = true
    cleanup()
  }

  /**
   * Starts the consumer and produce all pending events..
   */
  private def startConsumer(): Unit = {
    // start the consumer
    consumerProvider.startConsumer()

    this.synchronized {
      ready = true
      queuedEvents.foreach(addDurabilityEvent)
    }
  }

  /**
   * Externally visible interface for cleanup.
   */
  private def cleanup(): Unit = {
    this.synchronized {
      try {
        ready = false
        if (producerProvider != null) producerProvider.get.close()
      } finally {
        consumerProvider.shutdown()
      }
    }
  }

  /**
   * Creates the durability topic.
   * @param adminZkClient
   */
  private def ensureTopic(adminZkClient: AdminZkClient): Unit = {
    try {
      // try to create the topic
      adminZkClient.createTopic(config.topicName, config.configuredNumPartitions, config.configuredReplicationFactor)
      info(s"Created topic ${config.topicName} with ${config.configuredNumPartitions} partitions")
    } catch {
      case _: TopicExistsException =>
        // topic already exists; query the number of partitions
        val numPartitions = adminZkClient.numPartitions(Set(config.topicName))(config.topicName)

        if (numPartitions != config.configuredNumPartitions)
          warn(s"Topic ${config.topicName} already exists. Mismatch between existing partition count $numPartitions " +
            s"and configured partition count ${config.configuredNumPartitions}.")
        else
          debug(s"Topic ${config.topicName} exists with $numPartitions partitions")
    }
  }

  /**
   * Hash method to determine source topicPartition's mapping to durabilityTopicPartition.
   * @param topicPartition
   * @return
   */
  private def toPartition(topicPartition: TopicPartition) : Int = topicPartition.hashCode() % config.configuredNumPartitions

  /**
   * Externally visible interface for sending the durability event.
   * @param event
   * @throws DurabilityServiceShuttingDownException
   */
  @throws[DurabilityServiceShuttingDownException]
  def addDurabilityEvent(event: AbstractDurabilityEvent): Unit = this.synchronized {
    if (isShutdown) throw new  DurabilityServiceShuttingDownException("Service shutting down")

    if (!ready) queuedEvents.:+(event)
    else producerProvider.get.send(new ProducerRecord[Array[Byte], Array[Byte]](
      config.topicName, toPartition(event.topicPartition), event.serializeKey, event.serializeValue))
  }
}

/**
 * Configuration related to Durability Topic management.
 * @param interBrokerClientConfigs inter broker client configuration for communication.
 * @param topicName name of the durability events topic
 * @param configuredNumPartitions number of partitions configured for the durability events topic
 * @param configuredReplicationFactor replication factor for the partitions.
 * @param brokerId the id of the broker
 * @param clusterId the id of the cluster
 * @param pollDurationMs the polling duration for the durability topic consumer.
 * @param requestTimeoutMs the timeout value used by producer while sending events.
 */
case class DurabilityTopicConfig(interBrokerClientConfigs: Supplier[util.Map[String, AnyRef]],
                                 topicName: String,
                                 configuredNumPartitions: Short,
                                 configuredReplicationFactor: Short,
                                 brokerId: Int,
                                 clusterId: String,
                                 pollDurationMs: Long,
                                 requestTimeoutMs: Integer) {
  def toProducerProperties(clientId: String): Properties = {

    val properties = new Properties

    interBrokerClientConfigs.get.entrySet.forEach(x => properties.put(x.getKey, x.getValue))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
    properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, Integer.toString(2000))
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(Integer.MAX_VALUE))
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(requestTimeoutMs))
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    // Explicitly remove the metrics reporter configuration, as it is not expected to be configured for durability topic clients
    properties.remove(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG)
    properties
  }
}
