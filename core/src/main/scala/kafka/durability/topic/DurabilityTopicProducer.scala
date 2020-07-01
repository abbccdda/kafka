/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.topic

import kafka.utils.Logging
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.producer.KafkaProducer

/**
 * DurabilityTopicProducer provides producer needed for sending the durability events to the durability topic partitions.
 * @param config
 */
class DurabilityTopicProducer(config: DurabilityTopicConfig) extends Logging {
  private val instanceId = new AtomicInteger(0)

  /**
   * Gets KafkaProducer instance which is responsible for sending  the events.
   * @return
   */
  lazy val get: KafkaProducer[Array[Byte], Array[Byte]] = {
    val id = DurabilityTopicClient.clientId(config.clusterId, config.brokerId, instanceId.getAndIncrement)
    new KafkaProducer[Array[Byte], Array[Byte]](config.toProducerProperties(id))
  }
}

case object DurabilityTopicClient {
  private val SEPARATOR = "-"

  private val CLIENT_ID_PREFIX = "__kafka.durabilityTopicManager."

  /**
   * Client id prefix to use for durability topic clients.
   *
   * @return Client id prefix to use
   */
  private def clientIdPrefix(): String = CLIENT_ID_PREFIX

  /**
   * Check if the clientId is one used by durability topic clients.
   *
   * @param clientId The client id to check
   * @return true if the client id is one used by durability topic clients; false otherwise
   */
  def isDurabilityTopicClient(clientId: String): Boolean = {
    clientId != null && clientId.startsWith(CLIENT_ID_PREFIX)
  }

  // visible for testing
  def clientId(clusterId: String, brokerId: Int, instanceId: Int): String =
    DurabilityTopicClient.clientIdPrefix() + SEPARATOR + clusterId + SEPARATOR + brokerId + SEPARATOR + instanceId

}
