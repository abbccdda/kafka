/**
 * Copyright (C) 2020 Confluent Inc.
 * All Rights Reserved.
 */

package kafka.controller

import kafka.server.KafkaConfig

/*
 * The DataBalancer provides cluster balancing services for the cluster controller; it
 * monitors cluster load and will suggest/generate partition reassignments so that
 * brokers are equally loaded (for some definition of equal), taking into account
 * partition counts, disk usage, CPU, and network usage, among other factors).
 *
 * This is a facade object around the CruiseControl code. This object is expected to
 * be long-lived (lifetime of the broker) even as the node gains and loses responsibility
 * for doing data balancing -- analogous to (and co-located with) the controller.
 */
trait DataBalancer {
  def startUp() : Unit

  def shutdown() : Unit
}

object DataBalancer {
  def apply(kafkaConfig: KafkaConfig): DataBalancer = new KafkaDataBalancer(kafkaConfig)
}

// Concrete implementation: the KafkaDataBalancer
class KafkaDataBalancer(kafkaConfig: KafkaConfig) extends DataBalancer {
  // TODO: Convert this into a KafkaCruiseControlConfig.
  private val balancerConfig = kafkaConfig

  override def startUp() : Unit = { }

  override def shutdown() : Unit = { }

}

object KafkaDataBalancer {
  def apply(kafkaConfig: KafkaConfig) = {
    new KafkaDataBalancer(kafkaConfig)
  }
}
