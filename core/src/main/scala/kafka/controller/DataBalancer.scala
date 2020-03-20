/**
 * Copyright (C) 2020 Confluent Inc.
 */

package kafka.controller

import kafka.server.KafkaConfig
import org.slf4j.{Logger, LoggerFactory}

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
  private val log: Logger = LoggerFactory.getLogger(classOf[DataBalancer])
  def apply(kafkaConfig: KafkaConfig): Option[DataBalancer] = {
    log.info("DataBalancer: attempting startup")
    try {
      Some(Class.forName("io.confluent.databalancer.KafkaDataBalancer")
        .getConstructor(classOf[KafkaConfig]).newInstance(kafkaConfig).asInstanceOf[DataBalancer])
    } catch {
      case e : ClassNotFoundException => {
        log.warn("Unable to find data balancer class", e)
        None
      }
      case e: Exception => {
        log.error(s"Unexpected Exception ", e)
        None
      }
    }
  }
}

