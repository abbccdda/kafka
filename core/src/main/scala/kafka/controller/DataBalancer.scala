/**
 * Copyright (C) 2020 Confluent Inc.
 */

package kafka.controller

import kafka.server.KafkaConfig
import org.apache.kafka.common.config.internals.ConfluentConfigs
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
  def startUp(): Unit

  def shutdown(): Unit

  def updateConfig(newConfig: KafkaConfig): Unit
}

object DataBalancer {
  private val log: Logger = LoggerFactory.getLogger(classOf[DataBalancer])
  def apply(kafkaConfig: KafkaConfig): Option[DataBalancer] = {
    val dataBalancerClassName = Option(kafkaConfig.getString(ConfluentConfigs.BALANCER_CLASS_CONFIG))
      .filter(!_.isEmpty).getOrElse((ConfluentConfigs.BALANCER_CLASS_DEFAULT));
    log.info(s"DataBalancer: attempting startup with ${dataBalancerClassName}")
    try {
      Some(Class.forName(dataBalancerClassName)
        .getConstructor(classOf[KafkaConfig]).newInstance(kafkaConfig).asInstanceOf[DataBalancer])
    } catch {
      case e: ClassNotFoundException => {
        log.warn(s"Unable to load data balancer class $dataBalancerClassName")
        None
      }
      case e: Exception => {
        log.error(s"Data balancer class load of ${dataBalancerClassName} failed: ", e)
        None
      }
    }
  }
}

