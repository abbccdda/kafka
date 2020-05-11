/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util

import org.apache.kafka.clients._
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}

import scala.jdk.CollectionConverters._
import scala.collection.Map


/**
  * Network client used for clients connecting to other cluster links.
  *
  * @param clusterLinkConfig Link config
  * @param throttleTimeSensorName Name of sensor used to track throttle time
  * @param metadata Metadata, which may be empty when manual metadata updater is used
  * @param metadataUpdater Metadata updater, which may be empty if default updater is used
  * @param metrics Kafka metrics instance
  * @param metricTags metric tags for this client
  * @param time Time instance used by the client
  * @param clientId Client-id for requests from this client
  * @param logContext Log context instance
  */
class ClusterLinkNetworkClient(clusterLinkConfig: ClusterLinkConfig,
                               throttleTimeSensorName: String,
                               metadata: Option[Metadata],
                               metadataUpdater: Option[MetadataUpdater],
                               metrics: Metrics,
                               metricTags: Map[String, String],
                               time: Time,
                               clientId: String,
                               logContext: LogContext) extends Reconfigurable {

  private val throttleTimeSensor = metrics.sensor(throttleTimeSensorName)
  private val channelBuilder = ChannelBuilders.clientChannelBuilder(
    clusterLinkConfig.securityProtocol,
    JaasContext.Type.CLIENT,
    clusterLinkConfig,
    null,
    clusterLinkConfig.saslMechanism,
    time,
    true,
    logContext
  )
  private val selector = new Selector(
    NetworkReceive.UNLIMITED,
    clusterLinkConfig.connectionsMaxIdleMs,
    metrics,
    time,
    "cluster-link",
    metricTags.asJava,
    false,
    channelBuilder,
    logContext
  )
  val networkClient = createNetworkClient(selector)

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def reconfigurableConfigs(): util.Set[String] = {
    channelBuilder match {
      case reconfigurable: Reconfigurable =>
        reconfigurable.reconfigurableConfigs()
      case _ =>
        util.Collections.emptySet()
    }
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    channelBuilder match {
      case reconfigurable: Reconfigurable =>
        reconfigurable.validateReconfiguration(configs)
      case _ =>
    }
  }

  override def reconfigure(newConfigs: util.Map[String, _]): Unit = {
    channelBuilder match {
      case reconfigurable: Reconfigurable =>
        reconfigurable.reconfigure(newConfigs)
      case _ =>
    }
  }

  def initiateClose(): Unit = {
    networkClient.initiateClose()
  }

  def close(): Unit = {
    networkClient.close()
  }

  protected def createNetworkClient(selector: Selector): KafkaClient = {
    new NetworkClient(
      metadataUpdater.orNull,
      metadata.orNull,
      selector,
      clientId,
      1,
      0,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      clusterLinkConfig.replicaSocketReceiveBufferBytes,
      clusterLinkConfig.requestTimeoutMs,
      clusterLinkConfig.dnsLookup,
      time,
      true,
      new ApiVersions,
      throttleTimeSensor,
      logContext)
  }
}

