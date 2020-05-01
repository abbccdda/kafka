/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.utils.{Logging, ShutdownableThread}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.Cluster

import scala.collection.{Map, mutable}

trait MetadataListener {
  def onNewMetadata(newCluster: Cluster): Unit
}

trait MetadataRefreshListener {
  def onMetadataRequestUpdate(): Unit
}

class ClusterLinkMetadataThread(clusterLinkConfig: ClusterLinkConfig,
                                clusterLinkMetadata: ClusterLinkMetadata,
                                metrics: Metrics,
                                time: Time)
  extends ShutdownableThread(s"LinkMetadata-${clusterLinkMetadata.linkName}") with MetadataRefreshListener with Logging {

  private val logContext = new LogContext(s"[ClusterLinkMetadataClient ${clusterLinkMetadata.linkName}] ")
  private[link] val clusterLinkClient = createNetworkClient(clusterLinkConfig, clusterLinkMetadata)

  private val metadataListeners = mutable.Buffer[MetadataListener]()
  @volatile private var currentMetadataCluster: Cluster = _
  clusterLinkMetadata.setRefreshListener(this)

  override def doWork(): Unit = {
    try {
      clusterLinkClient.networkClient.poll(Long.MaxValue.toLong, time.milliseconds)

      val newMetadataCluster = clusterLinkMetadata.fetch()
      if (newMetadataCluster ne currentMetadataCluster) {
        debug(s"Process cluster link metadata $newMetadataCluster")
        metadataListeners.foreach(_.onNewMetadata(newMetadataCluster))
        currentMetadataCluster = newMetadataCluster
      }
    } catch {
      case e: Exception => error("Failed to refresh metadata", e)
    }
  }

  override def initiateShutdown(): Boolean = {
    clusterLinkClient.initiateClose()
    super.initiateShutdown()
  }

  override def shutdown(): Unit = {
    clusterLinkClient.close()
    super.shutdown()
  }

  def addListener(listener: MetadataListener): Unit = {
    metadataListeners += listener
  }

  protected def createNetworkClient(clusterLinkConfig: ClusterLinkConfig,
                                    clusterLinkMetadata: ClusterLinkMetadata): ClusterLinkNetworkClient = {
    new ClusterLinkNetworkClient(
      clusterLinkConfig,
      clusterLinkMetadata.throttleTimeSensorName,
      Some(clusterLinkMetadata),
      metadataUpdater = None,
      metrics,
      Map("link-name" -> clusterLinkMetadata.linkName),
      time,
      s"cluster-link-metadata-${clusterLinkMetadata.linkName}-broker-${clusterLinkMetadata.brokerConfig.brokerId}",
      logContext)
  }

  override def onMetadataRequestUpdate(): Unit = {
    clusterLinkClient.networkClient.wakeup()
  }
}
