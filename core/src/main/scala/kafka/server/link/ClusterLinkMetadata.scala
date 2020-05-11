/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.server.KafkaConfig
import org.apache.kafka.clients._
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.utils.LogContext

import scala.jdk.CollectionConverters._

object ClusterLinkMetadata {
  def throttleTimeSensorName(linkName: String): String = s"linked-fetcher-throttle-time-$linkName"
}

/**
  * Metadata for a cluster link. A single metadata instance is managed to track metadata of all
  * topics mirrored on a link. This metadata is periodically refreshed by a ClusterLinkMetadataThread.
  *
  * Thread safety:
  *   - ClusterLinkMetadata is thread-safe. Operations are synchronized using the instance lock
  *     that is also used for synchronization in the base Metadata class.
  */
class ClusterLinkMetadata(val brokerConfig: KafkaConfig,
                          val linkName: String,
                          metadataRefreshBackoffMs: Long,
                          metadataMaxAgeMs: Long)
  extends Metadata(metadataRefreshBackoffMs, metadataMaxAgeMs,
    new LogContext(s"[ClusterLinkMetadata brokerId=${brokerConfig.brokerId}, link=$linkName] "),
    new ClusterResourceListeners) {

  val throttleTimeSensorName = ClusterLinkMetadata.throttleTimeSensorName(linkName)

  private var linkedTopics = Set.empty[String]
  @volatile private var metadataRefreshListener: Option[MetadataRefreshListener] = None

  override def requestUpdate(): Int = {
    val updateVersion = super.requestUpdate()
    metadataRefreshListener.foreach(_.onMetadataRequestUpdate())
    updateVersion
  }

  private[link] def setTopics(topics: Set[String]): Unit = {
    synchronized {
      this.linkedTopics = topics
    }
    requestUpdate()
  }

  override def newMetadataRequestBuilder(): MetadataRequest.Builder = {
    synchronized {
      new MetadataRequest.Builder(linkedTopics.toList.asJava, false /* allowAutoTopicCreation */)
    }
  }

  override def retainTopic(topic: String, isInternal: Boolean, nowMs: Long): Boolean = {
    synchronized {
      linkedTopics.contains(topic)
    }
  }

  private[link] def setRefreshListener(listener: MetadataRefreshListener): Unit = {
    this.metadataRefreshListener = Some(listener)
  }
}

