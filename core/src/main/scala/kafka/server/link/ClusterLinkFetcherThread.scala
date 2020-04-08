/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.cluster.BrokerEndPoint
import kafka.server._
import kafka.tier.fetcher.TierStateFetcher
import org.apache.kafka.clients.FetchSessionHandler.FetchRequestData
import org.apache.kafka.clients.ManualMetadataUpdater
import org.apache.kafka.common.errors.InvalidMetadataException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{EpochEndOffset, FetchRequest, ListOffsetRequest, OffsetsForLeaderEpochRequest}
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.Map

object ClusterLinkFetcherThread {

  def apply(name: String,
            fetcherId: Int,
            brokerConfig: KafkaConfig,
            clusterLinkConfig: ClusterLinkConfig,
            clusterLinkMetadata: ClusterLinkMetadata,
            fetcherManager: ClusterLinkFetcherManager,
            sourceBroker: BrokerEndPoint,
            failedPartitions: FailedPartitions,
            replicaMgr: ReplicaManager,
            quota: ReplicaQuota,
            metrics: Metrics,
            time: Time,
            tierStateFetcher: Option[TierStateFetcher]): ClusterLinkFetcherThread = {

    val brokerId = brokerConfig.brokerId
    val logContext = new LogContext(s"[ClusterLinkFetcher brokerId=$brokerId " +
      s"fetcherId=$fetcherId] source(link=${clusterLinkMetadata.linkName}, leaderId=${sourceBroker.id})] ")

    val clusterLinkClient = new ClusterLinkNetworkClient(
      clusterLinkConfig,
      clusterLinkMetadata.throttleTimeSensorName,
      metadata = None,
      Some(new ManualMetadataUpdater),
      metrics: Metrics,
      Map("link-name" -> clusterLinkMetadata.linkName, "broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString),
      time,
      s"link-${clusterLinkMetadata.linkName}-broker-$brokerId-fetcher-$fetcherId",
      logContext
    )
    val leaderEndpoint = new ReplicaFetcherBlockingSend(sourceBroker,
      brokerConfig,
      clusterLinkConfig.replicaSocketTimeoutMs,
      time,
      clusterLinkClient.networkClient,
      reconfigurableChannelBuilder = None)

    new ClusterLinkFetcherThread(name, fetcherId, brokerConfig,
      clusterLinkConfig, clusterLinkMetadata, fetcherManager, sourceBroker,
      failedPartitions, replicaMgr, quota, metrics, time, tierStateFetcher,
      clusterLinkClient, leaderEndpoint, Some(logContext))
  }
}

class ClusterLinkFetcherThread(name: String,
                               fetcherId: Int,
                               brokerConfig: KafkaConfig,
                               clusterLinkConfig: ClusterLinkConfig,
                               clusterLinkMetadata: ClusterLinkMetadata,
                               fetcherManager: ClusterLinkFetcherManager,
                               sourceBroker: BrokerEndPoint,
                               failedPartitions: FailedPartitions,
                               replicaMgr: ReplicaManager,
                               quota: ReplicaQuota,
                               metrics: Metrics,
                               time: Time,
                               tierStateFetcher: Option[TierStateFetcher],
                               private[link] val clusterLinkClient: ClusterLinkNetworkClient,
                               leaderEndpointBlockingSend: BlockingSend,
                               logContextOpt: Option[LogContext] = None)
  extends ReplicaFetcherThread(name = name,
                               fetcherId = fetcherId,
                               sourceBroker = sourceBroker,
                               brokerConfig = brokerConfig,
                               failedPartitions = failedPartitions,
                               replicaMgr = replicaMgr,
                               metrics = metrics,
                               time = time,
                               quota,
                               tierStateFetcher = tierStateFetcher,
                               Some(leaderEndpointBlockingSend),
                               logContextOpt) {

  private val maxWait = clusterLinkConfig.replicaFetchWaitMaxMs
  private val minBytes = clusterLinkConfig.replicaFetchMinBytes
  private val maxBytes = clusterLinkConfig.replicaFetchResponseMaxBytes

  override protected val fetchSize = clusterLinkConfig.replicaFetchMaxBytes

  override protected def fetchRequestBuilder(fetchData: FetchRequestData): FetchRequest.Builder = {
    FetchRequest.Builder.forConsumer(maxWait, minBytes, fetchData.toSend)
      .setMaxBytes(maxBytes)
      .toForget(fetchData.toForget)
      .metadata(fetchData.metadata)
  }

  override protected def offsetsForLeaderEpochRequestBuilder(partitions: Map[TopicPartition, EpochData]): OffsetsForLeaderEpochRequest.Builder = {
    OffsetsForLeaderEpochRequest.Builder.forConsumer(partitions.asJava)
  }

  override protected def listOffsetRequestBuilder(partitionTimestamps: Map[TopicPartition, ListOffsetRequest.PartitionData]): ListOffsetRequest.Builder = {
    ListOffsetRequest.Builder.forConsumer(false /* require_timestamp */ , IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(partitionTimestamps.asJava)
  }

  // We only support cluster linking when source cluster supports OffsetsForLeaderEpoch
  override def isOffsetForLeaderEpochSupported: Boolean = true

  // We don't expect to handle tiered exceptions from the source cluster, error will be processed by the caller
  override protected def onOffsetTiered(topicPartition: TopicPartition, requestEpoch: Option[Int]): Boolean = false

  override protected def onPartitionFenced(tp: TopicPartition, requestEpoch: Option[Int]): Boolean = {
    debug(s"onPartitionFenced $tp : request metadata ")
    clusterLinkMetadata.requestUpdate()
    super.onPartitionFenced(tp, requestEpoch)
  }

  override protected def handlePartitionsWithErrors(partitions: Map[TopicPartition, Errors], methodName: String): Unit = {
    if (partitions.values.exists(_.exception.isInstanceOf[InvalidMetadataException])) {
      debug(s"Request metadata update because of errors $partitions")
      clusterLinkMetadata.requestUpdate()
    }
    super.handlePartitionsWithErrors(partitions, methodName)
  }

  override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
    val epochOffsets = super.fetchEpochEndOffsets(partitions)
    epochOffsets.foreach { case (tp, epochOffset) =>
      val currentSourceEpoch = clusterLinkMetadata.currentLeader(tp).epoch.orElse(-1)
      val offsetEpoch = epochOffset.leaderEpoch
      val endOffset = epochOffset.endOffset
      fetcherManager.partition(tp).foreach { partition =>
        if (!epochOffset.hasError) {
          debug(s"Processing offsets from linked leader for $tp")
          if (endOffset > 0 && offsetEpoch > 0 && offsetEpoch < currentSourceEpoch) {
            debug(s"Update linked leader epoch cache $tp sourceEpoch=${offsetEpoch + 1} startOffset=$endOffset")
            partition.localLogOrException.maybeAssignEpochStartOffset(offsetEpoch + 1, endOffset)
          }
          partition.linkedLeaderOffsetsPending(false)
        }
      }
    }
    epochOffsets
  }
}
