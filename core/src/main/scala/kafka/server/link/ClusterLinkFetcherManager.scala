/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util

import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.server._
import kafka.tier.fetcher.TierStateFetcher
import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients._
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.errors.ReassignmentInProgressException
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.{Avg, Max}
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common._

import scala.collection.{Map, Set, mutable}
import scala.collection.JavaConverters._


/**
  * Fetcher manager associated with one cluster link. Each ClusterLinkFetcherManager manages
  * fetcher threads for replicating data from replicas associated with that link. One or more fetcher
  * threads, each of which has one connection to a source broker, may be associated with every pair of
  * (sourceBroker, destinationBroker). In addition to these fetcher threads which are very similar to
  * the threads managed for inter-broker replication within a cluster, ClusterLinkFetcherManager also
  * maintains one thread for metadata management.
  *
  * Thread safety:
  *   - FetcherManagers are thread-safe. The lock of the base AbstractFetcherManager is used
  *     for synchronization.
  */
class ClusterLinkFetcherManager(linkName: String,
                                initialConfig: ClusterLinkConfig,
                                brokerConfig: KafkaConfig,
                                replicaManager: ReplicaManager,
                                adminManager: AdminManager,
                                quota: ReplicaQuota,
                                metrics: Metrics,
                                time: Time,
                                threadNamePrefix: Option[String] = None,
                                tierStateFetcher: Option[TierStateFetcher] = None)
  extends AbstractFetcherManager[ClusterLinkFetcherThread](
    name = s"ClusterLinkFetcherManager on broker ${brokerConfig.brokerId} for $linkName",
    clientId = s"ClusterLinkFetcher-$linkName",
    numFetchers = initialConfig.numClusterLinkFetchers) with MetadataListener  {

  private val linkedPartitions = mutable.Map[TopicPartition, PartitionAndState]()
  private val unassignedPartitions = mutable.Set[TopicPartition]()

  @volatile private var metadata: ClusterLinkMetadata = _
  @volatile private var metadataRefreshThread: ClusterLinkMetadataThread = _
  @volatile private var clusterLinkConfig = initialConfig
  initialize()

  private def initialize(): Unit = {
    info(s"Initializing fetcher manager for cluster link $linkName")

    val throttleTimeSensor = metrics.sensor(ClusterLinkMetadata.throttleTimeSensorName(linkName))
    val tags = util.Collections.singletonMap("link-name", linkName)
    val throttleTimeAvg = new MetricName("fetch-throttle-time-avg", "cluster-link",
      "The average throttle time in ms", tags)
    val throttleTimeMax = new MetricName("fetch-throttle-time-max", "cluster-link",
      "The maximum throttle time in ms", tags)
    throttleTimeSensor.add(throttleTimeAvg, new Avg)
    throttleTimeSensor.add(throttleTimeMax, new Max)
    initializeMetadata()
  }

  def startup(): Unit = {
    debug("Starting fetcher manager")
    metadataRefreshThread.start()
  }

  private def initializeMetadata(): Unit = {
    val config = clusterLinkConfig
    metadata = new ClusterLinkMetadata(brokerConfig,
      linkName,
      config.metadataRefreshBackoffMs,
      config.metadataMaxAgeMs)
    metadataRefreshThread = new ClusterLinkMetadataThread(config, metadata, metrics, time)
    metadataRefreshThread.addListener(this)
    val addresses = ClientUtils.parseAndValidateAddresses(
      config.bootstrapServers,
      config.dnsLookup)
    metadata.bootstrap(addresses)
  }

  /**
    * Reconfigures cluster link clients. If only dynamic configs are updated (e.g. SSL keystore),
    * changes are applied to existing clients without any disruption. If non-dynamic configs are
    * updated (e.g. bootstrap servers), metadata and fetcher threads are restarted to recreate all clients.
    *
    * At most one reconfiguration may be in progress at any time.
    */
  def reconfigure(newConfig: ClusterLinkConfig): Unit = {
    val oldConfig = currentConfig
    val currentProps = oldConfig.originals
    val newProps = newConfig.originals
    val changeMap = newProps.asScala.filter { case (k, v) => v != currentProps.get(k) }
    val deletedKeys = currentProps.asScala.filter { case (k, _) => !newProps.containsKey(k) }

    if (changeMap.nonEmpty || deletedKeys.nonEmpty) {
      val restartMetadata = lock synchronized {
        val updatedKeys = changeMap.keySet ++ deletedKeys.keySet
        info(s"Reconfiguring link $linkName with new configs updated=$updatedKeys newConfig=${newConfig.values}")
        if (SslConfigs.RECONFIGURABLE_CONFIGS.containsAll(updatedKeys.asJava)) {
          debug(s"Reconfiguring cluster link fetchers with updated configs: $updatedKeys")
          metadataRefreshThread.clusterLinkClient.validateReconfiguration(newProps)
          val newConfigValues = newConfig.values
          metadataRefreshThread.clusterLinkClient.reconfigure(newConfigValues)
          fetcherThreadMap.values.map(_.clusterLinkClient).foreach(_.reconfigure(newConfigValues))
          this.clusterLinkConfig = newConfig
          false
        } else {
          debug(s"Recreating cluster link fetchers with updated configs: $updatedKeys")
          fetcherThreadMap.values.foreach(_.partitionsAndOffsets.keySet.foreach(unassignedPartitions.add))
          this.clusterLinkConfig = newConfig
          closeAllFetchers()
          true
        }
      }
      // Restart metadata thread without holding fetcher manager lock since metadata thread
      // acquires fetcher manager lock to process new metadata
      if (restartMetadata) {
        metadataRefreshThread.shutdown()
        initializeMetadata()
        updateMetadataTopics()
        metadataRefreshThread.start()
      }
    } else
      debug("Not reconfiguring fetcher manager since configs haven't changed")
  }

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ClusterLinkFetcherThread = {
    val prefix = threadNamePrefix.map(prefix => s"$prefix:").getOrElse("")
    val threadName = s"${prefix}ClusterLinkFetcherThread-$fetcherId-$linkName-${sourceBroker.id}"

    ClusterLinkFetcherThread(threadName, fetcherId, brokerConfig,
      clusterLinkConfig, metadata, this, sourceBroker, failedPartitions,
      replicaManager, quota, metrics, time, tierStateFetcher)
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    metadataRefreshThread.shutdown()
    metrics.removeSensor(metadata.throttleTimeSensorName)
    info("shutdown completed")
  }

  override def onNewMetadata(newCluster: Cluster): Unit = {
    val linkedEpochChanges = mutable.Map[Partition, Int]()
    lock synchronized {
      val updatedPartitions = mutable.Set[TopicPartition]()
      debug(s"onNewMetadata linkedPartitions ${linkedPartitions.keySet} unassigned $unassignedPartitions : $newCluster")
      val partition0Topics = linkedPartitions.keySet.filter(_.partition == 0).map(_.topic).toSet
      if (partition0Topics.nonEmpty)
        maybeUpdatePartitionCount(partition0Topics, newCluster)

      linkedPartitions.foreach { case (tp, partitionAndState) =>
        val partition = partitionAndState.partition
        val oldLeaderAndEpoch = partitionAndState.sourceLeaderAndEpoch
        val newLeaderAndEpoch = metadata.currentLeader(tp)
        if (oldLeaderAndEpoch != LeaderAndEpoch.noLeaderOrEpoch() && oldLeaderAndEpoch != newLeaderAndEpoch)
            updatedPartitions += tp
        val newEpoch = newLeaderAndEpoch.epoch.orElse(-1)
        val oldEpoch = partition.getLinkedLeaderEpoch.getOrElse(-1)
        if (newEpoch >= 0 && oldEpoch < newEpoch) {
          partition.linkedLeaderOffsetsPending(true)
          updatedPartitions += tp
          linkedEpochChanges += partition -> newEpoch
        }
      }

      val partitionsToReassign = updatedPartitions.diff(unassignedPartitions)
      if (partitionsToReassign.nonEmpty) {
        removeFetcherForPartitions(partitionsToReassign)
        unassignedPartitions ++= partitionsToReassign
      }

      // Assign partitions to fetchers if we have leader metadata for any of the unassigned partitions
      maybeAddLinkedFetchers()
    }

    // Update linked epoch in ZK without holding fetcher manager lock.
    val failedUpdates = linkedEpochChanges.count { case (partition, newEpoch) =>
      !partition.updateLinkedLeaderEpoch(newEpoch)
    }
    if (failedUpdates > 0)
      metadata.requestUpdate()
  }

  private[link] def addLinkedFetcherForPartitions(partitions: Iterable[Partition]): Unit = {
    debug(s"addLinkedFetcherForPartitions $partitions")
    lock synchronized {
      partitions.foreach { partition =>
        partition.linkedLeaderOffsetsPending(true)
        linkedPartitions += partition.topicPartition -> new PartitionAndState(partition)
        unassignedPartitions += partition.topicPartition
      }
      updateMetadataTopics()
      maybeAddLinkedFetchers()
    }
  }

  private[link] def removeLinkedFetcherForPartitions(partitions: collection.Set[TopicPartition], retainMetadata: Boolean): Unit = {
    debug(s"removeLinkedFetcherForPartitions $partitions retainMetadata=$retainMetadata")
    lock synchronized {
      removeFetcherForPartitions(partitions)
      partitions.foreach { tp =>
        if (!retainMetadata) {
          unassignedPartitions.remove(tp)
          val partitionAndState = linkedPartitions.remove(tp)
          partitionAndState.foreach(_.partition.linkedLeaderOffsetsPending(false))
        }
      }
      updateMetadataTopics()
      if (retainMetadata)
        metadata.requestUpdate()
    }
  }

  private def updateMetadataTopics(): Unit = {
    metadata.setTopics(linkedPartitions.keySet.map(_.topic).toSet)
  }

  def isEmpty: Boolean = {
    lock synchronized {
      linkedPartitions.isEmpty
    }
  }

  def currentConfig: ClusterLinkConfig = clusterLinkConfig

  private[link] def currentMetadata: ClusterLinkMetadata = metadata

  private[link] def partition(tp: TopicPartition): Option[Partition] = {
    linkedPartitions.get(tp).map(_.partition)
  }

  private def maybeAddLinkedFetchers(): Unit = {
    lock synchronized {
      val assignablePartitions = mutable.Map[TopicPartition, InitialFetchState]()
      unassignedPartitions.foreach { tp =>
        val partitionAndState = linkedPartitions
          .getOrElse(tp, throw new IllegalStateException(s"Linked partition not found $tp"))
        val partition = partitionAndState.partition
        val leaderAndEpoch = metadata.currentLeader(tp)
        if (leaderAndEpoch.leader.isPresent && leaderAndEpoch.epoch.isPresent) {
          val sourceEpoch = leaderAndEpoch.epoch.get
          if (partition.getLeaderEpoch >= sourceEpoch) {
            val leader = leaderAndEpoch.leader.get
            val initialFetchState = InitialFetchState(
              BrokerEndPoint(leader.id, leader.host, leader.port),
              sourceEpoch,
              partition.localLogOrException.localLogEndOffset)
            debug(s"Adding fetcher for linked partition $tp $initialFetchState, localEpoch=${partition.getLeaderEpoch}")
            assignablePartitions += tp -> initialFetchState
            partitionAndState.sourceLeaderAndEpoch = leaderAndEpoch
          }
        }
      }

      addFetcherForPartitions(assignablePartitions)
      assignablePartitions.keySet.foreach(unassignedPartitions.remove)

      if (unassignedPartitions.nonEmpty || linkedPartitions.keySet.exists(failedPartitions.contains)) {
        debug(s"Request metadata due to unassigned partitions: $unassignedPartitions")
        metadata.requestUpdate()
      }
    }
  }

  private def maybeUpdatePartitionCount(topics: Set[String], cluster: Cluster): Unit = {

    def createPartitionsRequestData(topic: String): Option[CreatePartitionsTopic] = {
      val sourcePartitionCount = cluster.partitionCountForTopic(topic)
      val destPartitionCount = partitionCount(topic)
      if (destPartitionCount < sourcePartitionCount) {
        logger.debug(s"Increasing partitions for linked topic $topic from $destPartitionCount to $sourcePartitionCount")
        Some(new CreatePartitionsTopic().setName(topic).setCount(sourcePartitionCount).setAssignments(null))
      }  else if (destPartitionCount > sourcePartitionCount) {
        warn(s"Topic $topic has $destPartitionCount destination partitions, but only $sourcePartitionCount" +
          s" source partitions. This may be a transient issue or it could indicate that the source partition was" +
          s" deleted and recreated")
        None
      } else
        None
    }

    def callback(results: Map[String, ApiError]): Unit = {
      val (successful, failed) = results.partition(_._2 == ApiError.NONE)
      if (failed.nonEmpty)
        error(s"Could not update partition counts for $failed")
      if (successful.nonEmpty)
        debug(s"Updated partition counts for $topics : ${successful.keySet}")
    }

    val newPartitions = topics.flatMap(createPartitionsRequestData).toSeq
    if (newPartitions.nonEmpty) {
      try {
        adminManager.createPartitions(
          brokerConfig.requestTimeoutMs,
          newPartitions,
          validateOnly = false,
          brokerConfig.interBrokerListenerName,
          callback)
      } catch {
        case _: ReassignmentInProgressException =>
          debug(s"Reassignment is in progress, partitions could not be updated for $newPartitions")
        case e: Throwable =>
          error(s"Could not update partition counts: $newPartitions", e)
      }
    }
  }

  protected def partitionCount(topic: String): Int = {
    val topicMetadata = replicaManager.metadataCache
      .getTopicMetadata(Set(topic), brokerConfig.interBrokerListenerName)
    if (topicMetadata.isEmpty) 0 else topicMetadata.head.partitions.size
  }
}

class PartitionAndState(val partition: Partition) {
  var sourceLeaderAndEpoch: LeaderAndEpoch = LeaderAndEpoch.noLeaderOrEpoch()
}
