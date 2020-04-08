/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.Properties

import kafka.api.KAFKA_2_3_IV1
import kafka.cluster.Partition
import kafka.server.{KafkaConfig, ReplicaManager, ReplicaQuota}
import kafka.tier.fetcher.TierStateFetcher
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, mutable}

/**
  * Replica manager for linked replicas. One ClusterLinkReplicaManager instance is present in
  * each broker to manage replicas for which this broker is the leader of the destination partition
  * and the source partition is in a cluster link. Every link is managed by a ClusterLinkFetcherManager.
  *
  * Thread safety:
  *   - ClusterLinkReplicaManager is thread-safe. All update operations including creation and
  *     deletion of links as well as addition and removal of topic mirrors are performed while
  *     holding the `managerLock`.
  *   - Locking order: ClusterLinkReplicaManager.managerLock -> ClusterLinkFetcherManager.lock
  *
  */
class ClusterLinkReplicaManager(brokerConfig: KafkaConfig,
                                replicaManager: ReplicaManager,
                                quota: ReplicaQuota,
                                metrics: Metrics,
                                time: Time,
                                threadNamePrefix: Option[String] = None,
                                tierStateFetcher: Option[TierStateFetcher]) extends Logging {

  private val managerLock = new Object
  private val fetcherManagers = mutable.Map[String, ClusterLinkFetcherManager]()

  def addClusterLink(linkName: String, configs: Properties): Unit = {
    // Support for OffsetsForLeaderEpoch in clients was added in 2.3.0. This is the minimum supported version
    // for cluster linking.
    if (brokerConfig.interBrokerProtocolVersion <= KAFKA_2_3_IV1)
      throw new InvalidClusterLinkException(s"Cluster linking is not supported with inter-broker protocol version ${brokerConfig.interBrokerProtocolVersion}")

     managerLock synchronized {
       if (fetcherManagers.contains(linkName))
         throw new InvalidClusterLinkException(s"Cluster link '$linkName' exists")
       fetcherManagers.put(linkName, newClusterLinkFetcherManager(linkName, configs))
    }
  }

  def removeClusterLink(linkName: String): Unit = {
     managerLock synchronized {
      val manager = fetcherManagers.getOrElse(linkName,
        throw new InvalidClusterLinkException(s"Cluster link '$linkName' not found"))
      if (manager.isEmpty) {
        fetcherManagers.remove(linkName)
        manager.shutdown()
      } else {
        throw new IllegalStateException("Cluster link cannot be deleted since some local topics are currently linked to this cluster")
      }
    }
  }

  def addLinkedFetcherForPartitions(partitions: collection.Set[Partition]): Unit = {
    if (partitions.nonEmpty) {
      debug(s"addLinkedFetcherForPartitions $partitions")
      val unknownClusterLinks = mutable.Map[String, Iterable[TopicPartition]]()
        managerLock synchronized {
        partitions.groupBy(_.getClusterLink.getOrElse(""))
          .foreach { case (linkName, linkPartitions) =>
            if (!linkName.isEmpty) {
              fetcherManagers.get(linkName) match {
                case Some(manager) =>
                  manager.addLinkedFetcherForPartitions(linkPartitions)
                case None =>
                  unknownClusterLinks += linkName -> linkPartitions.map(_.topicPartition)
              }
            }
          }
      }
      if (unknownClusterLinks.nonEmpty) {
        error(s"Cannot add linked fetcher for $unknownClusterLinks")
        throw new InvalidClusterLinkException(s"Unknown cluster links: $unknownClusterLinks")
      }
    }
  }

  def removeLinkedFetcherAndMetadataForPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    managerLock synchronized {
      fetcherManagers.values.foreach(_.removeLinkedFetcherForPartitions(partitions, retainMetadata = false))
    }
  }

  /**
    * Removes fetcher for the specified partitions. If the partition doesn't has a cluster link
    * any more, metadata for the partition is also deleted.
    */
  def removeLinkedFetcherForPartitions(partitionStates: Map[Partition, LeaderAndIsrPartitionState]): Unit = {
    managerLock synchronized {
      val (unlinkedPartitions, linkedPartitions)  = partitionStates.partition { case (partition, partitionState) =>
        partitionState.clusterLink == null || partitionState.clusterLink.isEmpty
      }
      fetcherManagers.values.foreach { fetcherManager =>
        if (unlinkedPartitions.nonEmpty) {
          fetcherManager.removeLinkedFetcherForPartitions(unlinkedPartitions.map(_._1.topicPartition).toSet,
            retainMetadata = false)
        }
        if (linkedPartitions.nonEmpty) {
          fetcherManager.removeLinkedFetcherForPartitions(linkedPartitions.map(_._1.topicPartition).toSet,
            retainMetadata = true)
        }
      }
    }
  }

  def shutdownIdleFetcherThreads(): Unit = {
     managerLock synchronized {
      fetcherManagers.values.foreach(_.shutdownIdleFetcherThreads())
    }
  }

  def shutdown(): Unit = {
    info("shutting down")
     managerLock synchronized {
      fetcherManagers.values.foreach(_.shutdown())
    }
    info("shutdown completed")
  }

  protected def newClusterLinkFetcherManager(linkName: String, configs: Properties): ClusterLinkFetcherManager = {
    val clusterLinkConfig = new ClusterLinkConfig(configs)
    val manager = new ClusterLinkFetcherManager(
      linkName,
      clusterLinkConfig,
      brokerConfig,
      replicaManager,
      quota,
      metrics,
      time,
      threadNamePrefix,
      tierStateFetcher)
    manager.start()
    manager
  }

  def fetcherManager(linkName: String): Option[ClusterLinkFetcherManager] = {
    managerLock synchronized {
      fetcherManagers.get(linkName)
    }
  }
}
