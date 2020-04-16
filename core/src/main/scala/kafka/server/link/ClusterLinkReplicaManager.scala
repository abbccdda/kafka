/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import kafka.api.KAFKA_2_3_IV1
import kafka.cluster.Partition
import kafka.server.{KafkaConfig, ReplicaManager, ReplicaQuota}
import kafka.tier.fetcher.TierStateFetcher
import kafka.utils.Logging
import org.apache.kafka.clients.admin.{Admin, ConfluentAdmin}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, mutable}

/**
  * Replica manager for linked replicas. One ClusterLinkReplicaManager instance is present in
  * each broker to manage replicas for which this broker is the leader of the destination partition
  * and the source partition is in a linked cluster. Every link is managed by a ClusterLinkFetcherManager.
  *
  * Thread safety:
  *   - ClusterLinkReplicaManager is thread-safe. All update operations including creation and
  *     deletion of links as well as addition and removal of topic mirrors are performed while
  *     holding the `managersLock`.
  *   - Locking order: ClusterLinkReplicaManager.managersLock -> ClusterLinkFetcherManager.lock
  *                    ClusterLinkReplicaManager.managersLock -> ClusterLinkClientManager.lock
  *
  */
class ClusterLinkReplicaManager(brokerConfig: KafkaConfig,
                                replicaManager: ReplicaManager,
                                quota: ReplicaQuota,
                                metrics: Metrics,
                                time: Time,
                                threadNamePrefix: Option[String] = None,
                                tierStateFetcher: Option[TierStateFetcher]) extends Logging {

  private case class Managers(val fetcherManager: ClusterLinkFetcherManager, val clientManager: ClusterLinkClientManager)

  private val managersLock = new Object
  private val managers = mutable.Map[String, Managers]()

  def addClusterLink(linkName: String, config: ClusterLinkConfig): Unit = {
    // Support for OffsetsForLeaderEpoch in clients was added in 2.3.0. This is the minimum supported version
    // for cluster linking.
    if (brokerConfig.interBrokerProtocolVersion <= KAFKA_2_3_IV1)
      throw new InvalidClusterLinkException(s"Cluster linking is not supported with inter-broker protocol version ${brokerConfig.interBrokerProtocolVersion}")

    managersLock synchronized {
      if (managers.contains(linkName))
        throw new InvalidClusterLinkException(s"Cluster link '$linkName' exists")

      val clientManager = new ClusterLinkClientManager(linkName, config,
        (cfg: ClusterLinkConfig) => Admin.create(cfg.originals).asInstanceOf[ConfluentAdmin])
      clientManager.startup()

      managers.put(linkName, Managers(newClusterLinkFetcherManager(linkName, config), clientManager))
    }
  }

  def removeClusterLink(linkName: String): Unit = {
    managersLock synchronized {
      managers.get(linkName) match {
        case Some(Managers(fetcherManager, clientManager)) =>
          if (fetcherManager.isEmpty) {
            managers.remove(linkName)
            fetcherManager.shutdown()
            clientManager.shutdown()
          } else {
            throw new IllegalStateException("Cluster link cannot be deleted since some local topics are currently linked to this cluster")
          }
        case None =>
          throw new InvalidClusterLinkException(s"Cluster link '$linkName' not found")
      }
    }
  }

  def addPartitions(partitions: collection.Set[Partition]): Unit = {
    if (partitions.nonEmpty) {
      debug(s"addPartitions $partitions")
      val unknownClusterLinks = mutable.Map[String, Iterable[TopicPartition]]()
      managersLock synchronized {
        partitions.groupBy(_.getClusterLink.getOrElse(""))
          .foreach { case (linkName, linkPartitions) =>
            if (!linkName.isEmpty) {
              managers.get(linkName) match {
                case Some(Managers(fetcherManager, clientManager)) =>
                  fetcherManager.addLinkedFetcherForPartitions(linkPartitions)

                  val firstPartitionTopics = linkPartitions.filter(_.topicPartition.partition == 0).map(_.topicPartition.topic)
                  if (firstPartitionTopics.nonEmpty) {
                    clientManager.addTopics(firstPartitionTopics)
                  }

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

  def removePartitionsAndMetadata(partitions: collection.Set[TopicPartition]): Unit = {
    val firstPartitionTopics = partitions.filter(_.partition == 0).map(_.topic).toSet
    managersLock synchronized {
      managers.values.foreach { case Managers(fetcherManager, clientManager) =>
        fetcherManager.removeLinkedFetcherForPartitions(partitions, retainMetadata = false)
        if (firstPartitionTopics.nonEmpty) {
          clientManager.removeTopics(firstPartitionTopics)
        }
      }
    }
  }

  /**
    * Removes the managers for the specified partitions. If the partition doesn't have a cluster link
    * any more, metadata for the partition is also deleted.
    */
  def removePartitions(partitionStates: Map[Partition, LeaderAndIsrPartitionState]): Unit = {
    val firstPartitionTopics = partitionStates.map(_._1.topicPartition).filter(_.partition == 0).map(_.topic).toSet
    managersLock synchronized {
      val (unlinkedPartitions, linkedPartitions)  = partitionStates.partition { case (partition, partitionState) =>
        partitionState.clusterLink == null || partitionState.clusterLink.isEmpty
      }
      managers.values.foreach { case Managers(fetcherManager, clientManager) =>
        if (unlinkedPartitions.nonEmpty) {
          fetcherManager.removeLinkedFetcherForPartitions(unlinkedPartitions.map(_._1.topicPartition).toSet,
            retainMetadata = false)
        }
        if (linkedPartitions.nonEmpty) {
          fetcherManager.removeLinkedFetcherForPartitions(linkedPartitions.map(_._1.topicPartition).toSet,
            retainMetadata = true)
        }
        if (firstPartitionTopics.nonEmpty) {
          clientManager.removeTopics(firstPartitionTopics)
        }
      }
    }
  }

  def shutdownIdleFetcherThreads(): Unit = {
    managersLock synchronized {
      managers.values.foreach(_.fetcherManager.shutdownIdleFetcherThreads())
    }
  }

  def shutdown(): Unit = {
    info("shutting down")
    managersLock synchronized {
      managers.values.foreach { case Managers(fetcherManager, clientManager) =>
        fetcherManager.shutdown()
        clientManager.shutdown()
      }
    }
    info("shutdown completed")
  }

  protected def newClusterLinkFetcherManager(linkName: String, config: ClusterLinkConfig): ClusterLinkFetcherManager = {
    val manager = new ClusterLinkFetcherManager(
      linkName,
      config,
      brokerConfig,
      replicaManager,
      quota,
      metrics,
      time,
      threadNamePrefix,
      tierStateFetcher)
    manager.startup()
    manager
  }

  def fetcherManager(linkName: String): Option[ClusterLinkFetcherManager] = {
    managersLock synchronized {
      managers.get(linkName).map(_.fetcherManager)
    }
  }

  def clientManager(linkName: String): Option[ClusterLinkClientManager] = {
    managersLock synchronized {
      managers.get(linkName).map(_.clientManager)
    }
  }
}
