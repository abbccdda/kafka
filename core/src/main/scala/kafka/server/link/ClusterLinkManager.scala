/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util
import java.util.Properties

import kafka.api.KAFKA_2_3_IV1
import kafka.cluster.Partition
import kafka.server.{AdminManager, KafkaConfig, ReplicaManager, ReplicaQuota}
import kafka.tier.fetcher.TierStateFetcher
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.{Admin, ConfluentAdmin}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ClusterLinkExistsException, ClusterLinkInUseException, ClusterLinkNotFoundException, InvalidClusterLinkException}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, mutable}

/**
  * Cluster link manager for managing cluster links and linked replicas. One ClusterLinkManager instance
  * is present in each broker to manage replicas for which this broker is the leader of the destination partition
  * and the source partition is in a linked cluster. Each link is managed by a collection of manager instances.
  * A ClusterLinkFetcherManager manages replication from the source cluster.
  *
  *
  * Thread safety:
  *   - ClusterLinkManager is thread-safe. All update operations including creation and
  *     deletion of links as well as addition and removal of topic mirrors are performed while
  *     holding the `managersLock`.
  *   - Locking order: ClusterLinkManager.managersLock -> ClusterLinkFetcherManager.lock
  *                    ClusterLinkManager.managersLock -> ClusterLinkClientManager.lock
  *
  */
class ClusterLinkManager(brokerConfig: KafkaConfig,
                         clusterId: String,
                         quota: ReplicaQuota,
                         zkClient: KafkaZkClient,
                         metrics: Metrics,
                         time: Time,
                         threadNamePrefix: Option[String] = None,
                         tierStateFetcher: Option[TierStateFetcher]) extends Logging {

  private case class Managers(fetcherManager: ClusterLinkFetcherManager, clientManager: ClusterLinkClientManager)

  private val managersLock = new Object
  private val managers = mutable.Map[String, Managers]()
  val scheduler = new ClusterLinkScheduler
  val admin = new ClusterLinkAdminManager(brokerConfig, clusterId, zkClient, this)
  var replicaManager: ReplicaManager = _
  var adminManager: AdminManager = _

  def startup(replicaManager: ReplicaManager, adminManager: AdminManager): Unit = {
    this.replicaManager = replicaManager
    this.adminManager = adminManager
    scheduler.startup()
  }

  def addOrUpdateClusterLink(linkName: String, configs: Properties): Unit = {
    val exists = managersLock synchronized {
      if (!managers.contains(linkName)) {
        addClusterLink(linkName, configs)
        false
      } else
        true
    }
    if (exists) {
      val newConfigs = new util.HashMap[String, String]
      configs.stringPropertyNames.forEach(key => newConfigs.put(key, configs.getProperty(key)))
      reconfigureClusterLink(linkName, newConfigs)
    }
  }

  def addClusterLink(linkName: String, configs: Properties): Unit = {
    // Support for OffsetsForLeaderEpoch in clients was added in 2.3.0. This is the minimum supported version
    // for cluster linking.
    if (brokerConfig.interBrokerProtocolVersion <= KAFKA_2_3_IV1)
      throw new InvalidClusterLinkException(s"Cluster linking is not supported with inter-broker protocol version ${brokerConfig.interBrokerProtocolVersion}")

    val config = new ClusterLinkConfig(configs)
    managersLock synchronized {
      if (managers.contains(linkName))
        throw new ClusterLinkExistsException(s"Cluster link '$linkName' exists")

      val clientManager = new ClusterLinkClientManager(linkName, scheduler, zkClient, config,
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
            throw new ClusterLinkInUseException("Cluster link cannot be deleted since some local topics are currently linked to this cluster")
          }
        case None =>
          throw new ClusterLinkNotFoundException(s"Cluster link '$linkName' not found")
      }
    }
  }

  def reconfigureClusterLink(linkName: String, newProps: util.Map[String, _]): Unit = {
    val linkManagers = managersLock synchronized {
      managers.getOrElse(linkName, throw new ClusterLinkNotFoundException(s"Cluster link not found: $linkName"))
    }
    val newConfig = new ClusterLinkConfig(newProps)
    linkManagers.fetcherManager.reconfigure(newConfig)
    linkManagers.clientManager.reconfigure(newConfig)
  }

  def addPartitions(partitions: collection.Set[Partition]): Unit = {
    if (partitions.nonEmpty) {
      debug(s"addPartitions $partitions")
      val unknownClusterLinks = mutable.Map[String, Iterable[TopicPartition]]()
      managersLock synchronized {
        partitions.filter(_.isActiveLinkDestination).groupBy(_.getClusterLink.getOrElse(""))
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
        throw new ClusterLinkNotFoundException(s"Unknown cluster links: $unknownClusterLinks")
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
      val (linkedPartitions, unlinkedPartitions)  = partitionStates.partition { case (_, partitionState) =>
        Partition.clusterLinkTopicState(partitionState).exists(_.shouldSync)
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
    scheduler.shutdown()
    info("shutdown completed")
  }

  protected def newClusterLinkFetcherManager(linkName: String, config: ClusterLinkConfig): ClusterLinkFetcherManager = {
    val manager = new ClusterLinkFetcherManager(
      linkName,
      config,
      brokerConfig,
      replicaManager,
      adminManager,
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
