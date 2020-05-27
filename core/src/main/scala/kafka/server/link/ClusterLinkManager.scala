/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.time.Duration
import java.util.{Collections, Properties, ServiceLoader}

import kafka.api.KAFKA_2_3_IV1
import kafka.cluster.Partition
import kafka.controller.KafkaController
import kafka.server.{AdminManager, KafkaConfig, ReplicaManager, ReplicaQuota}
import kafka.server.link.ClusterLinkManager._
import kafka.tier.fetcher.TierStateFetcher
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.{ClientInterceptor, CommonClientConfigs, NetworkClient}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, ConfluentAdmin, KafkaAdminClient}
import org.apache.kafka.common.{Endpoint, TopicPartition}
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

object ClusterLinkManager {
  val DestinationTenantPrefixProp = "cluster.link.destination.tenant.prefix"

  def tenantInterceptor(destTenantPrefix: String): ClientInterceptor = {
    val configs = Collections.singletonMap(DestinationTenantPrefixProp, destTenantPrefix)
    ServiceLoader.load(classOf[ClientInterceptor]).asScala
      .find { interceptor =>
        try {
          interceptor.configure(configs)
          true
        } catch {
          case _: Throwable => false
        }
      }.getOrElse(throw new InvalidClusterLinkException("Cluster link interceptor not found"))
  }
}

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
                         tierStateFetcher: Option[TierStateFetcher])
  extends ClusterLinkFactory.LinkManager with Logging {

  private case class Managers(fetcherManager: ClusterLinkFetcherManager, clientManager: ClusterLinkClientManager)

  private val managersLock = new Object
  private val managers = mutable.Map[String, Managers]()
  val scheduler = new ClusterLinkScheduler
  val admin = new ClusterLinkAdminManager(brokerConfig, clusterId, zkClient, this)
  val configEncoder = new ClusterLinkConfigEncoder(brokerConfig)

  private var replicaManager: ReplicaManager = _

  var adminManager: AdminManager = _
  private var controller: KafkaController = _
  private var authorizer: Option[Authorizer] = _
  private var interBrokerEndpoint: Endpoint = _
  private var destAdminClient: Admin = _

  def startup(interBrokerEndpoint: Endpoint,
              replicaManager: ReplicaManager,
              adminManager: AdminManager,
              controller: KafkaController,
              authorizer: Option[Authorizer]): Unit = {
    this.interBrokerEndpoint = interBrokerEndpoint
    this.replicaManager = replicaManager
    this.adminManager = adminManager
    this.controller = controller
    this.authorizer = authorizer
    scheduler.startup()
  }

  /**
    * Process link update notifications. This is invoked to add existing cluster links during
    * broker start up and to update existing cluster links when config update notifications
    * are processed. All updates are expected to be processed on a single thread.
    */
  def processClusterLinkChanges(linkName: String, persistentProps: Properties): Unit = {
    val clusterLinkProps = configEncoder.clusterLinkProps(persistentProps)

    val existingManager = managersLock synchronized {
      val linkManager = managers.get(linkName)
      if (persistentProps.isEmpty) {
        if (managers.contains(linkName))
          removeClusterLink(linkName)
        None
      } else if (linkManager.isEmpty) {
        addClusterLink(linkName, clusterLinkProps)
        None
      } else {
        linkManager
      }
    }
    existingManager.foreach(manager => reconfigureClusterLink(manager, clusterLinkProps))
  }

  def addClusterLink(linkName: String, clusterLinkProps: ClusterLinkProps): Unit = {
    // Support for OffsetsForLeaderEpoch in clients was added in 2.3.0. This is the minimum supported version
    // for cluster linking.
    if (brokerConfig.interBrokerProtocolVersion <= KAFKA_2_3_IV1)
      throw new InvalidClusterLinkException(s"Cluster linking is not supported with inter-broker protocol version ${brokerConfig.interBrokerProtocolVersion}")

    val config = clusterLinkProps.config
    managersLock synchronized {
      if (managers.contains(linkName))
        throw new ClusterLinkExistsException(s"Cluster link '$linkName' exists")

      val clientInterceptor = clusterLinkProps.tenantPrefix.map(tenantInterceptor)
      val clientManager = new ClusterLinkClientManager(linkName, scheduler, zkClient, config,
        authorizer, controller,
        (cfg: ClusterLinkConfig) => newSourceAdmin(linkName, cfg, clientInterceptor),
        () => getOrCreateDestAdmin())
      clientManager.startup()

      val fetcherManager = new ClusterLinkFetcherManager(
        linkName,
        config,
        clientInterceptor,
        brokerConfig,
        replicaManager,
        getOrCreateDestAdmin(),
        quota,
        metrics,
        time,
        threadNamePrefix,
        tierStateFetcher)
      fetcherManager.startup()

      managers.put(linkName, Managers(fetcherManager, clientManager))
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

  private def reconfigureClusterLink(linkManagers: Managers, newProps: ClusterLinkProps): Unit = {
    val newConfig = newProps.config
    linkManagers.fetcherManager.reconfigure(newConfig)
    linkManagers.clientManager.reconfigure(newConfig)
  }

  def addPartitions(partitions: collection.Set[Partition]): Unit = {
    if (partitions.nonEmpty) {
      debug(s"addPartitions $partitions")
      val unknownClusterLinks = mutable.Map[String, Iterable[TopicPartition]]()
      managersLock synchronized {
        partitions.filter(_.isActiveLinkDestinationLeader).groupBy(_.getClusterLink.getOrElse(""))
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
        Partition.clusterLinkShouldSync(partitionState)
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
    if (scheduler != null)
      scheduler.shutdown()
    admin.shutdown()
    if (destAdminClient != null)
      destAdminClient.close(Duration.ZERO)
    info("shutdown completed")
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

  private def newSourceAdmin(linkName: String,
                             config: ClusterLinkConfig,
                             clientInterceptor: Option[ClientInterceptor]): ConfluentAdmin = {
    val configs = config.originals
    configs.put(CommonClientConfigs.CLIENT_ID_CONFIG, s"cluster-link-admin-${brokerConfig.brokerId}-$linkName")
    val confluentAdmin = Admin.create(configs).asInstanceOf[ConfluentAdmin]
    clientInterceptor.foreach { interceptor =>
      confluentAdmin match {
        case adminClient: KafkaAdminClient =>
          adminClient.client match {
            case networkClient: NetworkClient => networkClient.interceptor(interceptor)
            case client => throw new IllegalStateException(s"Network interceptor not supported for $client")
          }
        case client => throw new IllegalStateException(s"Network interceptor not supported for adminClient $client")
      }
    }
    confluentAdmin
  }

  private def getOrCreateDestAdmin(): Admin = {
    // Create an admin client for the destination cluster using the inter-broker listener
    if (destAdminClient == null) {
      val adminConfigs = ConfluentConfigs.interBrokerClientConfigs(brokerConfig, interBrokerEndpoint)
      adminConfigs.remove(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG)
      adminConfigs.put(CommonClientConfigs.CLIENT_ID_CONFIG, s"cluster-link-admin-${brokerConfig.brokerId}")
      destAdminClient = Admin.create(adminConfigs)
    }
    destAdminClient
  }
}

