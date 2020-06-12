/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.time.Duration
import java.util.{Collections, Properties, ServiceLoader, UUID}

import kafka.api.KAFKA_2_3_IV1
import kafka.cluster.Partition
import kafka.controller.KafkaController
import kafka.server.{AdminManager, KafkaConfig, ReplicaManager, ReplicaQuota}
import kafka.server.link.ClusterLinkManager._
import kafka.tier.fetcher.TierStateFetcher
import kafka.utils.Logging
import kafka.zk.{AdminZkClient, ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.{ClientInterceptor, CommonClientConfigs, NetworkClient}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, ConfluentAdmin, KafkaAdminClient}
import org.apache.kafka.common.{Endpoint, TopicPartition}
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.{Measurable, MetricConfig, Metrics}
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
  *   - Locking order: ClusterLinkManager.updateLock -> ClusterLinkManager.lock
  *                    ClusterLinkManager.lock -> ClusterLinkFetcherManager.lock
  *                    ClusterLinkManager.lock -> ClusterLinkClientManager.lock
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

  private val adminZkClient = new AdminZkClient(zkClient)

  // Protects `managers`, `linkData`, and `clearTopicMirrors`.
  private val lock = new Object
  private val managers = mutable.Map[UUID, Managers]()
  private val linkData = mutable.Map[String, ClusterLinkData]()
  private val clearTopicMirrors = mutable.Map[UUID, ClusterLinkClearTopicMirrors]()

  // Lock that must be acquired to ensure consistency between persistent metadata and in-memory
  // data structures for cluster links.
  private val updateLock = new Object

  val scheduler = new ClusterLinkScheduler
  val admin = new ClusterLinkAdminManager(brokerConfig, clusterId, zkClient, this)
  val configEncoder = new ClusterLinkConfigEncoder(brokerConfig)

  private var replicaManager: ReplicaManager = _

  var adminManager: AdminManager = _
  var controller: KafkaController = _
  private var authorizer: Option[Authorizer] = _
  private var interBrokerEndpoint: Endpoint = _
  private var destAdminClient: ConfluentAdmin = _

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
    val linkCount: Measurable = (_: MetricConfig, _: Long) => {
      linkData.size
    }
    metrics.addMetric(metrics.metricName("link-count",
      "cluster-link-metrics",
      "Number of links for this cluster"), linkCount)
    scheduler.startup()
  }

  /**
    * Process link update notifications. This is invoked to add existing cluster links during
    * broker start up and to update existing cluster links when config update notifications
    * are processed.
    */
  def processClusterLinkChanges(linkId: UUID, persistentProps: Properties): Unit = {
    val clusterLinkData = zkClient.getClusterLinks(Set(linkId)).get(linkId)

    clusterLinkData match {
      case Some(cld) =>
        if (!cld.isDeleted && persistentProps.isEmpty)
          throw new IllegalStateException(s"Cluster link configuration properties not found for '$linkId'")
      case None =>
        if (!persistentProps.isEmpty)
          throw new IllegalStateException(s"Cluster link configuration properties found for deleted cluster link '$linkId'")
    }

    updateLock synchronized {
      val existingManager = lock synchronized {
        managers.get(linkId)
      }
      existingManager match {
        case Some(manager) => clusterLinkData match {
          case Some(cld) if cld.isDeleted =>
            lock synchronized {
              if (linkData.get(cld.linkName).exists(_.linkId == linkId))
                removeClusterLink(linkId)
            }
          case Some(cld) =>
            reconfigureClusterLink(manager, configEncoder.clusterLinkConfig(persistentProps))
          case None =>
            if (!persistentProps.isEmpty)
              throw new IllegalStateException(s"Found config properties for cluster link '$linkId' with no metadata")
            // OK, the cluster link has been deleted by the controller, but the local purge task has not
            // yet detected it. It will eventually discover the cluster link is gone and remove the manager.
        }
        case None => clusterLinkData match {
          case Some(cld) if cld.isDeleted =>
            startPurgeClusterLink(linkId)
          case Some(cld) =>
            addClusterLink(cld, configEncoder.clusterLinkConfig(persistentProps))
          case None =>
            // OK, cluster link has been deleted and is already removed.
        }
      }
    }
  }

  /**
    * Creates a persistent cluster link with the provided data.
    *
    * @param clusterLinkData the cluster link's data to create
    * @param clusterLinkConfig the cluster link's configs
    * @param persistentProps the properties that are persisted for the cluster link
    * @throws ClusterLinkExistsException if the cluster link name already exists
    */
  def createClusterLink(clusterLinkData: ClusterLinkData,
                        clusterLinkConfig: ClusterLinkConfig,
                        persistentProps: Properties): Unit = updateLock synchronized {
    ensureLinkNameDoesntExist(clusterLinkData.linkName)
    if (fetcherManager(clusterLinkData.linkId).nonEmpty)
      throw new ClusterLinkExistsException(s"Cluster link with ID '${clusterLinkData.linkId}' already exists")

    info(s"Creating cluster link with data '$clusterLinkData'")
    adminZkClient.createClusterLink(clusterLinkData, persistentProps)
    addClusterLink(clusterLinkData, clusterLinkConfig)
  }

  /**
    * Lists the clusters links.
    *
    * @return the cluster links
    */
  def listClusterLinks(): Seq[ClusterLinkData] = updateLock synchronized {
    linkData.values.toSeq
  }

  /**
    * Updates the cluster link configuration for the given link name, invoking the provided callback with the
    * current configuration. The callback should update the configuration and return `true` if the update should
    * be performed, otherwise `false` if no change should be performed.
    *
    * @param linkName the link name to update
    * @param updateCallback updates the current configuration with the desired changes, returning whether the
    *                       update should proceed
    * @throws ClusterLinkNotFoundException if the cluster link with the provided name is not found
    */
  def updateClusterLinkConfig(linkName: String,
                              updateCallback: Properties => Boolean): Unit = updateLock synchronized {
    val linkId = resolveLinkIdOrThrow(linkName)
    val currentConfig = adminZkClient.fetchClusterLinkConfig(linkId)
    val configProps = configEncoder.decode(currentConfig)
    if (updateCallback(configProps)) {
      info(s"Updating cluster link '$linkName' with new configuration ${new ClusterLinkConfig(configProps)}")
      val persistentProps = configEncoder.encode(configProps)
      adminZkClient.changeClusterLinkConfig(linkId, persistentProps)
      reconfigureClusterLink(managers(linkId), configEncoder.clusterLinkConfig(persistentProps))
    }
  }

  /**
    * Persistently deletes cluster link with the provided name and ID.
    *
    * @param linkName the cluster link's name to delete
    * @param linkId the cluster link's ID to delete
    * @throws ClusterLinkNotFoundException if the cluster link name is not found
    * @throws ClusterLinkNotFoundException if the cluster link name doesn't resolve to the given ID
    */
  def deleteClusterLink(linkName: String, linkId: UUID): Unit = updateLock synchronized {
    val clusterLinkData = lock synchronized {
      linkData.get(linkName) match {
        case Some(cld) if cld.linkId != linkId =>
          debug(s"Found cluster link for name '$linkName' with ID '${cld.linkId}' that doesn't match expected '$linkId'")
          throw new ClusterLinkNotFoundException(s"Cluster link '$linkName' with ID '$linkId' not found")
        case Some(cld) if cld.isDeleted =>
          throw new IllegalStateException(s"Unexpected deleted cluster link '$linkId'")
        case Some(cld) =>
          ClusterLinkData(cld.linkName, cld.linkId, cld.clusterId, cld.tenantPrefix, isDeleted = true)
        case None =>
          throw new ClusterLinkNotFoundException(s"Cluster link '$linkName' not found")
      }
    }
    info(s"Deleting cluster link with name '$linkName' and ID '$linkId'")
    adminZkClient.setClusterLink(clusterLinkData)
    removeClusterLink(linkId)
  }

  /**
    * Adds a cluster link with the provided data to the manager.
    *
    * It's required that the cluster link does not already exist in the manager and that the
    * update lock is held.
    */
  private def addClusterLink(clusterLinkData: ClusterLinkData, config: ClusterLinkConfig): Unit = {
    // Support for OffsetsForLeaderEpoch in clients was added in 2.3.0. This is the minimum supported version
    // for cluster linking.
    if (brokerConfig.interBrokerProtocolVersion <= KAFKA_2_3_IV1)
      throw new InvalidClusterLinkException(s"Cluster linking is not supported with inter-broker protocol version ${brokerConfig.interBrokerProtocolVersion}")

    val linkName = clusterLinkData.linkName
    val linkId = clusterLinkData.linkId
    lock synchronized {
      if (clusterLinkData.isDeleted)
        throw new IllegalStateException(s"Cluster link with ID '$linkId' is already deleted")
      if (managers.contains(linkId))
        throw new IllegalStateException(s"Cluster link with ID '$linkId' already exists")
      if (linkData.contains(linkName))
        throw new IllegalStateException(s"Cluster link with name '$linkName' already exists")

      val clientInterceptor = clusterLinkData.tenantPrefix.map(tenantInterceptor)
      val clientManager = new ClusterLinkClientManager(clusterLinkData, scheduler, zkClient, config,
        authorizer, controller, metrics,
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

      managers.put(linkId, Managers(fetcherManager, clientManager))
      linkData.put(linkName, clusterLinkData)
    }
  }

  /**
    * Removes the specified cluster link from the manager.
    *
    * It's required that the cluster link exists within the manager and that the update lock
    * is held.
    */
  private def removeClusterLink(linkId: UUID): Unit = {
    lock synchronized {
      val linkName = linkData.values.find(_.linkId == linkId).map(_.linkName).getOrElse(
        throw new IllegalStateException(s"Attempted to remove non-existent cluster link with ID '$linkId'"))
      linkData.remove(linkName)
      managers.get(linkId) match {
        case Some(Managers(fetcherManager, clientManager)) =>
          fetcherManager.shutdown()
          clientManager.shutdown()
        case None =>
          throw new IllegalStateException(s"Attempted to remove non-existent cluster link with ID '$linkId'")
      }
    }

    startPurgeClusterLink(linkId)
  }

  /**
    * Reconfigures the cluster link managers with new properties.
    *
    * It's required that the update lock is held.
    */
  private def reconfigureClusterLink(linkManagers: Managers, newConfig: ClusterLinkConfig): Unit = {
    val currentProps = linkManagers.fetcherManager.currentConfig.originals
    val newProps = newConfig.originals
    val altered = newProps.asScala.filter { case (k, v) => v != currentProps.get(k) }
    val deleted = currentProps.asScala.filter { case (k, _) => !newProps.containsKey(k) }
    val updatedKeys = altered.keySet ++ deleted.keySet

    if (updatedKeys.nonEmpty) {
      linkManagers.fetcherManager.reconfigure(newConfig, updatedKeys)
      linkManagers.clientManager.reconfigure(newConfig, updatedKeys)
    }
  }

  def addPartitions(partitions: collection.Set[Partition]): Unit = {
    if (partitions.nonEmpty) {
      debug(s"addPartitions $partitions")
      val unknownClusterLinks = mutable.Map[UUID, Iterable[TopicPartition]]()
      lock synchronized {
        partitions.filter(_.isActiveLinkDestinationLeader).groupBy(_.getClusterLinkId)
          .foreach { case (linkId, linkPartitions) =>
            linkId.foreach { lid =>
              managers.get(lid) match {
                case Some(Managers(fetcherManager, clientManager)) =>
                  fetcherManager.addLinkedFetcherForPartitions(linkPartitions)

                  val firstPartitionTopics = linkPartitions.filter(_.topicPartition.partition == 0).map(_.topicPartition.topic)
                  if (firstPartitionTopics.nonEmpty) {
                    clientManager.addTopics(firstPartitionTopics)
                  }

                case None =>
                  unknownClusterLinks += lid -> linkPartitions.map(_.topicPartition)
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
    lock synchronized {
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
    lock synchronized {
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
    lock synchronized {
      managers.values.foreach(_.fetcherManager.shutdownIdleFetcherThreads())
    }
  }

  def shutdown(): Unit = {
    info("shutting down")
    lock synchronized {
      managers.values.foreach { case Managers(fetcherManager, clientManager) =>
        fetcherManager.shutdown()
        clientManager.shutdown()
      }
      clearTopicMirrors.values.foreach(_.shutdown())
    }
    if (scheduler != null)
      scheduler.shutdown()
    admin.shutdown()
    if (destAdminClient != null)
      destAdminClient.close(Duration.ZERO)
    info("shutdown completed")
  }

  def fetcherManager(linkId: UUID): Option[ClusterLinkFetcherManager] = {
    lock synchronized {
      managers.get(linkId).map(_.fetcherManager)
    }
  }

  def clientManager(linkId: UUID): Option[ClusterLinkClientManager] = {
    lock synchronized {
      managers.get(linkId).map(_.clientManager)
    }
  }

  /**
    * Returns the cluster link ID for the provided cluster link name.
    *
    * @param linkName the cluster link name
    * @return the cluster link's ID, or none if not found
    */
  def resolveLinkId(linkName: String): Option[UUID] = {
    lock synchronized {
      linkData.get(linkName).map(_.linkId)
    }
  }

  /**
    * Returns the cluster link ID for the provided cluster link name, or throws if it's not found.
    *
    * @param linkName the cluster link name
    * @return the cluster link's ID
    * @throws ClusterLinkNotFoundException if the cluster link is not found
    */
  def resolveLinkIdOrThrow(linkName: String): UUID = {
    resolveLinkId(linkName).getOrElse(
      throw new ClusterLinkNotFoundException(s"Cluster link '$linkName' does not exist."))
  }

  /**
    * Tests whether a cluster link with the provided link name exists, and if so, throws an exception.
    *
    * @param linkName the cluster link name
    * @throws ClusterLinkExistsException if the cluster link with the provided name already exists
    */
  def ensureLinkNameDoesntExist(linkName: String): Unit = {
    if (resolveLinkId(linkName).nonEmpty)
      throw new ClusterLinkExistsException(s"Cluster link '$linkName' already exists.")
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

  private def getOrCreateDestAdmin(): ConfluentAdmin = {
    // Create an admin client for the destination cluster using the inter-broker listener
    if (destAdminClient == null) {
      val adminConfigs = ConfluentConfigs.interBrokerClientConfigs(brokerConfig, interBrokerEndpoint)
      adminConfigs.remove(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG)
      adminConfigs.put(CommonClientConfigs.CLIENT_ID_CONFIG, s"cluster-link-admin-${brokerConfig.brokerId}")
      destAdminClient = Admin.create(adminConfigs).asInstanceOf[ConfluentAdmin]
    }
    destAdminClient
  }

  /**
    * Begins the removal of all references to the cluster link in the cluster if not already active.
    *
    * @param linkId the cluster link ID
    */
  private def startPurgeClusterLink(linkId: UUID): Unit = {
    lock synchronized {
      if (controller != null) {
        if (!clearTopicMirrors.contains(linkId)) {
          val task = new ClusterLinkClearTopicMirrors(linkId, scheduler, zkClient, controller,
            getOrCreateDestAdmin(), () => purgeClusterLink(linkId))
          clearTopicMirrors.put(linkId, task)
          task.startup()
        }
      } else {
        purgeClusterLink(linkId)
      }
    }
  }

  /**
    * Removes all references to a cluster link from the manager.
    *
    * @param linkId the cluster link ID
    */
  private def purgeClusterLink(linkId: UUID): Unit = updateLock synchronized {
    debug(s"Purging cluster link '$linkId'")

    // In the event of failure, purging will be retried on next controller restart.
    if (controller.isActive)
      try {
        adminZkClient.deleteClusterLink(linkId)
      } catch {
        case e: ClusterLinkNotFoundException => // OK
        case e: Throwable => warn(s"Failed to delete cluster link '$linkId'")
      }
    lock synchronized {
      managers.remove(linkId)
      clearTopicMirrors.remove(linkId)
    }
  }
}
