/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.{Properties, UUID}
import java.util.concurrent.CompletableFuture

import kafka.cluster.Partition
import kafka.controller.KafkaController
import kafka.server.{DelayedFuturePurgatory, KafkaConfig, ReplicaManager, ReplicaQuota}
import kafka.tier.fetcher.TierStateFetcher
import kafka.utils.Logging
import kafka.zk.{ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.admin.{Config, TopicDescription}
import org.apache.kafka.common.{Endpoint, TopicPartition}
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.{AlterMirrorsRequest, AlterMirrorsResponse, ClusterLinkListing, NewClusterLink}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.{Map, Seq}

case class LinkedTopicInfo(description: TopicDescription, config: Config)

object ClusterLinkFactory {

  def linkMetricTags(linkName: String) = Map("link-name" -> linkName)

  def createLinkManager(brokerConfig: KafkaConfig,
                        clusterId: String,
                        quota: ReplicaQuota,
                        zkClient: KafkaZkClient,
                        metrics: Metrics,
                        time: Time,
                        threadNamePrefix: Option[String] = None,
                        tierStateFetcher: Option[TierStateFetcher]): LinkManager = {
    if (brokerConfig.clusterLinkEnable) {
      new ClusterLinkManager(brokerConfig,
        clusterId,
        quota,
        zkClient,
        metrics,
        time,
        threadNamePrefix,
        tierStateFetcher)
    } else {
      ClusterLinkDisabled.LinkManager
    }
  }

  trait LinkManager {
    def startup(interBrokerEndpoint: Endpoint,
                replicaManager: ReplicaManager,
                adminManager: kafka.server.AdminManager,
                controller: KafkaController,
                authorizer: Option[Authorizer]): Unit

    def createClusterLink(clusterLinkData: ClusterLinkData,
                          clusterLinkConfig: ClusterLinkConfig,
                          persistentProps: Properties): Unit

    def listClusterLinks(): Seq[ClusterLinkData]

    def updateClusterLinkConfig(linkName: String,
                                updateCallback: Properties => Boolean): Unit

    def deleteClusterLink(linkName: String, linkId: UUID): Unit

    def processClusterLinkChanges(linkId: UUID, persistentProps: Properties): Unit

    def addPartitions(partitions: collection.Set[Partition]): Unit

    def removePartitionsAndMetadata(partitions: collection.Set[TopicPartition]): Unit

    def removePartitions(partitionStates: Map[Partition, LeaderAndIsrPartitionState]): Unit

    def shutdownIdleFetcherThreads(): Unit

    def shutdown(): Unit

    def admin: ClusterLinkFactory.AdminManager

    def configEncoder: ClusterLinkConfigEncoder

    def fetcherManager(linkId: UUID): Option[ClusterLinkFetcherManager]

    def clientManager(linkId: UUID): Option[ClusterLinkClientManager]

    def resolveLinkId(linkName: String): Option[UUID]

    def resolveLinkIdOrThrow(linkName: String): UUID

    def ensureLinkNameDoesntExist(linkName: String): Unit
  }

  trait AdminManager {
    def clusterLinkManager: ClusterLinkFactory.LinkManager

    def purgatory: DelayedFuturePurgatory

    def createClusterLink(newClusterLink: NewClusterLink,
                          tenantPrefix: Option[String],
                          validateOnly: Boolean,
                          validateLink: Boolean,
                          timeoutMs: Int): CompletableFuture[Void]

    def listClusterLinks(linkNames: Option[Set[String]], includeTopics: Boolean): Seq[ClusterLinkListing]

    def deleteClusterLink(linkName: String, validateOnly: Boolean, force: Boolean): Unit

    def alterMirror(op: AlterMirrorsRequest.Op, validateOnly: Boolean): CompletableFuture[AlterMirrorsResponse.Result]
  }
}

object ClusterLinkDisabled {

  private def exception(): ClusterAuthorizationException = {
    new ClusterAuthorizationException("Cluster linking is not enabled in this cluster.")
  }

  /**
    * Cluster link manager used when cluster linking is disabled.
    * 1. For methods invoked through the API to manage cluster links, throw an exception.
    * 2. For ZooKeeper notification about cluster links that were previously created, log an error.
    * 3. Use no-op for methods used by replica manager since we are not mirroring.
    */
  object LinkManager extends ClusterLinkFactory.LinkManager with Logging {

    override def startup(interBrokerEndpoint: Endpoint,
                         replicaManager: ReplicaManager,
                         adminManager: kafka.server.AdminManager,
                         controller: KafkaController,
                         authorizer: Option[Authorizer]): Unit = {}

    override def createClusterLink(clusterLinkData: ClusterLinkData,
                                   clusterLinkConfig: ClusterLinkConfig,
                                   persistentProps: Properties): Unit = {
      throw exception()
    }

    override def updateClusterLinkConfig(linkName: String,
                                         updateCallback: Properties => Boolean): Unit = {
      throw exception()
    }

    override def listClusterLinks(): Seq[ClusterLinkData] = {
      throw exception()
    }

    override def deleteClusterLink(linkName: String, linkId: UUID): Unit = {
      throw exception()
    }

    override def processClusterLinkChanges(linkId: UUID, persistentProps: Properties): Unit = {
      error(s"Cluster link $linkId not updated since cluster links are not enabled")
    }

    override def addPartitions(partitions: collection.Set[Partition]): Unit = {}

    override def removePartitionsAndMetadata(partitions: collection.Set[TopicPartition]): Unit = {}

    override def removePartitions(partitionStates: Map[Partition, LeaderAndIsrPartitionState]): Unit = {}

    override def shutdownIdleFetcherThreads(): Unit = {}

    override def shutdown(): Unit = {}

    override def admin: ClusterLinkFactory.AdminManager = ClusterLinkDisabled.AdminManager

    override def configEncoder: ClusterLinkConfigEncoder = {
      throw exception()
    }

    override def fetcherManager(linkId: UUID): Option[ClusterLinkFetcherManager] = {
      throw exception()
    }

    override def clientManager(linkId: UUID): Option[ClusterLinkClientManager] = {
      throw exception()
    }

    override def resolveLinkId(linkName: String): Option[UUID] = {
      throw exception()
    }

    override def resolveLinkIdOrThrow(linkName: String): UUID = {
      throw exception()
    }

    override def ensureLinkNameDoesntExist(linkName: String): Unit = {
      throw exception()
    }
  }

  /**
    * Cluster link admin manager used when cluster linking disabled. All methods
    * invoked through the API throw an exception.
    */
  object AdminManager extends ClusterLinkFactory.AdminManager {

    override def clusterLinkManager: ClusterLinkFactory.LinkManager = {
      throw exception()
    }

    override def purgatory: DelayedFuturePurgatory = {
      throw exception()
    }

    override def createClusterLink(newClusterLink: NewClusterLink,
                                   tenantPrefix: Option[String],
                                   validateOnly: Boolean,
                                   validateLink: Boolean,
                                   timeoutMs: Int): CompletableFuture[Void] = {
      throw exception()
    }

    override def listClusterLinks(linkNames: Option[Set[String]], includeTopics: Boolean): Seq[ClusterLinkListing] = {
      throw exception()
    }

    override def deleteClusterLink(linkName: String, validateOnly: Boolean, force: Boolean): Unit = {
      throw exception()
    }

    override def alterMirror(op: AlterMirrorsRequest.Op, validateOnly: Boolean): CompletableFuture[AlterMirrorsResponse.Result] = {
      throw exception()
    }
  }
}
