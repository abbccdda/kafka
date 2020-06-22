/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.time.Duration
import java.util.Optional
import java.util.concurrent.{CompletableFuture, ExecutionException}

import kafka.controller.KafkaController
import kafka.utils.{CoreUtils, Logging}
import kafka.zk.{AdminZkClient, ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.ClusterLinkPausedException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.replica.ReplicaStatus
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.{Map, Set, mutable}
import scala.jdk.CollectionConverters._

object ClusterLinkClientManager {
  case class TopicInfo(description: TopicDescription, config: Config)
}

/**
  * The ClusterLinkClientManager is responsible for managing an admin client instance for the
  * cluster link, and manages periodic tasks that may use the admin client.
  *
  * Thread safety:
  *   - ClusterLinkClientManager is thread-safe.
  */
class ClusterLinkClientManager(val linkData: ClusterLinkData,
                               val scheduler: ClusterLinkScheduler,
                               val zkClient: KafkaZkClient,
                               @volatile private var config: ClusterLinkConfig,
                               authorizer: Option[Authorizer],
                               controller: KafkaController,
                               metrics: Metrics,
                               linkAdminFactory: ClusterLinkConfig => ConfluentAdmin,
                               destAdminFactory: () => Admin) extends Logging {

  @volatile private var admin: Option[ConfluentAdmin] = null

  private var clusterLinkSyncAcls: Option[ClusterLinkSyncAcls] = None
  private var clusterLinkSyncOffsets: Option[ClusterLinkSyncOffsets] = None
  private var clusterLinkSyncTopicConfigs: Option[ClusterLinkSyncTopicsConfigs] = None

  // Protects `topics` and `config`.
  private val lock = new Object

  // Contains the set of linked topics that this client manager is responsible for, i.e. the set of topics
  // for which the leader of a topic's first partition is on the local broker.
  private val topics = mutable.Set[String]()

  val adminZkClient = new AdminZkClient(zkClient)

  def startup(): Unit = {
    if (isActive()) {
      createAndSetAdmin()
      startupTasks()
    }
  }

  private def startupTasks(): Unit = {
    val tags = Map(
      "link-name" -> linkData.linkName,
      "link-id" -> linkData.linkId.toString)

    clusterLinkSyncOffsets = Some(new ClusterLinkSyncOffsets(this, linkData,
      controller, destAdminFactory, metrics, tags.asJava))
    clusterLinkSyncOffsets.get.startup()

    clusterLinkSyncTopicConfigs = Some(new ClusterLinkSyncTopicsConfigs(this,
      config.topicConfigSyncMs, metrics, tags.asJava))
    clusterLinkSyncTopicConfigs.get.startup()

    if (config.aclSyncEnable) {
      authorizer.getOrElse(throw new IllegalArgumentException("ACL migration is enabled but "
        + "authorizer.class.name is not set. Please set authorizer.class.name to proceed with ACL "
        + "migration."))
      config.aclFilters.getOrElse(throw new IllegalArgumentException("ACL migration is enabled "
        + "but acl.filters is not set. Please set acl.filters to proceed with ACL migration."))
      clusterLinkSyncAcls = Some(new ClusterLinkSyncAcls(this, controller,
        metrics, tags.asJava))
      clusterLinkSyncAcls.get.startup()
    }
  }

  def shutdown(): Unit = {
    if (isActive()) {
      shutdownTasks()
      setAdmin(null)
    }
  }

  private def shutdownTasks(): Unit = {
    clusterLinkSyncTopicConfigs.foreach(_.shutdown())
    clusterLinkSyncOffsets.foreach(_.shutdown())
    clusterLinkSyncAcls.foreach(_.shutdown())
  }

  def reconfigure(newConfig: ClusterLinkConfig, updatedKeys: Set[String]): Unit = {
    lock synchronized {
      val oldActive = isActive()
      config = newConfig
      val newActive = isActive()

      (oldActive, newActive) match {
        case (false, false) =>
          // Paused; do nothing.
        case (false, true) =>
          createAndSetAdmin()
          startupTasks()
        case (true, false) =>
          shutdownTasks()
          setAdmin(None)
        case (true, true) =>
          if (updatedKeys.diff(ClusterLinkConfig.ReplicationProps).nonEmpty)
            createAndSetAdmin()
      }
    }
  }

  def addTopics(addTopics: Set[String]): Unit = {
    lock synchronized {
      addTopics.foreach { topic =>
        if (topics.add(topic))
          debug(s"Added topic '$topic' for link '${linkData.linkName}'")
      }
    }
  }

  def removeTopics(removeTopics: Set[String]): Unit = {
    lock synchronized {
      removeTopics.foreach { topic =>
        if (topics.remove(topic))
          debug(s"Removed topic '$topic' for link '${linkData.linkName}'")
      }
    }
  }

  def getTopics: Set[String] = lock synchronized {
    topics.toSet
  }

  /**
    * Gets the admin client that is used to talk to the remote cluster over the cluster link.
    *
    * @throws ClusterLinkPausedException if the cluster link is paused
    * @throws IllegalStateException if the client manager has not been initialized
    * @return the admin client for the remote cluster
    */
  def getAdmin: ConfluentAdmin = {
    val currentAdmin = admin
    if (currentAdmin == null)
      throw new IllegalStateException(s"Client manager for ${linkData.linkName} not initialized")
    currentAdmin.getOrElse(throw new ClusterLinkPausedException(s"Cluster link for ${linkData.linkName} is paused"))
  }

  def getAuthorizer: Option[Authorizer] = authorizer

  // for testing purposes
  def getSyncAclTask: Option[ClusterLinkSyncAcls] = clusterLinkSyncAcls

  private def isActive(): Boolean = !config.clusterLinkPaused

  private def createAndSetAdmin(): Unit = {
    setAdmin(Some(linkAdminFactory(config)))
  }

  private def setAdmin(newAdmin: Option[ConfluentAdmin]): Unit = {
    val oldAdmin = admin
    admin = newAdmin
    if (oldAdmin != null)
      oldAdmin.foreach(a => CoreUtils.swallow(a.close(Duration.ZERO), this))
  }

  /**
    * Fetches the number of partitions for a remote topic.
    *
    * @param topic the remote topic to fetch the number of partitions for
    * @param timeoutMs the timeout, in milliseconds
    * @return a future with the topic's number of partitions
    */
  def fetchTopicPartitions(topic: String, timeoutMs: Int): CompletableFuture[Int] = {
    val result = new CompletableFuture[Int]
    try {
      val describeTopicsOptions = new DescribeTopicsOptions().timeoutMs(timeoutMs)
      val describeTopicsResult = getAdmin.describeTopics(Seq(topic).asJava, describeTopicsOptions)
      scheduler.scheduleWhenComplete("FetchTopicPartitions", describeTopicsResult.all, () => {
        result.complete(describeTopicsResult.values.get(topic).get.partitions.size)
        ()
      })
    } catch {
      case e: Throwable =>
        result.completeExceptionally(fetchTopicInfoWrapException(topic, e, "fetching partitions"))
    }
    result
  }

  /**
    * Fetches information about a remote topic, verifying topic read access and returning the resulting information.
    *
    * @param topic the remote topic to fetch information for
    * @param timeoutMs the timeout, in milliseconds
    * @return a future with the topic's description and configuration
    */
  def fetchTopicInfo(topic: String, timeoutMs: Int): CompletableFuture[ClusterLinkClientManager.TopicInfo] = {
    val result = new CompletableFuture[ClusterLinkClientManager.TopicInfo]

    try {
      val describeTopicsOptions = new DescribeTopicsOptions().timeoutMs(timeoutMs).includeAuthorizedOperations(true)
      val describeTopicsResult = getAdmin.describeTopics(Seq(topic).asJava, describeTopicsOptions)

      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val describeConfigsOptions = new DescribeConfigsOptions().timeoutMs(timeoutMs)
      val describeConfigsResult = getAdmin.describeConfigs(Seq(resource).asJava, describeConfigsOptions)

      val futures = KafkaFuture.allOf(describeTopicsResult.all, describeConfigsResult.all)
      scheduler.scheduleWhenComplete("FetchTopicInfo", futures, () => {
        fetchTopicInfoHandleResults(topic, describeConfigsResult.values.get(resource), describeTopicsResult.values.get(topic), result)
      })
    } catch {
      case e: Throwable =>
        result.completeExceptionally(fetchTopicInfoWrapException(topic, e, "preparing client to fetch information"))
    }

    result
  }

  private def fetchTopicInfoHandleResults(topic: String,
                                          configFuture: KafkaFuture[Config],
                                          descriptionFuture: KafkaFuture[TopicDescription],
                                          result: CompletableFuture[ClusterLinkClientManager.TopicInfo]): Unit = {
    def maybeThrowException[T](topic: String, future: KafkaFuture[T], action: String): T = {
      try {
        future.get
      } catch {
        case e: ExecutionException => throw fetchTopicInfoWrapException(topic, e.getCause, action)
        case e: Throwable => throw fetchTopicInfoWrapException(topic, e, action)
      }
    }

    try {
      val description = maybeThrowException(topic, descriptionFuture, "fetching description")
      val config = maybeThrowException(topic, configFuture, "fetching configuration")
      result.complete(ClusterLinkClientManager.TopicInfo(description, config))
    } catch {
      case e: Throwable => result.completeExceptionally(e)
    }
  }

  private def fetchTopicInfoWrapException(topic: String, e: Throwable, action: String): Throwable = {
    val error = ApiError.fromThrowable(e)
    error.error.exception(s"While $action for topic '$topic' over cluster link '${linkData.linkName}': ${error.messageWithFallback}")
  }

  /**
    * Retrieves the replica status of the replicas for the provided partitions over the cluster link.
    *
    * @param partitions the partitions to fetch replica status for
    * @return a map of partition to the replica status
    */
  def replicaStatus(partitions: Set[TopicPartition]): Map[TopicPartition, CompletableFuture[Seq[ReplicaStatus]]] = {
    val options = new ReplicaStatusOptions().includeLinkedReplicas(false)
    getAdmin.replicaStatus(partitions.asJava, options).result.asScala.map { case (tp, future) =>
      val completableFuture = new CompletableFuture[Seq[ReplicaStatus]]
      future.whenComplete((res, ex) => Option(ex) match {
        case Some(e) => completableFuture.completeExceptionally(e)
        case None => completableFuture.complete(res.asScala.map { rs =>
          new ReplicaStatus(rs.brokerId(), rs.isLeader(), rs.isObserver(), rs.isIsrEligible(),
            rs.isInIsr(), rs.isCaughtUp(), rs.logStartOffset(), rs.logEndOffset(),
            rs.lastCaughtUpTimeMs(), rs.lastFetchTimeMs(), Optional.of(linkData.linkName))
        }.toSeq)
      })
      tp -> completableFuture
    }.toMap
  }

  def currentConfig: ClusterLinkConfig = config
}
