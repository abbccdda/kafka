/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.time.Duration
import java.util.concurrent.{CompletableFuture, ExecutionException}

import kafka.controller.KafkaController
import kafka.utils.{CoreUtils, Logging}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.server.authorizer.Authorizer

import scala.collection.{Set, mutable}
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
class ClusterLinkClientManager(val linkName: String,
                               val scheduler: ClusterLinkScheduler,
                               val zkClient: KafkaZkClient,
                               private var config: ClusterLinkConfig,
                               authorizer: Option[Authorizer],
                               controller: KafkaController,
                               linkAdminFactory: ClusterLinkConfig => ConfluentAdmin,
                               destAdminFactory: () => Admin,
                               tenantPrefix: Option[String] = None) extends Logging {

  @volatile private var admin: Option[ConfluentAdmin] = None

  private var clusterLinkSyncAcls: Option[ClusterLinkSyncAcls] = None
  private var clusterLinkSyncOffsets: Option[ClusterLinkSyncOffsets] = None
  private var clusterLinkSyncTopicConfigs: ClusterLinkSyncTopicsConfigs = _

  // Protects `topics` and `config`.
  private val lock = new Object

  // Contains the set of linked topics that this client manager is responsible for, i.e. the set of topics
  // for which the leader of a topic's first partition is on the local broker.
  private val topics = mutable.Set[String]()

  val adminZkClient = new AdminZkClient(zkClient)

  def startup(): Unit = {
    setAdmin(Some(linkAdminFactory(config)))

    clusterLinkSyncOffsets = Some(new ClusterLinkSyncOffsets(this, config, controller,destAdminFactory, tenantPrefix))
    clusterLinkSyncOffsets.get.startup()

    clusterLinkSyncTopicConfigs = new ClusterLinkSyncTopicsConfigs(this, config.topicConfigSyncMs)
    clusterLinkSyncTopicConfigs.startup()

    if (config.aclSyncEnable) {
      authorizer.getOrElse(throw new IllegalArgumentException("ACL migration is enabled but "
        + "authorizer.class.name is not set. Please set authorizer.class.name to proceed with ACL "
        + "migration."))
      config.aclFilters.getOrElse(throw new IllegalArgumentException("ACL migration is enabled "
        + "but acl.filters is not set. Please set acl.filters to proceed with ACL migration."))
      clusterLinkSyncAcls = Some(new ClusterLinkSyncAcls(this, config, controller))
      clusterLinkSyncAcls.get.startup()
    }
  }

  def shutdown(): Unit = {
    clusterLinkSyncOffsets.foreach(_.shutdown())
    clusterLinkSyncAcls.foreach(_.shutdown())
    setAdmin(None)
  }

  def reconfigure(newConfig: ClusterLinkConfig): Unit = {
    lock synchronized {
      config = newConfig
      setAdmin(Some(linkAdminFactory(config)))
    }
  }

  def addTopics(addTopics: Set[String]): Unit = {
    lock synchronized {
      addTopics.foreach { topic =>
        if (topics.add(topic))
          debug(s"Added topic '$topic' for link '$linkName'")
      }
    }
  }

  def removeTopics(removeTopics: Set[String]): Unit = {
    lock synchronized {
      removeTopics.foreach { topic =>
        if (topics.remove(topic))
          debug(s"Removed topic '$topic' for link '$linkName'")
      }
    }
  }

  def getTopics: Set[String] = lock synchronized {
    topics.toSet
  }

  def getAdmin: ConfluentAdmin = admin.getOrElse(throw new IllegalStateException(s"Client manager for $linkName not initialized"))

  def getAuthorizer: Option[Authorizer] = authorizer

  // for testing purposes
  def getSyncAclTask: Option[ClusterLinkSyncAcls] = clusterLinkSyncAcls

  private def setAdmin(newAdmin: Option[ConfluentAdmin]): Unit = {
    val oldAdmin = admin
    admin = newAdmin
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
        case e: ExecutionException => throw fetchTopicInfoWrapException(topic, e, action)
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
    error.error.exception(s"While $action for topic '$topic' over cluster link '$linkName': ${error.messageWithFallback}")
  }

}
