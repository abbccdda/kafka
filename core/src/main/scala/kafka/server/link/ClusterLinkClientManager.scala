/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.time.Duration
import java.util.concurrent.{CompletableFuture, ExecutionException}

import kafka.controller.KafkaController
import kafka.utils.{CoreUtils, Logging}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{Config, ConfluentAdmin, DescribeConfigsOptions, DescribeTopicsOptions, TopicDescription}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.server.authorizer.Authorizer

import scala.jdk.CollectionConverters._
import scala.collection.Set
import scala.collection.mutable

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
                               private val authorizer: Option[Authorizer],
                               private val controller: KafkaController,
                               private val adminFactory: ClusterLinkConfig => ConfluentAdmin) extends Logging {

  @volatile private var admin: Option[ConfluentAdmin] = None

  private var clusterLinkSyncAcls: Option[ClusterLinkSyncAcls] = None
  private var clusterLinkSyncTopicConfigs: ClusterLinkSyncTopicsConfigs = _

  // Protects `topics` and `config`.
  private val lock = new Object

  // Contains the set of linked topics that this client manager is responsible for, i.e. the set of topics
  // for which the leader of a topic's first partition is on the local broker.
  private val topics = mutable.Set[String]()

  val adminZkClient = new AdminZkClient(zkClient)

  def startup(): Unit = {
    setAdmin(Some(adminFactory(config)))

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
    clusterLinkSyncAcls.foreach(_.shutdown())
    setAdmin(None)
  }

  def reconfigure(newConfig: ClusterLinkConfig): Unit = {
    lock synchronized {
      config = newConfig
      setAdmin(Some(adminFactory(config)))
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
    * Fetches information about a remote topic.
    *
    * @param topic the remote topic to fetch information for
    * @param timeoutMs the timeout, in milliseconds
    * @return a future for the topic's description and its configuration
    */
  def fetchTopicInfo(topic: String, timeoutMs: Int): CompletableFuture[ClusterLinkClientManager.TopicInfo] = {
    val result = new CompletableFuture[ClusterLinkClientManager.TopicInfo]

    try {
      val options = new DescribeTopicsOptions().timeoutMs(timeoutMs)
      val describeTopicsResult = getAdmin.describeTopics(Seq(topic).asJava, options)
      scheduler.scheduleWhenComplete("FetchTopicInfoDescription", describeTopicsResult.all, () => {
        fetchTopicInfoHandleDescription(topic, timeoutMs, describeTopicsResult.values.get(topic), result)
      })
    } catch {
      case e: Throwable => throw fetchTopicInfoWrapException(topic, e)
    }

    result
  }

  private def fetchTopicInfoHandleDescription(topic: String,
                                              timeoutMs: Int,
                                              descriptionFuture: KafkaFuture[TopicDescription],
                                              result: CompletableFuture[ClusterLinkClientManager.TopicInfo]): Unit = {
    try {
      val description = descriptionFuture.get
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val options = new DescribeConfigsOptions().timeoutMs(timeoutMs)
      val describeConfigsResult = getAdmin.describeConfigs(Seq(resource).asJava, options)
      scheduler.scheduleWhenComplete("FetchTopicInfoConfig", describeConfigsResult.all, () => {
        fetchTopicInfoHandleConfig(topic, description, describeConfigsResult.values.get(resource), result)
      })
    } catch {
      case e: ExecutionException => result.completeExceptionally(fetchTopicInfoWrapException(topic, e.getCause))
      case e: Throwable => result.completeExceptionally(fetchTopicInfoWrapException(topic, e))
    }
  }

  private def fetchTopicInfoHandleConfig(topic: String,
                                         description: TopicDescription,
                                         configFuture: KafkaFuture[Config],
                                         result: CompletableFuture[ClusterLinkClientManager.TopicInfo]): Unit = {
    try {
      val config = configFuture.get
      result.complete(ClusterLinkClientManager.TopicInfo(description, config))
    } catch {
      case e: ExecutionException => result.completeExceptionally(fetchTopicInfoWrapException(topic, e.getCause))
      case e: Throwable => result.completeExceptionally(fetchTopicInfoWrapException(topic, e))
    }
  }

  private def fetchTopicInfoWrapException(topic: String, e: Throwable): Throwable = {
    val error = ApiError.fromThrowable(e)
    error.error.exception(s"While fetching topic '$topic's info over cluster link '$linkName': " +
      s"${error.messageWithFallback}")
  }

}
