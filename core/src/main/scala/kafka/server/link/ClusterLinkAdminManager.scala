/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.time.Duration
import java.util.{Properties, UUID}
import java.util.concurrent.{CompletableFuture, ExecutionException}

import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{Admin, DescribeClusterOptions}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.errors.{ClusterLinkExistsException, ClusterLinkInUseException, ClusterLinkNotFoundException, InvalidConfigurationException, InvalidRequestException, UnsupportedVersionException}
import org.apache.kafka.common.requests.{ApiError, ClusterLinkListing, NewClusterLink}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._
import scala.collection.Seq

class ClusterLinkAdminManager(val config: KafkaConfig,
                              val clusterId: String,
                              val zkClient: KafkaZkClient,
                              val clusterLinkReplicaManager: () => ClusterLinkReplicaManager) extends Logging {

  this.logIdent = "[Cluster Link Admin Manager on Broker " + config.brokerId + "]: "

  private val adminZkClient = new AdminZkClient(zkClient)

  def createClusterLink(newClusterLink: NewClusterLink,
                        validateOnly: Boolean,
                        validateLink: Boolean,
                        timeoutMs: Int): CompletableFuture[Void] = {

    val linkName = newClusterLink.linkName
    ClusterLinkUtils.validateLinkName(linkName)

    if (clusterLinkReplicaManager().clientManager(linkName).isDefined)
      throw new ClusterLinkExistsException(s"Cluster link '$linkName' already exists")

    val props = new Properties()
    props ++= newClusterLink.configs.asScala
    ClusterLinkConfig.validate(props)

    val expectedClusterId = Option(newClusterLink.clusterId)
    if (expectedClusterId.exists(_ == clusterId))
      throw new InvalidRequestException(s"Requested cluster ID matches local cluster ID '$clusterId' - cannot create cluster link to self")

    val result = new CompletableFuture[Void]()
    if (validateLink) {
      clusterLinkReplicaManager().scheduler.schedule("CreateClusterLink",
        () => try {
          val linkClusterId = validateClusterLink(expectedClusterId, props, timeoutMs)
          finishCreateClusterLink(linkName, linkClusterId, props, validateOnly)
          result.complete(null)
        } catch {
          case e: Throwable => result.completeExceptionally(e)
        })
    } else {
      finishCreateClusterLink(linkName, expectedClusterId, props, validateOnly)
      result.complete(null)
    }
    result
  }

  def listClusterLinks(): Seq[ClusterLinkListing] = {
    adminZkClient.getAllClusterLinks().map { info =>
      new ClusterLinkListing(info.linkName, info.linkId, info.clusterId.orNull)
    }
  }

  def deleteClusterLink(linkName: String, validateOnly: Boolean, force: Boolean): Unit = {
    ClusterLinkUtils.validateLinkName(linkName)

    if (!clusterLinkReplicaManager().clientManager(linkName).isDefined)
      throw new ClusterLinkNotFoundException(s"Cluster link '$linkName' not found")

    val allTopics = zkClient.getAllTopicsInCluster()
    if (allTopics.nonEmpty) {
      val topicsInUse = zkClient.getClusterLinkForTopics(allTopics).filter(_._2 == linkName).values
      if (topicsInUse.nonEmpty) {
        if (force)
          throw new UnsupportedVersionException("Force deletion not yet implemented")
        else
          throw new ClusterLinkInUseException(s"Cluster link '$linkName' in used by topics: $topicsInUse")
      }
    }

    if (!validateOnly) {
      adminZkClient.deleteClusterLink(linkName)

      try {
        clusterLinkReplicaManager().removeClusterLink(linkName)
      } catch {
        case _: ClusterLinkNotFoundException => // Ignore, this may have been done due to config callback.
        case e: Throwable => warn(s"Encountered error while removing cluster link '$linkName'", e)
      }
    }
  }

  private def finishCreateClusterLink(linkName: String, linkClusterId: Option[String], props: Properties, validateOnly: Boolean): Unit = {
    if (!validateOnly) {
      adminZkClient.createClusterLink(linkName, UUID.randomUUID(), linkClusterId, props)

      try {
        clusterLinkReplicaManager().addClusterLink(linkName, new ClusterLinkConfig(props))
      } catch {
        case _: ClusterLinkExistsException => // Ignore, this may have been done due to config callback.
        case e: Throwable => warn(s"Encountered error while adding cluster link '$linkName'", e)
      }
    }
  }

  /**
    * Validate the cluster for the cluster link contains the expected cluster ID and topics are readable.
    *
    * @param expectedClusterId the expected cluster ID, or empty if it should be resolved
    * @param props the properties to use for contacting the remote cluster
    * @param timeoutMs the request timeout
    * @return the actual cluster ID
    */
  private def validateClusterLink(expectedClusterId: Option[String], props: Properties, timeoutMs: Int): Option[String] = {
    val admin = try {
      Admin.create(props)
    } catch {
      case e: Throwable =>
        throw new InvalidConfigurationException("Unable to create client using provided properties when validating the cluster link", e)
    }
    try {
      validateClusterLinkWithAdmin(admin, expectedClusterId, props, timeoutMs)
    } finally {
      CoreUtils.swallow(admin.close(), this)
    }
  }

  private def validateClusterLinkWithAdmin(admin: Admin, expectedClusterId: Option[String], props: Properties, timeoutMs: Int): Option[String] = {
    def throwExceptionFor(e: Throwable) = {
      val error = ApiError.fromThrowable(e)
      throw error.error.exception(s"Unable to validate cluster link while due to error: ${error.messageWithFallback}")
    }

    val linkClusterId = try {
      Option(admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMs)).clusterId.get)
    } catch {
      case e: ExecutionException => throwExceptionFor(e.getCause)
      case e: Throwable => throwExceptionFor(e)
    }

    linkClusterId match {
      case Some(lcid) =>
        expectedClusterId.foreach { ecid =>
          if (ecid != lcid)
            throw new InvalidRequestException(s"Expected cluster ID '$ecid' does not match resolved cluster ID '$lcid'")
        }
      case None =>
        expectedClusterId.foreach { ecid =>
          throw new InvalidRequestException(s"Expected cluster ID '$ecid' does not match due to no resolved cluster ID")
        }
    }

    // Issue a fetch request to the remote cluster to verify topic read access.
    val partition = List(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)).asJavaCollection
    val consumerProps = new Properties()
    consumerProps ++= props
    consumerProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "0")
    consumerProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps,
      new ByteArrayDeserializer(), new ByteArrayDeserializer())
    try {
      consumer.assign(partition)
      consumer.seekToBeginning(partition)
      consumer.poll(Duration.ofMillis(timeoutMs))
    } catch {
      case e: Throwable => throwExceptionFor(e)
    } finally {
      consumer.close()
    }

    linkClusterId
  }

}
