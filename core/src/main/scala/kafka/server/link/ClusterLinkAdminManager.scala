/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.util.{Properties, UUID}
import java.util.concurrent.{CompletableFuture, ExecutionException}

import kafka.server.{DelayedFuturePurgatory, KafkaConfig}
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{Admin, DescribeClusterOptions}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.errors.{ClusterAuthorizationException, ClusterLinkExistsException, ClusterLinkInUseException, ClusterLinkNotFoundException, InvalidConfigurationException, InvalidRequestException, UnknownTopicOrPartitionException, UnsupportedVersionException}
import org.apache.kafka.common.requests.{AlterMirrorsResponse, AlterMirrorsRequest, ApiError, ClusterLinkListing, NewClusterLink}

import scala.collection.JavaConverters._
import scala.collection.Seq

class ClusterLinkAdminManager(val config: KafkaConfig,
                              val clusterId: String,
                              val zkClient: KafkaZkClient,
                              val clusterLinkManager: ClusterLinkManager) extends Logging {

  this.logIdent = "[Cluster Link Admin Manager on Broker " + config.brokerId + "]: "

  private val adminZkClient = new AdminZkClient(zkClient)
  val purgatory = new DelayedFuturePurgatory(purgatoryName = "ClusterLink", brokerId = config.brokerId)

  def shutdown(): Unit = {
    purgatory.shutdown()
  }

  def createClusterLink(newClusterLink: NewClusterLink,
                        validateOnly: Boolean,
                        validateLink: Boolean,
                        timeoutMs: Int): CompletableFuture[Void] = {

    clusterLinkManager.ensureClusterLinkEnabled()
    val linkName = newClusterLink.linkName
    ClusterLinkUtils.validateLinkName(linkName)

    if (clusterLinkManager.clientManager(linkName).isDefined)
      throw new ClusterLinkExistsException(s"Cluster link '$linkName' already exists")

    val props = new Properties()
    props ++= newClusterLink.configs.asScala
    ClusterLinkConfig.validate(props)

    val expectedClusterId = Option(newClusterLink.clusterId)
    if (expectedClusterId.contains(clusterId))
      throw new InvalidRequestException(s"Requested cluster ID matches local cluster ID '$clusterId' - cannot create cluster link to self")

    val result = new CompletableFuture[Void]()
    if (validateLink) {
      clusterLinkManager.scheduler.schedule("CreateClusterLink",
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
    clusterLinkManager.ensureClusterLinkEnabled()
    adminZkClient.getAllClusterLinks().map { info =>
      new ClusterLinkListing(info.linkName, info.linkId, info.clusterId.orNull)
    }
  }

  def deleteClusterLink(linkName: String, validateOnly: Boolean, force: Boolean): Unit = {
    clusterLinkManager.ensureClusterLinkEnabled()
    ClusterLinkUtils.validateLinkName(linkName)

    if (clusterLinkManager.clientManager(linkName).isEmpty)
      throw new ClusterLinkNotFoundException(s"Cluster link '$linkName' not found")

    val allTopics = zkClient.getAllTopicsInCluster()
    if (allTopics.nonEmpty) {
      val topicsInUse = zkClient.getClusterLinkForTopics(allTopics).filter(_._2.linkName == linkName).values
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
        clusterLinkManager.removeClusterLink(linkName)
      } catch {
        case _: ClusterLinkNotFoundException => // Ignore, this may have been done due to config callback.
        case e: Throwable => warn(s"Encountered error while removing cluster link '$linkName'", e)
      }
    }
  }

  def alterMirror(op: AlterMirrorsRequest.Op, validateOnly: Boolean): CompletableFuture[AlterMirrorsResponse.Result] = {
    val result = new CompletableFuture[AlterMirrorsResponse.Result]()

    op match {
      case subOp: AlterMirrorsRequest.StopTopicMirrorOp =>
        val topic = subOp.topic
        Topic.validate(topic)
        if (!clusterLinkManager.adminManager.metadataCache.contains(topic))
          throw new UnknownTopicOrPartitionException(s"Topic $topic not found")

        // Validate the mirror can be stopped.
        val newClusterLink = zkClient.getClusterLinkForTopics(Set(subOp.topic)).get(subOp.topic) match {
          case Some(clusterLink) =>
            val linkName = clusterLink.linkName
            clusterLink match {
              case _: ClusterLinkTopicState.Mirror | _: ClusterLinkTopicState.FailedMirror =>
                // TODO: Save the log end offsets. For now, an empty array is a valid state.
                new ClusterLinkTopicState.StoppedMirror(linkName, List.empty[Long])
              case _: ClusterLinkTopicState.StoppedMirror =>
                throw new InvalidRequestException(s"Topic '${subOp.topic}' has already stopped its mirror from '$linkName'")
            }

          case None =>
            throw new InvalidRequestException(s"Topic '${subOp.topic}' is not mirrored")
        }

        if (!validateOnly)
          zkClient.setTopicClusterLink(subOp.topic, Some(newClusterLink))
        result.complete(new AlterMirrorsResponse.StopTopicMirrorResult())

      case _ =>
        throw new UnsupportedVersionException(s"Unknown alter mirrors op type")
    }

    result
  }

  private def finishCreateClusterLink(linkName: String, linkClusterId: Option[String], props: Properties, validateOnly: Boolean): Unit = {
    if (!validateOnly) {
      adminZkClient.createClusterLink(linkName, UUID.randomUUID(), linkClusterId, props)

      try {
        clusterLinkManager.addClusterLink(linkName, props)
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
      val aclSyncEnabled = new ClusterLinkConfig(props).aclSyncEnable
      val describeResult = admin.describeCluster(new DescribeClusterOptions()
        .includeAuthorizedOperations(aclSyncEnabled)
        .timeoutMs(timeoutMs))
      if (aclSyncEnabled && !describeResult.authorizedOperations().get.contains(AclOperation.DESCRIBE))
        throw new ClusterAuthorizationException("ACL sync was requested, but link credentials don't have DESCRIBE access for the source cluster")
      Option(describeResult.clusterId.get)
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

    linkClusterId
  }

}
