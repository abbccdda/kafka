/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.util.{Properties, UUID}
import java.util.concurrent.{CompletableFuture, ExecutionException}

import kafka.api.KAFKA_2_3_IV1
import kafka.server.{DelayedFuturePurgatory, KafkaConfig}
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import kafka.zk.{ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.admin.{Admin, DescribeClusterOptions}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.errors.{ClusterAuthorizationException, ClusterLinkInUseException, InvalidClusterLinkException, InvalidConfigurationException, InvalidRequestException, UnknownTopicOrPartitionException, UnsupportedVersionException}
import org.apache.kafka.common.requests.{AlterMirrorsRequest, AlterMirrorsResponse, ApiError, ClusterLinkListing, NewClusterLink}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

class ClusterLinkAdminManager(val config: KafkaConfig,
                              val clusterId: String,
                              val zkClient: KafkaZkClient,
                              val clusterLinkManager: ClusterLinkManager)
  extends ClusterLinkFactory.AdminManager with Logging {

  this.logIdent = "[Cluster Link Admin Manager on Broker " + config.brokerId + "]: "

  val purgatory = new DelayedFuturePurgatory(purgatoryName = "ClusterLink", brokerId = config.brokerId)

  def shutdown(): Unit = {
    purgatory.shutdown()
  }

  def createClusterLink(newClusterLink: NewClusterLink,
                        tenantPrefix: Option[String],
                        validateOnly: Boolean,
                        validateLink: Boolean,
                        timeoutMs: Int): CompletableFuture[Void] = {

    // Support for OffsetsForLeaderEpoch in clients was added in 2.3.0. This is the minimum supported version
    // for cluster linking.
    if (config.interBrokerProtocolVersion <= KAFKA_2_3_IV1)
      throw new InvalidClusterLinkException(s"Cluster linking is not supported with inter-broker protocol version ${config.interBrokerProtocolVersion}")

    val linkName = newClusterLink.linkName
    ClusterLinkUtils.validateLinkName(linkName)
    clusterLinkManager.ensureLinkNameDoesntExist(linkName)

    val props = new Properties()
    props ++= newClusterLink.configs.asScala
    val linkConfig = new ClusterLinkConfig(props)

    val expectedClusterId = Option(newClusterLink.clusterId)
    if (expectedClusterId.contains(clusterId))
      throw new InvalidRequestException(s"Requested cluster ID matches local cluster ID '$clusterId' - cannot create cluster link to self")

    val result = new CompletableFuture[Void]
    val persistentProps = clusterLinkManager.configEncoder.encode(props)
    if (validateLink) {
      clusterLinkManager.scheduler.schedule("CreateClusterLink",
        () => try {
          val linkClusterId = validateClusterLink(expectedClusterId, props, timeoutMs)
          finishCreateClusterLink(linkName, linkClusterId, tenantPrefix, linkConfig, persistentProps, validateOnly)
          result.complete(null)
        } catch {
          case e: Throwable => result.completeExceptionally(e)
        })
    } else {
      try {
        finishCreateClusterLink(linkName, expectedClusterId, tenantPrefix, linkConfig, persistentProps, validateOnly)
        result.complete(null)
      } catch {
        case e: Throwable => result.completeExceptionally(e)
      }
    }
    result
  }

  def listClusterLinks(): Seq[ClusterLinkListing] = {
    clusterLinkManager.listClusterLinks.map { clusterLinkData =>
      new ClusterLinkListing(clusterLinkData.linkName, clusterLinkData.linkId, clusterLinkData.clusterId.orNull)
    }
  }

  def deleteClusterLink(linkName: String, validateOnly: Boolean, force: Boolean): Unit = {
    ClusterLinkUtils.validateLinkName(linkName)

    val linkId = clusterLinkManager.resolveLinkIdOrThrow(linkName)
    if (!force) {
      val allTopics = zkClient.getAllTopicsInCluster()
      if (allTopics.nonEmpty) {
        val topicsInUse = zkClient.getClusterLinkForTopics(allTopics).filter { case (_, state) =>
          state.mirrorIsEstablished && state.linkId == linkId
        }.keys
        if (topicsInUse.nonEmpty)
          throw new ClusterLinkInUseException(s"Cluster link '$linkName' with ID '$linkId' in used by topics: $topicsInUse")
      }
    }

    if (!validateOnly)
      clusterLinkManager.deleteClusterLink(linkName, linkId)
  }

  def alterMirror(op: AlterMirrorsRequest.Op, validateOnly: Boolean): CompletableFuture[AlterMirrorsResponse.Result] = {
    val result = new CompletableFuture[AlterMirrorsResponse.Result]

    op match {
      case subOp: AlterMirrorsRequest.StopTopicMirrorOp =>
        val topic = subOp.topic
        Topic.validate(topic)
        if (!clusterLinkManager.adminManager.metadataCache.contains(topic))
          throw new UnknownTopicOrPartitionException(s"Topic $topic not found")

        // Validate the mirror can be stopped.
        val newClusterLink = zkClient.getClusterLinkForTopics(Set(topic)).get(topic) match {
          case Some(clusterLink) =>
            val linkName = clusterLink.linkName
            clusterLink match {
              case _: ClusterLinkTopicState.Mirror | _: ClusterLinkTopicState.FailedMirror =>
                // TODO: Save the log end offsets. For now, an empty array is a valid state.
                new ClusterLinkTopicState.StoppedMirror(linkName, clusterLink.linkId, List.empty[Long])
              case _: ClusterLinkTopicState.StoppedMirror =>
                throw new InvalidRequestException(s"Topic '$topic' has already stopped its mirror from '$linkName'")
            }

          case None =>
            throw new InvalidRequestException(s"Topic '$topic' is not mirrored")
        }

        if (!validateOnly)
          zkClient.setTopicClusterLink(topic, Some(newClusterLink))
        result.complete(new AlterMirrorsResponse.StopTopicMirrorResult())

      case subOp: AlterMirrorsRequest.ClearTopicMirrorOp =>
        val topic = subOp.topic
        Topic.validate(topic)
        if (!clusterLinkManager.adminManager.metadataCache.contains(topic))
          throw new UnknownTopicOrPartitionException(s"Topic $topic not found")

        // Note we return success if the cluster link is already cleared.
        if (!validateOnly && zkClient.getClusterLinkForTopics(Set(topic)).get(topic).nonEmpty) {
          zkClient.setTopicClusterLink(topic, clusterLink = None)
        }
        result.complete(new AlterMirrorsResponse.ClearTopicMirrorResult())

      case _ =>
        throw new UnsupportedVersionException(s"Unknown alter mirrors op type")
    }

    result
  }

  private def finishCreateClusterLink(linkName: String,
                                      linkClusterId: Option[String],
                                      tenantPrefix: Option[String],
                                      linkConfig: ClusterLinkConfig,
                                      persistentProps: Properties,
                                      validateOnly: Boolean): Unit = {
    if (!validateOnly) {
      val clusterLinkData = ClusterLinkData(linkName, UUID.randomUUID(), linkClusterId, tenantPrefix, isDeleted = false)
      clusterLinkManager.createClusterLink(clusterLinkData, linkConfig, persistentProps)
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
