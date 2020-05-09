/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.util.Properties
import java.util.concurrent.{CompletableFuture, ExecutionException}

import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.common.errors.{InvalidClusterLinkException, InvalidConfigurationException, InvalidRequestException, TimeoutException, UnsupportedVersionException}
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.requests.CreateTopicsRequest.NO_NUM_PARTITIONS

import scala.collection.JavaConverters._

object ClusterLinkUtils extends Logging {

  /**
    * Validates the provided cluster link name, ensuring it's non-empty and contains only legal characters.
    *
    * @throws InvalidClusterLinkException if the link name is invalid
    */
  def validateLinkName(linkName: String): Unit = {
    val maxLength = 200

    if (linkName eq null)
      throw new InvalidClusterLinkException("Cluster link name is null")
    if (linkName.isEmpty)
      throw new InvalidClusterLinkException("Cluster link name is empty")
    if (linkName.equals(".") || linkName.equals(".."))
      throw new InvalidClusterLinkException("Link name cannot be \".\" or \"..\"")
    if (linkName.length > maxLength)
      throw new InvalidClusterLinkException(s"Link name exceeds maximum size of '$maxLength' characters")

    val isValid = linkName.forall { c =>
      (c >= 'a' && c <= 'z') ||
      (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') ||
      c == '.' ||
      c == '_' ||
      c == '-'
    }
    if (!isValid)
      throw new InvalidClusterLinkException(s"Link name '$linkName' is illegal, valid characters: [a-zA-Z0-9._-]")
  }

  private object LogConfigAction extends Enumeration {
    type LogConfigAction = Value

    // Independent: Never mirror the source's value. Instead, allow the value to be modified on the source.
    // NonDefault: Only mirror if it's not a default value on the source, otherwise use the broker's local default.
    //             This makes the config immutable on the destination.
    // Always: Always mirror. This makes the config immutable on the destination.
    val Independent, NonDefault, Always = Value
  }

  private val independentConfigs = List(
    // Cluster-centric
    LogConfig.TopicPlacementConstraintsProp,
    LogConfig.UncleanLeaderElectionEnableProp,
    LogConfig.MessageDownConversionEnableProp,
    LogConfig.AppendRecordInterceptorClassesProp,

    // Schema
    LogConfig.KeySchemaValidationEnableProp,
    LogConfig.ValueSchemaValidationEnableProp,
    LogConfig.KeySchemaValidationStrategyProp,
    LogConfig.ValueSchemaValidationStrategyProp,

    // Tiering
    LogConfig.TierEnableProp,
    LogConfig.TierLocalHotsetBytesProp,
    LogConfig.TierLocalHotsetMsProp,
    LogConfig.TierSegmentHotsetRollMinBytesProp,
    LogConfig.PreferTierFetchMsProp,

    // Rebalance throttling
    KafkaConfig.LeaderReplicationThrottledReplicasProp,
    KafkaConfig.FollowerReplicationThrottledReplicasProp
  )

  private val nonDefaultConfigs = List(
    LogConfig.SegmentBytesProp,
    LogConfig.SegmentMsProp,
    LogConfig.SegmentJitterMsProp,
    LogConfig.SegmentIndexBytesProp,
    LogConfig.FlushMessagesProp,
    LogConfig.FlushMsProp,
    LogConfig.RetentionBytesProp,
    LogConfig.RetentionMsProp,
    LogConfig.IndexIntervalBytesProp,
    LogConfig.DeleteRetentionMsProp,
    LogConfig.MinCompactionLagMsProp,
    LogConfig.MaxCompactionLagMsProp,
    LogConfig.FileDeleteDelayMsProp,
    LogConfig.MinCleanableDirtyRatioProp,
    LogConfig.MinInSyncReplicasProp,
    LogConfig.PreAllocateEnableProp,
    LogConfig.MessageFormatVersionProp,
    LogConfig.MessageTimestampTypeProp,
    LogConfig.MessageTimestampDifferenceMaxMsProp,
    LogConfig.SegmentSpeculativePrefetchEnableProp
  )

  private val alwaysConfigs = List(
    LogConfig.CleanupPolicyProp,
    LogConfig.MaxMessageBytesProp,
    LogConfig.CompressionTypeProp
  )

  private val configAction =
    (independentConfigs.map(_ -> LogConfigAction.Independent) ++
      nonDefaultConfigs.map(_ -> LogConfigAction.NonDefault) ++
      alwaysConfigs.map(_ -> LogConfigAction.Always)).toMap

  private def getConfigAction(name: String): LogConfigAction.Value = {
    configAction.get(name) match {
      case Some(action) => action
      case None =>
        warn(s"Unhandled configuration key '$name'")
        LogConfigAction.Always
    }
  }

  /**
    * Validates the log config properties for a mirrored topic. If the local properties contains any
    * configurations that aren't permissible to be set by a mirrored topic, then an exception is thrown.
    *
    * @param topic the topic's name
    * @param localProps the initial properties of the topic
    * @return the initial log config properties
    * @throws InvalidConfigurationException if the local properties set an immutable config
    */
  def validateMirrorProps(topic: String, localProps: Properties): Unit =
    resolveMirrorProps(localProps, None, (name: String) =>
      throw new InvalidConfigurationException(s"Cannot set configuration '$name' for mirror topic '$topic'")
    )

  /**
    * Initializes the log config properties for a mirrored topic. If the local properties contains
    * any configurations that aren't permissible to be set by a mirrored topic, then an exception is
    * thrown.
    *
    * @param topic the topic's name
    * @param localProps the initial properties of the topic
    * @param remoteConfig the log config of the source topic
    * @return the initial log config properties
    * @throws InvalidConfigurationException if the local properties set an immutable config
    */
  def initMirrorProps(topic: String, localProps: Properties, remoteConfig: Config): Properties =
    resolveMirrorProps(localProps, Some(remoteConfig), (name: String) =>
      throw new InvalidConfigurationException(s"Cannot set configuration '$name' for mirror topic '$topic'")
    )

  /**
    * Updates the log config properties for a mirrored topic to reflect changes of the source. Any configs
    * set in the local properties that are mirrored (immutable) are assumed to be due to mirroring, and
    * therefore it's not considered an error if they exist.
    *
    * @param topic the topic's name
    * @param localProps the current properties of the topic
    * @param remoteConfig the log config of the source topic
    * @return the updated log config properties
    */
  def updateMirrorProps(topic: String, localProps: Properties, remoteConfig: Config): Properties =
    resolveMirrorProps(localProps, Some(remoteConfig), (name: String) =>
      warn(s"Unexpected configuration '$name' set for mirror topic '$topic'")
    )

  /**
    * Resolves the log config properties for a mirrored topic.
    *
    * @param localProps the current properties of the topic
    * @param remoteConfig the log config of the source topic
    * @param onInvalidConfig the action to take when an invalid config is detected
    * @return the resolve log config properties
    */
  private def resolveMirrorProps(localProps: Properties, remoteConfig: Option[Config], onInvalidConfig: String => Unit): Properties = {
    val newLocalProps = new Properties()
    val remoteEntries = remoteConfig.map(_.entries.asScala.map(e => e.name -> e).toMap).getOrElse(Map.empty)

    LogConfig.configNames.foreach { name =>
      getConfigAction(name) match {
        case LogConfigAction.Independent =>
          val value = localProps.get(name)
          if (value != null)
            newLocalProps.put(name, value)

        case LogConfigAction.NonDefault =>
          if (localProps.containsKey(name))
            onInvalidConfig(name)
          remoteEntries.get(name) match {
            case Some(remoteEntry) =>
              if (!remoteEntry.isDefault)
                newLocalProps.put(name, remoteEntry.value)
            case None =>
          }

        case LogConfigAction.Always =>
          if (localProps.containsKey(name))
            onInvalidConfig(name)
          remoteEntries.get(name).foreach(e => newLocalProps.put(e.name, e.value))
      }
    }

    newLocalProps
  }

  /*
   * Result from mirror topic creation resolution.
   *
   * @param configs the updated topic's configs
   * @param topicState the initial cluster link topic state for the topic
   * @param numPartitions the required number of partitions for the topic, or NO_NUM_PARTITIONS if any
   */
  case class ResolveCreateTopic(configs: Properties, topicState: Option[ClusterLinkTopicState], numPartitions: Int)

  /*
   * Resolves mirror topic creation information for the provided creatable topic. If the topic is not to be mirrored,
   * then the result will be valid but not contain any mirror information.
   *
   * @param topic the creatable topic
   * @param configs the creatable topic's configs
   * @param validateOnly whether the creation should only be validated
   * @param topicInfo the remote topic's information if this is a mirror topic, otherwise none if not. This *must* be completed.
   *                  If 'validateOnly' and this is none, then the remote topic information won't be validated.
   */
  def resolveCreateTopic(topic: CreatableTopic,
                         configs: Properties,
                         validateOnly: Boolean,
                         topicInfo: Option[CompletableFuture[ClusterLinkClientManager.TopicInfo]]): ResolveCreateTopic = {
    val mirrorTopic = Option(topic.mirrorTopic)
    Option(topic.linkName) match {
      case Some(linkName) =>
        validateLinkName(linkName)
        validateMirrorProps(topic.name, configs)

        mirrorTopic match {
          case Some(mt) =>
            if (mt != topic.name)
              throw new UnsupportedVersionException("Topic renaming for mirroring not yet supported.")
          case None =>
            throw new InvalidRequestException("Mirror topic not set.")
        }

        if (topic.numPartitions != NO_NUM_PARTITIONS)
          throw new InvalidRequestException("Cannot specify both mirror topic and number of partitions.")
        if (!topic.assignments.isEmpty)
          throw new InvalidRequestException("Cannot specify both mirror topic and partition assignments.")

        // If the mirror info is defined, then the actual topic creation is being performed. Otherwise if not, then
        // the request is only being validated.
        topicInfo match {
          case Some(ti) =>
            val info = try {
              if (!ti.isDone)
                throw new IllegalStateException("Mirror information must have been resolved.")
              ti.get
            } catch {
              case e: ExecutionException =>
                throw e.getCause
              case _: TimeoutException =>
                throw new TimeoutException(s"Timed out while fetching topic information over cluster link '$linkName'.")
            }

            val newConfigs = ClusterLinkUtils.initMirrorProps(topic.name, configs, info.config)
            ResolveCreateTopic(newConfigs, Some(new ClusterLinkTopicState.Mirror(linkName)), info.description.partitions.size)

          case None =>
            if (!validateOnly)
              throw new IllegalStateException("Mirror information must be provided if 'validateOnly' is not set.")
            ResolveCreateTopic(configs, None, NO_NUM_PARTITIONS)
        }

      case None =>
        if (mirrorTopic.nonEmpty)
          throw new InvalidRequestException("Cannot create mirror topic, cluster link name not specified.")
        ResolveCreateTopic(configs, None, NO_NUM_PARTITIONS)
    }
  }

}
