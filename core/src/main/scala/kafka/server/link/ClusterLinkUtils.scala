/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.util.Properties

import kafka.log.LogConfig
import kafka.utils.Logging
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.common.errors.{InvalidClusterLinkException, InvalidConfigurationException}

import scala.collection.JavaConverters._

object ClusterLinkUtils extends Logging {

  /**
    * Validates the provided cluster link name, ensuring it's non-empty and contains only legal characters.
    *
    * @throws InvalidClusterLinkException if the link name is invalid
    */
  def validateLinkName(linkName: String): Unit = {
    val maxLength = 200;

    if (linkName eq null)
      throw new InvalidClusterLinkException("Cluster link name is null")
    if (linkName.isEmpty)
      throw new InvalidClusterLinkException("Cluster link name is empty")
    if (linkName.equals(".") || linkName.equals(".."))
      throw new InvalidClusterLinkException("Link name cannot be \".\" or \"..\"");
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
    LogConfig.LeaderReplicationThrottledReplicasProp,
    LogConfig.FollowerReplicationThrottledReplicasProp
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
    resolveMirrorProps(localProps, remoteConfig, (name: String) =>
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
    resolveMirrorProps(localProps, remoteConfig, (name: String) =>
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
  private def resolveMirrorProps(localProps: Properties, remoteConfig: Config, onInvalidConfig: String => Unit): Properties = {
    val newLocalProps = new Properties()
    val remoteEntries = remoteConfig.entries.asScala.map(e => e.name -> e).toMap

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

}
