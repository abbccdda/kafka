/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.util.Properties
import java.util.concurrent.CompletableFuture

import kafka.log.LogConfig
import org.apache.kafka.clients.admin.{Config, ConfigEntry, TopicDescription}
import org.apache.kafka.common.{Node, TopicPartitionInfo}
import org.apache.kafka.common.errors.{InvalidClusterLinkException, InvalidConfigurationException, InvalidRequestException, TimeoutException, UnsupportedVersionException}
import org.apache.kafka.common.message.CreateTopicsRequestData
import org.apache.kafka.common.requests.CreateTopicsRequest.NO_NUM_PARTITIONS
import org.junit.Test
import org.junit.Assert._
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._

class ClusterLinkUtilsTest {

  @Test
  def testValidLinkNames(): Unit = {
    def assertValid(linkName: String): Unit = {
      ClusterLinkUtils.validateLinkName(linkName)
    }

    assertValid("ABCXYZ.abcxyz-0123456789_")
    assertValid("...")
    assertValid("_.-")
    assertValid("0123456789" * 20)
  }

  @Test
  def testInvalidLinkNames(): Unit = {
    def assertInvalid(linkName: String): Unit = {
      intercept[InvalidClusterLinkException] {
        ClusterLinkUtils.validateLinkName(linkName)
      }
    }

    assertInvalid(null)
    assertInvalid("")
    assertInvalid(".")
    assertInvalid("..")
    assertInvalid("test/link")
    assertInvalid("test:link")
    assertInvalid("test\\link")
    assertInvalid("test\nlink")
    assertInvalid("test\tlink")
    assertInvalid("0123456789" * 30)
  }

  @Test
  def testValidateMirrorProps(): Unit = {
    val props = makeProperties(Seq(LogConfig.MessageDownConversionEnableProp -> "true"))
    ClusterLinkUtils.validateMirrorProps("test", props)
  }

  @Test
  def testValidateMirrorPropsEmpty(): Unit = {
    ClusterLinkUtils.validateMirrorProps("test", new Properties())
  }

  @Test(expected = classOf[InvalidConfigurationException])
  def testValidateInvalidMirrorProps(): Unit = {
    val props = makeProperties(Seq(LogConfig.CleanupPolicyProp -> "compact"))
    ClusterLinkUtils.validateMirrorProps("test", props)
  }

  @Test
  def testInitMirrorProps(): Unit = {
    val remoteConfig = makeConfig(Seq(
      (LogConfig.CleanupPolicyProp, "compact", false),
      (LogConfig.MaxMessageBytesProp, "10485760", true),
      (LogConfig.UncleanLeaderElectionEnableProp, "true", false),
      (LogConfig.PreferTierFetchMsProp, "100000", true),
      (LogConfig.SegmentMsProp, "100000", false),
      (LogConfig.SegmentBytesProp, "1048576", true)
    ))

    val localProps = makeProperties(Seq(LogConfig.MessageDownConversionEnableProp -> "true"))
    val newLocalProps = ClusterLinkUtils.initMirrorProps("test", localProps, remoteConfig)
    val expectedLocalProps = makeProperties(Seq(
      LogConfig.MessageDownConversionEnableProp -> "true",
      LogConfig.CleanupPolicyProp -> "compact",
      LogConfig.MaxMessageBytesProp -> "10485760",
      LogConfig.SegmentMsProp -> "100000"
    ))
    assertEquals(expectedLocalProps, newLocalProps)
  }

  @Test(expected = classOf[InvalidConfigurationException])
  def testInitMirrorPropsLocalNonDefault(): Unit = {
    val localProps = makeProperties(Seq(LogConfig.SegmentMsProp -> "100000"))
    ClusterLinkUtils.initMirrorProps("test", localProps, makeConfig(Seq.empty))
  }

  @Test(expected = classOf[InvalidConfigurationException])
  def testInitMirrorPropsLocalAlways(): Unit = {
    val localProps = makeProperties(Seq(LogConfig.CleanupPolicyProp -> "compact"))
    ClusterLinkUtils.initMirrorProps("test", localProps, makeConfig(Seq.empty))
  }

  @Test
  def testInitMirrorPropsUnknownConfigIgnored(): Unit = {
    val remoteConfig = makeConfig(Seq(("bad.config.key", "12345", false)))
    val localProps = new Properties()
    val newLocalProps = ClusterLinkUtils.initMirrorProps("test", localProps, remoteConfig)
    assertTrue(newLocalProps.isEmpty)
  }

  @Test
  def testUpdateMirrorProps(): Unit = {
    val remoteConfig = makeConfig(Seq(
      (LogConfig.CleanupPolicyProp, "compact", false),
      (LogConfig.MaxMessageBytesProp, "10485760", true),
      (LogConfig.UncleanLeaderElectionEnableProp, "true", false),
      (LogConfig.PreferTierFetchMsProp, "100000", true),
      (LogConfig.SegmentMsProp, "100000", false),
      (LogConfig.SegmentBytesProp, "1048576", true)
    ))

    val localProps = makeProperties(Seq(
      LogConfig.CleanupPolicyProp -> "delete",
      LogConfig.MaxMessageBytesProp -> "524288",
      LogConfig.MessageDownConversionEnableProp -> "true",
      LogConfig.UncleanLeaderElectionEnableProp -> "false",
      LogConfig.SegmentMsProp -> "200000",
      LogConfig.SegmentBytesProp -> "1048576",
    ))

    val expectedLocalProps = makeProperties(Seq(
      LogConfig.CleanupPolicyProp -> "compact",
      LogConfig.MaxMessageBytesProp -> "10485760",
      LogConfig.MessageDownConversionEnableProp -> "true",
      LogConfig.UncleanLeaderElectionEnableProp -> "false",
      LogConfig.SegmentMsProp -> "100000",
    ))

    val newLocalProps = ClusterLinkUtils.updateMirrorProps("test", localProps, remoteConfig)
    assertEquals(expectedLocalProps, newLocalProps)
  }

  @Test
  def testUpdateMirrorPropsUnknownConfigIgnored(): Unit = {
    val remoteConfig = makeConfig(Seq(("bad.config.key", "12345", false)))
    val localProps = makeProperties(Seq("another.bad.config.key" -> "23456"))
    val newLocalProps = ClusterLinkUtils.updateMirrorProps("test", localProps, remoteConfig)
    assertTrue(newLocalProps.isEmpty)
  }

  @Test
  def testResolveCreateTopicStandard(): Unit = {
    val configs = makeProperties(Seq(LogConfig.UncleanLeaderElectionEnableProp -> "true"))
    val topic = makeCreatableTopic("test-topic", 4, None, None)
    val result = ClusterLinkUtils.resolveCreateTopic(topic, configs, validateOnly = false, None)
    assertEquals(configs, result.configs)
    assertTrue(result.topicState.isEmpty)
    assertEquals(NO_NUM_PARTITIONS, result.numPartitions)
  }

  @Test
  def testResolveCreateTopicMirror(): Unit = {
    val configs = makeProperties(Seq(LogConfig.UncleanLeaderElectionEnableProp -> "true"))
    val remoteConfig = makeConfig(Seq((LogConfig.CleanupPolicyProp, "compact", false)))
    val expectedConfigs = makeProperties(Seq(
      LogConfig.UncleanLeaderElectionEnableProp -> "true",
      LogConfig.CleanupPolicyProp -> "compact"
    ))

    val result1 = ClusterLinkUtils.resolveCreateTopic(
      makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, Some("link-name"), Some("test-topic")),
      configs, validateOnly = true, None)
    assertEquals(configs, result1.configs)
    assertTrue(result1.topicState.isEmpty)
    assertEquals(NO_NUM_PARTITIONS, result1.numPartitions)

    val node = new Node(0, "localhost", 9092)
    val nodeList = List(node).asJava
    val partitionInfos = List(
      new TopicPartitionInfo(0, node, nodeList, nodeList),
      new TopicPartitionInfo(1, node, nodeList, nodeList),
      new TopicPartitionInfo(2, node, nodeList, nodeList)
    )

    val description = new TopicDescription("test-topic", false, partitionInfos.asJava)

    val future = new CompletableFuture[ClusterLinkClientManager.TopicInfo]
    future.complete(ClusterLinkClientManager.TopicInfo(description, remoteConfig))
    val result2 = ClusterLinkUtils.resolveCreateTopic(
      makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, Some("link-name"), Some("test-topic")),
      configs, validateOnly = false, Some(future))
    assertEquals(expectedConfigs, result2.configs)
    assertTrue(result2.topicState.get.isInstanceOf[ClusterLinkTopicState.Mirror])
    assertEquals(Some("link-name"), result2.topicState.get.activeLinkName)
    assertEquals(3, result2.numPartitions)
  }

  @Test
  def testResolveCreateTopicMirrorErrors(): Unit = {
    val validConfigs = makeProperties(Seq(LogConfig.UncleanLeaderElectionEnableProp -> "true"))
    val invalidConfigs = makeProperties(Seq(LogConfig.CleanupPolicyProp -> "compact"))

    intercept[InvalidRequestException] {
      ClusterLinkUtils.resolveCreateTopic(
        makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, Some("link-name"), None),
        validConfigs, validateOnly = false, None)
    }
    intercept[InvalidRequestException] {
      ClusterLinkUtils.resolveCreateTopic(
        makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, None, Some("test-topic")),
        validConfigs, validateOnly = false, None)
    }
    intercept[InvalidRequestException] {
      ClusterLinkUtils.resolveCreateTopic(
        makeCreatableTopic("test-topic", 4, Some("link-name"), Some("test-topic")),
        validConfigs, validateOnly = false, None)
    }
    intercept[UnsupportedVersionException] {
      ClusterLinkUtils.resolveCreateTopic(
        makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, Some("link-name"), Some("different-topic")),
        validConfigs, validateOnly = false, None)
    }
    intercept[InvalidConfigurationException] {
      ClusterLinkUtils.resolveCreateTopic(
        makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, Some("link-name"), Some("test-topic")),
        invalidConfigs, validateOnly = false, None)
    }
    intercept[IllegalStateException] {
      ClusterLinkUtils.resolveCreateTopic(
        makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, Some("link-name"), Some("test-topic")),
        validConfigs, validateOnly = false, None)
    }
    intercept[IllegalStateException] {
      val future = new CompletableFuture[ClusterLinkClientManager.TopicInfo]
      ClusterLinkUtils.resolveCreateTopic(
        makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, Some("link-name"), Some("test-topic")),
        validConfigs, validateOnly = false, Some(future))
    }
    intercept[TimeoutException] {
      val future = new CompletableFuture[ClusterLinkClientManager.TopicInfo]
      future.completeExceptionally(new TimeoutException("timeout"))
      ClusterLinkUtils.resolveCreateTopic(
        makeCreatableTopic("test-topic", NO_NUM_PARTITIONS, Some("link-name"), Some("test-topic")),
        validConfigs, validateOnly = false, Some(future))
    }
  }

  private def makeConfig(entries: Seq[(String, String, Boolean)]): Config = {
    val configEntries = entries.map { entry =>
      val source = if (entry._3)
        ConfigEntry.ConfigSource.DEFAULT_CONFIG
      else
        ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG
      new ConfigEntry(entry._1, entry._2, source, false, false, List.empty[ConfigEntry.ConfigSynonym].asJava)
    }
    new Config(configEntries.asJavaCollection)
  }

  private def makeProperties(entries: Seq[(String, String)]): Properties = {
    val props = new Properties()
    entries.foreach { case (name, value) =>
      props.put(name, value)
    }
    props
  }

  private def makeCreatableTopic(name: String,
                                 numPartitions: Int,
                                 linkName: Option[String],
                                 mirrorTopic: Option[String]): CreateTopicsRequestData.CreatableTopic = {
    new CreateTopicsRequestData.CreatableTopic()
      .setName(name)
      .setNumPartitions(numPartitions)
      .setReplicationFactor(3)  // Doesn't matter
      .setLinkName(linkName.orNull)
      .setMirrorTopic(mirrorTopic.orNull)
  }

}
