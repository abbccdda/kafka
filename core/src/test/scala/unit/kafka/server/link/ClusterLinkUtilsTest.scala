/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server.link

import java.util.Properties

import kafka.log.LogConfig
import org.apache.kafka.clients.admin.{Config, ConfigEntry}
import org.apache.kafka.common.errors.{InvalidClusterLinkException, InvalidConfigurationException}
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
  def testInitMirrorProps(): Unit = {
    val remoteConfig = makeConfig(Seq(
      (LogConfig.CleanupPolicyProp, "compact", false),
      (LogConfig.MaxMessageBytesProp, "10485760", true),
      (LogConfig.UncleanLeaderElectionEnableProp, "true", false),
      (LogConfig.PreferTierFetchMsProp, "100000", true),
      (LogConfig.SegmentMsProp, "100000", false),
      (LogConfig.SegmentBytesProp, "1048576", true)
    ))

    val localProps = new Properties()
    localProps.put(LogConfig.MessageDownConversionEnableProp, "true")

    val newLocalProps = ClusterLinkUtils.initMirrorProps("test", localProps, remoteConfig)

    val expectedLocalProps = new Properties()
    expectedLocalProps.put(LogConfig.MessageDownConversionEnableProp, "true")
    expectedLocalProps.put(LogConfig.CleanupPolicyProp, "compact")
    expectedLocalProps.put(LogConfig.MaxMessageBytesProp, "10485760")
    expectedLocalProps.put(LogConfig.SegmentMsProp, "100000")
    assertEquals(expectedLocalProps, newLocalProps)
  }

  @Test(expected = classOf[InvalidConfigurationException])
  def testInitMirrorPropsLocalNonDefault(): Unit = {
    val localProps = new Properties()
    localProps.put(LogConfig.SegmentMsProp, "100000")
    ClusterLinkUtils.initMirrorProps("test", localProps, makeConfig(Seq.empty))
  }

  @Test(expected = classOf[InvalidConfigurationException])
  def testInitMirrorPropsLocalAlways(): Unit = {
    val localProps = new Properties()
    localProps.put(LogConfig.CleanupPolicyProp, "compact")
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

}
