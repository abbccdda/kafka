/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import org.apache.kafka.common.utils.Time
import org.junit.Test
import org.junit.Assert._

class ClusterLinkTopicStateTest {

  @Test
  def testFromJsonString(): Unit = {
    val linkName = "test-link"
    val timeMs = 123456789
    val result = ClusterLinkTopicState.fromJsonString(
      s"""|{
          |  "Mirror": {
          |    "version": 1,
          |    "time_ms": $timeMs,
          |    "link_name": "$linkName"
          |  }
          |}""".stripMargin)
    assertEquals(ClusterLinkTopicState.Mirror(linkName, timeMs), result)
  }

  @Test
  def testToJsonString(): Unit = {
    val linkName = "test-link"
    val timeMs = 123456789
    val result = ClusterLinkTopicState.Mirror(linkName, timeMs).toJsonString
    assertEquals(s"""|{
                     |  "Mirror": {
                     |    "version": 1,
                     |    "time_ms": $timeMs,
                     |    "link_name": "$linkName"
                     |  }
                     |}""".stripMargin.replaceAll(" ", "").replaceAll("\n", ""), result)
  }

  @Test
  def testMirror(): Unit = {
    val linkName = "test-link"
    val timeMs = Time.SYSTEM.milliseconds()
    val state = ClusterLinkTopicState.Mirror(linkName, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.Mirror]
    assertEquals(linkName, data.linkName)
    assertEquals(timeMs, data.timeMs)
    assertTrue(data.state.shouldSync)
    assertTrue(data.mirrorIsEstablished)
  }

  @Test
  def testFailedMirror(): Unit = {
    val linkName = "test-link"
    val timeMs = Time.SYSTEM.milliseconds()
    val state = ClusterLinkTopicState.FailedMirror(linkName, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.FailedMirror]
    assertEquals(linkName, data.linkName)
    assertEquals(timeMs, data.timeMs)
    assertFalse(data.state.shouldSync)
    assertTrue(data.mirrorIsEstablished)
  }

  @Test
  def testStoppedMirror(): Unit = {
    val linkName = "test-link"
    val logEndOffsets = List[Long](12345, 23456, 34567)
    val timeMs = Time.SYSTEM.milliseconds()
    val state = ClusterLinkTopicState.StoppedMirror(linkName, logEndOffsets, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.StoppedMirror]
    assertEquals(linkName, data.linkName)
    assertEquals(logEndOffsets, data.logEndOffsets)
    assertEquals(timeMs, data.timeMs)
    assertFalse(data.state.shouldSync)
    assertFalse(data.mirrorIsEstablished)
  }

  @Test(expected = classOf[IllegalStateException])
  def testBadEntry(): Unit = {
    ClusterLinkTopicState.fromJsonString(
      """|{
         |  "unexpected": {
         |    "version": 1,
         |    "time_ms": 123456789,
         |    "link_name": "test-link"
         |  }
         |}""".stripMargin)
  }

  @Test(expected = classOf[IllegalStateException])
  def testBadVersion(): Unit = {
    ClusterLinkTopicState.fromJsonString(
      """|{
         |  "Mirror": {
         |    "version": 0,
         |    "time_ms": 123456789,
         |    "link_name": "test-link"
         |  }
         |}""".stripMargin)
  }

  @Test(expected = classOf[IllegalStateException])
  def testMultipleEntries(): Unit = {
    ClusterLinkTopicState.fromJsonString(
      """|{
         |  "Mirror": {
         |    "version": 1,
         |    "time_ms": 123456789,
         |    "link_name": "test-link-1",
         |  },
         |  "Mirror": {
         |    "version": 1,
         |    "time_ms": 123456789,
         |    "link_name": "test-link-2"
         |  }
         |}""".stripMargin)
  }

}
