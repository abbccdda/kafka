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
          |  "mirror": {
          |    "version": 1,
          |    "time_ms": $timeMs,
          |    "link_name": "$linkName"
          |  }
          |}""".stripMargin)
    assertEquals(new ClusterLinkTopicState.Mirror(linkName, timeMs), result)
  }

  @Test
  def testToJsonString(): Unit = {
    val linkName = "test-link"
    val timeMs = 123456789
    val result = new ClusterLinkTopicState.Mirror(linkName, timeMs).toJsonString
    assertEquals(s"""|{
                     |  "mirror": {
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
    val state = new ClusterLinkTopicState.Mirror(linkName, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.Mirror]
    assertEquals(linkName, data.linkName)
    assertEquals(timeMs, data.timeMs)
    assertTrue(data.shouldSync)
  }

  @Test
  def testFailedMirror(): Unit = {
    val linkName = "test-link"
    val timeMs = Time.SYSTEM.milliseconds()
    val state = new ClusterLinkTopicState.FailedMirror(linkName, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.FailedMirror]
    assertEquals(linkName, data.linkName)
    assertEquals(timeMs, data.timeMs)
    assertFalse(data.shouldSync)
  }

  @Test
  def testStoppedMirror(): Unit = {
    val linkName = "test-link"
    val logEndOffsets = List[Long](12345, 23456, 34567)
    val timeMs = Time.SYSTEM.milliseconds()
    val state = new ClusterLinkTopicState.StoppedMirror(linkName, logEndOffsets.toSeq, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.StoppedMirror]
    assertEquals(linkName, data.linkName)
    assertEquals(logEndOffsets, data.logEndOffsets)
    assertEquals(timeMs, data.timeMs)
    assertFalse(data.shouldSync)
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
         |  "mirror": {
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
         |  "mirror": {
         |    "version": 1,
         |    "time_ms": 123456789,
         |    "link_name": "test-link-1",
         |  },
         |  "mirror": {
         |    "version": 1,
         |    "time_ms": 123456789,
         |    "link_name": "test-link-2"
         |  }
         |}""".stripMargin)
  }

}
