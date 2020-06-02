/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.UUID

import org.apache.kafka.common.utils.Time
import org.junit.Test
import org.junit.Assert._

class ClusterLinkTopicStateTest {

  val linkName = "test-link"
  val linkId = UUID.randomUUID()

  @Test
  def testFromJsonString(): Unit = {
    val timeMs = 123456789
    val result = ClusterLinkTopicState.fromJsonString(
      s"""|{
          |  "Mirror": {
          |    "version": 1,
          |    "time_ms": $timeMs,
          |    "link_name": "$linkName",
          |    "link_id": "$linkId"
          |  }
          |}""".stripMargin)
    assertEquals(ClusterLinkTopicState.Mirror(linkName, linkId, timeMs), result)
  }

  @Test
  def testToJsonString(): Unit = {
    val timeMs = 123456789
    val result = ClusterLinkTopicState.Mirror(linkName, linkId, timeMs).toJsonString
    assertEquals(s"""|{
                     |  "Mirror": {
                     |    "version": 1,
                     |    "time_ms": $timeMs,
                     |    "link_name": "$linkName",
                     |    "link_id": "$linkId"
                     |  }
                     |}""".stripMargin.replaceAll(" ", "").replaceAll("\n", ""), result)
  }

  @Test
  def testMirror(): Unit = {
    val timeMs = Time.SYSTEM.milliseconds()
    val state = ClusterLinkTopicState.Mirror(linkName, linkId, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.Mirror]
    assertEquals(linkName, data.linkName)
    assertEquals(linkId, data.linkId)
    assertEquals(timeMs, data.timeMs)
    assertTrue(data.state.shouldSync)
    assertTrue(data.mirrorIsEstablished)
  }

  @Test
  def testFailedMirror(): Unit = {
    val timeMs = Time.SYSTEM.milliseconds()
    val state = ClusterLinkTopicState.FailedMirror(linkName, linkId, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.FailedMirror]
    assertEquals(linkName, data.linkName)
    assertEquals(linkId, data.linkId)
    assertEquals(timeMs, data.timeMs)
    assertFalse(data.state.shouldSync)
    assertTrue(data.mirrorIsEstablished)
  }

  @Test
  def testStoppedMirror(): Unit = {
    val logEndOffsets = List[Long](12345, 23456, 34567)
    val timeMs = Time.SYSTEM.milliseconds()
    val state = ClusterLinkTopicState.StoppedMirror(linkName, linkId, logEndOffsets, timeMs)

    val result = ClusterLinkTopicState.fromJsonString(state.toJsonString)
    val data = result.asInstanceOf[ClusterLinkTopicState.StoppedMirror]
    assertEquals(linkName, data.linkName)
    assertEquals(linkId, data.linkId)
    assertEquals(logEndOffsets, data.logEndOffsets)
    assertEquals(timeMs, data.timeMs)
    assertFalse(data.state.shouldSync)
    assertFalse(data.mirrorIsEstablished)
  }

  @Test(expected = classOf[IllegalStateException])
  def testBadEntry(): Unit = {
    ClusterLinkTopicState.fromJsonString(
      s"""|{
          |  "Unexpected": {
          |    "version": 1,
          |    "time_ms": 123456789,
          |    "link_name": "$linkName",
          |    "link_id": "$linkId"
          |  }
          |}""".stripMargin)
  }

  @Test(expected = classOf[IllegalStateException])
  def testBadVersion(): Unit = {
    ClusterLinkTopicState.fromJsonString(
      s"""|{
          |  "Mirror": {
          |    "version": 0,
          |    "time_ms": 123456789,
          |    "link_name": "$linkName",
          |    "link_id": "$linkId"
          |  }
          |}""".stripMargin)
  }

  @Test(expected = classOf[IllegalStateException])
  def testMultipleEntries(): Unit = {
    ClusterLinkTopicState.fromJsonString(
      s"""|{
          |  "Mirror": {
          |    "version": 1,
          |    "time_ms": 123456789,
          |    "link_name": "test-link-1",
          |    "link_id": "${UUID.randomUUID()}"
          |  },
          |  "FailedMirror": {
          |    "version": 1,
          |    "time_ms": 123456789,
          |    "link_name": "test-link-2",
          |    "link_id": "${UUID.randomUUID()}"
          |  }
          |}""".stripMargin)
  }

}
