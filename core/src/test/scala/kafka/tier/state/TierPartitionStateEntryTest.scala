/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.PrintStream
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.Collections

import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test
import kafka.tier.tools.DumpTierPartitionState

class TierPartitionStateEntryTest {
  @Test
  def readWriteHeaderOnly(): Unit = {
    val dir = TestUtils.tempDir()
    val topic = "topic_A"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val factory = new FileTierPartitionStateFactory()
    val state = factory.initState(dir, tp, true)

      try {
        state.beginCatchup()
        state.onCatchUpComplete()
        state.append(
        new TierTopicInitLeader(
          tp,
          9,
          java.util.UUID.randomUUID(),
          0))
        state.close()

        val state2 = factory.initState(dir, tp, true)
        state2.onTieringEnable()
        assertEquals(9, state.tierEpoch())
      } finally {
      dir.delete()
    }
  }


  @Test
  def serializeDeserializeTest(): Unit = {
    val dir = TestUtils.tempDir()
    val topic = "topic_A"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val n = 200
    val epoch = 0
    val factory = new FileTierPartitionStateFactory()
    val state = factory.initState(dir, tp, true)

    state.beginCatchup()
    state.onCatchUpComplete()
    val path = state.path
    try {
      state.append(
        new TierTopicInitLeader(
          tp,
          epoch,
          java.util.UUID.randomUUID(),
          0))
      var size = 0
      for (i <- 0 until n) {
        state.append(new TierObjectMetadata(tp, epoch, i * 2, 1, 1, i, i, true, false, 0))
        size += i
      }
      state.flush()

      val segmentOffsets = state.segmentOffsets.iterator

      for (i <- 0 until n) {
        val startOffset = i * 2L
        assertEquals(startOffset, segmentOffsets.next)
        assertEquals(startOffset, state.metadata(startOffset).get().startOffset())
      }
      assertFalse(segmentOffsets.hasNext)

      // flush to commit end offset
      state.flush()

      assertEquals(n, state.numSegments())
      assertEquals(size, state.totalSize)
      assertEquals(0L, state.startOffset().get())
      assertEquals(n * 2 - 1 : Long, state.endOffset().get())

      state.close()

      checkInvalidFileReset(dir, tp, path)
    }
    finally {
      dir.delete()
    }
  }

  @Test
  def updateEpochTest(): Unit = {
    val dir = TestUtils.tempDir()
    val topic = "topic_A"
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val n = 200
    val epoch = 0
    val factory = new FileTierPartitionStateFactory()
    val state = factory.initState(dir, tp, true)

    state.beginCatchup()
    state.onCatchUpComplete()
    val path = state.path
    try {
      state.append(
        new TierTopicInitLeader(
          tp,
          epoch,
          java.util.UUID.randomUUID(),
          0))
      var size = 0
      for (i <- 0 until n) {
        state.append(new TierObjectMetadata(tp, epoch, i * 2, 1, 1, i, i, true, false, 0))
        size += i
      }

      state.flush()
      state.append(
        new TierTopicInitLeader(
          tp,
          epoch + 1,
          java.util.UUID.randomUUID(),
          0))
      state.close()

      val reopenedState = factory.initState(dir, tp, true)
      assertEquals(1, reopenedState.tierEpoch())
      assertEquals(size, reopenedState.totalSize())
      reopenedState.close()
    }
    finally {

      dir.delete()
    }
  }


  @Test
  def flushAvailabilityTest(): Unit = {
    val dir = TestUtils.tempDir()
    val topic = "topic_A"
    val partition = 0
    val partitionDir = new File(dir.getAbsolutePath + "/" + topic + "-" + partition)
    partitionDir.mkdir()

    val tp = new TopicPartition(topic, partition)
    val factory = new FileTierPartitionStateFactory()
    val state = factory.initState(partitionDir, tp, true)
    val path = state.path

    state.beginCatchup()
    state.onCatchUpComplete()
    try {
      assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tp, 0, java.util.UUID.randomUUID(), 0)))
      assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierObjectMetadata(tp, 0, 0, 100, 100, 0, 0, false, false, 0)))

      assertTrue(state.uncommittedEndOffset().isPresent)
      assertFalse(state.endOffset().isPresent)
      assertEquals(100L, state.uncommittedEndOffset().get())
      assertFalse(state.startOffset().isPresent)
      assertEquals(0, state.segmentOffsets().size())
      assertEquals(0, state.segmentOffsets(0, Long.MaxValue).size())
      state.flush()
      assertTrue(state.metadata(0L).isPresent)
      assertEquals(0L, state.startOffset().get())
      assertEquals(100L, state.endOffset().get())
      assertArrayEquals(util.Arrays.asList(0L).toArray, state.segmentOffsets().toArray())
      assertArrayEquals(util.Arrays.asList(0L).toArray, state.segmentOffsets(0, Long.MaxValue).toArray())

      assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierObjectMetadata(tp, 0, 100, 100, 200, 0, 0, false, false, 0)))
      assertEquals(0L, state.startOffset().get())
      assertEquals(100L, state.endOffset().get())
      assertEquals(200L, state.uncommittedEndOffset().get())
      assertTrue(state.metadata(0L).isPresent)
      assertEquals(100, state.metadata(100L).get().endOffset())
      assertArrayEquals(util.Arrays.asList(0L).toArray, state.segmentOffsets().toArray())
      assertArrayEquals(util.Arrays.asList(0L).toArray, state.segmentOffsets(0, Long.MaxValue).toArray())
      state.flush()
      assertEquals(0L, state.startOffset().get())
      assertEquals(200L, state.endOffset().get())
      assertEquals(200, state.metadata(101L).get().endOffset())
      assertTrue(state.metadata(0L).isPresent)
      assertTrue(state.metadata(101L).isPresent)
      assertArrayEquals(util.Arrays.asList(0L, 101L).toArray, state.segmentOffsets().toArray())
      assertArrayEquals(util.Arrays.asList(0L, 101L).toArray, state.segmentOffsets(0, Long.MaxValue).toArray())
      val numSegments = state.numSegments()
      state.close()

      validateConsoleDumpedEntries(partitionDir, numSegments)
    } finally {
      dir.delete()
    }
  }


  private def validateConsoleDumpedEntries(partitionDir: File, numSegments: Int) = {
    val sysOut = System.out
    val baos = new ByteArrayOutputStream()
    System.setOut(new PrintStream(baos))
    try {
      val args: Array[String] = new Array[String](1)
      args(0) = partitionDir.getAbsolutePath
      DumpTierPartitionState.main(args)

    } finally {
      System.setOut(sysOut)
    }
    val numLines = baos.toString.split("\n").length
    assertEquals(numSegments + 1, numLines)
  }

  /*
    Check that tier partition state is reset if the file is read and is invalid.
    This will cause the tier partition state to be re-materialized from the tier topic.
    */
  private def checkInvalidFileReset(baseDir: File, tp: TopicPartition, path: String) = {
    // write some garbage to the end to test truncation
    val channel = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val buf = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN)
    buf.putShort(80)
    buf.putInt(1)
    buf.flip()
    channel.position(channel.size())
    channel.write(buf)
    channel.close()

    // re-open to force truncate
    val state2 = new FileTierPartitionState(baseDir, tp, true, false)
    assertEquals(-1, state2.tierEpoch())
    assertEquals(TierPartitionStatus.INIT, state2.status())
  }
}
