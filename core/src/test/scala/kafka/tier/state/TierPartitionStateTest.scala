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

import kafka.log.Log
import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Before, Test}
import kafka.tier.tools.DumpTierPartitionState
import org.apache.kafka.common.utils.Utils

class TierPartitionStateTest {
  val factory = new FileTierPartitionStateFactory()
  val parentDir = TestUtils.tempDir()
  val dir = TestUtils.randomPartitionLogDir(parentDir)
  val tp = Log.parseTopicPartitionName(dir)
  val state = new FileTierPartitionState(dir, tp, true)

  @Before
  def setup(): Unit = {
    state.beginCatchup()
    state.onCatchUpComplete()
  }

  @After
  def teardown(): Unit = {
    state.close()
    dir.delete()
    parentDir.delete()
  }

  @Test
  def readWriteHeaderOnly(): Unit = {
    state.append(new TierTopicInitLeader(tp, 9, java.util.UUID.randomUUID(), 0))
    assertEquals(9, state.tierEpoch())
    state.close()

    val reopenedState = factory.initState(dir, tp, true)
    assertEquals(9, reopenedState.tierEpoch())
    reopenedState.close()
  }

  @Test
  def serializeDeserializeTest(): Unit = {
    val numSegments = 200
    var currentSegments = 0L
    val epoch = 0

    val path = state.path
    state.append(new TierTopicInitLeader(tp, epoch, java.util.UUID.randomUUID(), 0))
    var size = 0
    for (i <- 0 until numSegments) {
      state.append(new TierObjectMetadata(tp, epoch, i * 2, 1, 1, i, i, true, false, false, 0))
      size += i
      currentSegments += 1
    }
    state.flush()

    var segmentOffsets = state.segmentOffsets.iterator
    for (i <- 0 until numSegments) {
      val startOffset = i * 2L
      assertEquals(startOffset, segmentOffsets.next)
      assertEquals(startOffset, state.metadata(startOffset).get().startOffset())
    }
    assertFalse(segmentOffsets.hasNext)
    assertEquals(currentSegments, state.numSegments())
    assertEquals(size, state.totalSize)
    assertEquals(0L, state.startOffset().get())
    assertEquals(currentSegments * 2 - 1 : Long, state.committedEndOffset().get())

    // append more segments after flush
    for (i <- numSegments until numSegments * 2) {
      state.append(new TierObjectMetadata(tp, epoch, i * 2, 1, 1, i, i, true, false, false, 0))
      size += i
      currentSegments += 1
    }
    state.flush()

    segmentOffsets = state.segmentOffsets.iterator
    for (i <- 0L until currentSegments) {
      val startOffset = i * 2L
      assertEquals(startOffset, segmentOffsets.next)
      assertEquals(startOffset, state.metadata(startOffset).get().startOffset())
    }
    assertFalse(segmentOffsets.hasNext)
    assertEquals(currentSegments, state.numSegments())
    assertEquals(size, state.totalSize)
    assertEquals(0L, state.startOffset().get())
    assertEquals(currentSegments * 2 - 1, state.committedEndOffset().get())

    state.close()
    checkInvalidFileReset(dir, tp, path)
  }

  @Test
  def updateEpochTest(): Unit = {
    val n = 200
    val epoch = 0

    state.append(new TierTopicInitLeader(tp, epoch, java.util.UUID.randomUUID(), 0))
    var size = 0
    for (i <- 0 until n) {
      state.append(new TierObjectMetadata(tp, epoch, i * 2, 1, 1, i, i, true, false, false, 0))
      size += i
    }

    state.flush()
    state.append(new TierTopicInitLeader(tp, epoch + 1, java.util.UUID.randomUUID(), 0))
    state.close()

    val reopenedState = factory.initState(dir, tp, true)
    assertEquals(1, reopenedState.tierEpoch())
    assertEquals(size, reopenedState.totalSize())
    reopenedState.close()
  }

  @Test
  def flushAvailabilityTest(): Unit = {
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tp, 0, java.util.UUID.randomUUID(), 0)))
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierObjectMetadata(tp, 0, 0, 100, 100, 0, 0, false, false, false, 0)))

    // committedEndOffset is unavailable before first flush
    assertEquals(100L, state.endOffset.get)
    assertFalse(state.committedEndOffset.isPresent)
    assertEquals(1, state.segmentOffsets.size)

    // committedEndOffset equals endOffset after flush
    state.flush()
    assertEquals(100L, state.endOffset.get)
    assertEquals(100L, state.committedEndOffset.get)

    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierObjectMetadata(tp, 0, 100, 100, 200, 0, 0, false, false, false, 0)))
    assertEquals(0L, state.startOffset.get)
    assertEquals(100L, state.committedEndOffset.get)
    assertEquals(200L, state.endOffset.get)

    state.flush()
    assertEquals(0L, state.startOffset.get)
    assertEquals(200L, state.committedEndOffset().get())
    val numSegments = state.segmentOffsets.size
    state.close()

    validateConsoleDumpedEntries(dir, numSegments)
  }

  @Test
  def testUpgrade(): Unit = {
    val numSegments = 200
    val epoch = 0
    val initialVersion = state.version

    state.append(new TierTopicInitLeader(tp, epoch, java.util.UUID.randomUUID(), 0))
    var size = 0
    for (i <- 0 until numSegments) {
      state.append(new TierObjectMetadata(tp, epoch, i * 2, 1, 1, i, i, true, false, false, 0))
      size += i
    }
    state.flush()

    assertEquals(numSegments, state.numSegments)
    state.close()

    val upgradedVersion = (initialVersion + 1).toByte
    val upgradedState = new FileTierPartitionState(dir, tp, true, upgradedVersion)
    assertEquals(upgradedVersion, upgradedState.version)
    assertEquals(numSegments, upgradedState.numSegments)
    upgradedState.close()
  }

  private def validateConsoleDumpedEntries(partitionDir: File, numSegments: Int) = {
    val sysOut = System.out
    val contents = new ByteArrayOutputStream()
    System.setOut(new PrintStream(contents))
    try {
      val args: Array[String] = new Array[String](1)
      args(0) = partitionDir.getAbsolutePath
      DumpTierPartitionState.main(args)
    } finally {
      System.setOut(sysOut)
    }
    val lines = contents.toString.split("\n")
    val numLines = lines.filter(_.startsWith("TierObjectMetadata")).length
    assertEquals(numSegments, numLines)
  }

  /*
    Check that tier partition state is reset if the file is read and is invalid.
    This will cause the tier partition state to be re-materialized from the tier topic.
    */
  private def checkInvalidFileReset(baseDir: File, tp: TopicPartition, path: String) = {
    // write some garbage to the end to test truncation
    var channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ, StandardOpenOption.WRITE)

    // read file contents
    val preContents = ByteBuffer.allocate(channel.size.toInt)
    Utils.readFully(channel, preContents, 0)
    preContents.flip()

    // append garbage
    val buf = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN)
    buf.putShort(80)
    buf.putInt(1)
    buf.flip()
    channel.position(channel.size())
    channel.write(buf)
    channel.close()

    // re-open file and check if contents are same as before appending garbage
    val state = new FileTierPartitionState(baseDir, tp, true)
    state.close()
    channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ)
    val postContents = ByteBuffer.allocate(channel.size.toInt)
    Utils.readFully(channel, postContents, 0)
    postContents.flip()

    assertEquals(preContents, postContents)
  }
}
