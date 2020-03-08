/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Optional, UUID}

import kafka.log.{Log, LogConfig}
import kafka.server.LogDirFailureChannel
import kafka.tier.{TierTestUtils, TopicIdPartition}
import kafka.tier.domain.{AbstractTierMetadata, TierSegmentDeleteComplete, TierSegmentDeleteInitiate, TierSegmentUploadComplete, TierSegmentUploadInitiate, TierTopicInitLeader}
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.tools.DumpTierPartitionState
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.Mockito.{mock, when}
import org.scalatest.Assertions.assertThrows

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionException

class TierPartitionStateTest {
  val factory = new TierPartitionStateFactory(true)
  val parentDir = TestUtils.tempDir()
  val dir = TestUtils.randomPartitionLogDir(parentDir)
  val tp = Log.parseTopicPartitionName(dir)
  val tpid = new TopicIdPartition(tp.topic, UUID.randomUUID, tp.partition)
  val logDirFailureChannel = new LogDirFailureChannel(5)
  val state = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
  val logConfig = mock(classOf[LogConfig])

  @Before
  def setup(): Unit = {
    state.setTopicId(tpid.topicId)
    state.beginCatchup()
    state.onCatchUpComplete()
    when(logConfig.tierEnable).thenReturn(true)
    TierTestUtils.initTierTopicOffset()
  }

  @After
  def teardown(): Unit = {
    state.close()
    dir.delete()
    parentDir.delete()
  }

  @Test
  def readWriteHeaderOnly(): Unit = {
    state.append(new TierTopicInitLeader(tpid, 9, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    assertEquals(9, state.tierEpoch())
    state.close()

    val reopenedState = factory.initState(dir, tp, logConfig, logDirFailureChannel)
    assertEquals(9, reopenedState.tierEpoch())
    reopenedState.close()
  }

  @Test
  def testPreviousOffsetEvent(): Unit = {
    val initLeaderEvent = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0)
    assertEquals(AppendResult.ACCEPTED, state.append(initLeaderEvent, 1))
    assertEquals(AppendResult.FENCED, state.append(initLeaderEvent, 1))
    assertEquals(AppendResult.FENCED, state.append(initLeaderEvent, 0))
  }

  @Test
  def testOffsetIsIncremented(): Unit = {
    val initLeaderEvent = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0)
    val newInitLeaderEvent = new TierTopicInitLeader(tpid, 1, java.util.UUID.randomUUID(), 0)
    assertEquals(AppendResult.ACCEPTED, state.append(initLeaderEvent, TierTestUtils.nextTierTopicOffset))
    assertEquals("Last consumed offset mismatch", 0, state.lastConsumedSrcOffset())
    assertEquals(AppendResult.ACCEPTED, state.append(newInitLeaderEvent, TierTestUtils.nextTierTopicOffset))
    assertEquals("Last consumed offset mismatch", 1, state.lastConsumedSrcOffset())
  }

  @Test
  def serializeDeserializeTest(): Unit = {
    val numSegments = 200
    var currentSegments = 0L
    val epoch = 0

    val path = state.flushedPath
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    var size = 0
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      size += i
      currentSegments += 1
    }
    state.flush()

    var segmentOffsets = state.segmentOffsets.iterator
    for (i <- 0 until numSegments) {
      val startOffset = i * 2L
      assertEquals(startOffset, segmentOffsets.next)
      assertEquals(startOffset, state.metadata(startOffset).get().baseOffset())
    }
    assertFalse(segmentOffsets.hasNext)
    assertEquals(currentSegments, state.numSegments())
    assertEquals(size, state.totalSize)
    assertEquals(0L, state.startOffset().get())
    assertEquals(currentSegments * 2 - 1 : Long, state.committedEndOffset())

    // append more segments after flush
    for (i <- numSegments until numSegments * 2) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      size += i
      currentSegments += 1
    }
    state.flush()

    segmentOffsets = state.segmentOffsets.iterator
    for (i <- 0L until currentSegments) {
      val startOffset = i * 2L
      assertEquals(startOffset, segmentOffsets.next)
      assertEquals(startOffset, state.metadata(startOffset).get().baseOffset())
    }
    assertFalse(segmentOffsets.hasNext)
    assertEquals(currentSegments, state.numSegments())
    assertEquals(size, state.totalSize)
    assertEquals(0L, state.startOffset().get())
    assertEquals(currentSegments * 2 - 1, state.committedEndOffset())

    state.close()
    checkInvalidFileReset(dir, tp, path)
  }


  @Test
  def segmentGapTest(): Unit = {
    val epoch = 0

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    val objectId1 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId1, 0, 50, 100, 0, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId1), TierTestUtils.nextTierTopicOffset))

    val objectId2 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId2, 75, 150, 100, 0, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId2), TierTestUtils.nextTierTopicOffset))
    state.flush()

    assertEquals(objectId1, state.metadata(50).get().objectId())
    assertEquals(objectId2, state.metadata(51).get().objectId())
    assertFalse(state.metadata(151).isPresent)

    state.close()
  }

  /**
   * The offsets of uploaded segments can have an overlap as when leader changes each replica rolls segments independently
   * so segment boundaries may not be aligned perfectly. This test verifies the state metadata targetOffset to objectId
   * map. State metadata startOffsets can be different for runtime and rematerialized view, this can happen for case when
   * segments are deleted and then state is re-materialized, the test verifies this behavior.
   */
  @Test
  def segmentOverlapTest(): Unit = {
    val epoch = 0

    // add two segments
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    val objectId1 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId1, 0, 50, 100, 0, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId1), TierTestUtils.nextTierTopicOffset))

    val objectId2 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId2, 25, 150, 100, 0, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId2), TierTestUtils.nextTierTopicOffset))
    state.flush()

    // verify objectId target offsets
    assertEquals(objectId1, state.metadata(24).get().objectId())
    assertEquals(objectId1, state.metadata(25).get().objectId())
    assertEquals(objectId1, state.metadata(50).get().objectId())
    assertEquals(objectId2, state.metadata(51).get().objectId())
    assertFalse(state.metadata(151).isPresent)

    // delete all segments tiered
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId1), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectId1), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId2), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectId2), TierTestUtils.nextTierTopicOffset))
    state.flush()

    // verify the endOffset is tracked as expected
    assertEquals(150L, state.endOffset())
    assertEquals(150L, state.committedEndOffset())

    // upload another object with overlap with the current endOffset
    val objectId3 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId3, 75, 175, 100, 0, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId3), TierTestUtils.nextTierTopicOffset))
    state.flush()

    // verify objectId target overlap offsets
    assertFalse(state.metadata(150).isPresent)
    assertEquals(objectId3, state.metadata(151).get().objectId())
    assertEquals(objectId3, state.metadata(175).get().objectId())
    assertFalse(state.metadata(176).isPresent)

    state.close()

    // test state with overlap segment is materialized correctly
    val reopenedState = factory.initState(dir, tp, logConfig, logDirFailureChannel)
    assertEquals(175L, reopenedState.endOffset)
    assertEquals(175L, reopenedState.committedEndOffset)

    // verify objectId target overlap offsets
    // state metadata startOffsets are different for runtime and rematerialized view, this is expected
    assertFalse(reopenedState.metadata(74).isPresent)
    assertEquals(objectId3, reopenedState.metadata(75).get().objectId())
    assertEquals(objectId3, reopenedState.metadata(175).get().objectId())
    assertFalse(reopenedState.metadata(176).isPresent)

    reopenedState.close()
  }


  @Test
  def updateEpochTest(): Unit = {
    val n = 200
    val epoch = 0

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    var size = 0
    for (i <- 0 until n) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      size += i
    }

    state.flush()
    state.append(new TierTopicInitLeader(tpid, epoch + 1, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    state.close()


    val reopenedState = factory.initState(dir, tp, logConfig, logDirFailureChannel)
    assertEquals(1, reopenedState.tierEpoch())
    assertEquals(size, reopenedState.totalSize())
    reopenedState.close()
  }

  /**
   * Verifies that state.endOffset and the state.committedEndOffset are correctly recorded at runtime and initialized during
   * re-materialization of the state.
   */
  @Test
  def updateEndOffsetTest(): Unit = {
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset))

    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 100, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))

    // committedEndOffset is unavailable before first flush
    assertEquals(100L, state.endOffset)
    assertEquals(-1L, state.committedEndOffset)
    assertEquals(1, state.segmentOffsets.size)

    // committedEndOffset equals endOffset after flush
    state.flush()
    assertEquals(100L, state.endOffset)
    assertEquals(100L, state.committedEndOffset)

    val reopenedState = factory.initState(dir, tp, logConfig, logDirFailureChannel)
    assertEquals(100L, reopenedState.endOffset)
    assertEquals(100L, reopenedState.committedEndOffset)
    reopenedState.close()
  }

  @Test
  def flushAvailabilityTest(): Unit = {
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset))

    var objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 100, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))

    // committedEndOffset is unavailable before first flush
    assertEquals(100L, state.endOffset)
    assertEquals(-1L, state.committedEndOffset)
    assertEquals(1, state.segmentOffsets.size)

    // committedEndOffset equals endOffset after flush
    state.flush()
    assertEquals(100L, state.endOffset)
    assertEquals(100L, state.committedEndOffset)

    objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 100, 200, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))
    assertEquals(0L, state.startOffset.get)
    assertEquals(100L, state.committedEndOffset)
    assertEquals(200L, state.endOffset)

    state.flush()
    assertEquals(0L, state.startOffset.get)
    assertEquals(200L, state.committedEndOffset())
    val numSegments = state.segmentOffsets.size
    state.close()

    validateConsoleDumpedEntries(dir, numSegments)
  }

  @Test
  def testReopenFileAfterVersionChange(): Unit = {
    val numSegments = 200
    val epoch = 0
    val initialVersion = state.version
    val expectedEndOffset = (2*numSegments - 1).toLong

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    var size = 0
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      size += i
    }
    state.flush()

    assertEquals(numSegments, state.numSegments)
    assertEquals(expectedEndOffset, state.endOffset())
    assertEquals(expectedEndOffset, state.committedEndOffset())
    state.close()

    val upgradedVersion = (initialVersion + 1).toByte
    val upgradedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true, true, upgradedVersion)
    assertEquals(upgradedVersion, upgradedState.version)
    assertEquals(numSegments, upgradedState.numSegments)
    assertEquals(expectedEndOffset, upgradedState.endOffset())
    assertEquals(expectedEndOffset, upgradedState.committedEndOffset())
    upgradedState.close()
  }

  @Test
  def testOngoingUploadNotVisibleToReaders(): Unit = {
    val numSegments = 20
    val epoch = 0
    var offset = 0L

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      offset += 1
    }

    assertEquals(offset, state.endOffset)
    assertEquals(numSegments, state.segmentOffsets.size)

    // initiate a new upload
    val inProgressObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, inProgressObjectId, offset, offset + 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))

    // upload must not be visible to readers
    assertEquals(offset, state.endOffset)
    assertEquals(numSegments, state.segmentOffsets.size)

    // complete upload
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, inProgressObjectId), TierTestUtils.nextTierTopicOffset))
    assertEquals(offset + 1, state.endOffset)
    assertEquals(numSegments + 1, state.segmentOffsets.size)
  }

  @Test
  def testMetadataReadReturnsValidSegments(): Unit = {
    var epoch = 0

    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset))

    // upload few segments at epoch=0
    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 100, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))

    // fenced segment
    val fencedObjectId = UUID.randomUUID()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, fencedObjectId, 101, 200, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))

    // append object at the same offset range as the fenced segment
    epoch += 1
    val expectedObjectId =  UUID.randomUUID
    state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, expectedObjectId, 150, 200, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, expectedObjectId), TierTestUtils.nextTierTopicOffset))

    assertEquals(2, state.numSegments())
    assertEquals(1, state.fencedSegments().size())

    // delete the fenced segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, fencedObjectId), TierTestUtils.nextTierTopicOffset))

    assertEquals(2, state.numSegments())
    assertEquals(0, state.fencedSegments().size())

    // validate that the correct segment is returned
    assertTrue(state.metadata(149).isPresent)
    assertEquals(expectedObjectId, state.metadata(149).get().objectId())
  }

  @Test
  def testMultipleInitiatesScannedCorrectlyOnReload(): Unit = {
    var epoch = 0
    var offset = 0

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, UUID.randomUUID(), offset, offset + 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    epoch += 1
    offset += 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, UUID.randomUUID(), offset, offset + 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    epoch += 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    val initialFenced = state.fencedSegments()

    assertEquals(2, initialFenced.size())
    // close state and reopen to allow scanning to check in progress uploads
    state.close()


    val state2 = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertFalse(state2.setTopicId(tpid.topicId))

    val afterReloadFenced = state2.fencedSegments()
    assertEquals(initialFenced, afterReloadFenced)
  }

  @Test
  def testUploadAtLowerEpochFenced(): Unit = {
    val numSegments = 20
    var epoch = 0
    var offset = 0

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      offset += 1
    }

    // upload few segments at epoch=1
    epoch = 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      offset += 1
    }

    // attempt to upload at epoch=0 must be fenced
    val fencedObjectId = UUID.randomUUID
    assertEquals(AppendResult.FENCED, state.append(new TierSegmentUploadInitiate(tpid, epoch - 1, fencedObjectId, offset, offset + 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))

    // unsuccessful initiate uploads are not tracked as fenced
    assertEquals(0, state.fencedSegments.size)
    assertEquals(numSegments * 2, state.segmentOffsets.size)

    // reopen state and validate state remains the same
    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    try {
      assertEquals(0, reopenedState.fencedSegments.size)
      assertEquals(numSegments * 2, reopenedState.segmentOffsets.size)
    } finally {
      reopenedState.close()
    }
  }

  @Test
  def testOngoingUploadFenced(): Unit = {
    val numSegments = 20
    val epoch = 0
    var offset = 0

    // upload few segments
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset)
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      offset += 1
    }

    val abortedObjectIds = new ListBuffer[UUID]
    val numAbortedSegments = 5

    // upload segments without completing them
    for (_ <- 0 until numAbortedSegments) {
      abortedObjectIds += UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, abortedObjectIds.last, offset, offset + 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    }

    val ongoingUpload = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, ongoingUpload, offset, offset + 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))

    // all but the last in-progress upload must now be fenced
    assertEquals(numAbortedSegments, state.fencedSegments.size)
    assertEquals(abortedObjectIds.toSet, state.fencedSegments.asScala.map(_.objectId).toSet)
    assertEquals(numSegments, state.numSegments)

    // must have the same state after reopening the file
    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    try {
      assertEquals(numAbortedSegments, reopenedState.fencedSegments.size)
      assertEquals(abortedObjectIds.toSet, reopenedState.fencedSegments.asScala.map(_.objectId).toSet)
      assertEquals(numSegments, reopenedState.numSegments)

      // complete the ongoing upload
      assertEquals(AppendResult.ACCEPTED, reopenedState.append(new TierSegmentUploadComplete(tpid, epoch, ongoingUpload), TierTestUtils.nextTierTopicOffset))
      assertEquals(ongoingUpload, reopenedState.metadata(reopenedState.segmentOffsets.last).get.objectId)
    } finally {
      reopenedState.close()
    }
  }

  @Test
  def testOngoingUploadFencedAfterLeaderChange(): Unit = {
    val numSegments = 20
    var epoch = 0
    var offset = 0
    val objectIds = for (_ <- 0 until numSegments) yield UUID.randomUUID

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)
    for (i <- 0 until numSegments) {
      val objectId = objectIds(i)
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      offset += 1
    }

    // begin an upload at epoch=0
    val abortedObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, abortedObjectId, offset, offset + 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))

    // begin deletion at epoch=0
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(0)), TierTestUtils.nextTierTopicOffset))

    // leader change; epoch=1
    epoch = 1
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffset))

    // both segment being uploaded and deleted must now be fenced
    assertEquals(2, state.fencedSegments.size)
    assertEquals(Set(abortedObjectId, objectIds(0)), state.fencedSegments.asScala.map(_.objectId).toSet)

    // attempt to complete upload must be fenced
    assertEquals(AppendResult.FENCED, state.append(new TierSegmentUploadComplete(tpid, epoch - 1, abortedObjectId), TierTestUtils.nextTierTopicOffset))
  }

  @Test
  // Tests what happens when a TierPartitionState is reopened with both fenced and unfenced segments at the same offset
  def testFencedSegmentHandlingOnReopen(): Unit = {
    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, 0, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)

    // begin an upload at epoch=0
    val abortedObjectId = UUID.randomUUID

    // initiate an upload to be fenced
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, abortedObjectId, 0, 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    // transition to epoch=1
    state.append(new TierTopicInitLeader(tpid, 1, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)
    // check segment is fenced
    val fenced = state.fencedSegments().stream().findFirst()
    assertEquals(fenced.get().objectId(), abortedObjectId)
    // try to complete fenced upload, should be fenced
    assertEquals(AppendResult.FENCED, state.append(new TierSegmentUploadComplete(tpid, 0, abortedObjectId), TierTestUtils.nextTierTopicOffset))
    val completedObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 1, completedObjectId, 0, 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    // delete initiated upload in between initiate and upload of overlapping segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, 1, abortedObjectId), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 1, completedObjectId), TierTestUtils.nextTierTopicOffset))

    // check fenced segment is removed after delete initiate for fenced segment
    val fencedBefore = state.fencedSegments()
    assertEquals(0, fencedBefore.size())
    assertEquals(completedObjectId, state.metadata(0).get().objectId())

    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    try {
      // check segments are seekable and fencedSegments list is the same after file is reopened
      assertArrayEquals(fencedBefore.toArray, reopenedState.fencedSegments().toArray)
      assertTrue(reopenedState.metadata(0).isPresent)
      assertEquals(completedObjectId, reopenedState.metadata(0).get().objectId())
    } finally {
      reopenedState.close()
    }
  }

  @Test
  // Tests what happens when a fenced segment is deleted when another segment has completed with the same offset
  def testFencedSegmentHandlingOnDeletion(): Unit = {

    state.append(new TierTopicInitLeader(tpid, 0, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)

    // begin an upload at epoch=0
    val abortedObjectId = UUID.randomUUID
    // initiate an upload that will be fenced
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, abortedObjectId, 0, 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    // transition to epoch=1
    state.append(new TierTopicInitLeader(tpid, 1, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)
    val completedObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 1, completedObjectId, 0, 1, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 1, completedObjectId), TierTestUtils.nextTierTopicOffset))

    // completed segment should be able to be looked up
    assertEquals(completedObjectId, state.metadata(0).get().objectId())
    // delete the fenced segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, 1, abortedObjectId), TierTestUtils.nextTierTopicOffset))
    // completed segment should still be able to be looked up after fenced segment deletion
    assertEquals(completedObjectId, state.metadata(0).get().objectId())
  }

  @Test
  def testDeleteSegments(): Unit = {
    val numSegments = 20
    val epoch = 0
    var offset = 0
    val objectIds = new ListBuffer[UUID]
    val tierObjectStore = mock(classOf[TierObjectStore])

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      offset += 1
      objectIds += objectId
    }

    val numSegmentsToDelete = 5
    for (i <- 0 until numSegmentsToDelete) {
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i)), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i)), TierTestUtils.nextTierTopicOffset))
    }

    val validObjectIds = objectIds.takeRight(numSegments - numSegmentsToDelete)
    val foundObjectIds = TierUtils.tieredSegments(state.segmentOffsets, state, Optional.of(tierObjectStore)).asScala.map(_.metadata.objectId).toList

    assertEquals(validObjectIds.size, state.numSegments)
    assertEquals(validObjectIds, foundObjectIds)
  }

  @Test
  def testDeleteSegmentsWithOverlap(): Unit = {
    val numSegments = 20
    val epoch = 0
    var offset = 0
    val objectIds = new ListBuffer[UUID]
    val endOffsets = new ListBuffer[Long]
    val tierObjectStore = mock(classOf[TierObjectStore])
    var size = 0

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 10, 100, i, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      objectIds += objectId
      endOffsets += (offset + 10)
      offset += 5
      size += i
    }

    val numSegmentsToDelete = 5
    for (i <- 0 until numSegmentsToDelete) {
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i)), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i)), TierTestUtils.nextTierTopicOffset))
      size -= i
    }

    val validObjectIds = objectIds.takeRight(numSegments - numSegmentsToDelete)
    val foundObjectIds = TierUtils.tieredSegments(state.segmentOffsets, state, Optional.of(tierObjectStore)).asScala.map(_.metadata.objectId).toList
    assertEquals(validObjectIds.size, state.numSegments)
    assertEquals(validObjectIds, foundObjectIds)

    val validEndOffsets = endOffsets.takeRight(numSegments - numSegmentsToDelete)
    val foundEndOffsets = TierUtils.tieredSegments(state.segmentOffsets, state, Optional.of(tierObjectStore)).asScala.map(_.endOffset).toList
    assertEquals(validEndOffsets, foundEndOffsets)
    assertEquals(size, state.totalSize)

    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    try {
      assertEquals(size, reopenedState.totalSize)
    } finally {
      reopenedState.close()
    }
  }

  /**
   * Operations such as retention and deleteRecords() can result in segments to be deleted from tiered storage. The
   * deletionTask determines the "segment" to be deleted and then updates the LogStartOffset to segment.endOffset + 1
   * However for performance reasons the checkpoint LogStartOffset is executed via async job or on clean shutdown. On
   * an unclean shutdown when the broker is re-elected as a leader it should not re-upload the previously tiered segment
   * if deleteInitiate was registered for the segment previously. This is ensured by tracking the endOffset materialized
   * on reload of file partition state file. This test validates that endOffset and totalSize are correctly materialized
   * after state file is re-opened (mimics the broker unclean shutdown case) for transitions to deleteInitiate and
   * deleteComplete state.
   */
  @Test
  def testEndOffsetIsTrackedForDeleteSegments(): Unit = {
    val numSegments = 20
    var epoch = 0
    var offset = 0
    val objectIds = new ListBuffer[UUID]
    var endOffset = 0L

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset,
        offset + 10, 100, 1, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
      objectIds += objectId
      endOffset = offset + 10
      offset += 5
    }

    def maybeIncrementEpochAndValidateTierState(state: FileTierPartitionState, isLeader: Boolean,
                                                expectedEndOffset: Long, expectedSize: Long): Unit = {
      // Before
      assertEquals("FileTierPartitionState endOffset at run time", expectedEndOffset, state.endOffset())
      assertEquals("FileTierPartitionState totalSize at run time", expectedSize, state.totalSize())
      if (isLeader) {
        epoch = epoch + 1
        state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)
      }
      // Mimic a broker restart verify partition state endOffset and totalSize is same as before restart.
      state.close()
      val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
      try {
        // After
        assertEquals("FileTierPartitionState endOffset materialized value", expectedEndOffset, reopenedState.endOffset())
        assertEquals("FileTierPartitionState totalSize materialized value", expectedSize, reopenedState.totalSize())
      } finally {
        reopenedState.close()
      }
    }

    var currentState = state
    try {
      // As Leader (increment epoch)
      // Transition each segment to DeleteInitiate and then since leader restarts the segment should be transitioned to
      // fenced state
      for (i <- 0 until numSegments/2) {
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i)), TierTestUtils.nextTierTopicOffset))
        maybeIncrementEpochAndValidateTierState(currentState, true, endOffset, numSegments - (i + 1))
        currentState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
      }
      // As a follower (does not increment epoch)
      // Transition each segment to DeleteInitiate and then DeleteComplete and at each step verify state before and after
      // reopening the FileTierPartitionState.
      for (i <- numSegments/2 until numSegments) {
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i)), TierTestUtils.nextTierTopicOffset))
        maybeIncrementEpochAndValidateTierState(currentState, false, endOffset, numSegments - (i + 1))
        currentState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i)), TierTestUtils.nextTierTopicOffset))
        maybeIncrementEpochAndValidateTierState(currentState, false, endOffset, numSegments - (i + 1))
        currentState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
      }
    }finally {
      currentState.close()
    }
  }

  /**
   * Verifies that endOffset and totalSize is updated for deleteInitiate fenced segment during both runtime and on reload
   * (broker restart) of tier partition state. Additionally verifies for uploadInitiate fenced segment the endOffset and
   * totalSize is NOT modified.
   */
  @Test
  def testEndOffsetIsTrackedForSegmentsFencedOnDeleteInitiate(): Unit = {
    var epoch = 0
    var offset = 0L
    var endOffset = 0L
    var objectId = UUID.randomUUID

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset,
      offset + 10, 100, 1, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
    endOffset = offset + 10

    // 1. deleteInitiate the one and only segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))
    // 2. uploadInitiate an new segment
    objectId = UUID.randomUUID
    offset += 5
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset,
      offset + 10, 100, 1, false, false, false), TierTestUtils.nextTierTopicOffset))

    // New leader
    epoch = 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 1), TierTestUtils.nextTierTopicOffset)
    // Verify that both deleteInitiate and uploadInitiate are fenced as they are from previous epoch and endOffset,
    // totalSize is tracked correctly.
    assertEquals(2, state.fencedSegments.size)
    assertEquals("FileTierPartitionState endOffset runtime value", endOffset, state.endOffset())
    assertEquals("FileTierPartitionState totalSize runtime value", 0, state.totalSize())

    // Broker restarts: reopen state and validate again.
    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    try {
      assertEquals(reopenedState.toString(), 2, reopenedState.fencedSegments.size)
      assertEquals("FileTierPartitionState endOffset materialized value", endOffset, reopenedState.endOffset())
      assertEquals("FileTierPartitionState totalSize materialized value", 0, reopenedState.totalSize())
    } finally {
      reopenedState.close()
    }
  }

  @Test
  def testMaterializedOffset(): Unit = {
    // Make sure it's initialized with -1.
    assertEquals(-1, state.lastConsumedSrcOffset)

    // Send materialization request at offset 100.
    state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), 100)
    assertEquals(100, state.lastConsumedSrcOffset)

    // Send previous offset request, simulating possible duplicates during recovery or during transition from catchup.
    state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), 98)
    assertEquals(100, state.lastConsumedSrcOffset)

    // Broker restart.
    state.close()
    val restartState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertEquals(100, restartState.lastConsumedSrcOffset)

    // Test for the state version.
    assertEquals(3, state.version())
  }

  @Test
  def testIllegalTransitions(): Unit = {
    var stateWithFencingDisabledOpt = None: Option[FileTierPartitionState]
    try {
      val tp = Log.parseTopicPartitionName(dir)
      val tpid = new TopicIdPartition(tp.topic, UUID.randomUUID, tp.partition)
      stateWithFencingDisabledOpt = Some(FileTierPartitionState.createWithStateUpdateFailureFencingDisabled(dir, logDirFailureChannel, tp, true))
      stateWithFencingDisabledOpt.get.setTopicId(tpid.topicId)
      stateWithFencingDisabledOpt.get.beginCatchup()
      stateWithFencingDisabledOpt.get.onCatchUpComplete()

      def assertIllegal(metadata: AbstractTierMetadata): Unit = {
        assertThrows[IllegalStateException] {
          stateWithFencingDisabledOpt.get.append(metadata, TierTestUtils.nextTierTopicOffset)
        }
      }

      assertEquals(AppendResult.ACCEPTED, stateWithFencingDisabledOpt.get.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset))

      // 1. first transition must always start from UploadInitiate
      assertIllegal(new TierSegmentUploadComplete(tpid, 0, UUID.randomUUID))
      assertIllegal(new TierSegmentDeleteInitiate(tpid, 0, UUID.randomUUID))
      assertIllegal(new TierSegmentDeleteComplete(tpid, 0, UUID.randomUUID))

      // 2. cannot transition to DeleteComplete unless the previous state is DeleteInitiate
      val objectId = UUID.randomUUID
      val deleteComplete = new TierSegmentDeleteComplete(tpid, 0, objectId)

      assertEquals(AppendResult.ACCEPTED, stateWithFencingDisabledOpt.get.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertIllegal(deleteComplete)
      assertEquals(AppendResult.ACCEPTED, stateWithFencingDisabledOpt.get.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))
      assertIllegal(deleteComplete)
      assertEquals(AppendResult.ACCEPTED, stateWithFencingDisabledOpt.get.append(new TierSegmentDeleteInitiate(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, stateWithFencingDisabledOpt.get.append(deleteComplete, TierTestUtils.nextTierTopicOffset))
    } finally {
      stateWithFencingDisabledOpt.get.close();
    }
  }

  @Test
  def testStateUpdateFailureFencingEnabled() {
    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, objectId, 0), TierTestUtils.nextTierTopicOffset))

    // 1. upon first illegal transition, the failure should be fenced
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))
    assertEquals(TierPartitionStatus.ERROR, state.status)

    // 2. fenced failure is not unblocked even if a legal transition is tried
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(TierPartitionStatus.ERROR, state.status)
  }

  @Test
  def testStateUpdateFailureFencingFlushMechanism_Regular() {
    // --- BEFORE FENCING ---
    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, objectId, 0), TierTestUtils.nextTierTopicOffset))

    val channelMutableBeforeFencing = FileChannel.open(FileTierPartitionState.mutableFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelMutableBeforeFencing.size() > 0)
    val stateMutableBeforeFencing = ByteBuffer.allocate(10000);
    channelMutableBeforeFencing.read(stateMutableBeforeFencing, 0)
    channelMutableBeforeFencing.close

    state.flush

    val channelFlushedBeforeFencing = FileChannel.open(FileTierPartitionState.flushedFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelFlushedBeforeFencing.size() > 0)
    val headerFlushedBeforeFencing: Optional[Header] = FileTierPartitionState.readHeader(channelFlushedBeforeFencing)
    assertTrue(headerFlushedBeforeFencing.isPresent)
    assertEquals(headerFlushedBeforeFencing.get.status, TierPartitionStatus.ONLINE)
    val payloadFlushedBeforeFencing = ByteBuffer.allocate(10000);
    channelFlushedBeforeFencing.read(payloadFlushedBeforeFencing)
    channelFlushedBeforeFencing.close

    // --- TRIGGER FENCING ---

    // 1. upon first illegal transition, the failure should be fenced
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))
    assertEquals(TierPartitionStatus.ERROR, state.status)

    // 2. fenced failure is not unblocked even if a legal transition is tried
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(TierPartitionStatus.ERROR, state.status)

    state.flush

    // --- AFTER FENCING ---

    // Check that the header of the flushed file contains TierPartitionStatus.ERROR status
    val channelFlushedAfterFencing = FileChannel.open(FileTierPartitionState.flushedFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelFlushedAfterFencing.size() > 0)
    val headerFlushedAfterFencing: Optional[Header] = FileTierPartitionState.readHeader(channelFlushedAfterFencing)
    assertTrue(headerFlushedAfterFencing.isPresent)
    assertEquals(headerFlushedAfterFencing.get.status, TierPartitionStatus.ERROR)
    // Check that the payload of the flushed file before fencing matches byte-by-byte with the
    // payload of the flushed file after fencing.
    val payloadFlushedAfterFencing = ByteBuffer.allocate(10000);
    channelFlushedAfterFencing.read(payloadFlushedAfterFencing)
    channelFlushedAfterFencing.close

    assertEquals(payloadFlushedBeforeFencing, payloadFlushedAfterFencing)

    // Check that the state of the error file after fencing matches byte-by-byte with the
    // state of the mutable file upon which fencing happened for the first time.
    val channelErrorAfterFencing = FileChannel.open(FileTierPartitionState.errorFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelErrorAfterFencing.size() > 0)
    val stateErrorAfterFencing = ByteBuffer.allocate(10000);
    channelErrorAfterFencing.read(stateErrorAfterFencing)

    assertEquals(stateMutableBeforeFencing, stateErrorAfterFencing)
  }

  @Test
  def testStateUpdateFailureFencingFlush_DuringAbsentHeader() {
    // --- BEFORE FENCING ---
    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, objectId, 0), TierTestUtils.nextTierTopicOffset))

    val channelMutableBeforeFencing = FileChannel.open(FileTierPartitionState.mutableFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelMutableBeforeFencing.size() > 0)
    val stateMutableBeforeFencing = ByteBuffer.allocate(10000);
    channelMutableBeforeFencing.read(stateMutableBeforeFencing, 0)
    channelMutableBeforeFencing.close

    state.flush

    // --- TRIGGER FENCING ---

    // 1. upon first illegal transition, the failure should be fenced
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))
    assertEquals(TierPartitionStatus.ERROR, state.status)

    // 2. fenced failure is not unblocked even if a legal transition is tried
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(TierPartitionStatus.ERROR, state.status)

    // Manaully nuke the file (it should get recreated during flush with header TierPartitionStatus.ERROR)
    Files.delete(FileTierPartitionState.flushedFilePath(state.basePath))

    state.flush

    // --- AFTER FENCING ---

    // Check that the header of the flushed file contains TierPartitionStatus.ERROR status
    val channelFlushedAfterFencing = FileChannel.open(FileTierPartitionState.flushedFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelFlushedAfterFencing.size() > 0)
    val headerFlushedAfterFencing: Optional[Header] = FileTierPartitionState.readHeader(channelFlushedAfterFencing)
    assertTrue(headerFlushedAfterFencing.isPresent)
    assertEquals(
      headerFlushedAfterFencing.get,
      new Header(
        tpid.topicId(),
        state.version(),
        -1,
        TierPartitionStatus.ERROR,
        -1L,
        -1L))
  }

  @Test
  def testIdempotencyDeleteAfterComplete(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0)
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false)
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1)
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1)
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1)
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false)
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2)
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 0, objectId2)
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 0, objectId2)

    testDuplicateAppend(initLeader, Seq.empty, AppendResult.ACCEPTED)

    // try delete immediately after upload order
    val currentTransitions: ListBuffer[AbstractTierMetadata] = ListBuffer()
    for (transition <- Seq(uploadInitiate1, uploadComplete1, deleteInitiate1, deleteComplete1, uploadInitiate2, uploadComplete2, deleteInitiate2, deleteComplete2)) {
      testDuplicateAppend(transition, currentTransitions, AppendResult.ACCEPTED)
      currentTransitions += transition
    }
  }

  @Test
  def testIdempotencyDelayedDelete(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0)
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false)
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1)
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1)
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1)
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false)
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2)
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 0, objectId2)
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 0, objectId2)

    testDuplicateAppend(initLeader, Seq.empty, AppendResult.ACCEPTED)

    // try delayed delete order
    val currentTransitions: ListBuffer[AbstractTierMetadata] = ListBuffer()
    for (transition <- Seq(uploadInitiate1, uploadComplete1, uploadInitiate2, uploadComplete2, deleteInitiate1, deleteComplete1, deleteInitiate2, deleteComplete2)) {
      testDuplicateAppend(transition, currentTransitions, AppendResult.ACCEPTED)
      currentTransitions += transition
    }
  }

  @Test
  def testIdempotencySimultaneousDelete(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0)
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false)
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1)
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1)
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1)
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false)
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2)
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 0, objectId2)
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 0, objectId2)

    testDuplicateAppend(initLeader, Seq.empty, AppendResult.ACCEPTED)

    // try multiple simultaneous delete initiate orders, then stage the delete completes
    val currentTransitions: ListBuffer[AbstractTierMetadata] = ListBuffer()
    for (transition <- Seq(uploadInitiate1, uploadComplete1, uploadInitiate2, uploadComplete2, deleteInitiate1, deleteInitiate2, deleteComplete1, deleteComplete2)) {
      testDuplicateAppend(transition, currentTransitions, AppendResult.ACCEPTED)
      currentTransitions += transition
    }
  }

  @Test
  def testIdempotentencyFencing(): Unit = {
    val initLeader1 = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0)
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false)
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1)
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1)
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1)
    val initLeader2 = new TierTopicInitLeader(tpid, 1, java.util.UUID.randomUUID, 0)
    val fencedUploadId = UUID.randomUUID
    val fencedUploadInitiate = new TierSegmentUploadInitiate(tpid, 0, fencedUploadId, 10, 20, 100, 100, false, false, false)
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 1, objectId2, 10, 20, 100, 100, false, false, false)
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 1, objectId2)
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 1, objectId2)
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 1, objectId2)

    testDuplicateAppend(initLeader1, Seq.empty, AppendResult.ACCEPTED)

    // try delete immediately after upload order
    val currentTransitions: ListBuffer[AbstractTierMetadata] = ListBuffer()
    for (transition <- Seq(uploadInitiate1, uploadComplete1, deleteInitiate1, deleteComplete1, initLeader2)) {
      testDuplicateAppend(transition, currentTransitions, AppendResult.ACCEPTED)
      currentTransitions += transition
    }

    // test fence case
    testDuplicateAppend(fencedUploadInitiate, currentTransitions, AppendResult.FENCED)
    currentTransitions += fencedUploadInitiate

    for (transition <- Seq(uploadInitiate2, uploadComplete2, deleteInitiate2, deleteComplete2)) {
      testDuplicateAppend(transition, currentTransitions, AppendResult.ACCEPTED)
      currentTransitions += transition
    }
  }

  @Test
  def testMaterializationListenerCompletion(): Unit = {
    val epoch = 3
    val baseOffsets = new util.TreeSet[Long]()
    val numOffsetsInSegment = 49
    state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset)

    // upload few segments: [0-49], [50-99], [100-149], [150-199]
    var baseOffset = 0
    for (_ <- 0 to 3) {
      val objectId = UUID.randomUUID
      val endOffset = baseOffset + numOffsetsInSegment

      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, baseOffset, endOffset, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId), TierTestUtils.nextTierTopicOffset))

      baseOffsets.add(baseOffset)
      baseOffset = endOffset + 1
    }

    // initiate upload for segment [200-249]
    val lastObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, lastObjectId, baseOffset, baseOffset + 49, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(baseOffsets.last + numOffsetsInSegment, state.endOffset)
    assertEquals(-1, state.committedEndOffset)

    // Verify that materialization listeners are completed with the right segment metadata. In addition, we must flush
    // the state file before completing the listener.
    assertEquals(baseOffsets.floor(49), state.materializationListener(49).get(0, TimeUnit.MILLISECONDS).baseOffset)
    assertEquals(baseOffsets.last + numOffsetsInSegment, state.committedEndOffset)

    assertEquals(baseOffsets.floor(50), state.materializationListener(50).get(0, TimeUnit.MILLISECONDS).baseOffset)
    assertEquals(baseOffsets.floor(155), state.materializationListener(155).get(0, TimeUnit.MILLISECONDS).baseOffset)
    assertEquals(baseOffsets.floor(199), state.materializationListener(199).get(0, TimeUnit.MILLISECONDS).baseOffset)

    // Verify that listener is not completed for offsets that has not been materialized yet
    val promise = state.materializationListener(200)
    assertFalse(promise.isDone)

    // complete upload for segment [200-249]; this should also complete the materialization listener
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, lastObjectId), TierTestUtils.nextTierTopicOffset))

    // materialization listener must now be completed and the state file should have been flushed
    assertTrue(promise.isDone)
    assertEquals(200, promise.get.baseOffset)
    assertEquals(lastObjectId, promise.get.objectId)
    assertEquals(200 + numOffsetsInSegment, state.committedEndOffset)

    // must be able to register a new materialization listener
    assertFalse(state.materializationListener(500).isDone)
  }

  @Test
  def testPreviousMaterializationListenerCancelled(): Unit = {
    val promise_1 = state.materializationListener(200)
    assertFalse(promise_1.isDone)

    // register another listener; this will cause the first one to be cancelled exceptionally
    val promise_2 = state.materializationListener(400)
    assertFalse(promise_2.isDone)
    assertThrows[IllegalStateException] {
      try {
        promise_1.get(0, TimeUnit.MILLISECONDS)
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
  }

  @Test
  def testMaterializationListenerAfterClose(): Unit = {
    state.close()
    assertThrows[Exception] {
      state.materializationListener(200).get(0, TimeUnit.MILLISECONDS)
    }
  }

  private def testDuplicateAppend(metadata: AbstractTierMetadata, previousTransitions: Seq[AbstractTierMetadata], expected: AppendResult): Unit = {
    assertEquals(metadata.toString, expected, state.append(metadata, TierTestUtils.nextTierTopicOffset))

    previousTransitions.foreach { metadata =>
      val result = state.append(metadata, TierTestUtils.nextTierTopicOffset)
      assertTrue(Set(AppendResult.FENCED, AppendResult.ACCEPTED)(result))
    }

    val segments = state.segmentOffsets
    val fencedSegments = state.fencedSegments
    val size = state.totalSize

    // append duplicate
    assertEquals(expected, state.append(metadata, TierTestUtils.nextTierTopicOffset))

    // assert the tier partition state does not change after a duplicate append
    assertEquals(segments, state.segmentOffsets)
    assertEquals(fencedSegments, state.fencedSegments)
    assertEquals(size, state.totalSize)
  }

  private def validateConsoleDumpedEntries(partitionDir: File, numSegments: Int): Unit = {
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
    val headerLines = lines.count(_.startsWith("Header"))
    assertEquals(1, headerLines)
    val numLines = lines.count(_.startsWith("TierObjectMetadata"))
    assertEquals(numSegments, numLines)
  }

  /*
    Check that tier partition state is reset if the file is read and is invalid.
    This will cause the tier partition state to be re-materialized from the tier topic.
    */
  private def checkInvalidFileReset(baseDir: File, tp: TopicPartition, path: String): Unit = {
    // write some garbage to the end to test truncation
    val channel = FileChannel.open(Paths.get(path), StandardOpenOption.READ, StandardOpenOption.WRITE)

    // append garbage
    val buf = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN)
    buf.putShort(80)
    buf.putInt(1)
    buf.flip()
    channel.position(channel.size())
    channel.write(buf)
    channel.close()

    // re-open file and check if contents are same as before appending garbage
    val state = new FileTierPartitionState(baseDir, logDirFailureChannel, tp, true)
    assertEquals(TierPartitionStatus.CATCHUP, state.status)
    assertEquals(0, state.segmentOffsets.size)
    assertEquals(0, state.fencedSegments.size)
    assertEquals(-1, state.tierEpoch)

    // appending metadata to reopened file should be permissible
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffset))
    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 100, 100, 100, false, false, false), TierTestUtils.nextTierTopicOffset))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId), TierTestUtils.nextTierTopicOffset))

    assertEquals(0, state.tierEpoch)
    assertEquals(1, state.segmentOffsets.size)
    state.close()
  }
}
