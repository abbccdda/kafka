/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{Optional, UUID}

import kafka.log.{Log, LogConfig}
import kafka.tier.TopicIdPartition
import kafka.tier.domain.{AbstractTierMetadata, TierSegmentDeleteComplete, TierSegmentDeleteInitiate, TierSegmentUploadComplete, TierSegmentUploadInitiate, TierTopicInitLeader}
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.tools.DumpTierPartitionState
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.function.ThrowingRunnable
import org.junit.{After, Before, Test}
import org.mockito.Mockito.{mock, when}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class TierPartitionStateTest {
  val factory = new TierPartitionStateFactory(true)
  val parentDir = TestUtils.tempDir()
  val dir = TestUtils.randomPartitionLogDir(parentDir)
  val tp = Log.parseTopicPartitionName(dir)
  val tpid = new TopicIdPartition(tp.topic, UUID.randomUUID, tp.partition)
  val state = new FileTierPartitionState(dir, tp, true)
  val logConfig = mock(classOf[LogConfig])

  @Before
  def setup(): Unit = {
    state.setTopicId(tpid.topicId)
    state.beginCatchup()
    state.onCatchUpComplete()
    when(logConfig.tierEnable).thenReturn(true)
  }

  @After
  def teardown(): Unit = {
    state.close()
    dir.delete()
    parentDir.delete()
  }

  @Test
  def readWriteHeaderOnly(): Unit = {
    state.append(new TierTopicInitLeader(tpid, 9, java.util.UUID.randomUUID(), 0))
    assertEquals(9, state.tierEpoch())
    state.close()

    val reopenedState = factory.initState(dir, tp, logConfig)
    assertEquals(9, reopenedState.tierEpoch())
    reopenedState.close()
  }

  @Test
  def serializeDeserializeTest(): Unit = {
    val numSegments = 200
    var currentSegments = 0L
    val epoch = 0

    val path = state.flushedPath
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    var size = 0
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
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
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
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

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    val objectId1 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId1, 0, 50, 100, 0, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId1)))

    val objectId2 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId2, 75, 150, 100, 0, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId2)))
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    val objectId1 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId1, 0, 50, 100, 0, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId1)))

    val objectId2 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId2, 25, 150, 100, 0, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId2)))
    state.flush()

    // verify objectId target offsets
    assertEquals(objectId1, state.metadata(24).get().objectId())
    assertEquals(objectId1, state.metadata(25).get().objectId())
    assertEquals(objectId1, state.metadata(50).get().objectId())
    assertEquals(objectId2, state.metadata(51).get().objectId())
    assertFalse(state.metadata(151).isPresent)

    // delete all segments tiered
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId1)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectId1)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId2)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectId2)))
    state.flush()

    // verify the endOffset is tracked as expected
    assertEquals(150L, state.endOffset())
    assertEquals(150L, state.committedEndOffset())

    // upload another object with overlap with the current endOffset
    val objectId3 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId3, 75, 175, 100, 0, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId3)))
    state.flush()

    // verify objectId target overlap offsets
    assertFalse(state.metadata(150).isPresent)
    assertEquals(objectId3, state.metadata(151).get().objectId())
    assertEquals(objectId3, state.metadata(175).get().objectId())
    assertFalse(state.metadata(176).isPresent)

    state.close()

    // test state with overlap segment is materialized correctly
    val reopenedState = factory.initState(dir, tp, logConfig)
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

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    var size = 0
    for (i <- 0 until n) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      size += i
    }

    state.flush()
    state.append(new TierTopicInitLeader(tpid, epoch + 1, java.util.UUID.randomUUID(), 0))
    state.close()

    val reopenedState = factory.initState(dir, tp, logConfig)
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
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0)))

    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 100, 100, 100, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId)))

    // committedEndOffset is unavailable before first flush
    assertEquals(100L, state.endOffset)
    assertEquals(-1L, state.committedEndOffset)
    assertEquals(1, state.segmentOffsets.size)

    // committedEndOffset equals endOffset after flush
    state.flush()
    assertEquals(100L, state.endOffset)
    assertEquals(100L, state.committedEndOffset)

    val reopenedState = factory.initState(dir, tp, logConfig)
    assertEquals(100L, reopenedState.endOffset)
    assertEquals(100L, reopenedState.committedEndOffset)
    reopenedState.close()
  }

  @Test
  def flushAvailabilityTest(): Unit = {
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0)))

    var objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 100, 100, 100, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId)))

    // committedEndOffset is unavailable before first flush
    assertEquals(100L, state.endOffset)
    assertEquals(-1L, state.committedEndOffset)
    assertEquals(1, state.segmentOffsets.size)

    // committedEndOffset equals endOffset after flush
    state.flush()
    assertEquals(100L, state.endOffset)
    assertEquals(100L, state.committedEndOffset)

    objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 100, 200, 100, 100, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId)))
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
  def testUpgrade(): Unit = {
    val numSegments = 200
    val epoch = 0
    val initialVersion = state.version
    val expectedEndOffset = (2*numSegments - 1).toLong

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    var size = 0
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      size += i
    }
    state.flush()

    assertEquals(numSegments, state.numSegments)
    assertEquals(expectedEndOffset, state.endOffset())
    assertEquals(expectedEndOffset, state.committedEndOffset())
    state.close()

    val upgradedVersion = (initialVersion + 1).toByte
    val upgradedState = new FileTierPartitionState(dir, tp, true, upgradedVersion)
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      offset += 1
    }

    assertEquals(offset, state.endOffset)
    assertEquals(numSegments, state.segmentOffsets.size)

    // initiate a new upload
    val inProgressObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, inProgressObjectId, offset, offset + 1, 100, 100, false, false, false)))

    // upload must not be visible to readers
    assertEquals(offset, state.endOffset)
    assertEquals(numSegments, state.segmentOffsets.size)

    // complete upload
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, inProgressObjectId)))
    assertEquals(offset + 1, state.endOffset)
    assertEquals(numSegments + 1, state.segmentOffsets.size)
  }

  @Test
  def testMultipleInitiatesScannedCorrectlyOnReload(): Unit = {
    var epoch = 0
    var offset = 0

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, UUID.randomUUID(), offset, offset + 1, 100, 100, false, false, false)))
    epoch += 1
    offset += 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, UUID.randomUUID(), offset, offset + 1, 100, 100, false, false, false)))
    epoch += 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    val initialFenced = state.fencedSegments()

    assertEquals(2, initialFenced.size())
    // close state and reopen to allow scanning to check in progress uploads
    state.close()


    val state2 = new FileTierPartitionState(dir, tp, true)
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      offset += 1
    }

    // upload few segments at epoch=1
    epoch = 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      offset += 1
    }

    // attempt to upload at epoch=0 must be fenced
    val fencedObjectId = UUID.randomUUID
    assertEquals(AppendResult.FENCED, state.append(new TierSegmentUploadInitiate(tpid, epoch - 1, fencedObjectId, offset, offset + 1, 100, 100, false, false, false)))

    // unsuccessful initiate uploads are not tracked as fenced
    assertEquals(0, state.fencedSegments.size)
    assertEquals(numSegments * 2, state.segmentOffsets.size)

    // reopen state and validate state remains the same
    state.close()
    val reopenedState = new FileTierPartitionState(dir, tp, true)
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0))
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      offset += 1
    }

    val abortedObjectIds = new ListBuffer[UUID]
    val numAbortedSegments = 5

    // upload segments without completing them
    for (_ <- 0 until numAbortedSegments) {
      abortedObjectIds += UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, abortedObjectIds.last, offset, offset + 1, 100, 100, false, false, false)))
    }

    val ongoingUpload = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, ongoingUpload, offset, offset + 1, 100, 100, false, false, false)))

    // all but the last in-progress upload must now be fenced
    assertEquals(numAbortedSegments, state.fencedSegments.size)
    assertEquals(abortedObjectIds.toSet, state.fencedSegments.asScala.map(_.objectId).toSet)
    assertEquals(numSegments, state.numSegments)

    // must have the same state after reopening the file
    state.close()
    val reopenedState = new FileTierPartitionState(dir, tp, true)
    try {
      assertEquals(numAbortedSegments, reopenedState.fencedSegments.size)
      assertEquals(abortedObjectIds.toSet, reopenedState.fencedSegments.asScala.map(_.objectId).toSet)
      assertEquals(numSegments, reopenedState.numSegments)

      // complete the ongoing upload
      assertEquals(AppendResult.ACCEPTED, reopenedState.append(new TierSegmentUploadComplete(tpid, epoch, ongoingUpload)))
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
    state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID, 0))
    for (i <- 0 until numSegments) {
      val objectId = objectIds(i)
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      offset += 1
    }

    // begin an upload at epoch=0
    val abortedObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, abortedObjectId, offset, offset + 1, 100, 100, false, false, false)))

    // begin deletion at epoch=0
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(0))))

    // leader change; epoch=1
    epoch = 1
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0)))

    // both segment being uploaded and deleted must now be fenced
    assertEquals(2, state.fencedSegments.size)
    assertEquals(Set(abortedObjectId, objectIds(0)), state.fencedSegments.asScala.map(_.objectId).toSet)

    // attempt to complete upload must be fenced
    assertEquals(AppendResult.FENCED, state.append(new TierSegmentUploadComplete(tpid, epoch - 1, abortedObjectId)))
  }

  @Test
  def testDeleteSegments(): Unit = {
    val numSegments = 20
    val epoch = 0
    var offset = 0
    val objectIds = new ListBuffer[UUID]
    val tierObjectStore = mock(classOf[TierObjectStore])

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0))
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      offset += 1
      objectIds += objectId
    }

    val numSegmentsToDelete = 5
    for (i <- 0 until numSegmentsToDelete) {
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i))))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i))))
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0))
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 10, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      objectIds += objectId
      endOffsets += (offset + 10)
      offset += 5
      size += i
    }

    val numSegmentsToDelete = 5
    for (i <- 0 until numSegmentsToDelete) {
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i))))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i))))
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
    val reopenedState = new FileTierPartitionState(dir, tp, true)
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0))
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset,
        offset + 10, 100, 1, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
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
        state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0))
      }
      // Mimic a broker restart verify partition state endOffset and totalSize is same as before restart.
      state.close()
      val reopenedState = new FileTierPartitionState(dir, tp, true)
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
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i))))
        maybeIncrementEpochAndValidateTierState(currentState, true, endOffset, numSegments - (i + 1))
        currentState = new FileTierPartitionState(dir, tp, true)
      }
      // As a follower (does not increment epoch)
      // Transition each segment to DeleteInitiate and then DeleteComplete and at each step verify state before and after
      // reopening the FileTierPartitionState.
      for (i <- numSegments/2 until numSegments) {
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i))))
        maybeIncrementEpochAndValidateTierState(currentState, false, endOffset, numSegments - (i + 1))
        currentState = new FileTierPartitionState(dir, tp, true)
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i))))
        maybeIncrementEpochAndValidateTierState(currentState, false, endOffset, numSegments - (i + 1))
        currentState = new FileTierPartitionState(dir, tp, true)
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset,
      offset + 10, 100, 1, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
    endOffset = offset + 10

    // 1. deleteInitiate the one and only segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId)))
    // 2. uploadInitiate an new segment
    objectId = UUID.randomUUID
    offset += 5
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset,
      offset + 10, 100, 1, false, false, false)))

    // New leader
    epoch = 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 1))
    // Verify that both deleteInitiate and uploadInitiate are fenced as they are from previous epoch and endOffset,
    // totalSize is tracked correctly.
    assertEquals(2, state.fencedSegments.size)
    assertEquals("FileTierPartitionState endOffset runtime value", endOffset, state.endOffset())
    assertEquals("FileTierPartitionState totalSize runtime value", 0, state.totalSize())

    // Broker restarts: reopen state and validate again.
    state.close()
    val reopenedState = new FileTierPartitionState(dir, tp, true)
    try {
      assertEquals(reopenedState.toString(), 2, reopenedState.fencedSegments.size)
      assertEquals("FileTierPartitionState endOffset materialized value", endOffset, reopenedState.endOffset())
      assertEquals("FileTierPartitionState totalSize materialized value", 0, reopenedState.totalSize())
    } finally {
      reopenedState.close()
    }
  }

  @Test
  def testIllegalTransitions(): Unit = {
    def assertIllegal(metadata: AbstractTierMetadata): Unit = {
      assertThrows(classOf[IllegalStateException], new ThrowingRunnable {
        override def run(): Unit = state.append(metadata)
      })
    }
    state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0))

    // 1. first transition must always start from UploadInitiate
    assertIllegal(new TierSegmentUploadComplete(tpid, 0, UUID.randomUUID))
    assertIllegal(new TierSegmentDeleteInitiate(tpid, 0, UUID.randomUUID))
    assertIllegal(new TierSegmentDeleteComplete(tpid, 0, UUID.randomUUID))

    // 2. cannot transition to DeleteComplete unless the previous state is DeleteInitiate
    val objectId = UUID.randomUUID
    val deleteComplete = new TierSegmentDeleteComplete(tpid, 0, objectId)

    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false)))
    assertIllegal(deleteComplete)
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId)))
    assertIllegal(deleteComplete)
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, 0, objectId)))
    assertEquals(AppendResult.ACCEPTED, state.append(deleteComplete))
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

  private def testDuplicateAppend(metadata: AbstractTierMetadata, previousTransitions: Seq[AbstractTierMetadata], expected: AppendResult): Unit = {
    assertEquals(metadata.toString, expected, state.append(metadata))

    previousTransitions.foreach { metadata =>
      val result = state.append(metadata)
      assertTrue(Set(AppendResult.FENCED, AppendResult.ACCEPTED)(result))
    }

    val segments = state.segmentOffsets
    val fencedSegments = state.fencedSegments
    val size = state.totalSize

    // append duplicate
    assertEquals(expected, state.append(metadata))

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
    val state = new FileTierPartitionState(baseDir, tp, true)
    assertEquals(TierPartitionStatus.CATCHUP, state.status)
    assertEquals(0, state.segmentOffsets.size)
    assertEquals(0, state.fencedSegments.size)
    assertEquals(-1, state.tierEpoch)

    // appending metadata to reopened file should be permissible
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0)))
    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 100, 100, 100, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId)))

    assertEquals(0, state.tierEpoch)
    assertEquals(1, state.segmentOffsets.size)
    state.close()
  }
}
