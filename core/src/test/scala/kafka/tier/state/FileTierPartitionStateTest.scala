/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.TimeUnit
import java.util.{Optional, TreeSet, UUID}

import kafka.log.{Log, LogConfig}
import kafka.server.LogDirFailureChannel
import kafka.tier.{TierTestUtils, TopicIdPartition}
import kafka.tier.domain.{AbstractTierMetadata, TierPartitionFence, TierSegmentDeleteComplete, TierSegmentDeleteInitiate, TierSegmentUploadComplete, TierSegmentUploadInitiate, TierTopicInitLeader}
import kafka.tier.domain.TierPartitionForceRestore
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.TierPartitionState.RestoreResult
import kafka.tier.tools.DumpTierPartitionState
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.Mockito.{mock, when}
import org.scalatest.Assertions.assertThrows

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.Seq
import scala.concurrent.ExecutionException

class FileTierPartitionStateTest {
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
  def testTopicWithTierDisabledDoesNotHaveTierPartitionStateFile(): Unit = {
    // A topic partition with tiering disabled must not open FileTierPartitionState channel and thereby have a persistent
    // FileTierPartitionState file on disk. When this file is present, we assume that tiering is currently enabled, or was
    // enabled earlier for this topic partition and the partition may have some data on tiered storage that needs to be
    // made available to the MergedLog.
    val dir2 = TestUtils.randomPartitionLogDir(parentDir)
    val tp2 = Log.parseTopicPartitionName(dir2)
    val fp = new FileTierPartitionState(dir2, logDirFailureChannel, tp2, false)
    val flushedFilePath = FileTierPartitionState.flushedFilePath(fp.basePath())
    assertFalse(Files.exists(flushedFilePath))
    // assign topic Id. FileTierPartitionState should still not get created as tiering is disabled
    fp.setTopicId(UUID.randomUUID())
    assertFalse(Files.exists(flushedFilePath))
    // when broker reboots, a new FileTierPartitionState object will be created on the same topic partition directory
    val fp2 = new FileTierPartitionState(dir2, logDirFailureChannel, tp2, false)
    val flushedFilePath2 = FileTierPartitionState.flushedFilePath(fp2.basePath())
    assertFalse(Files.exists(flushedFilePath2))
    // enable tiering. FileTierPartitionState must now get created
    fp.enableTierConfig()
    assertTrue(Files.exists(flushedFilePath))
    fp.close()
    fp2.close()
    dir2.delete()
  }

  @Test
  def readWriteHeaderOnly(): Unit = {
    state.append(new TierTopicInitLeader(tpid, 9, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    assertEquals(9, state.tierEpoch())
    state.close()

    val reopenedState = factory.initState(dir, tp, logConfig, logDirFailureChannel).asInstanceOf[FileTierPartitionState]
    assertFalse(reopenedState.dirty())
    assertEquals(9, reopenedState.tierEpoch())
    reopenedState.close()
  }

  @Test
  def testPreviousOffsetEvent(): Unit = {
    val initLeaderEvent = new TierTopicInitLeader(tpid, 0, UUID.randomUUID, 0)
    assertEquals(AppendResult.ACCEPTED, state.append(initLeaderEvent, new OffsetAndEpoch(2, Optional.of(2))))
    assertEquals(AppendResult.FENCED, state.append(initLeaderEvent, new OffsetAndEpoch(1, Optional.of(2))))
    assertEquals(AppendResult.FENCED, state.append(initLeaderEvent, new OffsetAndEpoch(0, Optional.of(0))))
  }

  @Test
  def testAppendEpochValidation_1(): Unit = {
    val initLeader_1 = new TierTopicInitLeader(tpid, 1, UUID.randomUUID, 0)
    var offsetAndEpoch = new OffsetAndEpoch(0, Optional.of(1))
    assertEquals(AppendResult.ACCEPTED, state.append(initLeader_1, offsetAndEpoch))

    offsetAndEpoch = new OffsetAndEpoch(100, Optional.of(5))
    val initLeader_2 = new TierTopicInitLeader(tpid, 5, UUID.randomUUID, 1)
    assertEquals(AppendResult.ACCEPTED, state.append(initLeader_2, offsetAndEpoch))
    assertEquals(offsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)

    // append with a lower offset but a higher epoch fails
    val initLeader_3 = new TierTopicInitLeader(tpid, 7, UUID.randomUUID, 2)
    val errorOffsetAndEpoch = new OffsetAndEpoch(0, Optional.of(6))
    assertEquals(AppendResult.FAILED, state.append(initLeader_3, errorOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch)
    assertEquals(offsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
  }

  @Test
  def testAppendEpochValidation_2(): Unit = {
    val initLeader_1 = new TierTopicInitLeader(tpid, 1, UUID.randomUUID, 0)
    var offsetAndEpoch = new OffsetAndEpoch(0, Optional.of(1))
    assertEquals(AppendResult.ACCEPTED, state.append(initLeader_1, offsetAndEpoch))

    offsetAndEpoch = new OffsetAndEpoch(100, Optional.of(5))
    val initLeader_2 = new TierTopicInitLeader(tpid, 5, UUID.randomUUID, 1)
    assertEquals(AppendResult.ACCEPTED, state.append(initLeader_2, offsetAndEpoch))
    assertEquals(offsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)

    // append with a higher offset but a lower epoch fails
    val initLeader_3 = new TierTopicInitLeader(tpid, 7, UUID.randomUUID, 2)
    val errorOffsetAndEpoch = new OffsetAndEpoch(200, Optional.of(4))
    assertEquals(AppendResult.FAILED, state.append(initLeader_3, errorOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch)
    assertEquals(offsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
  }

  @Test
  def testOffsetIsIncremented(): Unit = {
    val initLeaderEvent = new TierTopicInitLeader(tpid, 0, UUID.randomUUID, 0)
    val newInitLeaderEvent = new TierTopicInitLeader(tpid, 1, UUID.randomUUID, 0)
    var offsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()

    assertEquals(AppendResult.ACCEPTED, state.append(initLeaderEvent, offsetAndEpoch))
    assertEquals("Last consumed offset mismatch", offsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)

    offsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(newInitLeaderEvent, offsetAndEpoch))
    assertEquals("Last consumed offset mismatch", offsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
  }

  @Test
  def serializeDeserializeTest(): Unit = {
    val numSegments = 200
    var currentSegments = 0L
    val epoch = 0

    val path = state.flushedPath
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    var size = 0
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      val uploadStateOffset = state.lastLocalMaterializedSrcOffsetAndEpoch()
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false, uploadStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId, uploadStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
      size += i
      currentSegments += 1
    }
    state.flush()

    var segments = state.segments
    for (i <- 0 until numSegments) {
      val startOffset = i * 2L
      assertEquals(startOffset, segments.next.baseOffset())
      assertEquals(startOffset, state.metadata(startOffset).get().baseOffset())
    }
    assertFalse(segments.hasNext)
    assertEquals(currentSegments, state.numSegments())
    assertEquals(size, state.totalSize)
    assertEquals(0L, state.startOffset().get())
    assertEquals(currentSegments * 2 - 1 : Long, state.committedEndOffset())

    // append more segments after flush
    for (i <- numSegments until numSegments * 2) {
      val objectId = UUID.randomUUID
      val uploadStateOffset = state.lastLocalMaterializedSrcOffsetAndEpoch()
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, i * 2, i * 2 + 1, 100, i, false, false, false, uploadStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId, uploadStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
      size += i
      currentSegments += 1
    }
    state.flush()

    segments = state.segments
    for (i <- 0L until currentSegments) {
      val startOffset = i * 2L
      assertEquals(startOffset, segments.next.baseOffset())
      assertEquals(startOffset, state.metadata(startOffset).get().baseOffset())
    }
    assertFalse(segments.hasNext)
    assertEquals(currentSegments, state.numSegments())
    assertEquals(size, state.totalSize)
    assertEquals(0L, state.startOffset().get())
    assertEquals(currentSegments * 2 - 1, state.committedEndOffset())

    state.close()
    checkInvalidFileKafkaStorageExceptionOnInit(dir, tp, path)
  }

  @Test
  def segmentGapTest(): Unit = {
    val epoch = 0

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    val objectId1 = UUID.randomUUID
    val uploadStateOffset = state.lastLocalMaterializedSrcOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId1, 0, 50, 100, 0, false, false, false, uploadStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId1, uploadStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    val uploadStateOffset2 = state.lastLocalMaterializedSrcOffsetAndEpoch()
    val objectId2 = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId2, 75, 150, 100, 0, false, false, false, uploadStateOffset2), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId2, uploadStateOffset2), TierTestUtils.nextTierTopicOffsetAndEpoch()))
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    val objectId1 = UUID.randomUUID
    uploadInitateAndComplete(0, objectId1, 0, 50, 0)

    val objectId2 = UUID.randomUUID
    uploadInitateAndComplete(0, objectId2, 25, 150, 0)
    state.flush()

    // verify objectId target offsets
    assertEquals(objectId1, state.metadata(24).get().objectId())
    assertEquals(objectId1, state.metadata(25).get().objectId())
    assertEquals(objectId1, state.metadata(50).get().objectId())
    assertEquals(objectId2, state.metadata(51).get().objectId())
    assertFalse(state.metadata(151).isPresent)

    // delete all segments tiered
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId1, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectId1, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId2, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectId2, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    state.flush()

    // verify the endOffset is tracked as expected
    assertEquals(150L, state.endOffset())
    assertEquals(150L, state.committedEndOffset())

    // upload another object with overlap with the current endOffset
    val objectId3 = UUID.randomUUID
    val uploadStateOffset3 = state.lastLocalMaterializedSrcOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId3, 75, 175, 100, 0, false, false, false, uploadStateOffset3), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId3, uploadStateOffset3), TierTestUtils.nextTierTopicOffsetAndEpoch()))
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

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    var size = 0
    for (i <- 0 until n) {
      uploadInitateAndComplete(0, UUID.randomUUID(), i * 2, i * 2 + 1, i)
      size += i
    }

    state.flush()
    state.append(new TierTopicInitLeader(tpid, epoch + 1, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
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
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    uploadInitateAndComplete(0, UUID.randomUUID(), 0, 100, 100)

    // committedEndOffset is unavailable before first flush
    assertEquals(100L, state.endOffset)
    assertEquals(-1L, state.committedEndOffset)
    assertEquals(1, state.segments.asScala.size)

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
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    uploadInitateAndComplete(0, UUID.randomUUID(), 0, 100, 100)

    // committedEndOffset is unavailable before first flush
    assertEquals(100L, state.endOffset)
    assertEquals(-1L, state.committedEndOffset)
    assertEquals(1, state.segments.asScala.size)

    // committedEndOffset equals endOffset after flush
    state.flush()
    assertEquals(100L, state.endOffset)
    assertEquals(100L, state.committedEndOffset)

    uploadInitateAndComplete(0, UUID.randomUUID(), 100, 200, 100)
    assertEquals(0L, state.startOffset.get)
    assertEquals(100L, state.committedEndOffset)
    assertEquals(200L, state.endOffset)

    state.flush()
    assertEquals(0L, state.startOffset.get)
    assertEquals(200L, state.committedEndOffset())
    val numSegments = state.segments.asScala.size
    state.close()

    validateConsoleDumpedEntries(dir, numSegments)
  }

  @Test
  def testReopenFileAfterVersionChange(): Unit = {
    val numSegments = 200
    val epoch = 0
    val initialVersion = state.version
    val expectedEndOffset = (2*numSegments - 1).toLong

    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    var size = 0
    for (i <- 0 until numSegments) {
      uploadInitateAndComplete(epoch, UUID.randomUUID(), i * 2, i * 2 + 1, 100)
      size += i
    }
    state.flush()

    assertEquals(numSegments, state.numSegments)
    assertEquals(expectedEndOffset, state.endOffset())
    assertEquals(expectedEndOffset, state.committedEndOffset())
    state.close()

    val upgradedVersion = (initialVersion + 1).toByte
    val upgradedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true, upgradedVersion)
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    for (i <- 0 until numSegments) {
      uploadInitateAndComplete(epoch, UUID.randomUUID(), offset, offset + 1, 100)
      offset += 1
    }

    assertEquals(offset, state.endOffset)
    assertEquals(numSegments, state.segments.asScala.size)

    // initiate a new upload
    val inProgressObjectId = UUID.randomUUID
    val inProgressStateOffset = currentStateOffset()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, inProgressObjectId, offset, offset + 1, 100, 100, false, false, false, inProgressStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // upload must not be visible to readers
    assertEquals(offset, state.endOffset)
    assertEquals(numSegments, state.segments.asScala.size)

    // complete upload
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, inProgressObjectId, currentStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(offset + 1, state.endOffset)
    assertEquals(numSegments + 1, state.segments.asScala.size)
  }

  @Test
  def testMetadataReadReturnsValidSegments(): Unit = {
    var epoch = 0
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // upload few segments at epoch=0
    uploadInitateAndComplete(epoch, UUID.randomUUID(), 0, 100, 100)

    // fenced segment
    val fencedObjectId = UUID.randomUUID()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, fencedObjectId, 101, 200, 100, 100, false, false, false, state.lastLocalMaterializedSrcOffsetAndEpoch()),
      TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // append object at the same offset range as the fenced segment
    epoch += 1
    val expectedObjectId =  UUID.randomUUID
    state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    uploadInitateAndComplete(epoch, expectedObjectId, 150, 200, 100)

    assertEquals(2, state.numSegments())
    assertEquals(1, state.fencedSegments().size())

    // delete the fenced segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, fencedObjectId, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))

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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, UUID.randomUUID(), offset, offset + 1, 100, 100, false, false, false, state.lastLocalMaterializedSrcOffsetAndEpoch()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    epoch += 1
    offset += 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, UUID.randomUUID(), offset, offset + 1, 100, 100, false, false, false, state.lastLocalMaterializedSrcOffsetAndEpoch()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    epoch += 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    val initialFenced = state.fencedSegments()

    assertEquals(2, initialFenced.size())
    // close state and reopen to allow scanning to check in progress uploads
    state.close()

    val state2 = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertFalse(state2.dirty())
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    for (i <- 0 until numSegments) {
      uploadInitateAndComplete(epoch, UUID.randomUUID(), offset, offset + 1, 100)
      offset += 1
    }

    // upload few segments at epoch=1
    epoch = 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    for (i <- 0 until numSegments) {
      uploadInitateAndComplete(epoch, UUID.randomUUID(), offset, offset + 1, 100)
      offset += 1
    }

    // attempt to upload at epoch=0 must be fenced
    val fencedObjectId = UUID.randomUUID
    assertEquals(AppendResult.FENCED, state.append(new TierSegmentUploadInitiate(tpid, epoch - 1, fencedObjectId, offset, offset + 1, 100, 100, false, false, false, state.lastLocalMaterializedSrcOffsetAndEpoch()), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // unsuccessful initiate uploads are not tracked as fenced
    assertEquals(0, state.fencedSegments.size)
    assertEquals(numSegments * 2, state.segments.asScala.size)

    // reopen state and validate state remains the same
    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertFalse(reopenedState.dirty())
    try {
      assertEquals(0, reopenedState.fencedSegments.size)
      assertEquals(numSegments * 2, reopenedState.segments.asScala.size)
    } finally {
      reopenedState.close()
    }
  }

  @Test
  def testUploadAtHigherEpochFailed(): Unit = {
    var epoch = 0
    var offset = 0

    // upload a segment at epoch=0
    var nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), nextOffsetAndEpoch)

    val stateOffset = state.lastLocalMaterializedSrcOffsetAndEpoch()
    val objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, 100, false, false, false, stateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId, stateOffset), nextOffsetAndEpoch))
    assertEquals(0, state.fencedSegments.size)
    assertEquals(1, state.segments.asScala.size)
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch)
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)

    // an attempt to upload at epoch=1 without init leader, should fail
    epoch += 1
    offset += 1
    val stateOffsetBeforeFail = state.lastLocalMaterializedSrcOffsetAndEpoch()
    val errorTierTopicOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadInitiate(tpid, epoch, UUID.randomUUID, offset, offset + 1, 100, 100, false, false, false, stateOffsetBeforeFail), errorTierTopicOffsetAndEpoch))
    assertEquals(errorTierTopicOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch)
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)

    // subsequent init leader (at the same epoch as the stray TierSegmentUploadInitiate), also does not go through
    assertEquals(AppendResult.FAILED, state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID, 0), errorTierTopicOffsetAndEpoch));
    assertEquals(TierPartitionStatus.ERROR, state.status())
    assertEquals(0, state.fencedSegments.size)
    assertEquals(1, state.segments.asScala.size)
    assertEquals(errorTierTopicOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch)
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
  }

  @Test
  def testDeleteAtHigherEpochFailed(): Unit = {
    var epoch = 0
    var offset = 0

    // Upload a segment at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    val objectId = UUID.randomUUID
    val stateOffset = state.lastLocalMaterializedSrcOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 1, 100, 100, false, false, false, stateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    val nextTierTopicOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId, stateOffset), nextTierTopicOffsetAndEpoch))
    assertEquals(0, state.fencedSegments.size)
    assertEquals(1, state.segments.asScala.size)
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch)
    assertEquals(nextTierTopicOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)

    // An attempt to delete at epoch=1 without init leader, should fail
    epoch += 1
    offset += 1
    val stateOffsetForFail = state.lastLocalMaterializedSrcOffsetAndEpoch()
    val errorTierTopicOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId, stateOffsetForFail), errorTierTopicOffsetAndEpoch))
    assertEquals(nextTierTopicOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorTierTopicOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch)

    // Subsequent init leader (at the same epoch as the stray TierSegmentDeleteInitiate), also does not go through
    assertEquals(AppendResult.FAILED, state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID, 0), errorTierTopicOffsetAndEpoch));
    assertEquals(nextTierTopicOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
    assertEquals(0, state.fencedSegments.size)
    assertEquals(1, state.segments.asScala.size)
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorTierTopicOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch)
  }

  @Test
  def testOngoingUploadFenced(): Unit = {
    val numSegments = 20
    val epoch = 0
    var offset = 0

    // upload few segments
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    for (i <- 0 until numSegments) {
      uploadInitateAndComplete(epoch, UUID.randomUUID(), offset, offset + 1, 100)
      offset += 1
    }

    val abortedObjectIds = new ListBuffer[UUID]
    val numAbortedSegments = 5

    // upload segments without completing them
    for (_ <- 0 until numAbortedSegments) {
      abortedObjectIds += UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, abortedObjectIds.last, offset, offset + 1, 100, 100, false, false, false, state.lastLocalMaterializedSrcOffsetAndEpoch()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    }

    val ongoingUpload = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, ongoingUpload, offset, offset + 1, 100, 100, false, false, false, state.lastLocalMaterializedSrcOffsetAndEpoch()), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // all but the last in-progress upload must now be fenced
    assertEquals(numAbortedSegments, state.fencedSegments.asScala.size)
    assertEquals(abortedObjectIds.toSet, state.fencedSegments.asScala.map(_.objectId).toSet)
    assertEquals(numSegments, state.numSegments)

    // must have the same state after reopening the file
    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertFalse(reopenedState.dirty())
    try {
      assertEquals(numAbortedSegments, reopenedState.fencedSegments.size)
      assertEquals(abortedObjectIds.toSet, reopenedState.fencedSegments.asScala.map(_.objectId).toSet)
      assertEquals(numSegments, reopenedState.numSegments)

      // complete the ongoing upload
      assertEquals(AppendResult.ACCEPTED, reopenedState.append(new TierSegmentUploadComplete(tpid, epoch, ongoingUpload, state.lastLocalMaterializedSrcOffsetAndEpoch()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
      assertEquals(ongoingUpload, reopenedState.segments.asScala.toList.last.objectId)
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
    state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    for (i <- 0 until numSegments) {
      uploadInitateAndComplete(epoch, objectIds(i), offset, offset + 1, 100)
      offset += 1
    }

    // begin an upload at epoch=0
    val abortedObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, abortedObjectId, offset, offset + 1, 100, 100, false, false, false, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // begin deletion at epoch=0
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(0), currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // leader change; epoch=1
    epoch = 1
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // both segment being uploaded and deleted must now be fenced
    assertEquals(2, state.fencedSegments.size)
    assertEquals(Set(abortedObjectId, objectIds(0)), state.fencedSegments.asScala.map(_.objectId).toSet)

    // attempt to complete upload must be fenced
    assertEquals(AppendResult.FENCED, state.append(new TierSegmentUploadComplete(tpid, epoch - 1, abortedObjectId, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
  }

  @Test
  // Tests what happens when a TierPartitionState is reopened with both fenced and unfenced segments at the same offset
  def testFencedSegmentHandlingOnReopen(): Unit = {
    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, 0, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    // begin an upload at epoch=0
    val abortedObjectId = UUID.randomUUID

    // initiate an upload to be fenced
    val initialUploadStateOffset = state.lastLocalMaterializedSrcOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, abortedObjectId, 0, 1, 100, 100, false, false, false, initialUploadStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    // transition to epoch=1
    state.append(new TierTopicInitLeader(tpid, 1, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    // check segment is fenced
    val fenced = state.fencedSegments().stream().findFirst()
    assertEquals(fenced.get().objectId(), abortedObjectId)
    // try to complete fenced upload, should be fenced
    assertEquals(AppendResult.FENCED, state.append(new TierSegmentUploadComplete(tpid, 0, abortedObjectId, initialUploadStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    val completedObjectId = UUID.randomUUID
    val overlappingStateOffset = currentStateOffset()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 1, completedObjectId, 0, 1, 100, 100, false, false, false, overlappingStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    // delete initiated upload in between initiate and upload of overlapping segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, 1, abortedObjectId, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 1, completedObjectId, overlappingStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // check fenced segment is removed after delete initiate for fenced segment
    val fencedBefore = state.fencedSegments()
    assertEquals(0, fencedBefore.size())
    assertEquals(completedObjectId, state.metadata(0).get().objectId())

    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertFalse(reopenedState.dirty())
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

    state.append(new TierTopicInitLeader(tpid, 0, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    // begin an upload at epoch=0
    val abortedObjectId = UUID.randomUUID
    val abortedStateOffset = currentStateOffset()
    // initiate an upload that will be fenced
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, abortedObjectId, 0, 1, 100, 100, false, false, false, abortedStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    // transition to epoch=1
    state.append(new TierTopicInitLeader(tpid, 1, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    val completedObjectId = UUID.randomUUID
    uploadInitateAndComplete(1, completedObjectId, 0, 1, 100)

    // completed segment should be able to be looked up
    assertEquals(completedObjectId, state.metadata(0).get().objectId())
    // delete the fenced segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, 1, abortedObjectId, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    // completed segment should still be able to be looked up after fenced segment deletion
    assertEquals(completedObjectId, state.metadata(0).get().objectId())
  }

  @Test
  def testDeleteSegments(): Unit = {
    val numSegments = 20
    val epoch = 0
    var offset = 0
    val objectIds = new ListBuffer[UUID]

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      uploadInitateAndComplete(epoch, objectId, offset, offset, i)
      offset += 1
      objectIds += objectId
    }

    val numSegmentsToDelete = 5
    for (i <- 0 until numSegmentsToDelete) {
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i), currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i), currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    }

    val validObjectIds = objectIds.takeRight(numSegments - numSegmentsToDelete)
    val foundObjectIds = state.segments.asScala.map(_.objectId).toList

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
    var size = 0

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      uploadInitateAndComplete(epoch, objectId, offset, offset + 10, i)
      objectIds += objectId
      endOffsets += (offset + 10)
      offset += 5
      size += i
    }

    val numSegmentsToDelete = 5
    for (i <- 0 until numSegmentsToDelete) {
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i), currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i), currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
      size -= i
    }

    val validObjectIds = objectIds.takeRight(numSegments - numSegmentsToDelete)
    val foundObjectIds = state.segments.asScala.map(_.objectId).toList
    assertEquals(validObjectIds.size, state.numSegments)
    assertEquals(validObjectIds, foundObjectIds)

    val validEndOffsets = endOffsets.takeRight(numSegments - numSegmentsToDelete)
    val foundEndOffsets = state.segments.asScala.map(_.endOffset).toList
    assertEquals(validEndOffsets, foundEndOffsets)
    assertEquals(size, state.totalSize)

    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertFalse(reopenedState.dirty())
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      uploadInitateAndComplete(epoch, objectId, offset, offset + 10, 1)
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
        state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
      }
      // Mimic a broker restart verify partition state endOffset and totalSize is same as before restart.
      state.close()
      val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
      assertFalse(reopenedState.dirty())
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
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i), currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
        maybeIncrementEpochAndValidateTierState(currentState, true, endOffset, numSegments - (i + 1))
        currentState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
      }
      // As a follower (does not increment epoch)
      // Transition each segment to DeleteInitiate and then DeleteComplete and at each step verify state before and after
      // reopening the FileTierPartitionState.
      for (i <- numSegments/2 until numSegments) {
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i), currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
        maybeIncrementEpochAndValidateTierState(currentState, false, endOffset, numSegments - (i + 1))
        currentState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
        assertEquals(AppendResult.ACCEPTED, currentState.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i), currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
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
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    uploadInitateAndComplete(epoch, objectId, offset, offset + 10, 1)
    endOffset = offset + 10

    // 1. deleteInitiate the one and only segment
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectId, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    // 2. uploadInitiate an new segment
    objectId = UUID.randomUUID
    offset += 5
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset,
      offset + 10, 100, 1, false, false, false, currentStateOffset()), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // New leader
    epoch = 1
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID(), 1), TierTestUtils.nextTierTopicOffsetAndEpoch())
    // Verify that both deleteInitiate and uploadInitiate are fenced as they are from previous epoch and endOffset,
    // totalSize is tracked correctly.
    assertEquals(2, state.fencedSegments.size)
    assertEquals("FileTierPartitionState endOffset runtime value", endOffset, state.endOffset())
    assertEquals("FileTierPartitionState totalSize runtime value", 0, state.totalSize())

    // Broker restarts: reopen state and validate again.
    state.close()
    val reopenedState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertFalse(reopenedState.dirty())
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
    assertEquals(new OffsetAndEpoch(-1L, Optional.empty()), state.lastLocalMaterializedSrcOffsetAndEpoch)
    assertEquals(new OffsetAndEpoch(-1L, Optional.empty()), state.lastFlushedSrcOffsetAndEpoch)

    // Send materialization request at offset 100.
    val offsetAndEpoch = new OffsetAndEpoch(100L, Optional.of(3))
    state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), offsetAndEpoch)
    assertEquals(offsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)

    // Send previous offset request, simulating possible duplicates during recovery or during transition from catchup.
    state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), new OffsetAndEpoch(98L, Optional.of(3)))
    assertEquals(offsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)

    // Broker restart.
    state.close()
    val restartState = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    assertEquals(offsetAndEpoch, restartState.lastLocalMaterializedSrcOffsetAndEpoch)
    assertEquals(new OffsetAndEpoch(-1L, Optional.empty()), state.lastFlushedSrcOffsetAndEpoch)
  }

  @Test
  def testAllowedTransitionUploadInitiateToDeleteInitiate(): Unit = {
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    val objectId = UUID.randomUUID
    val stateOffset = currentStateOffset()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false, stateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, 0, objectId, stateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
  }

  @Test
  def testIllegalTransitionInitLeaderToUploadComplete(): Unit = {
    val nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), nextOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    val stateOffset = currentStateOffset()
    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadComplete(tpid, 0, UUID.randomUUID, stateOffset), errorOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status())
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())
  }

  @Test
  def testIllegalTransitionInitLeaderToDeleteInitiate(): Unit = {
    val nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), nextOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    val stateOffset = currentStateOffset()
    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentDeleteInitiate(tpid, 0, UUID.randomUUID, stateOffset), errorOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status())
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())
  }

  @Test
  def testIllegalTransitionInitLeaderToDeleteComplete(): Unit = {
    val nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), nextOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())
    val stateOffset = currentStateOffset()

    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentDeleteComplete(tpid, 0, UUID.randomUUID, stateOffset), errorOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status())
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())
  }

  @Test
  def testIllegalTransitionUploadInitiateToDeleteComplete(): Unit = {
    var nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), nextOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    val objectId = UUID.randomUUID
    val stateOffset = currentStateOffset()
    nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false, stateOffset), nextOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentDeleteComplete(tpid, 0, objectId, stateOffset), errorOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ERROR, state.status())
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())
  }

  @Test
  def testIllegalTransitionUploadCompleteDeleteComplete(): Unit = {
    var nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0), nextOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    val objectId = UUID.randomUUID
    val stateOffset = currentStateOffset()
    nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false, stateOffset), nextOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId, stateOffset), nextOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    val stateOffsetBeforeFence = currentStateOffset()
    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentDeleteComplete(tpid, 0, objectId, stateOffsetBeforeFence), errorOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status())
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())
  }

  @Test
  def testStateUpdateFailureFencingEnabled(): Unit = {
    val objectId = UUID.randomUUID
    val nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, objectId, 0), nextOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ONLINE, state.status)
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    // 1. upon first illegal transition, the failure should be fenced
    val stateOffset = currentStateOffset()
    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId, stateOffset), errorOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())

    // 2. fenced failure is not unblocked even if a legal transition is tried
    val stateOffset2 = currentStateOffset()
    val nextErrorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false, stateOffset2), nextErrorOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())
  }

  @Test
  def testStateUpdateFailureFencingFlushMechanism_ViaBadEvent(): Unit = {
    // --- BEFORE FENCING ---
    val objectId1 = UUID.randomUUID
    var nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    val tierEpoch = 0
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, tierEpoch, objectId1, 0), nextOffsetAndEpoch))

    val endOffsetBeforeFencing = 10
    val objectId2 = UUID.randomUUID
    val stateOffsetUpload1 = currentStateOffset()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId2, 0, endOffsetBeforeFencing, 100, 100, false, false, false, stateOffsetUpload1), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    val nextOffsetAndEpochBeforeFencing = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId2, stateOffsetUpload1), nextOffsetAndEpochBeforeFencing))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(nextOffsetAndEpochBeforeFencing, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    state.flush

    val channelFlushedBeforeFencing = FileChannel.open(FileTierPartitionState.flushedFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelFlushedBeforeFencing.size() > 0)
    val headerFlushedBeforeFencing: Optional[Header] = FileTierPartitionState.readHeader(channelFlushedBeforeFencing)
    assertTrue(headerFlushedBeforeFencing.isPresent)
    assertEquals(headerFlushedBeforeFencing.get.status, TierPartitionStatus.ONLINE)
    val serializedStateFlushedBeforeFencing = ByteBuffer.allocate(10000);
    channelFlushedBeforeFencing.read(serializedStateFlushedBeforeFencing)
    channelFlushedBeforeFencing.close

    // Add few events after flush.
    val objectId3 = UUID.randomUUID
    nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    val stateOffsetUpload2 = currentStateOffset()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId3, endOffsetBeforeFencing + 1, endOffsetBeforeFencing + 10, 100, 100, false, false, false, stateOffsetUpload2), nextOffsetAndEpoch))
    nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId3, stateOffsetUpload2), nextOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    // Useful for later assertions in this test
    val channelMutableBeforeFencing = FileChannel.open(FileTierPartitionState.mutableFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelMutableBeforeFencing.size() > 0)
    val stateMutableBeforeFencing = ByteBuffer.allocate(10000);
    channelMutableBeforeFencing.read(stateMutableBeforeFencing, 0)
    channelMutableBeforeFencing.close

    // --- TRIGGER FENCING ---

    // 1. upon first illegal transition, the failure should be fenced
    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    val objectId4 = UUID.randomUUID
    val stateOffsetUpload3 = currentStateOffset()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId4, stateOffsetUpload3), errorOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())

    // 2. fenced failure is not unblocked even if a legal transition is tried
    val stateOffsetUpload4 = currentStateOffset()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId4, endOffsetBeforeFencing + 11, endOffsetBeforeFencing + 20, 100, 100, false, false, false, stateOffsetUpload4), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())

    state.flush()

    // --- AFTER FENCING ---

    // Check that the header of the flushed file contains TierPartitionStatus.ERROR status
    val channelFlushedAfterFencing = FileChannel.open(FileTierPartitionState.flushedFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelFlushedAfterFencing.size() > 0)
    val headerFlushedAfterFencing: Optional[Header] = FileTierPartitionState.readHeader(channelFlushedAfterFencing)
    assertTrue(headerFlushedAfterFencing.isPresent)
    assertEquals(
      new Header(
        tpid.topicId(),
        state.version(),
        tierEpoch,
        TierPartitionStatus.ERROR,
        0,
        10,
        OffsetAndEpoch.EMPTY,
        nextOffsetAndEpochBeforeFencing,
        errorOffsetAndEpoch,
        OffsetAndEpoch.EMPTY),
      headerFlushedAfterFencing.get)

    // Check that the serializedState of the mutable file before fencing matches byte-by-byte with the
    // serializedState of the flushed file after fencing.
    val serializedStateFlushedAfterFencing = ByteBuffer.allocate(10000);
    channelFlushedAfterFencing.read(serializedStateFlushedAfterFencing)
    channelFlushedAfterFencing.close()

    assertEquals(serializedStateFlushedBeforeFencing, serializedStateFlushedAfterFencing)

    // Check that the state of the error file after fencing matches byte-by-byte with the
    // state of the mutable file upon which fencing happened for the first time.
    val channelErrorAfterFencing = FileChannel.open(FileTierPartitionState.errorFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelErrorAfterFencing.size() > 0)
    val stateErrorAfterFencing = ByteBuffer.allocate(10000);
    channelErrorAfterFencing.read(stateErrorAfterFencing)

    assertEquals(stateMutableBeforeFencing, stateErrorAfterFencing)

    // Check that the errorOffsetAndEpoch is deserialized properly.
    state.close
    val newState = factory.initState(dir, tp, logConfig, logDirFailureChannel).asInstanceOf[FileTierPartitionState]
    assertEquals(newState.lastFlushedErrorOffsetAndEpoch(), errorOffsetAndEpoch)
    newState.close
  }

  @Test
  def testStateUpdateFailureFencingFlushMechanism_ViaPartitionFenceEvent(): Unit = {
    // --- BEFORE FENCING ---
    val objectId1 = UUID.randomUUID
    val nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    val tierEpoch = 0
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, tierEpoch, objectId1, 0), nextOffsetAndEpoch))

    val endOffsetBeforeFencing = 10
    val objectId2 = UUID.randomUUID
    val stateOffsetUpload1 = currentStateOffset()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId2, 0, endOffsetBeforeFencing, 100, 100, false, false, false, stateOffsetUpload1), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    val nextOffsetAndEpochBeforeFencing = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId2, stateOffsetUpload1), nextOffsetAndEpochBeforeFencing))
    assertEquals(TierPartitionStatus.ONLINE, state.status())
    assertEquals(nextOffsetAndEpochBeforeFencing, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch())

    // Useful for later assertions in this test
    val channelMutableBeforeFencing = FileChannel.open(FileTierPartitionState.mutableFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelMutableBeforeFencing.size() > 0)
    val serializedStateMutableBeforeFencing = ByteBuffer.allocate(10000);
    channelMutableBeforeFencing.read(serializedStateMutableBeforeFencing, 0)
    channelMutableBeforeFencing.close

    // --- TRIGGER FENCING ---

    // 1. upon first PartitionFence event, the event should cause the state to be fenced
    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    val objectId3 = UUID.randomUUID
    assertEquals(AppendResult.FAILED, state.append(new TierPartitionFence(tpid, objectId3), errorOffsetAndEpoch))
    assertEquals(errorOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())

    val stateOffsetUpload2 = currentStateOffset()
    // 2. fenced failure is not unblocked even if a legal transition is tried
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadInitiate(tpid, 0, UUID.randomUUID, endOffsetBeforeFencing + 1, endOffsetBeforeFencing + 10, 100, 100, false, false, false, stateOffsetUpload2), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(errorOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch())
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch())

    state.flush

    // --- AFTER FENCING ---

    // Check that the header of the flushed file contains TierPartitionStatus.ERROR status
    val channelFlushedAfterFencing = FileChannel.open(FileTierPartitionState.flushedFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelFlushedAfterFencing.size() > 0)
    val headerFlushedAfterFencing: Optional[Header] = FileTierPartitionState.readHeader(channelFlushedAfterFencing)
    assertTrue(headerFlushedAfterFencing.isPresent)
    assertEquals(
      new Header(
        tpid.topicId(),
        state.version(),
        tierEpoch,
        TierPartitionStatus.ERROR,
        0,
        10,
        OffsetAndEpoch.EMPTY,
        errorOffsetAndEpoch,
        errorOffsetAndEpoch,
        OffsetAndEpoch.EMPTY),
      headerFlushedAfterFencing.get)

    // Check that the serializedState of the flushed file before fencing matches byte-by-byte with the
    // serializedState of the flushed file after fencing.
    val serializedStateFlushedAfterFencing = ByteBuffer.allocate(10000);
    channelFlushedAfterFencing.read(serializedStateFlushedAfterFencing)
    channelFlushedAfterFencing.close

    assertEquals(serializedStateMutableBeforeFencing, serializedStateFlushedAfterFencing)

    // Check that the errorOffsetAndEpoch is deserialized properly.
    state.close
    val newState = factory.initState(dir, tp, logConfig, logDirFailureChannel).asInstanceOf[FileTierPartitionState]
    assertEquals(newState.lastFlushedErrorOffsetAndEpoch(), errorOffsetAndEpoch)
    newState.close
  }

  @Test
  def testStateUpdateFailureFencingFlush_DuringAbsentHeader(): Unit = {
    // --- BEFORE FENCING ---
    val objectId = UUID.randomUUID
    val nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, objectId, 0), nextOffsetAndEpoch))
    assertEquals(TierPartitionStatus.ONLINE, state.status)
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
    assertEquals(OffsetAndEpoch.EMPTY, state.lastFlushedErrorOffsetAndEpoch)

    val channelMutableBeforeFencing = FileChannel.open(FileTierPartitionState.mutableFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelMutableBeforeFencing.size() > 0)
    val stateMutableBeforeFencing = ByteBuffer.allocate(10000);
    channelMutableBeforeFencing.read(stateMutableBeforeFencing, 0)
    channelMutableBeforeFencing.close

    state.flush

    // --- TRIGGER FENCING ---

    // 1. upon first illegal transition, the failure should be fenced
    val errorOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    val stateOffsetUpload1 = currentStateOffset()
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId, stateOffsetUpload1), errorOffsetAndEpoch))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch)

    // 2. fenced failure is not unblocked even if a legal transition is tried
    assertEquals(AppendResult.FAILED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false, stateOffsetUpload1), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(nextOffsetAndEpoch, state.lastLocalMaterializedSrcOffsetAndEpoch)
    assertEquals(TierPartitionStatus.ERROR, state.status)
    assertEquals(errorOffsetAndEpoch, state.lastFlushedErrorOffsetAndEpoch)

    // Manually nuke the file (it should get recreated during flush with header TierPartitionStatus.ERROR)
    Files.delete(FileTierPartitionState.flushedFilePath(state.basePath))
    state.flush

    // --- AFTER FENCING ---

    // Check that the header of the flushed file contains TierPartitionStatus.ERROR status
    val channelFlushedAfterFencing = FileChannel.open(FileTierPartitionState.flushedFilePath(state.basePath), StandardOpenOption.READ)
    assertTrue(channelFlushedAfterFencing.size() > 0)
    val headerFlushedAfterFencing: Optional[Header] = FileTierPartitionState.readHeader(channelFlushedAfterFencing)
    assertTrue(headerFlushedAfterFencing.isPresent)
    assertEquals(
      new Header(
        tpid.topicId(),
        state.version(),
        -1,
        TierPartitionStatus.ERROR,
        -1L,
        -1L,
        new OffsetAndEpoch(-1L, Optional.empty()),
        new OffsetAndEpoch(-1L, Optional.empty()),
        new OffsetAndEpoch(-1L, Optional.empty()),
        errorOffsetAndEpoch),
      headerFlushedAfterFencing.get)
  }

  @Test
  def testIdempotencyDeleteAfterComplete(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0)
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false, new OffsetAndEpoch(1, Optional.of(1)))
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1, new OffsetAndEpoch(1L, Optional.of(1)))
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1, new OffsetAndEpoch(2L, Optional.of(1)))
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1, new OffsetAndEpoch(2L, Optional.of(1)))
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(5L, Optional.of(1)))
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2, new OffsetAndEpoch(5L, Optional.of(1)))
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 0, objectId2, new OffsetAndEpoch(5L, Optional.of(1)))
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 0, objectId2, new OffsetAndEpoch(5L, Optional.of(1)))

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
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false, new OffsetAndEpoch(1L, Optional.of(3))) // 1
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1, new OffsetAndEpoch(1L, Optional.of(3))) // 2
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1, new OffsetAndEpoch(3L, Optional.of(3))) // 3
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1, new OffsetAndEpoch(3L, Optional.of(3))) // 4
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(4L, Optional.of(3))) // 6
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2, new OffsetAndEpoch(4L, Optional.of(3))) // 6
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 0, objectId2, new OffsetAndEpoch(6L, Optional.of(3)))
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 0, objectId2, new OffsetAndEpoch(6L, Optional.of(3)))

    testDuplicateAppend(initLeader, Seq.empty, AppendResult.ACCEPTED)

    // try delayed delete order
    val currentTransitions: ListBuffer[AbstractTierMetadata] = ListBuffer()
    for (transition <- Seq(uploadInitiate1, uploadComplete1, uploadInitiate2, uploadComplete2, deleteInitiate1, deleteComplete1, deleteInitiate2, deleteComplete2)) {
      testDuplicateAppend(transition, currentTransitions, AppendResult.ACCEPTED)
      currentTransitions += transition
    }
  }

  // Tests basic state restoration functionality and the fencing of restore messages
  @Test
  def testStateRestoreFunctionality(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0)
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false, new OffsetAndEpoch(1L, Optional.of(3))) // 1
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1, new OffsetAndEpoch(1L, Optional.of(3))) // 2
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1, new OffsetAndEpoch(3L, Optional.of(3))) // 3
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1, new OffsetAndEpoch(3L, Optional.of(3))) // 4
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(4L, Optional.of(3))) // 5
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2, new OffsetAndEpoch(5L, Optional.of(5))) // 6
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 0, objectId2, new OffsetAndEpoch(6L, Optional.of(5))) // 7
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 0, objectId2, new OffsetAndEpoch(6L, Optional.of(5))) // 8

    testDuplicateAppend(initLeader, Seq.empty, AppendResult.ACCEPTED)
    // try delayed delete order
    val currentTransitions: ListBuffer[AbstractTierMetadata] = ListBuffer()
    for (transition <- Seq(uploadInitiate1, uploadComplete1, uploadInitiate2, uploadComplete2, deleteInitiate1, deleteComplete1, deleteInitiate2, deleteComplete2)) {
      testDuplicateAppend(transition, currentTransitions, AppendResult.ACCEPTED)
      currentTransitions += transition
    }
    state.flush()
    val restorableStart = state.startOffset()
    val restorableEnd = state.endOffset()
    val restorableValidity = state.lastLocalMaterializedSrcOffsetAndEpoch();
    state.close()

    val originalBytes = Files.readAllBytes(Paths.get(state.flushedPath()))

    // create a new state and only apply partial metadata to it
    val newDir = TestUtils.randomPartitionLogDir(TestUtils.tempDir())
    val newState = factory.initState(newDir, tp, logConfig, logDirFailureChannel).asInstanceOf[FileTierPartitionState]
    newState.setTopicId(tpid.topicId)
    newState.beginCatchup()
    newState.onCatchUpComplete()

    testDuplicateAppend(newState, initLeader, Seq.empty, AppendResult.ACCEPTED)
    val newTransitions: ListBuffer[AbstractTierMetadata] = ListBuffer()
    // partially apply the same messages
    for (transition <- Seq(uploadInitiate1, uploadComplete1, uploadInitiate2)) {
      testDuplicateAppend(newState, transition, newTransitions, AppendResult.ACCEPTED)
      newTransitions += transition
    }
    newState.flush()

    val fenceMetadata = new TierPartitionFence(tpid, UUID.randomUUID())
    val fenceOffset = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, newState.append(fenceMetadata, fenceOffset))

    val recoveryMetadata = new TierPartitionForceRestore(tpid, UUID.randomUUID(), restorableStart.orElse(-1L), restorableEnd, restorableValidity, "hash")
    // recover the state correctly
    assertEquals(RestoreResult.SUCCEEDED, newState.restoreState(recoveryMetadata, ByteBuffer.wrap(originalBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // recovery for retried metadata should fail if the tier state topic offset is too old even if the validity offset is high enough
    assertEquals(RestoreResult.FAILED, newState.restoreState(new TierPartitionForceRestore(tpid, UUID.randomUUID(), restorableStart.orElse(-1L), restorableEnd, TierTestUtils.nextTierTopicOffsetAndEpoch(), "hash"),
      ByteBuffer.wrap(originalBytes), TierPartitionStatus.ONLINE,
      // stale offset and epoch
      new OffsetAndEpoch(0L, Optional.empty())))

    // a further restoration with a state offset lower than the current restore offset should fail too
    assertEquals(RestoreResult.FAILED, newState.restoreState(new TierPartitionForceRestore(tpid, UUID.randomUUID(), restorableStart.orElse(-1L), restorableEnd, fenceOffset, "hash"),
      ByteBuffer.wrap(originalBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch()))

    assertEquals(restorableEnd, newState.endOffset())

    // try appending one more piece of metadata and make sure it was flushed out properly and reopens correctly
    val objectId3 = UUID.randomUUID
    val upload3stateOffset = newState.lastLocalMaterializedSrcOffsetAndEpoch()
    val uploadInitiate3 = new TierSegmentUploadInitiate(tpid, 0, objectId3, 21, 50, 100, 100, false, false, false, upload3stateOffset)
    assertEquals(AppendResult.ACCEPTED, newState.append(uploadInitiate3, TierTestUtils.nextTierTopicOffsetAndEpoch()))
    val uploadComplete3 = new TierSegmentUploadComplete(tpid, 0, objectId3, upload3stateOffset)
    assertEquals(AppendResult.ACCEPTED, newState.append(uploadComplete3, TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(50, newState.endOffset())
    newState.close()
    val reopenedNewState = factory.initState(newDir, tp, logConfig, logDirFailureChannel).asInstanceOf[FileTierPartitionState]
    assertEquals(50, reopenedNewState.endOffset())
    reopenedNewState.close()
  }

  // Tests the case where a restore occurs and stale metadata for an old state must be fenced
  @Test
  def testStateRestoreFencingStaleMetadata(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0) // 0
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false, new OffsetAndEpoch(0, Optional.of(1))) // 1
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1, new OffsetAndEpoch(0, Optional.of(1))) // 2

    // objects for uploading with old state offset, this should be fenced
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(2, Optional.of(1))) // 5
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2, new OffsetAndEpoch(2, Optional.of(1))) // 6

    for (transition <- Seq(initLeader, uploadInitiate1, uploadComplete1)) {
      val nextEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
      val result = state.append(transition, nextEpoch)
      assertEquals(AppendResult.ACCEPTED, result)
    }
    state.flush()

    assertEquals(0L, state.startOffset().get())
    assertEquals(10, state.endOffset())

    val restorableStart = state.startOffset().orElse(-1L)
    val restorableEnd = state.endOffset()
    val restorableBytes = Files.readAllBytes(Paths.get(state.flushedPath()))

    // append uploads that will be rolled back
    assertEquals(AppendResult.ACCEPTED, state.append(uploadInitiate2, TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(uploadComplete2, TierTestUtils.nextTierTopicOffsetAndEpoch()))

    assertEquals(0L, state.startOffset().get())
    assertEquals(20, state.endOffset())

    assertEquals(AppendResult.FAILED, state.append(new TierPartitionFence(tpid, UUID.randomUUID()), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    val fenceOffset = TierTestUtils.nextTierTopicOffsetAndEpoch()
    val recoveryMetadata = new TierPartitionForceRestore(tpid, UUID.randomUUID(), restorableStart, restorableEnd, fenceOffset, "hash")

    // recover the state,
    state.restoreState(recoveryMetadata, ByteBuffer.wrap(restorableBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch())

    // rolled back
    assertEquals(0L, state.startOffset().get())
    assertEquals(10, state.endOffset())

    // append a buffered write that was appended to the tier state topic after the restore metadata
    assertEquals(AppendResult.RESTORE_FENCED, state.append(uploadInitiate2, TierTestUtils.nextTierTopicOffsetAndEpoch()))

    assertEquals(0L, state.startOffset().get())
    assertEquals(10, state.endOffset())
  }

  // Tests the case where a restore occurs and an upload must occur without a leader election
  @Test
  def testErrorStateRestoreUploadSameSegmentSameEpoch(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0) // 0
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false, new OffsetAndEpoch(1L, Optional.of(1))) // 1
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1, new OffsetAndEpoch(1L, Optional.of(1))) // 2
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(4, Optional.of(1))) // 5

    for (transition <- Seq(initLeader, uploadInitiate1, uploadComplete1, uploadInitiate2)) {
      val result = state.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.ACCEPTED, result)
    }
    state.flush()

    assertEquals(0L, state.startOffset().get())
    assertEquals(10, state.endOffset())
    assertEquals("upload 2 should not be fenced yet", 0, state.fencedSegments().size())

    val restorableStart = state.startOffset().orElse(-1L)
    val restorableEnd = state.endOffset()
    val restorableBytes = Files.readAllBytes(Paths.get(state.flushedPath()))

    state.close()

    val newDir = TestUtils.randomPartitionLogDir(TestUtils.tempDir())
    val newState = factory.initState(newDir, tp, logConfig, logDirFailureChannel).asInstanceOf[FileTierPartitionState]
    newState.setTopicId(tpid.topicId)
    newState.beginCatchup()
    newState.onCatchUpComplete()

    val fenceMetadata = new TierPartitionFence(tpid, UUID.randomUUID())
    val fenceOffset = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, newState.append(fenceMetadata, fenceOffset))

    val recoveryMetadata = new TierPartitionForceRestore(tpid, UUID.randomUUID(), restorableStart, restorableEnd, fenceOffset, "hash")
    // recover the state, including unfinished upload
    assertEquals(RestoreResult.SUCCEEDED, newState.restoreState(recoveryMetadata, ByteBuffer.wrap(restorableBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch())) // offset 7

    assertEquals(0L, newState.startOffset().get())
    assertEquals(10, newState.endOffset())
    assertEquals(TierTestUtils.currentOffsetAndEpoch(), newState.restoreOffsetAndEpoch())

    // upload the same segment as upload 3, but with new object ID signifying a recovery
    val objectId3 = UUID.randomUUID
    val uploadInitiate2v2 = new TierSegmentUploadInitiate(tpid, 0, objectId3, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(7, Optional.of(1))) // 5
    val uploadComplete2v2 = new TierSegmentUploadComplete(tpid, 0, objectId3, new OffsetAndEpoch(7, Optional.of(1))) // 6

    // newly written objects should be accepted due to higher source offset and epoch and higher stateOffsetAndEpoch
    for (transition <- Seq(uploadInitiate2v2, uploadComplete2v2)) {
      val result = newState.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.ACCEPTED, result)
    }

    assertEquals("object 2 should be fenced after recovery", 1, newState.fencedSegments().size())
    assertEquals("aborted upload before recovery should have been fenced", objectId2, newState.fencedSegments().iterator().next().objectId())
    assertEquals(0, newState.tierEpoch())

    // back to being in sync
    assertEquals(Optional.of(0: Long), newState.startOffset())
    assertEquals(20, newState.endOffset())
    newState.flush()
    newState.close()
  }

  // Tests the case where a restore occurs and an upload completes for the prior state
  @Test
  def testFenceContinuedUploadAfterRestore(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0) // 0
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false, new OffsetAndEpoch(1L, Optional.of(1))) // 1
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1, new OffsetAndEpoch(1L, Optional.of(1))) // 2
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(4, Optional.of(1))) // 3

    for (transition <- Seq(initLeader, uploadInitiate1, uploadComplete1, uploadInitiate2)) {
      val result = state.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.ACCEPTED, result)
    }
    state.flush()

    assertEquals(0L, state.startOffset().get())
    assertEquals(10, state.endOffset())
    assertEquals("upload 2 should not be fenced yet", 0, state.fencedSegments().size())
    val restorableStart = state.startOffset().orElse(-1L)
    val restorableEnd = state.endOffset()
    val restorableBytes = Files.readAllBytes(Paths.get(state.flushedPath()))
    state.close()

    // create a new state and apply all metadata to it
    val newDir = TestUtils.randomPartitionLogDir(TestUtils.tempDir())
    val newState = factory.initState(newDir, tp, logConfig, logDirFailureChannel).asInstanceOf[FileTierPartitionState]
    newState.setTopicId(tpid.topicId)
    newState.beginCatchup()
    newState.onCatchUpComplete()

    val fenceMetadata = new TierPartitionFence(tpid, UUID.randomUUID())
    val fenceOffset = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, newState.append(fenceMetadata, fenceOffset))

    val recoveryMetadata = new TierPartitionForceRestore(tpid, UUID.randomUUID(), restorableStart, restorableEnd, fenceOffset, "hash")
    // recover the state, including unfinished upload
    newState.restoreState(recoveryMetadata, ByteBuffer.wrap(restorableBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch()) // offset 4
    assertEquals(0L, newState.startOffset().get())
    assertEquals(10, newState.endOffset())
    assertEquals(TierTestUtils.currentOffsetAndEpoch(), newState.restoreOffsetAndEpoch())

    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2, new OffsetAndEpoch(3, Optional.of(1))) // 5

    // should be fenced due to restore epoch and offset
    for (transition <- Seq(uploadComplete2)) {
      val result = newState.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.RESTORE_FENCED, result)
    }

    // upload the same segment as upload 3, but with new object ID signifying a recovery
    val objectId3 = UUID.randomUUID
    val uploadInitiate2v2 = new TierSegmentUploadInitiate(tpid, 0, objectId3, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(7, Optional.of(1))) // 5
    val uploadComplete2v2 = new TierSegmentUploadComplete(tpid, 0, objectId3, new OffsetAndEpoch(7, Optional.of(1))) // 6

    // newly written objects should be accepted due to higher source offset and epoch and higher stateOffsetAndEpoch
    for (transition <- Seq(uploadInitiate2v2, uploadComplete2v2)) {
      val result = newState.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.ACCEPTED, result)
    }

    assertEquals("object 2 should be fenced after new upload", 1, newState.fencedSegments().size())
    assertEquals("aborted upload before recovery should have been fenced", objectId2, newState.fencedSegments().iterator().next().objectId())
    assertEquals(0, newState.tierEpoch())

    // back to being in sync
    assertEquals(Optional.of(0: Long), newState.startOffset())
    assertEquals(20, newState.endOffset())
    newState.flush()
    newState.close()
  }

  // Tests a tier state restore that causes truncation/rollback of some metadata.
  // The test then reapplies the same metadata with higher source offset and epoch
  @Test
  def testStateRestoreCausingTruncateAndReapply(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0) // 0
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false, new OffsetAndEpoch(1L, Optional.of(1))) // 1
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1, new OffsetAndEpoch(1L, Optional.of(1))) // 2
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1, new OffsetAndEpoch(3L, Optional.of(1))) // 3
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1, new OffsetAndEpoch(3L, Optional.of(1))) // 4
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(4, Optional.of(1))) // 5
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2, new OffsetAndEpoch(4, Optional.of(1))) // 6
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 0, objectId2, new OffsetAndEpoch(6, Optional.of(1))) // 7
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 0, objectId2, new OffsetAndEpoch(6, Optional.of(1))) // 8

    for (transition <- Seq(initLeader, uploadInitiate1, uploadComplete1, uploadInitiate2)) {
      val result = state.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.ACCEPTED, result)
    }
    state.flush()

    assertEquals(0L, state.startOffset().get())
    assertEquals(10, state.endOffset())

    val restorableStart = state.startOffset().orElse(-1L)
    val restorableEnd = state.endOffset()
    val restorableBytes = Files.readAllBytes(Paths.get(state.flushedPath()))

    state.close()

    // create a new state and apply all metadata to it
    val newDir = TestUtils.randomPartitionLogDir(TestUtils.tempDir())
    val newState = factory.initState(newDir, tp, logConfig, logDirFailureChannel).asInstanceOf[FileTierPartitionState]
    newState.setTopicId(tpid.topicId)
    newState.beginCatchup()
    newState.onCatchUpComplete()

    for (transition <- Seq(initLeader, uploadInitiate1, uploadComplete1, uploadInitiate2, uploadComplete2, deleteInitiate1, deleteComplete1, deleteInitiate2, deleteComplete2)) {
      val result = newState.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.ACCEPTED, result)
    }
    newState.flush()
    assertFalse(newState.startOffset().isPresent)
    assertEquals(20, newState.endOffset())

    val fenceMetadata = new TierPartitionFence(tpid, UUID.randomUUID())
    val fenceOffset = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, newState.append(fenceMetadata, fenceOffset))

    val recoveryMetadata = new TierPartitionForceRestore(tpid, UUID.randomUUID(), restorableStart, restorableEnd, fenceOffset, "hash")
    // recover the state
    newState.restoreState(recoveryMetadata, ByteBuffer.wrap(restorableBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch())
    assertEquals(0L, newState.startOffset().get())
    assertEquals(10, newState.endOffset())
    assertEquals(TierTestUtils.currentOffsetAndEpoch(), newState.restoreOffsetAndEpoch())

    // should be fenced due to restore epoch and offset
    for (transition <- Seq(uploadComplete2, deleteInitiate1, deleteComplete1, deleteInitiate2, deleteComplete2)) {
      val result = newState.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.RESTORE_FENCED, result)
    }

    // newly staged uploads and deletes after recovery
    val objectId3 = UUID.randomUUID
    val afterRestoreStateOffset = newState.lastLocalMaterializedSrcOffsetAndEpoch()
    val deleteInitiate1redo = new TierSegmentDeleteInitiate(tpid, 0, objectId1, afterRestoreStateOffset)
    val deleteComplete1redo = new TierSegmentDeleteComplete(tpid, 0, objectId1, afterRestoreStateOffset)
    val uploadInitiate3 = new TierSegmentUploadInitiate(tpid, 0, objectId3, 10, 20, 100, 100, false, false, false,
      new OffsetAndEpoch(afterRestoreStateOffset.offset() + 2, afterRestoreStateOffset.epoch()))
    val uploadComplete3 = new TierSegmentUploadComplete(tpid, 0, objectId3, new OffsetAndEpoch(afterRestoreStateOffset.offset() + 2, afterRestoreStateOffset.epoch()))
    val deleteInitiate3 = new TierSegmentDeleteInitiate(tpid, 0, objectId3, new OffsetAndEpoch(afterRestoreStateOffset.offset() + 4, afterRestoreStateOffset.epoch()))
    val deleteComplete3 = new TierSegmentDeleteComplete(tpid, 0, objectId3, new OffsetAndEpoch(afterRestoreStateOffset.offset() + 4, afterRestoreStateOffset.epoch()))

    // should be accepted due to being created after recovery
    for (transition <- Seq(deleteInitiate1redo, deleteComplete1redo, uploadInitiate3, uploadComplete3, deleteInitiate3, deleteComplete3)) {
      val result = newState.append(transition, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertEquals(AppendResult.ACCEPTED, result)
    }

    // back to being in sync
    assertFalse(newState.startOffset().isPresent)
    assertEquals(20, newState.endOffset())
    newState.flush()
    newState.close()
  }

  // tests migration of the header upon recovery of a tier state with an earlier version number
  @Test
  def testStateRestoreMigrateVersion(): Unit = {
    val originalVersion = 5
    // open with an older version number
    val state1 = new FileTierPartitionState(dir, logDirFailureChannel, tp, true, originalVersion.byteValue)
    state1.setTopicId(tpid.topicId)
    state1.beginCatchup()
    state1.onCatchUpComplete()
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0) // 0
    assertEquals(AppendResult.ACCEPTED, state1.append(initLeader, TierTestUtils.nextTierTopicOffsetAndEpoch()))
    val fenceMetadata = new TierPartitionFence(tpid, UUID.randomUUID())
    val fenceOffset = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(AppendResult.FAILED, state1.append(fenceMetadata, fenceOffset))
    state1.flush()
    val restorableStart = state.startOffset().orElse(-1L)
    val restorableEnd = state.endOffset()
    val restorableBytes = Files.readAllBytes(Paths.get(state.flushedPath()))
    state1.close()
    val recoveryMetadata = new TierPartitionForceRestore(tpid, UUID.randomUUID(), restorableStart, restorableEnd, fenceOffset, "hash")
    // test that it gets migrated to the latest version number (no version number supplied)
    val state2 = new FileTierPartitionState(dir, logDirFailureChannel, tp, true)
    // should have migrated to the latest version on open
    assertNotEquals(originalVersion, state2.version())
    // should have retained the latest version after restore
    assertEquals(RestoreResult.SUCCEEDED, state2.restoreState(recoveryMetadata, ByteBuffer.wrap(restorableBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertNotEquals(originalVersion, state2.version())
    state2.close()
  }

  @Test
  def testIdempotencySimultaneousDelete(): Unit = {
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0) // 0
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false, new OffsetAndEpoch(0L, Optional.of(1))) // 1
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1, new OffsetAndEpoch(0L, Optional.of(1))) // 2
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1, new OffsetAndEpoch(2L, Optional.of(1))) // 3
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1, new OffsetAndEpoch(2L, Optional.of(1))) // 4
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 0, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(4L, Optional.of(1))) // 5
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 0, objectId2, new OffsetAndEpoch(4L, Optional.of(1))) // 6
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 0, objectId2, new OffsetAndEpoch(6L, Optional.of(1))) // 7
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 0, objectId2, new OffsetAndEpoch(6L, Optional.of(1))) // 8

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
    val initLeader1 = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0) // 0
    val objectId1 = UUID.randomUUID
    val uploadInitiate1 = new TierSegmentUploadInitiate(tpid, 0, objectId1, 0, 10, 100, 100, false, false, false,  new OffsetAndEpoch(0L, Optional.of(1))) // 1
    val uploadComplete1 = new TierSegmentUploadComplete(tpid, 0, objectId1,  new OffsetAndEpoch(0L, Optional.of(1))) // 2
    val deleteInitiate1 = new TierSegmentDeleteInitiate(tpid, 0, objectId1,  new OffsetAndEpoch(2L, Optional.of(1))) // 3
    val deleteComplete1 = new TierSegmentDeleteComplete(tpid, 0, objectId1,  new OffsetAndEpoch(2L, Optional.of(1))) // 4
    val initLeader2 = new TierTopicInitLeader(tpid, 1, java.util.UUID.randomUUID, 0) // 5
    val fencedUploadId = UUID.randomUUID
    val fencedUploadInitiate = new TierSegmentUploadInitiate(tpid, 0, fencedUploadId, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(5L, Optional.of(1))) // 6
    val objectId2 = UUID.randomUUID
    val uploadInitiate2 = new TierSegmentUploadInitiate(tpid, 1, objectId2, 10, 20, 100, 100, false, false, false, new OffsetAndEpoch(6L, Optional.of(1))) // 7
    val uploadComplete2 = new TierSegmentUploadComplete(tpid, 1, objectId2, new OffsetAndEpoch(6L, Optional.of(1))) // 8
    val deleteInitiate2 = new TierSegmentDeleteInitiate(tpid, 1, objectId2, new OffsetAndEpoch(8L, Optional.of(1))) // 9
    val deleteComplete2 = new TierSegmentDeleteComplete(tpid, 1, objectId2, new OffsetAndEpoch(8L, Optional.of(1))) // 10

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
    val baseOffsets = new TreeSet[Long]()
    val numOffsetsInSegment = 49
    state.append(new TierTopicInitLeader(tpid, epoch, UUID.randomUUID, 0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    // upload few segments: [0-49], [50-99], [100-149], [150-199]
    var baseOffset = 0
    for (_ <- 0 to 3) {
      val endOffset = baseOffset + numOffsetsInSegment
      uploadInitateAndComplete(epoch, UUID.randomUUID(), baseOffset, endOffset, 100)

      baseOffsets.add(baseOffset)
      baseOffset = endOffset + 1
    }

    // initiate upload for segment [200-249]
    val lastObjectId = UUID.randomUUID
    val lastObjectStateOffset = currentStateOffset()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, lastObjectId, baseOffset, baseOffset + 49, 100, 100, false, false, false, lastObjectStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(baseOffsets.last + numOffsetsInSegment, state.endOffset)
    assertEquals(-1, state.committedEndOffset)

    // Verify that materialization listeners are completed with the right segment metadata. In addition, we must flush
    // the state file before completing the listener.
    assertEquals(baseOffsets.floor(49), state.materializeUpto(49.toLong).get(0, TimeUnit.MILLISECONDS).baseOffset)
    assertEquals(baseOffsets.last + numOffsetsInSegment, state.committedEndOffset)

    assertEquals(baseOffsets.floor(50), state.materializeUpto(50.toLong).get(0, TimeUnit.MILLISECONDS).baseOffset)
    assertEquals(baseOffsets.floor(155), state.materializeUpto(155.toLong).get(0, TimeUnit.MILLISECONDS).baseOffset)
    assertEquals(baseOffsets.floor(199), state.materializeUpto(199.toLong).get(0, TimeUnit.MILLISECONDS).baseOffset)

    // Verify that materializationLag returns 0L when listener is completed
    assertEquals(0L, state.materializationLag())

    // Verify that listener is not completed for offsets that has not been materialized yet
    val promise = state.materializeUpto(200.toLong)
    assertFalse(promise.isDone)

    // Verify that materializationLag is calculated correctly when listener is not completed (200-199)
    assertEquals(1L, state.materializationLag())

    // complete upload for segment [200-249]; this should also complete the materialization listener
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, lastObjectId, lastObjectStateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // materialization listener must now be completed and the state file should have been flushed
    assertTrue(promise.isDone)
    assertEquals(200, promise.get.baseOffset)
    assertEquals(lastObjectId, promise.get.objectId)
    assertEquals(200 + numOffsetsInSegment, state.committedEndOffset)

    // Verify that materializationLag returns 0L when listener is completed
    assertEquals(0L, state.materializationLag())

    // must be able to register a new materialization listener
    assertFalse(state.materializeUpto(500.toLong).isDone)

    // Verify that materializationLag is calculated correctly when listener is not completed (500-251)
    assertEquals(251L, state.materializationLag())
  }

  @Test
  def testPreviousMaterializationListenerCancelled(): Unit = {
    val promise_1 = state.materializeUpto(200)
    assertFalse(promise_1.isDone)

    // register another listener; this will cause the first one to be cancelled exceptionally
    val promise_2 = state.materializeUpto(400)
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
      state.materializeUpto(200).get(0, TimeUnit.MILLISECONDS)
    }
  }

  /*
  Check that tier partition state throws exception on enabling tierStorage, if the file is read and is invalid.
  This will cause the tier partition state to throw KafkaStorageException and also trigger crash of broker.
  */
  @Test
  def testEnableTierStorageWithInvalidFileThrowsKafkaStorageException(): Unit = {
    // An invalid FileTierPartitionState file must not get loaded. This test checks the loading of this file in enable tiered
    // storage path.
    val dir2 = TestUtils.randomPartitionLogDir(parentDir)
    val tp2 = Log.parseTopicPartitionName(dir2)
    val topicIdPartition = new TopicIdPartition(tp2.topic(), UUID.randomUUID, tp2.partition())
    val fp = new FileTierPartitionState(dir2, logDirFailureChannel, tp2, false)
    // create a FileTierPartitionState file. write header
    val flushedFilePath = FileTierPartitionState.flushedFilePath(fp.basePath())
    val channel = FileChannel.open(flushedFilePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val header = new Header(topicIdPartition.topicId, fp.version(), -1, TierPartitionStatus.INIT, -1L, -1L, OffsetAndEpoch.EMPTY, OffsetAndEpoch.EMPTY, OffsetAndEpoch.EMPTY, OffsetAndEpoch.EMPTY)
    val sizePrefix = header.payloadBuffer.remaining.toShort
    val sizeBuf = ByteBuffer.allocate(Header.HEADER_LENGTH_LENGTH).order(ByteOrder.LITTLE_ENDIAN)
    sizeBuf.putShort(sizePrefix)
    sizeBuf.flip
    Utils.writeFully(channel, 0, sizeBuf)
    Utils.writeFully(channel, Header.HEADER_LENGTH_LENGTH, header.payloadBuffer)
    // write some garbage to FileTierPartitionState file
    val buf = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN)
    buf.putShort(80)
    buf.putInt(1)
    buf.flip()
    Utils.writeFully(channel, channel.size(), buf)
    channel.close()
    // expect KafkaStorageException when enabling tier storage as the FileTierPartitionState file is corrupt
    assertThrows[KafkaStorageException] {
      fp.enableTierConfig()
    }
    assertEquals(fp.dir().getParent(), logDirFailureChannel.takeNextOfflineLogDir())
    assertTrue(Files.exists(FileTierPartitionState.errorFilePath(fp.basePath)))
    assertFalse(fp.status.isOpenForWrite)
    fp.close()
    dir2.delete()
  }

  private def currentStateOffset(): OffsetAndEpoch = {
    state.lastLocalMaterializedSrcOffsetAndEpoch()
  }

  private def uploadInitateAndComplete(epoch: Int, objectId: UUID, startOffset: Long, endOffset: Long, size: Int): Unit = {
    val stateOffset = state.lastLocalMaterializedSrcOffsetAndEpoch()
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, startOffset, endOffset, 100, size, false, false, false, stateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId, stateOffset), TierTestUtils.nextTierTopicOffsetAndEpoch()))
  }

  private def testDuplicateAppend(metadata: AbstractTierMetadata, previousTransitions: Seq[AbstractTierMetadata], expected: AppendResult): Unit = {
    testDuplicateAppend(state, metadata, previousTransitions, expected)
  }

  private def testDuplicateAppend(newState: TierPartitionState, metadata: AbstractTierMetadata, previousTransitions: Seq[AbstractTierMetadata], expected: AppendResult): Unit = {
    val nextOffsetAndEpoch = TierTestUtils.nextTierTopicOffsetAndEpoch()
    assertEquals(metadata.toString, expected, newState.append(metadata, nextOffsetAndEpoch))

    previousTransitions.foreach { metadata =>
      val result = newState.append(metadata, TierTestUtils.nextTierTopicOffsetAndEpoch())
      assertTrue(Set(AppendResult.FENCED, AppendResult.ACCEPTED)(result))
    }

    val segments = state.segments
    val fencedSegments = state.fencedSegments
    val size = state.totalSize

    // append duplicate
    assertEquals(expected, newState.append(metadata, TierTestUtils.nextTierTopicOffsetAndEpoch()))

    // assert the tier partition state does not change after a duplicate append
    assertEquals(segments.asScala.toList, state.segments.asScala.toList)
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
    Check that tier partition state throws exception on initializing tierState, if the file is read and is invalid.
    This will cause the tier partition state to throw KafkaStorageException and also trigger crash of broker.
    */
  private def checkInvalidFileKafkaStorageExceptionOnInit(baseDir: File, tp: TopicPartition, path: String): Unit = {
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

    val logDirFailureChannelLocal = new LogDirFailureChannel(5)
    assertThrows[KafkaStorageException] {
      new FileTierPartitionState(baseDir, logDirFailureChannelLocal, tp, true)
    }

    assertEquals(baseDir.getParent(), logDirFailureChannelLocal.takeNextOfflineLogDir())
  }

}
