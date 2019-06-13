/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{Optional, UUID}

import kafka.log.Log
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
import org.mockito.Mockito.mock

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class TierPartitionStateTest {
  val factory = new FileTierPartitionStateFactory()
  val parentDir = TestUtils.tempDir()
  val dir = TestUtils.randomPartitionLogDir(parentDir)
  val tp = Log.parseTopicPartitionName(dir)
  val tpid = new TopicIdPartition(tp.topic, UUID.randomUUID, tp.partition)
  val state = new FileTierPartitionState(dir, tp, true)

  @Before
  def setup(): Unit = {
    state.setTopicIdPartition(tpid)
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
    state.append(new TierTopicInitLeader(tpid, 9, java.util.UUID.randomUUID(), 0))
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
    assertEquals(currentSegments * 2 - 1 : Long, state.committedEndOffset().get())

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
    assertEquals(currentSegments * 2 - 1, state.committedEndOffset().get())

    state.close()
    checkInvalidFileReset(dir, tp, path)
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

    val reopenedState = factory.initState(dir, tp, true)
    assertEquals(1, reopenedState.tierEpoch())
    assertEquals(size, reopenedState.totalSize())
    reopenedState.close()
  }

  @Test
  def flushAvailabilityTest(): Unit = {
    assertEquals(TierPartitionState.AppendResult.ACCEPTED, state.append(new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID(), 0)))

    var objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 100, 100, 100, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId)))

    // committedEndOffset is unavailable before first flush
    assertEquals(100L, state.endOffset.get)
    assertFalse(state.committedEndOffset.isPresent)
    assertEquals(1, state.segmentOffsets.size)

    // committedEndOffset equals endOffset after flush
    state.flush()
    assertEquals(100L, state.endOffset.get)
    assertEquals(100L, state.committedEndOffset.get)

    objectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, 0, objectId, 100, 200, 100, 100, false, false, false)))
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, 0, objectId)))
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
    state.close()

    val upgradedVersion = (initialVersion + 1).toByte
    val upgradedState = new FileTierPartitionState(dir, tp, true, upgradedVersion)
    assertEquals(upgradedVersion, upgradedState.version)
    assertEquals(numSegments, upgradedState.numSegments)
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

    assertEquals(offset, state.endOffset.get)
    assertEquals(numSegments, state.segmentOffsets.size)

    // initiate a new upload
    val inProgressObjectId = UUID.randomUUID
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, inProgressObjectId, offset, offset + 1, 100, 100, false, false, false)))

    // upload must not be visible to readers
    assertEquals(offset, state.endOffset.get)
    assertEquals(numSegments, state.segmentOffsets.size)

    // complete upload
    assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, inProgressObjectId)))
    assertEquals(offset + 1, state.endOffset.get)
    assertEquals(numSegments + 1, state.segmentOffsets.size)
  }

  @Test
  def testMultipleInitiatesScannedCorrectlyOnReload(): Unit = {
    val numSegments = 20
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
    // setting topic ID partition will cause TierPartitionState to be reopened
    state.setTopicIdPartition(tpid)
    val afterReloadFenced = state.fencedSegments()
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
    val foundObjectIds = TierUtils.tieredSegments(state.segmentOffsets, state, Optional.of(tierObjectStore)).iterator.asScala.map(_.metadata.objectId).toList

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

    // upload few segments at epoch=0
    state.append(new TierTopicInitLeader(tpid, epoch, java.util.UUID.randomUUID, 0))
    for (i <- 0 until numSegments) {
      val objectId = UUID.randomUUID
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadInitiate(tpid, epoch, objectId, offset, offset + 10, 100, i, false, false, false)))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentUploadComplete(tpid, epoch, objectId)))
      objectIds += objectId
      endOffsets += (offset + 10)
      offset += 5
    }

    val numSegmentsToDelete = 5
    for (i <- 0 until numSegmentsToDelete) {
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteInitiate(tpid, epoch, objectIds(i))))
      assertEquals(AppendResult.ACCEPTED, state.append(new TierSegmentDeleteComplete(tpid, epoch, objectIds(i))))
    }

    val validObjectIds = objectIds.takeRight(numSegments - numSegmentsToDelete)
    val foundObjectIds = TierUtils.tieredSegments(state.segmentOffsets, state, Optional.of(tierObjectStore)).iterator.asScala.map(_.metadata.objectId).toList
    assertEquals(validObjectIds.size, state.numSegments)
    assertEquals(validObjectIds, foundObjectIds)

    val validEndOffsets = endOffsets.takeRight(numSegments - numSegmentsToDelete)
    val foundEndOffsets = TierUtils.tieredSegments(state.segmentOffsets, state, Optional.of(tierObjectStore)).iterator.asScala.map(_.endOffset).toList
    assertEquals(validEndOffsets, foundEndOffsets)
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
  def testDuplicateTransitions(): Unit = {
    val objectId = UUID.randomUUID
    val initLeader = new TierTopicInitLeader(tpid, 0, java.util.UUID.randomUUID, 0)
    val uploadInitiate = new TierSegmentUploadInitiate(tpid, 0, objectId, 0, 10, 100, 100, false, false, false)
    val uploadComplete = new TierSegmentUploadComplete(tpid, 0, objectId)
    val deleteInitiate = new TierSegmentDeleteInitiate(tpid, 0, objectId)
    val deleteComplete = new TierSegmentDeleteComplete(tpid, 0, objectId)

    def testDuplicateAppend(metadata: AbstractTierMetadata, previousTransitions: Seq[AbstractTierMetadata]): Unit = {
      assertEquals(AppendResult.ACCEPTED, state.append(metadata))

      // Transition to any of the previous states is illegal
      previousTransitions.foreach { metadata =>
        assertThrows(classOf[IllegalStateException], new ThrowingRunnable {
          override def run(): Unit = state.append(metadata)
        })
      }

      val segments = state.segmentOffsets
      val fencedSegments = state.fencedSegments
      val size = state.totalSize

      // append duplicate
      assertEquals(AppendResult.ACCEPTED, state.append(metadata))

      // assert the tier partition state does not change after a duplicate append
      assertEquals(segments, state.segmentOffsets)
      assertEquals(fencedSegments, state.fencedSegments)
      assertEquals(size, state.totalSize)
    }

    testDuplicateAppend(initLeader, Seq.empty)
    testDuplicateAppend(uploadInitiate, Seq.empty)
    testDuplicateAppend(uploadComplete, Seq(uploadInitiate))
    testDuplicateAppend(deleteInitiate, Seq(uploadInitiate, uploadComplete))
    testDuplicateAppend(deleteComplete, Seq(uploadInitiate, uploadComplete, deleteInitiate))
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
