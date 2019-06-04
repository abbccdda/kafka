/*
 Copyright 2018 Confluent Inc.
 */

package kafka.log

import java.util.UUID
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}

import kafka.server.epoch.EpochEntry
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogDirFailureChannel, TierFetchDataInfo, TierState}
import kafka.tier.{TierMetadataManager, TierTimestampAndOffset, TopicIdPartition}
import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.state.FileTierPartitionStateFactory
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.utils.{MockTime, Scheduler, TestUtils}
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.{Time, Utils}
import org.junit.Assert.{assertEquals, fail}
import org.junit.{After, Test}

import scala.collection.JavaConverters._

class MergedLogTest {
  val brokerTopicStats = new BrokerTopicStats
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val mockTime = new MockTime()
  val tierMetadataManager = new TierMetadataManager(new FileTierPartitionStateFactory(),
    Some(new MockInMemoryTierObjectStore(new TierObjectStoreConfig())),
    new LogDirFailureChannel(1),
    true)
  val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
  val messagesPerSegment = 20

  @After
  def tearDown() {
    brokerTopicStats.close()
    Utils.delete(tmpDir)
  }

  @Test
  def testReadFromTieredRegion(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val tierStart = ranges.firstTieredOffset
    val tierEnd = ranges.firstOverlapOffset.get - 1
    val offsetsToRead = List(tierStart, tierStart + 1, tierEnd - 1, tierEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, None, true, false)
      result match {
        case tierResult: TierFetchDataInfo => {
          val segmentBaseOffset = tierPartitionState.segmentOffsets.floor(offset)
          assertEquals(segmentBaseOffset, tierResult.fetchMetadata.segmentBaseOffset)
        }
        case _ => fail(s"Unexpected $result for read at $offset")
      }
    }
    log.close()
  }

  @Test
  def testCannotUploadPastRecoveryPoint(): Unit = {
    val noopScheduler = new Scheduler { // noopScheduler allows us to roll segments without scheduling a background flush
      override def startup(): Unit = ()
      override def shutdown(): Unit = ()
      override def isStarted: Boolean = true
      override def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit): Unit = ()
    }
    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createMergedLog(logConfig, scheduler = noopScheduler)
    val messagesToWrite = 10
    for (_ <- 0 until messagesToWrite) {
      val segmentStr = "foo"
      val messageStr = "bar"
      def createRecords = TestUtils.singletonRecords(("test" + segmentStr + messageStr).getBytes)
      log.appendAsLeader(createRecords, 0)
      log.roll()
    }

    assertEquals("Each message should create a single log segment", log.localLogSegments.size, 11)
    // Set the high watermark to the active segments end offset and flush up to the 4th segment
    log.onHighWatermarkIncremented(log.localLog.activeSegment.readNextOffset - 1)
    log.flush(4) // flushes up to 3, because the logic is flushing up to the provided offset - 1

    assertEquals("Expected tierable segments to include everything up to the segment before the last flushed segment segment",
      Vector(0, 1, 2, 3),
      log.tierableLogSegments.map(ls => ls.readNextOffset - 1).toVector)

    log.flush(8) // flushes up to 7, because the logic is flushing up to the provided offset - 1

    assertEquals("Expected tierable segments to include everything up to the segment before the last flushed segment segment",
      Vector(0, 1, 2, 3, 4, 5, 6, 7),
      log.tierableLogSegments.map(ls => ls.readNextOffset - 1).toVector)
  }

  @Test
  def testCannotUploadPastHighwatermark: Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createMergedLog(logConfig)
    val numSegments = 5

    for (segment <- 0 until numSegments) {
      for (message <- 0 until messagesPerSegment)
        log.appendAsLeader(createRecords(segment, message), leaderEpoch = 0)
      log.roll()
    }

    // Wait until recovery point has advanced so that tierable segments are bounded by the hwm only
    TestUtils.waitUntilTrue(() => log.recoveryPoint == log.logEndOffset, "Timed out waiting for recovery point to advance")

    var expectedTierableSegments = 0
    log.localLogSegments.foreach { segment =>
      log.onHighWatermarkIncremented(segment.baseOffset + 1)
      assertEquals(expectedTierableSegments, log.tierableLogSegments.size)
      expectedTierableSegments += 1
    }
  }

  @Test
  def testReadFromOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val overlapStart = ranges.firstOverlapOffset.get
    val overlapEnd = ranges.lastOverlapOffset.get
    val offsetsToRead = List(overlapStart, overlapStart + 1, overlapEnd - 1, overlapEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, None, true, false)
      result match {
        case localResult: FetchDataInfo => assertEquals(offset, localResult.records.records.iterator.next.offset)
        case _ => fail(s"Unexpected $result")
      }
    }
    log.close()
  }

  @Test
  def testReadAboveOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val localStart = ranges.lastOverlapOffset.get + 1
    val localEnd = ranges.lastLocalOffset - 1
    val offsetsToRead = List(localStart, localStart + 1, localEnd - 1, localEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, None, true, false)
      result match {
        case localResult: FetchDataInfo => assertEquals(offset, localResult.records.records.iterator.next.offset)
        case _ => fail(s"Unexpected $result")
      }
    }
    log.close()
  }

  @Test
  def testIncrementLogStartOffset(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)
    val offsets = List(ranges.firstOverlapOffset.get - 1,
      ranges.firstOverlapOffset.get,
      ranges.firstOverlapOffset.get + 1,
      ranges.lastOverlapOffset.get - 1,
      ranges.lastOverlapOffset.get,
      ranges.lastOverlapOffset.get + 1,
      log.activeSegment.baseOffset - 1,
      log.logEndOffset)
    val epochs =
      for (i <- 0 until offsets.size)
        yield i

    val offsetToEpoch: ConcurrentNavigableMap[Long, Long] = new ConcurrentSkipListMap

    for ((offset, epoch) <- offsets zip epochs) {
      log.leaderEpochCache.get.assign(epoch, offset)
      offsetToEpoch.put(offset, epoch)
    }

    offsets.foreach { offset =>
      log.maybeIncrementLogStartOffset(offset)
      assertEquals(offset, log.logStartOffset)

      // validate epoch cache truncation
      val expectedEpochs = offsetToEpoch.tailMap(offset)
      val epochsInLog = log.leaderEpochCache.get.epochEntries
      assertEquals(expectedEpochs.keySet.asScala.toList, epochsInLog.map(_.startOffset).toList)
      assertEquals(expectedEpochs.values.asScala.toList, epochsInLog.map(_.epoch).toList)
    }
    log.close()
  }

  @Test
  def testRetentionOnHotsetSegments(): Unit = {
    val numTieredSegments = 30
    val numHotsetSegments = 10
    val numUntieredSegments = 1   // this will be the active segment, with no data

    val numHotsetSegmentsToRetain = 2

    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = segmentSize * numHotsetSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments, numUntieredSegments, numHotsetSegments, logConfig)

    // All segments have been tiered, except the active segment. We should thus only retain `numHotsetSegmentsToRetain` segments + the active segment.
    val numDeleted = log.deleteOldSegments()
    assertEquals(numHotsetSegments - numHotsetSegmentsToRetain, numDeleted)
    assertEquals(numHotsetSegmentsToRetain + 1, log.localLogSegments.size)    // "+ 1" to account for the empty active segment

    // attempting deletion again is a NOOP
    assertEquals(0, log.deleteOldSegments())
    log.close()
  }

  @Test
  def testRetentionOnUntieredSegments(): Unit = {
    val numTieredSegments = 30
    val numHotsetSegments = 10
    val numUntieredSegments = 5

    val numHotsetSegmentsToRetain = 2

    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = segmentSize * numHotsetSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments, numUntieredSegments, numHotsetSegments, logConfig)

    // all hotset segments are deleted, and untiered segments are retained
    val numDeleted = log.deleteOldSegments()
    assertEquals(numHotsetSegments, numDeleted)
    assertEquals(numUntieredSegments, log.localLogSegments.size)

    // attempting deletion again is a NOOP
    assertEquals(0, log.deleteOldSegments())
    log.close()
  }

  @Test
  def testSizedBasedRetentionOnSegmentsWithProducerSnapshots(): Unit = {
    val segmentBytes = 1024
    // Setup retention to only keep 2 segments total
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, retentionBytes = segmentBytes * 1, tierEnable = true, tierLocalHotsetBytes = 0)
    val mergedLog = createMergedLog(logConfig)
    val tierPartitionState = tierMetadataManager.tierPartitionState(mergedLog.topicPartition).get
    val leaderEpoch = 0
    val topicIdPartition = new TopicIdPartition(mergedLog.topicPartition.topic(), UUID.randomUUID(), mergedLog.topicPartition.partition())
    // establish tier leadership for topicIdPartition
    tierPartitionState.setTopicIdPartition(topicIdPartition)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      leaderEpoch, java.util.UUID.randomUUID(), 0))
    val pid1 = 1L
    var lastOffset = 0L
    for (i <- 0 to 15) {
      val appendInfo = mergedLog.appendAsLeader(TestUtils.records(Seq(new SimpleRecord(mockTime
        .milliseconds(), new
          Array[Byte](128))),
        producerId = pid1, producerEpoch = 0, sequence = i),
        leaderEpoch = 0)
      lastOffset = appendInfo.lastOffset
    }
    mergedLog.onHighWatermarkIncremented(lastOffset)

    assertEquals("expected 4 log segments", 4, mergedLog.localLogSegments.size)
    assertEquals("expected producer state manager to contain some state", false, mergedLog.producerStateManager.isEmpty)
    assertEquals("expected retention to leave the log unchanged", 0, mergedLog.deleteOldSegments())

    // Remove the two oldest producer state snapshots to simulate them not being present (old behavior).
    for (_ <- 0 to 1) {
      val oldestOffset = mergedLog.producerStateManager.oldestSnapshotOffset.get
      mergedLog.producerStateManager.deleteSnapshotsBefore(oldestOffset + 1)
      val newOldestOffset = mergedLog.producerStateManager.oldestSnapshotOffset.get
      assertEquals("expected the oldest producer state snapshot offset to increase", newOldestOffset > oldestOffset, true)
    }

    // Nothing has been tiered, so nothing should be eligible for deletion
    assertEquals("expected no segments to be deleted due to retention", 0, mergedLog.deleteOldSegments())
    // "tier" the first two segments
    mergedLog.localLogSegments.take(2).foreach {
      segment =>
        val hasProducerState = mergedLog.producerStateManager.snapshotFileForOffset(segment.readNextOffset).isDefined
        val tierObjectMetadata = new TierObjectMetadata(topicIdPartition,
          leaderEpoch,
          segment.baseOffset,
          (segment.readNextOffset - segment.baseOffset - 1).toInt,
          segment.readNextOffset,
          segment.largestTimestamp,
          segment.size,
          true,
          hasProducerState,
          false,
          0.toByte)
        val appendResult = tierPartitionState.append(tierObjectMetadata)
        tierPartitionState.flush()
        assertEquals(AppendResult.ACCEPTED, appendResult)
    }
    // At this point we should have 2 segments which have been tiered. The third to last segment has producer state
    // (which has not been tiered yet), this should be sufficient to prevent deletion even though there are 2 segments
    // eligible for deletion due to breach of size-based retention.
    assertEquals("expected no segments to be deleted due to retention", 0, mergedLog.deleteOldSegments())

    // Tier the third segment
    mergedLog.localLogSegments.toList.lift(2).foreach {
      segment =>
        val hasProducerState = mergedLog.producerStateManager.snapshotFileForOffset(segment.readNextOffset).isDefined
        assertEquals("expected 3rd segment to have a producer state snapshot", true, hasProducerState)
        val tierObjectMetadata = new TierObjectMetadata(topicIdPartition,
          leaderEpoch,
          segment.baseOffset,
          (segment.readNextOffset - segment.baseOffset - 1).toInt,
          segment.readNextOffset,
          segment.largestTimestamp,
          segment.size,
          true,
          hasProducerState,
          false,
          0.toByte)
        val appendResult = tierPartitionState.append(tierObjectMetadata)
        tierPartitionState.flush()
        assertEquals(AppendResult.ACCEPTED, appendResult)
    }

    // Now that the 3rd segment has been tiered (and it does have a producer state snapshot), the segments eligible
    // for deletion should resemble:
    // [ 1 ] - [ 2 ] - [ 3 ]
    //  [ ]     [ ]     [P]
    // where only the 3rd segment has a [P] ProducerStateSnapshot. Because the range of eligible segments contains
    // a producer state snapshot, retention should succeed in deleting segments 1, 2, and 3.

    assertEquals("expected three segments to be deleted due to retention", 3, mergedLog.deleteOldSegments())

  }

  @Test
  def testRestoreStateFromTier(): Unit = {
    val numTieredSegments = 30
    val numHotsetSegments = 10
    val numUntieredSegments = 5

    val numHotsetSegmentsToRetain = 2

    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = segmentSize * numHotsetSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments, numUntieredSegments, numHotsetSegments, logConfig)

    val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get

    val entries = List(EpochEntry(0, 100))

    // leader reports that its start offset is 190
    // therefore we should restore state using a segment which covers offset 190
    // and restore state, and start replicating after that segment
    val leaderOffset = 190
    val metadata = tierPartitionState.metadata(leaderOffset).get()
    log.onRestoreTierState(metadata.endOffset() + 1, TierState(entries))

    // check that local log is trimmed to immediately after tiered metadata
    assertEquals(metadata.endOffset() + 1, log.localLog.localLogStartOffset)
    assertEquals(metadata.endOffset() + 1, log.localLog.logEndOffset)
    // check that leader epoch cache is correct
    assertEquals(List(EpochEntry(0, 100)), log.leaderEpochCache.get.epochEntries)
  }

  @Test
  def testSizeOfLogWithOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val numTieredSegments = 30
    val numLocalSegments = 50
    val numOverlapSegments = 10
    val log = createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlapSegments, logConfig)

    val segmentSize = log.localLogSegments.head.size
    val expectedLogSize = ((numTieredSegments - numOverlapSegments) + (numLocalSegments + numOverlapSegments - 1)) * segmentSize
    assertEquals(expectedLogSize, log.size)
  }

  @Test
  def testTierableSegments(): Unit = {
    val noopScheduler = new Scheduler { // noopScheduler allows us to roll segments without scheduling a background flush
      override def startup(): Unit = ()
      override def shutdown(): Unit = ()
      override def isStarted: Boolean = true
      override def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit): Unit = ()
    }

    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createMergedLog(logConfig, scheduler = noopScheduler)
    val messagesToWrite = 10
    for (_ <- 0 until messagesToWrite) {
      val segmentStr = "foo"
      val messageStr = "bar"
      def createRecords = TestUtils.singletonRecords(("test" + segmentStr + messageStr).getBytes)
      log.appendAsLeader(createRecords, 0)
      log.roll()
    }

    val topicIdPartition = new TopicIdPartition(log.topicPartition.topic(), UUID.randomUUID(), log.topicPartition.partition())
    val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
    val epoch = 0
    val tieredSegments = log.localLogSegments.take(2)

    // append an init message
    tierPartitionState.setTopicIdPartition(topicIdPartition)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      epoch, java.util.UUID.randomUUID(), 0))

    // append metadata for tiered segments
    tieredSegments.foreach { segment =>
      val tierObjectMetadata = new TierObjectMetadata(topicIdPartition,
        epoch,
        segment.baseOffset,
        (segment.readNextOffset - segment.baseOffset - 1).toInt,
        segment.readNextOffset,
        segment.largestTimestamp,
        segment.size,
        true,
        false,
        false,
        0.toByte)
      val appendResult = tierPartitionState.append(tierObjectMetadata)
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }

    // no segments should be tierable yet, as recovery point and highwatermark have not moved
    assertEquals(0, log.tierableLogSegments.size)

    // no segments are tierable after recovery point and highwatermark move to the end of first tiered segment
    log.onHighWatermarkIncremented(tieredSegments.head.readNextOffset - 1)
    log.flush(1)
    assertEquals(0, log.tierableLogSegments.size)

    // all non tiered segments become tierable after recovery point and highwatermark move to the end of the log
    log.onHighWatermarkIncremented(log.logEndOffset)
    log.flush(log.logEndOffset)
    assertEquals(log.localLogSegments.size - tieredSegments.size - 1, log.tierableLogSegments.size)
  }

  @Test
  def testTierableSegmentsOffsetForTimestamp(): Unit = {
    val noopScheduler = new Scheduler { // noopScheduler allows us to roll segments without scheduling a background flush
      override def startup(): Unit = ()
      override def shutdown(): Unit = ()
      override def isStarted: Boolean = true
      override def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit): Unit = ()
    }

    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createMergedLog(logConfig, scheduler = noopScheduler)
    val messagesToWrite = 10
    for (_ <- 0 until messagesToWrite) {
      val segmentStr = "foo"
      val messageStr = "bar"
      def createRecords = TestUtils.singletonRecords(("test" + segmentStr + messageStr).getBytes)
      log.appendAsLeader(createRecords, 0)
      log.roll()
    }

    val topicIdPartition = new TopicIdPartition(log.topicPartition.topic(), UUID.randomUUID(), log.topicPartition.partition())
    val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
    val epoch = 0
    val tieredSegments = log.localLogSegments.take(2)

    // append an init message
    tierPartitionState.setTopicIdPartition(topicIdPartition)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      epoch, java.util.UUID.randomUUID(), 0))

    // append metadata for tiered segments
    tieredSegments.foreach { segment =>
      val tierObjectMetadata = new TierObjectMetadata(topicIdPartition,
        epoch,
        segment.baseOffset,
        (segment.readNextOffset - segment.baseOffset - 1).toInt,
        segment.readNextOffset,
        segment.largestTimestamp,
        segment.size,
        true,
        false,
        false,
        0.toByte)
      val appendResult = tierPartitionState.append(tierObjectMetadata)
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }
    tierPartitionState.flush()

    // no segments should be tierable yet, as recovery point and highwatermark have not moved
    assertEquals(0, log.tierableLogSegments.size)

    // no segments are tierable after recovery point and highwatermark move to the end of first tiered segment
    log.onHighWatermarkIncremented(tieredSegments.head.readNextOffset - 1)
    log.flush(1)
    assertEquals(0, log.tierableLogSegments.size)

    // all non tiered segments become tierable after recovery point and highwatermark move to the end of the log
    log.onHighWatermarkIncremented(log.logEndOffset)
    log.flush(log.logEndOffset)
    assertEquals(log.localLogSegments.size - tieredSegments.size - 1, log.tierableLogSegments.size)


    val metadata = tierPartitionState.metadata(0).get()
    log.deleteOldSegments()
    val firstTimestamp = metadata.maxTimestamp()
    assertEquals(Some(new TierTimestampAndOffset(firstTimestamp, metadata)), log.fetchOffsetByTimestamp(firstTimestamp))
  }


  private def logRanges(log: MergedLog): LogRanges = {
    val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get

    val firstTieredOffset = log.logStartOffset
    val lastTieredOffset = tierPartitionState.metadata(tierPartitionState.segmentOffsets().last()).get.endOffset
    val firstLocalOffset = log.localLogSegments.head.baseOffset
    val lastLocalOffset = log.logEndOffset

    if (firstLocalOffset <= lastTieredOffset)
      LogRanges(firstTieredOffset, lastTieredOffset, firstLocalOffset, lastLocalOffset, Some(firstLocalOffset), Some(lastTieredOffset))
    else
      LogRanges(firstTieredOffset, lastTieredOffset, firstLocalOffset, lastLocalOffset, None, None)
  }

  private def createRecords(segmentIdx: Int, messageIdx: Int): MemoryRecords = {
    val segmentStr = "%06d".format(segmentIdx)
    val messageStr = "%06d".format(messageIdx)
    TestUtils.singletonRecords(("test" + segmentStr + messageStr).getBytes)
  }

  private def segmentSize: Long = createRecords(0, 0).sizeInBytes * messagesPerSegment

  private def createLogWithOverlap(numTieredSegments: Int, numLocalSegments: Int, numOverlap: Int, logConfig: LogConfig): MergedLog = {
    var log = createMergedLog(logConfig)
    var tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
    val topicIdPartition = new TopicIdPartition(log.topicPartition.topic(), UUID.randomUUID(), log.topicPartition.partition())
    tierPartitionState.setTopicIdPartition(topicIdPartition)
    tierPartitionState.onCatchUpComplete()

    val epoch = 0

    // create all segments as local initially
    for (segment <- 0 until (numTieredSegments + numLocalSegments - 1)) {
      for (message <- 0 until messagesPerSegment)
        log.appendAsLeader(createRecords(segment, message), leaderEpoch = 0)
      log.roll(None)
    }
    log.onHighWatermarkIncremented(log.logEndOffset)
    assertEquals(numTieredSegments + numLocalSegments, log.localLogSegments.size)

    // append an init message
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      epoch,
      java.util.UUID.randomUUID(),
      0))

    // initialize metadata for tiered segments
    val segmentsToTier = log.localLogSegments.take(numTieredSegments)
    segmentsToTier.foreach { segment =>
      val tierObjectMetadata = new TierObjectMetadata(topicIdPartition,
        epoch,
        segment.baseOffset,
        (segment.readNextOffset - segment.baseOffset - 1).toInt,
        segment.readNextOffset,
        segment.largestTimestamp,
        segment.size,
        true,
        false,
        false,
        0.toByte)
      val appendResult = tierPartitionState.append(tierObjectMetadata)
      tierPartitionState.flush()
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }

    val localSegmentsToDelete = segmentsToTier.take(numTieredSegments - numOverlap)
    log.localLog.deleteOldSegments(Some(localSegmentsToDelete.last.readNextOffset), _ => true)

    // close the log
    log.close()
    tierMetadataManager.close()

    // reopen
    log = createMergedLog(logConfig)
    log.onHighWatermarkIncremented(log.logEndOffset)
    tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
    tierPartitionState.onCatchUpComplete()

    // assert number of segments
    assertEquals(numLocalSegments + numOverlap, log.localLogSegments.size)
    log.uniqueLogSegments match {
      case (tierLogSegments, localLogSegments) =>
        assertEquals(numTieredSegments - numOverlap, tierLogSegments.size)
        assertEquals(numLocalSegments + numOverlap, localLogSegments.size)
        assertEquals(numTieredSegments, tierPartitionState.segmentOffsets.size)
    }
    log
  }

  private def createMergedLog(config: LogConfig,
                              brokerTopicStats: BrokerTopicStats = brokerTopicStats,
                              logStartOffset: Long = 0L,
                              recoveryPoint: Long = 0L,
                              scheduler: Scheduler = mockTime.scheduler,
                              time: Time = mockTime,
                              maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
                              producerIdExpirationCheckIntervalMs: Int = LogManager.ProducerIdExpirationCheckIntervalMs): MergedLog = {
    MergedLog(dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = recoveryPoint,
      scheduler = scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10),
      tierMetadataManagerOpt = Some(tierMetadataManager))
  }

  private case class LogRanges(val firstTieredOffset: Long,
                               val lastTieredOffset: Long,
                               val firstLocalOffset: Long,
                               val lastLocalOffset: Long,
                               val firstOverlapOffset: Option[Long],
                               val lastOverlapOffset: Option[Long])
}
