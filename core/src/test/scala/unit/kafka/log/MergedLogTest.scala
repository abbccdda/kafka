/*
 Copyright 2018 Confluent Inc.
 */

package kafka.log

import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, ScheduledFuture, TimeUnit}
import java.util.{Optional, UUID}

import com.yammer.metrics.core.Gauge
import kafka.api.LeaderAndIsr
import kafka.log.MergedLogTest.LogRanges
import kafka.metrics.KafkaYammerMetrics
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchLogEnd, LogDirFailureChannel, TierFetchDataInfo, TierState}
import kafka.server.epoch.EpochEntry
import kafka.tier.{TierTimestampAndOffset, TopicIdPartition}
import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.state.{FileTierPartitionState, TierPartitionState, TierPartitionStateFactory}
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStore, TierObjectStoreConfig}
import kafka.tier.topic.TierTopicConsumer
import kafka.tier.TierTestUtils
import kafka.tier.domain.TierObjectMetadata.State
import kafka.utils.{MockTask, MockTime, Scheduler, TestUtils, Throttler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.record.ControlRecordType
import org.apache.kafka.common.record.EndTransactionMarker
import org.apache.kafka.common.record.FileRecords.FileTimestampAndOffset
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.utils.CloseableIterator
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{After, Before, Test}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.util.Try

class MergedLogTest {
  val brokerTopicStats = new BrokerTopicStats
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val mockTime = new MockTime()
  val tierTopicConsumer = mock(classOf[TierTopicConsumer])
  val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
  val messagesPerSegment = 20
  val segmentBytes = MergedLogTest.createRecords(0, 0).sizeInBytes * messagesPerSegment
  val topicPartition = Log.parseTopicPartitionName(logDir)
  val topicIdPartition = new TopicIdPartition(topicPartition.topic, UUID.randomUUID, topicPartition.partition)
  val tierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig("cluster", 1))
  val tierLogComponents = TierLogComponents(Some(tierTopicConsumer), Some(tierObjectStore), new TierPartitionStateFactory(true))

  @Before
  def setUp(): Unit = {
    TierTestUtils.initTierTopicOffset()
  }

  @After
  def tearDown(): Unit = {
    brokerTopicStats.close()
    Utils.delete(tmpDir)
  }

  @Test
  def testLogSizeMetrics(): Unit = {
    val numTiered = 30
    val numOverlap = 3
    val numLocal = 15  // includes active segment

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
    var log = createLogWithOverlap(numTiered, numLocal, numOverlap, logConfig)
    log.close()

    // clear metrics and reopen log
    TestUtils.clearYammerMetrics()
    log = createMergedLog(logConfig)

    assertEquals(log.localLog.size, metricValue("Size"))
    assertEquals(tieredLogSegmentsList(log).map(_.size).sum, metricValue("TierSize"))
    assertEquals(log.size, metricValue("TotalSize"))

    assertEquals((numLocal + numOverlap - 1) * segmentBytes + log.activeSegment.size, metricValue("Size"))
    assertEquals((numTiered + numOverlap) * segmentBytes, metricValue("TierSize"))
    assertEquals((numTiered + numOverlap + numLocal - 1) * segmentBytes + log.activeSegment.size, metricValue("TotalSize"))

    log.close()
  }

  @Test
  def testCannotUploadPastRecoveryPoint(): Unit = {
    val noopScheduler = new Scheduler { // noopScheduler allows us to roll segments without scheduling a background flush
      override def startup(): Unit = ()
      override def shutdown(): Unit = ()
      override def isStarted: Boolean = true
      override def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit): ScheduledFuture[_] = {
        MockTask(name, fun, mockTime.milliseconds + delay, period = period, mockTime)
      }
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
    log.updateHighWatermark(log.localLog.activeSegment.readNextOffset - 1)
    log.flush(4) // flushes up to 3, because the logic is flushing up to the provided offset - 1

    assertEquals("Expected tierable segments to include everything up to the segment before the last flushed segment segment",
      Vector(0, 1, 2, 3),
      log.tierableLogSegments.map(ls => ls.readNextOffset - 1).toVector)

    log.flush(8) // flushes up to 7, because the logic is flushing up to the provided offset - 1

    assertEquals("Expected tierable segments to include everything up to the segment before the last flushed segment segment",
      Vector(0, 1, 2, 3, 4, 5, 6, 7),
      log.tierableLogSegments.map(ls => ls.readNextOffset - 1).toVector)

    log.close()
  }

  @Test
  def testCannotUploadPastHighwatermark(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = Int.MaxValue, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createMergedLog(logConfig)
    val numSegments = 5

    for (segment <- 0 until numSegments) {
      for (message <- 0 until messagesPerSegment)
        log.appendAsLeader(MergedLogTest.createRecords(segment, message), leaderEpoch = 0)
      log.roll()
    }

    // Wait until recovery point has advanced so that tierable segments are bounded by the hwm only
    TestUtils.waitUntilTrue(() => log.recoveryPoint == log.logEndOffset, "Timed out waiting for recovery point to advance")

    var expectedTierableSegments = 0
    log.localLogSegments.foreach { segment =>
      log.updateHighWatermark(segment.baseOffset + 1)
      assertEquals(expectedTierableSegments, log.tierableLogSegments.size)
      expectedTierableSegments += 1
    }

    log.close()
  }

  @Test
  def testReadFromTieredRegion(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
    var maxTimestamp = 0
    val log = createLogWithOverlap(30, 50, 10, logConfig, () => { maxTimestamp += 1; maxTimestamp } )
    val tierPartitionState = log.tierPartitionState
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val tierStart = ranges.firstTieredOffset
    val tierEnd = ranges.firstOverlapOffset.get - 1
    val offsetsToRead = List(tierStart, tierStart + 1, tierEnd - 1, tierEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, FetchLogEnd, minOneMessage = true, permitPreferredTierRead = true)
      result match {
        case tierResult: TierFetchDataInfo =>
          val segment = tierPartitionState.metadata(offset)
          assertEquals(segment.get().baseOffset(), tierResult.fetchMetadata.segmentBaseOffset)
        case _ => fail(s"Unexpected $result for read at $offset")
      }
    }

    assertTrue(tierPartitionState.endOffset > log.localLogSegments.head.baseOffset)
    assertEquals("Expected timestamp and offset from local segment not tiered segment",
      Some(new FileTimestampAndOffset(31, 601, Optional.of(0 : Integer))),
      log.fetchOffsetByTimestamp(log.localLogSegments.head.largestTimestamp))

    log.close()
  }

  @Test
  def testReadFromOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val overlapStart = ranges.firstOverlapOffset.get
    val overlapEnd = ranges.lastOverlapOffset.get
    val offsetsToRead = List(overlapStart, overlapStart + 1, overlapEnd - 1, overlapEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, FetchLogEnd, minOneMessage = true, permitPreferredTierRead = true)
      result match {
        case localResult: FetchDataInfo => assertEquals(offset, localResult.records.records.iterator.next.offset)
        case _ => fail(s"Unexpected $result")
      }
    }
    log.close()
  }

  @Test
  def testReadFromOverlapWithPreferTierFetch(): Unit = {
    // set `preferTierFetchMs` such that it falls in the middle of the hotset
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1,
      preferTierFetchMs = 55)

    def segmentMaxTimestampCbk(): Long = {
      val timestamp = mockTime.milliseconds
      mockTime.sleep(1)
      timestamp
    }

    // <---- Tiered Log ----> <---- Hotset ----> <---- Local Log ---->
    //      30 segments          10 segments          50 segments
    // 0                   29 30      35       39 40                 89 90
    //                                ^                                 ^
    //                                preferTierTime                    currentTime
    val log = createLogWithOverlap(30, 50, 10, logConfig,
      segmentMaxTimestampCbk = segmentMaxTimestampCbk)
    val tierPartitionState = log.tierPartitionState
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val overlapStart = ranges.firstOverlapOffset.get
    val overlapEnd = ranges.lastOverlapOffset.get
    val offsetsToRead = List(overlapStart, overlapStart + 1, overlapEnd - 1, overlapEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, FetchLogEnd, minOneMessage = true, permitPreferredTierRead = true)
      result match {
        case localResult: FetchDataInfo =>
          val baseOffset = localResult.fetchOffsetMetadata.segmentBaseOffset
          val segment = log.localLogSegments(baseOffset, baseOffset + 1).head
          assertTrue(segment.largestTimestamp >= mockTime.milliseconds - 55)
          assertEquals(offset, localResult.records.records.iterator.next.offset)

        case tierResult: TierFetchDataInfo =>
          val segment = tierPartitionState.metadata(offset).get
          assertTrue(segment.maxTimestamp < mockTime.milliseconds - 55)
          assertEquals(segment.baseOffset(), tierResult.fetchMetadata.segmentBaseOffset)
      }
    }
    log.close()
  }

  @Test
  def testReadFromOverlapWithPreferTierNotPermitted(): Unit = {
    // set `preferTierFetchMs` such that it falls in the middle of the hotset
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1,
      preferTierFetchMs = 55)

    def segmentMaxTimestampCbk(): Long = {
      val timestamp = mockTime.milliseconds
      mockTime.sleep(1)
      timestamp
    }

    // <---- Tiered Log ----> <---- Hotset ----> <---- Local Log ---->
    //      30 segments          10 segments          50 segments
    // 0                   29 30      35       39 40                 89 90
    //                                ^                                 ^
    //                                preferTierTime                    currentTime
    val log = createLogWithOverlap(30, 50, 10, logConfig,
      segmentMaxTimestampCbk = segmentMaxTimestampCbk)
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val overlapStart = ranges.firstOverlapOffset.get
    val overlapEnd = ranges.lastOverlapOffset.get
    val offsetsToRead = List(overlapStart, overlapStart + 1, overlapEnd - 1, overlapEnd)

    // Tier fetches must not be performed when `permitPreferredTierRead` is `false`
    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, FetchLogEnd, minOneMessage = true, permitPreferredTierRead = false)
      result match {
        case localResult: FetchDataInfo => assertEquals(offset, localResult.records.records.iterator.next.offset)
        case _ => fail(s"Unexpected $result")
      }
    }
    log.close()
  }

  @Test
  def testReadAboveOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createLogWithOverlap(30, 50, 10, logConfig)
    val ranges = logRanges(log)

    // reading from overlap should return local data
    val localStart = ranges.lastOverlapOffset.get + 1
    val localEnd = ranges.lastLocalOffset - 1
    val offsetsToRead = List(localStart, localStart + 1, localEnd - 1, localEnd)

    offsetsToRead.foreach { offset =>
      val result = log.read(offset, Int.MaxValue, FetchLogEnd, minOneMessage = true, permitPreferredTierRead = true)
      result match {
        case localResult: FetchDataInfo => assertEquals(offset, localResult.records.records.iterator.next.offset)
        case _ => fail(s"Unexpected $result")
      }
    }
    log.close()
  }

  @Test
  def testIncrementLogStartOffset(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
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

  /**
   * Tests for the forced roll on tiered logs. Verifies that call to log.maybeForceRoll will force roll
   * at the appropriate time.
   */
  @Test
  def testForceRollOnTieredSegments(): Unit = {
    def createRecords = TestUtils.singletonRecords(value = "test".getBytes, timestamp = mockTime.milliseconds)
    val recordSize = createRecords.sizeInBytes()

    // create a log.
    val tierLocalHotsetMs = 10 * 60 * 60L
    val numRecordsPerSegment = 10
    val logConfig = LogTest.createLogConfig(
      segmentBytes = numRecordsPerSegment * recordSize,
      tierEnable = true,
      tierSegmentHotsetRollMinBytes = 5 * recordSize,
      segmentMs = Long.MaxValue,
      retentionBytes = Long.MaxValue,
      tierLocalHotsetMs = tierLocalHotsetMs)

    // Create log with just active segment.
    val log = createMergedLog(logConfig)

    // Populate few segments, to ensure the test is setup to focus on verifying roll logic on
    // active segments.
    val numRolledSegments = 5
    for (_ <- 1 to (numRolledSegments * numRecordsPerSegment))
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    log.appendAsLeader(createRecords, 0)
    assertEquals(s"There should be $numRolledSegments rolled segments + 1 active segment", numRolledSegments + 1, log.numberOfSegments)
    log.maybeForceRoll
    assertEquals(s"There should be $numRolledSegments rolled segments + 1 active segment", numRolledSegments + 1, log.numberOfSegments)

    for (_ <- 1 to 2)
      log.appendAsLeader(createRecords, leaderEpoch = 0)

    // Make sure that append has not caused any size based roll.
    assertEquals(s"There should be $numRolledSegments rolled segments + 1 active segment", numRolledSegments + 1, log.numberOfSegments)
    // Before writing tierSegmentHotsetRollMinBytes, we make sure that there are 5 rolled segments + 1 active
    // segment even when tierLocalHotsetMs has elapsed.
    mockTime.sleep(tierLocalHotsetMs + 1)
    log.maybeForceRoll
    assertEquals(s"There should be $numRolledSegments rolled segments + 1 active segment", numRolledSegments + 1, log.numberOfSegments)

    // After writing tierSegmentHotsetRollMinBytes, make sure that maybeForceRoll rolls the active segment
    // as tierLocalHotsetMs has passed since first message in active segment.
    for (_ <- 3 to 5)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    log.maybeForceRoll
    assertEquals(s"There should be $numRolledSegments rolled segments + 2 active segments", 7, log.numberOfSegments)

    // After writing another tierSegmentHotsetRollMinBytes, make sure that roll does not automatically
    // trigger unless maybeForceRoll is called.
    for (_ <- 1 to 5)
      log.appendAsLeader(createRecords, leaderEpoch = 0)
    mockTime.sleep(tierLocalHotsetMs + 1)
    assertEquals(s"There should be $numRolledSegments rolled segments + 2 active segments", numRolledSegments + 2, log.numberOfSegments)

    // Now, trigger the roll and check if it has rolled the active segment.
    log.maybeForceRoll()
    assertEquals(s"There should be $numRolledSegments rolled segments + 4 active segments", numRolledSegments + 3, log.numberOfSegments)
  }

  @Test
  def testHotsetSizeRetentionOnTieredSegments(): Unit = {
    val numTieredSegments = 30
    val numOverlap = 10
    val numUntieredSegments = 1   // this will be the active segment, with no data

    val numHotsetSegmentsToRetain = 2

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetBytes = segmentBytes * numHotsetSegmentsToRetain,
      retentionMs = Long.MaxValue,
      retentionBytes = Long.MaxValue)
    val log = createLogWithOverlap(numTieredSegments, numUntieredSegments, numOverlap, logConfig)
    val initialLogStartOffset = log.logStartOffset

    // All segments have been tiered, except the active segment. We should thus only retain `numHotsetSegmentsToRetain` segments + the active segment.
    val numDeleted = log.deleteOldSegments()
    assertEquals(numOverlap - numHotsetSegmentsToRetain, numDeleted)
    assertEquals(numHotsetSegmentsToRetain + 1, log.localLogSegments.size)    // "+ 1" to account for the empty active segment

    // attempting deletion again is a NOOP
    assertEquals(0, log.deleteOldSegments())
    assertEquals(initialLogStartOffset, log.logStartOffset)  // log start offset must not change due to hotset retention
    log.close()
  }

  @Test
  def testHotsetTimeRetentionOnTieredSegments(): Unit = {
    val numTieredSegments = 30
    val numOverlap = 10
    val numLocalSegments = 3

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetMs = 999,
      retentionMs = Long.MaxValue,
      retentionBytes = Long.MaxValue)
    val log = createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlap, logConfig,
      segmentMaxTimestampCbk = () => mockTime.milliseconds - 1000)
    val initialLogStartOffset = log.logStartOffset

    // All segments in the overlap region are eligible for hotset retention
    MergedLogTest.deleteOldLogSegments(log, numOverlap)
    assertEquals(numLocalSegments, log.localLogSegments.size)    // only the non-overlap portion remains

    // attempting deletion again is a NOOP
    assertEquals(0, log.deleteOldSegments())
    assertEquals(initialLogStartOffset, log.logStartOffset)  // log start offset must not change due to hotset retention
    log.close()
  }

  @Test
  def testHotsetSizeRetentionOnUntieredSegments(): Unit = {
    val numTieredSegments = 30
    val numHotsetSegments = 10
    val numUntieredSegments = 5

    val numHotsetSegmentsToRetain = 2

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetBytes = segmentBytes * numHotsetSegmentsToRetain,
      retentionMs = Long.MaxValue,
      retentionBytes = Long.MaxValue)
    val log = createLogWithOverlap(numTieredSegments, numUntieredSegments, numHotsetSegments, logConfig)
    val initialLogStartOffset = log.logStartOffset

    // all hotset segments are deleted, and untiered segments are retained
    MergedLogTest.deleteOldLogSegments(log, numHotsetSegments)
    assertEquals(numUntieredSegments, log.localLogSegments.size)

    // attempting deletion again is a NOOP
    assertEquals(0, log.deleteOldSegments())
    assertEquals(initialLogStartOffset, log.logStartOffset)  // log start offset must not change due to hotset retention
    log.close()
  }

  @Test
  def testSizeRetentionOnTieredSegments(): Unit = {
    val numTieredSegments = 30
    val numLocalSegments = 10
    val numOverlap = 5
    val numSegmentsToRetain = 5

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetBytes = Long.MaxValue,
      retentionMs = Long.MaxValue,
      retentionBytes = segmentBytes *  numSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlap, logConfig)
    val initialLogStartOffset = log.logStartOffset
    assertEquals(numLocalSegments + numOverlap, log.localLogSegments.size)

    MergedLogTest.deleteOldLogSegments(log, numLocalSegments + numOverlap - (numSegmentsToRetain + 1))
    assertEquals(numSegmentsToRetain + 1, log.localLogSegments.size)
    assertTrue(log.logStartOffset > initialLogStartOffset)
    assertEquals(log.localLogStartOffset, log.logStartOffset)

    log.close()
  }

  @Test
  def testRecoverLogAfterLocalSegmentsLostAndLogStartOffsetLesserThanFirstUntieredOffset(): Unit = {
    val numTieredSegments = 10
    val numLocalSegments = 5
    val numOverlap = 0
    val numSegmentsToRetain = 15

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetBytes = Long.MaxValue,
      retentionMs = Long.MaxValue,
      retentionBytes = segmentBytes * numSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlap, logConfig)

    // Delete local log files when mergedLogStartOffset < firstUntieredOffset (= (1 + last tiered offset))
    assertEquals(numLocalSegments, log.localLogSegments.size)
    val files = log.localLogSegments.map(_.log.file)
    log.close()
    files.foreach(_.delete())

    // open log with an advanced log start offset to ensure the base offset for newly
    // created segments is sufficiently advanced
    val logRecovery = Try(createMergedLog(logConfig, logStartOffset = 20))
    assertTrue("expected log recovery to succeed", logRecovery.isSuccess)
    val recoveredLog = logRecovery.get

    assertEquals("Only 1 local segment expected after recovery", 1, recoveredLog.localLogSegments.size)
    assertTrue("First untiered offset is expected to be greater than merged log start offset ",
      tieredLogSegmentsList(recoveredLog).last.endOffset + 1 > recoveredLog.logStartOffset)
    assertEquals("baseOffset for first local segment after recovery must be mergedLogStartOffset",
      20, recoveredLog.localLogSegments.head.baseOffset)
    assertEquals("endOffset for the mergedLog after deletion and recovery must be equal to the baseOffset of first local segment ",
      recoveredLog.localLogSegments.head.baseOffset, recoveredLog.logEndOffset)

    recoveredLog.close()
  }

  /*
   * Test for the case when local log segment is partially deleted such that logEndOffset is greater than logStartOffset
   * but less than firstUntieredOffset. The recovered log's active segments baseOffset should be equal to the
   * firstUntieredOffset.
   */
  @Test
  def testRecoverLogAfterPartialLocalSegmentsLostAndLogStartOffsetLesserThanFirstUntieredOffset(): Unit = {
    val numTieredSegments = 10
    val numLocalSegments = 5
    val numOverlap = 5
    val numSegmentsToRetain = 15
    val numSegmentsToDrop = 3

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetBytes = Long.MaxValue,
      retentionMs = Long.MaxValue,
      retentionBytes = segmentBytes * numSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlap, logConfig)

    val predictedBaseOffset = log.localLogSegments.takeRight(numSegmentsToDrop+1).head.baseOffset

    // Delete latest 8 segments(All LocalSegment and 3 overlap segments) such that during recovery local segments
    // provides lastOffset to be less than firstUntieredOffset.
    assertEquals(numLocalSegments + numOverlap, log.localLogSegments.size)
    val files = log.localLogSegments.takeRight(3).map(_.log.file)
    log.close()
    files.foreach(_.delete())

    val logRecovery = Try(createMergedLog(logConfig))
    assertTrue("expected log recovery to succeed", logRecovery.isSuccess)
    val recoveredLog = logRecovery.get

    assertTrue("First untiered offset is expected to be greater than merged log start offset ",
      tieredLogSegmentsList(recoveredLog).last.endOffset + 1 > recoveredLog.logStartOffset)
    assertEquals("mergedLogStartOffset should be 0", 0L, recoveredLog.logStartOffset)
    assertEquals("baseOffset for first local segment after recovery should be log start offset", predictedBaseOffset, recoveredLog.activeSegment.baseOffset)
    assertEquals("endOffset for the mergedLog after deletion and recovery must be equal to the baseOffset " +
      "of first local segment ", recoveredLog.localLogSegments.last.readNextOffset, recoveredLog.logEndOffset)

    recoveredLog.close()
  }

  @Test
  def testRecoverLogAfterLocalSegmentsLostAndLogStartOffsetHigherThanFirstUntieredOffset(): Unit = {
    val numTieredSegments = 4
    val numLocalSegments = 1
    val numOverlap = 0
    val numSegmentsToRetain = 5

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetBytes = Long.MaxValue,
      retentionMs = Long.MaxValue,
      retentionBytes = segmentBytes * numSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlap, logConfig)
    assertEquals(numLocalSegments, log.localLogSegments.size)

    // Delete local log files when mergedLogStartOffset > firstUntieredOffset (= (1 + last tiered offset))
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord("a".getBytes),
      new SimpleRecord("b".getBytes),
      new SimpleRecord("c".getBytes),
      new SimpleRecord("d".getBytes)), leaderEpoch = 0)
    log.roll()

    val updatedLogStartOffset = log.localLogSegments.head.baseOffset + 2
    log.updateHighWatermark(updatedLogStartOffset)
    log.maybeIncrementLogStartOffset(updatedLogStartOffset)
    val files = log.localLogSegments.map(_.log.file)
    log.close()
    files.foreach(_.delete())

    val logRecovery = Try(createMergedLog(logConfig, mockTime.scheduler, updatedLogStartOffset))
    assertTrue("Expected log recovery to succeed", logRecovery.isSuccess)
    val recoveredLog = logRecovery.get

    assertEquals("Only 1 local segment expected", 1, recoveredLog.localLogSegments.size)
    assertTrue("First untiered offset is expected to be less than merged log start offset ",
      tieredLogSegmentsList(recoveredLog).last.endOffset + 1 < recoveredLog.logStartOffset)
    assertEquals("localLogSegment.head.baseOffset after recovery must be max(firstUntieredOffset, mergedLogStartOffset)",
      recoveredLog.logStartOffset, recoveredLog.localLogSegments.head.baseOffset)
    assertEquals("endOffset for the mergedLog after deletion and recovery must be equal to the baseOffset of first local segment ",
      recoveredLog.localLogSegments.head.baseOffset, recoveredLog.logEndOffset)

    recoveredLog.close()
  }

  @Test
  def testSizeRetentionOnSegmentsWithProducerSnapshots(): Unit = {
    val segmentBytes = 1024
    // Setup retention to only keep 2 segments total
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 0)
    val mergedLog = createMergedLog(logConfig)
    val tierPartitionState = mergedLog.tierPartitionState
    val leaderEpoch = 0
    // establish tier leadership for topicIdPartition
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      leaderEpoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
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
    mergedLog.updateHighWatermark(lastOffset)

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
        val result = TierTestUtils.uploadWithMetadata(tierPartitionState,
          topicIdPartition,
          leaderEpoch,
          UUID.randomUUID,
          segment.baseOffset,
          segment.readNextOffset - 1,
          segment.largestTimestamp,
          segment.lastModified,
          segment.size,
          false,
          true,
          hasProducerState)
        assertEquals(AppendResult.ACCEPTED, result)
        tierPartitionState.flush()
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
        val result = TierTestUtils.uploadWithMetadata(tierPartitionState,
          topicIdPartition,
          leaderEpoch,
          UUID.randomUUID,
          segment.baseOffset,
          segment.readNextOffset - 1,
          segment.largestTimestamp,
          segment.lastModified,
          segment.size,
          false,
          true,
          hasProducerState)
        assertEquals(AppendResult.ACCEPTED, result)
        tierPartitionState.flush()
    }

    // Now that the 3rd segment has been tiered (and it does have a producer state snapshot), the segments eligible
    // for deletion should resemble:
    // [ 1 ] - [ 2 ] - [ 3 ]
    //  [ ]     [ ]     [P]
    // where only the 3rd segment has a [P] ProducerStateSnapshot. Because the range of eligible segments contains
    // a producer state snapshot, retention should succeed in deleting segments 1, 2, and 3.

    assertEquals("expected three segments to be deleted due to retention", 3, mergedLog.deleteOldSegments())
    mergedLog.close()
  }

  @Test
  def testRetentionDeletesProducerStateSnapshotsAboveLocalLogStartOffset(): Unit = {
    val segmentBytes = 1024
    // Setup infinite retention, but with a hotset of 2 segments (2 * segmentBytes).
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = segmentBytes * 2, retentionBytes = -1, retentionMs = 100)
    val mergedLog = createMergedLog(logConfig)
    val tierPartitionState = mergedLog.tierPartitionState
    val leaderEpoch = 0
    // establish tier leadership for topicIdPartition
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      leaderEpoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    val pid1 = 1L
    var lastOffset = 0L
    for (i <- 0 to 20) {
      val appendInfo = mergedLog.appendAsLeader(TestUtils.records(Seq(new SimpleRecord(mockTime
        .milliseconds(), new
          Array[Byte](128))),
        producerId = pid1, producerEpoch = 0, sequence = i),
        leaderEpoch = 0)
      lastOffset = appendInfo.lastOffset
    }

    mergedLog.updateHighWatermark(lastOffset)
    assertEquals(tieredLogSegmentsList(mergedLog).size, 0)
    assertEquals("expected 5 log segments", 5, mergedLog.localLogSegments.size)
    assertEquals("expected producer state manager to contain some state", false, mergedLog.producerStateManager.isEmpty)
    val snapshotFiles = mergedLog.producerStateManager.listSnapshotFiles
    // 4 snapshot files, one for each segment except the active segment
    assertEquals("expected 4 producer state files", snapshotFiles.size, 4)

    // tier the first 3 segments
    mergedLog.localLogSegments.take(3).foreach {
      segment =>
        val hasProducerState = mergedLog.producerStateManager.snapshotFileForOffset(segment.readNextOffset).isDefined
        val result = TierTestUtils.uploadWithMetadata(tierPartitionState,
          topicIdPartition,
          leaderEpoch,
          UUID.randomUUID,
          segment.baseOffset,
          segment.readNextOffset - 1,
          segment.largestTimestamp,
          segment.lastModified,
          segment.size,
          false,
          true,
          hasProducerState)
        assertEquals(AppendResult.ACCEPTED, result)
        tierPartitionState.flush()
    }

    mergedLog.deleteOldSegments()
    assertTrue("expected to local log start offset to be greater than the merged log start offset",
      mergedLog.localLogStartOffset > mergedLog.logStartOffset
    )
    assertTrue("expected no tiered data to be deleted, so the mergedLog start offset is 0",
      mergedLog.logStartOffset == 0)
    assertTrue("expected only 1 snapshot file for each on-disk segment",
      mergedLog.localLogSegments.size == mergedLog.producerStateManager.listSnapshotFiles.length
    )
    mergedLog.producerStateManager
      .listSnapshotFiles
      .map(_.getName)
      .map(_.split('.')(0).toLong)
      .sorted
      .zip(mergedLog.localLogSegments)
      .foreach {
        case ((snapshotFileNameOffset, segment)) => {
          assertEquals("expected snapshot file to match segment base offset", snapshotFileNameOffset, segment.baseOffset)
        }
      }

    // ensure that even if we delete all data due to retention, we maintain one producer
    // snapshot file for the active segment.
    mockTime.sleep(1000)
    mergedLog.deleteOldSegments()
    assertTrue(mergedLog.localLogSegments.size == 1)
    assertTrue("expected only 1 snapshot file for each on-disk segment",
      1 == mergedLog.producerStateManager.listSnapshotFiles.length
    )
  }

  @Test
  def testProducerStateAdvanceLogStartOffset(): Unit = {
    val segmentBytes = 1024
    // Setup retention to only keep 2 segments total
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 0)
    val mergedLog = createMergedLog(logConfig)
    val tierPartitionState = mergedLog.tierPartitionState
    val leaderEpoch = 0
    // establish tier leadership for topicIdPartition
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      leaderEpoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    val pid = 137L
    val epoch = 5.toShort
    val seq = 0
    val records_1 = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord(mockTime.milliseconds - 1000, "foo".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "bar".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "baz".getBytes))

    mergedLog.appendAsLeader(records_1, leaderEpoch = 0)
    mergedLog.roll()
    mergedLog.updateHighWatermark(mergedLog.logEndOffset)
    assertEquals("expected an active producer", 1, mergedLog.producerStateManager.activeProducers.size)

    val result = TierTestUtils.uploadWithMetadata(tierPartitionState,
      topicIdPartition,
      leaderEpoch,
      UUID.randomUUID,
      0,
      2,
      10000,
      10000,
      10000,
      false,
      true,
      true)
    assertEquals(AppendResult.ACCEPTED, result)
    tierPartitionState.flush()

    assertEquals("no segments should have been deleted from hotset due to LSO",
      0, mergedLog.deleteOldSegments())
    val marker = new EndTransactionMarker(ControlRecordType.ABORT, 0)
    val endTxnRecords = MemoryRecords.withEndTransactionMarker(0, mockTime.milliseconds(), 0, pid, epoch, marker)
    mergedLog.appendAsLeader(endTxnRecords, origin = AppendOrigin.Coordinator, leaderEpoch = leaderEpoch)
    mergedLog.updateHighWatermark(mergedLog.logEndOffset)

    assertEquals("one segment should have been deleted from hotset after LSO advance",
      1, mergedLog.deleteOldSegments())
    
    mergedLog.close()
  }


  @Test
  def testRestoreProducerStateFirstUnstableOffset(): Unit = {
    val segmentBytes = 1024
    // Setup retention to only keep 2 segments total
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 0)
    val mergedLog = createMergedLog(logConfig)
    val tierPartitionState = mergedLog.tierPartitionState
    val leaderEpoch = 0
    // establish tier leadership for topicIdPartition
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      leaderEpoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    val pid = 137L
    val epoch = 5.toShort
    val seq = 0
    val records_1 = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord(mockTime.milliseconds - 1000, "foo".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "bar".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "baz".getBytes))

    mergedLog.appendAsLeader(records_1, leaderEpoch = 0)
    mergedLog.roll()
    mergedLog.updateHighWatermark(mergedLog.logEndOffset)
    assertEquals("expected an active producer", 1, mergedLog.producerStateManager.activeProducers.size)

    val result = TierTestUtils.uploadWithMetadata(tierPartitionState,
      topicIdPartition,
      leaderEpoch,
      UUID.randomUUID,
      0,
      2,
      10000,
      10000,
      10000,
      false,
      true,
      true)
    assertEquals(AppendResult.ACCEPTED, result)
    tierPartitionState.flush()

    mergedLog.producerStateManager.takeSnapshot()
    val producerState = mergedLog.producerStateManager.snapshotFileForOffset(3)
    val producerStateBuf = {
      val buf = ByteBuffer.allocate(1000)
      val inputStream = new FileInputStream(producerState.get)
      try {
        Utils.readFully(inputStream, buf)
      } finally {
        inputStream.close()
      }
      buf.flip()
      buf
    }
    assertEquals(0, mergedLog.deleteOldSegments())

    assertEquals(Some(0L), mergedLog.firstUnstableOffset)
    val marker = new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    val endTxnRecords = MemoryRecords.withEndTransactionMarker(0, mockTime.milliseconds(), 0, pid, epoch, marker)
    mergedLog.appendAsLeader(endTxnRecords, origin = AppendOrigin.Coordinator, leaderEpoch = leaderEpoch)
    mergedLog.updateHighWatermark(mergedLog.logEndOffset)

    assertEquals(None, mergedLog.firstUnstableOffset)
    assertEquals(1, mergedLog.deleteOldSegments())

    // restore producer state to restore ongoing transactions
    mergedLog.truncateAndRestoreTierState(tierPartitionState.endOffset()+1, new TierState(List(), Some(producerStateBuf)))

    assertEquals("expected an active producer after restore", 1, mergedLog.producerStateManager.activeProducers.size)

    assertEquals("first unstable offset should be the beginning of the local log after recovery",
      3L,
      mergedLog.localLog.firstUnstableOffset.get)

    // open a new log without closing the old one to simulate an unclean close
    val reopen = createMergedLog(logConfig)
    assertTrue(reopen.producerStateManager.activeProducers.contains(137))
  }

  @Test
  def testRestoreStateFromTier(): Unit = {
    val numTieredSegments = 30
    val numHotsetSegments = 10
    val numUntieredSegments = 5

    val numHotsetSegmentsToRetain = 2

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = segmentBytes * numHotsetSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments, numUntieredSegments, numHotsetSegments, logConfig)

    val tierPartitionState = log.tierPartitionState

    val entries = List(EpochEntry(0, 100))

    // leader reports that its start offset is 190
    // therefore we should restore state using a segment which covers offset 190
    // and restore state, and start replicating after that segment
    val leaderOffset = 190
    val metadata = tierPartitionState.metadata(leaderOffset).get()
    log.truncateAndRestoreTierState(metadata.endOffset() + 1, TierState(entries))

    // check that local log is trimmed to immediately after tiered metadata
    assertEquals(metadata.endOffset() + 1, log.localLog.localLogStartOffset)
    assertEquals(metadata.endOffset() + 1, log.localLog.logEndOffset)
    // check that leader epoch cache is correct
    assertEquals(List(EpochEntry(0, 100)), log.leaderEpochCache.get.epochEntries)
    log.close()
  }

  /*
   * Verifies the log metadata
   */
  private def verifyLogMetadata(log: AbstractLog, expectedLogStartOffset: Long, expectedLogEndOffset: Long,
                        expectedLocalLogStartOffset: Long, expectedLocalLogEndOffset: Long,
                        trustedTierState: TierState): Unit = {
    assertEquals("Unexpected Log Start Offset", expectedLogStartOffset, log.logStartOffset)
    assertEquals("Unexpected Log End Offset", expectedLogEndOffset, log.logEndOffset)
    assertEquals("Unexpected Local Log Start Offset", expectedLocalLogStartOffset, log.localLogStartOffset)
    assertEquals("Unexpected Local Log End Offset", expectedLocalLogEndOffset, log.localLogEndOffset)
    var epochHistoryMismatch = false
    var i = 0
    while (!epochHistoryMismatch && i < trustedTierState.leaderEpochState.size) {
      if (trustedTierState.leaderEpochState(i).epoch != log.leaderEpochCache.get.epochEntries(i).epoch ||
        trustedTierState.leaderEpochState(i).startOffset != log.leaderEpochCache.get.epochEntries(i).startOffset) {
        epochHistoryMismatch = true
      }
      i += 1
    }
    assertEquals(s"Mismatch between local log epoch cache and tiered epoch history", false, epochHistoryMismatch)
  }

  /*
   * Append the specified number of records to the log as leader. Allows for incrementing the leader
   * epoch at certain intervals.
   */
  private def appendToLogAsLeader(log: AbstractLog, startingLeaderEpoch: Int, numRecords: Int, incrementEpoch: Boolean = false): Unit = {
    var leaderEpoch = startingLeaderEpoch
    for (i <- 0 until numRecords) {
      def createRecords = TestUtils.singletonRecords(("test" + i).getBytes)
      if (incrementEpoch && (i % 10 == 0)) leaderEpoch += 1
      log.appendAsLeader(createRecords, leaderEpoch)
    }
  }

  @Test
  def testPreviouslyCompactedNonEmptySegmentIsTiered(): Unit = {
    val throttler = new Throttler(desiredRatePerSec = Double.MaxValue, checkIntervalMs = Long.MaxValue, time = mockTime)
    val cleaner = makeCleaner(Int.MaxValue, throttler, mockTime)

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = false, tierLocalHotsetBytes = 1)
    val log = createMergedLog(logConfig)
    val keyCount = messagesPerSegment/3

    // append messages to the log until we have messages upto keyCount
    while(log.logEndOffset <= keyCount) {
      log.appendAsLeader(record(log.logEndOffset.toInt, Array.fill(1)( 0: Byte)), leaderEpoch = 0)
    }
    log.updateHighWatermark(log.logEndOffset)

    val firstOffsetWithRecord = log.logEndOffset
    // append tombstones for some messages
    for(key <- 0 to firstOffsetWithRecord.toInt)
      log.appendAsLeader(tombstoneRecord(key), leaderEpoch = 0)
    log.updateHighWatermark(log.logEndOffset)

    // append messages to the log until we have 2 segments
    while(log.numberOfSegments < 2)
      log.appendAsLeader(record(log.logEndOffset.toInt, Array.fill(1)( 0: Byte)), leaderEpoch =0)
    log.roll()

    // run compaction to delete the tombstone record batches
    cleaner.clean(LogToClean(topicPartition, log, 0, log.activeSegment.baseOffset))

    // append messages to the log until we have 4 segments
    while(log.numberOfSegments < 4)
      log.appendAsLeader(record(log.logEndOffset.toInt, Array.fill(1)( 0: Byte)), leaderEpoch =0)
    log.roll()
    log.updateHighWatermark(log.logEndOffset)

    for(offset <- 0 to firstOffsetWithRecord.toInt) {
      // read at offset 0 and validate that `firstOffsetWithRecord` is returned
      val result = log.read(offset, Int.MaxValue, FetchLogEnd, minOneMessage = true, permitPreferredTierRead = false)
      result match {
        case localResult: FetchDataInfo =>
          assertEquals(firstOffsetWithRecord, localResult.records.records().iterator().next().offset())

        case tierResult: TierFetchDataInfo =>
          fail("unexpected fetch from tiered log")
      }
    }

    // enable tiered storage and upload two segments
    log.tierPartitionState.enableTierConfig()
    val epoch = 1
    val tieredSegments = log.localLogSegments.take(2)

    // append an init message
    log.tierPartitionState.setTopicId(topicIdPartition.topicId)
    log.tierPartitionState.onCatchUpComplete()
    log.tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    // append metadata for tiered segments
    tieredSegments.foreach { segment =>
      val appendResult = TierTestUtils.uploadWithMetadata(log.tierPartitionState,
        topicIdPartition,
        epoch,
        UUID.randomUUID,
        segment.baseOffset,
        segment.readNextOffset - 1,
        segment.largestTimestamp,
        segment.lastModified,
        segment.size,
        false,
        true)
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }
    log.updateHighWatermark(log.logEndOffset)

    // delete tiered segments from local log
    log.localLog.deleteOldSegments(Some(tieredSegments.last.readNextOffset), maxNumSegmentsToDelete = Int.MaxValue, retentionType = HotsetRetention)

    // read at offset 0 and validate that `firstOffsetWithRecord` is returned
    val result = log.read(0, Int.MaxValue, FetchLogEnd, minOneMessage = true, permitPreferredTierRead = false)
    result match {
      case localResult: FetchDataInfo =>
        fail("unexpected fetch from local log")

      case tierResult: TierFetchDataInfo =>
        assertEquals(0, tierResult.fetchMetadata.segmentBaseOffset)
    }
  }

  @Test
  def testRecoverLocalLogAtUncleanLeaderWithDivergence(): Unit = {
    // recover local log against a tier state that diverges from the local log. In such cases, recovery
    // logic will discard the local log and start local log at the last tiered offset.
    val numSegmentsToRetain = 5
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetBytes = Long.MaxValue,
      retentionMs = Long.MaxValue,
      retentionBytes = segmentBytes * numSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments = 4, numLocalSegments = 1, numOverlap = 2, logConfig)
    appendToLogAsLeader(log, LeaderAndIsr.initialLeaderEpoch, 50, incrementEpoch = true)
    // create a tier state that diverges from the local log's leader epoch history
    val trustedEpochCache = ListBuffer.empty ++= log.leaderEpochCache.get.epochEntries.toList
    trustedEpochCache(0) = EpochEntry(trustedEpochCache.head.epoch, trustedEpochCache.head.startOffset + 1)
    val trustedTierState = new TierState(trustedEpochCache.toList, None)
    // recover local log with the constructed tier state as reference
    val logStartOffsetBeforeRecovery = log.logStartOffset
    log.recoverLocalLogAfterUncleanLeaderElection(trustedTierState)
    // recovery must have discarded the local log as it diverged against the simulated tier state
    verifyLogMetadata(log, expectedLogStartOffset = logStartOffsetBeforeRecovery,
      expectedLogEndOffset = log.tierPartitionState.endOffset() + 1, expectedLocalLogStartOffset = log.tierPartitionState.endOffset() + 1,
      expectedLocalLogEndOffset = log.tierPartitionState.endOffset() + 1, trustedTierState)
  }

  @Test
  def testRecoverLocalLogAtUncleanLeaderWithoutDivergence(): Unit = {
    // recover local log under following conditions:
    // 1. trusted tier state does not diverge from the local log
    // 2. localLogStartOffset <= lastTieredOffset && localLogEndOffset >= lastTieredOffset
    // 3. localLogStartOffset > logStartOffset (not significant from recovery logic perspective but
    // this condition will be a common scenario)
    val numSegmentsToRetain = 5
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes,
      tierEnable = true,
      tierLocalHotsetBytes = Long.MaxValue,
      retentionMs = Long.MaxValue,
      retentionBytes = segmentBytes * numSegmentsToRetain)
    val log = createLogWithOverlap(numTieredSegments = 4, numLocalSegments = 1, numOverlap = 3, logConfig)
    appendToLogAsLeader(log, LeaderAndIsr.initialLeaderEpoch, 50, incrementEpoch = true)
    // create a tier state that is identical to the local log's leader epoch history
    val trustedEpochCache = ListBuffer.empty ++= log.leaderEpochCache.get.epochEntries
    val trustedTierState = new TierState(trustedEpochCache.toList, None)
    // recover local log with the constructed tier state as reference
    val logStartOffsetBeforeRecovery = log.logStartOffset
    val logEndOffsetBeforeRecovery = log.logEndOffset
    val localLogStartOffsetBeforeRecovery = log.localLogStartOffset
    log.recoverLocalLogAfterUncleanLeaderElection(trustedTierState)
    // recovery logic must keep local log because of conditions 1 and 2 mentioned above
    verifyLogMetadata(log, expectedLogStartOffset = logStartOffsetBeforeRecovery,
      expectedLogEndOffset = logEndOffsetBeforeRecovery, expectedLocalLogStartOffset = localLogStartOffsetBeforeRecovery,
      expectedLocalLogEndOffset = logEndOffsetBeforeRecovery, trustedTierState)
  }

  @Test
  def testRecoverLocalLogAtUncleanLeaderWithLocalLSOGreaterThanLastTieredOffset(): Unit = {
    // Recover local log under following conditions:
    // 1. tiered epoch state does not diverge from the local epoch cache
    // 2. localLogStartOffset > lastTieredOffset + 1
    // Local log must be discarded.
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
    val tierPartitionState = mock(classOf[FileTierPartitionState])
    val tierPartitionStateFactory = mock(classOf[TierPartitionStateFactory])
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val offsetToMetadata = new java.util.TreeMap[Long, TierObjectMetadata]()
    when(tierPartitionState.isTieringEnabled).thenReturn(true)
    when(tierPartitionState.mayContainTieredData()).thenReturn(true)
    when(tierPartitionState.topicIdPartition()).thenReturn(Optional.of(topicIdPartition))
    when(tierPartitionStateFactory.initState(logDir, topicPartition, logConfig, logDirFailureChannel)).thenReturn(tierPartitionState)
    doNothing().when(tierTopicConsumer).register(ArgumentMatchers.any(), ArgumentMatchers.any())
    val tierLogComponents = TierLogComponents(Some(tierTopicConsumer), Some(tierObjectStore), tierPartitionStateFactory)

    val log = MergedLog(logDir,
      logConfig,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      mockTime.scheduler,
      brokerTopicStats,
      mockTime,
      maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel,
      tierLogComponents)

    // Append to log such that we have 2 segments after the one containing offset 101L. We will simulate that all segments
    // till the one containing offset 100L have been tiered. We will further delete local segments till the first un-tiered
    // segment(included).
    // Example: For local segments as (0, 59)(60, 119)(120, 179)(180, 239), we will tier up to segment (60, 119) and delete
    // up to (120, 179), there by introducing a hole in the local log and tiered segments
    appendToLogAsLeader(log, LeaderAndIsr.initialLeaderEpoch, 120)
    while(log.localLogSegments(from = 100L, to = log.logEndOffset).size < 3) {
      appendToLogAsLeader(log, LeaderAndIsr.initialLeaderEpoch, 10)
    }
    val trustedEpochCache = ListBuffer.empty ++= log.leaderEpochCache.get.epochEntries
    val trustedTierState = new TierState(trustedEpochCache.toList, None)
    // simulate that local segments up to the one containing offset 100L are tiered
    val lastTieredOffset = log.localLogSegments(0L, 100L).last.readNextOffset - 1
    when(tierPartitionState.endOffset).thenReturn(lastTieredOffset)
    val it = log.localLogSegments(0L, 100L).toList.reverseIterator
    do {
      val tieredSegment = it.next()
      offsetToMetadata.put(tieredSegment.baseOffset, new TierObjectMetadata(topicIdPartition,
        LeaderAndIsr.initialLeaderEpoch,
        java.util.UUID.randomUUID(),
        tieredSegment.baseOffset,
        tieredSegment.readNextOffset - 1,
        tieredSegment.largestTimestamp,
        tieredSegment.size,
        State.SEGMENT_UPLOAD_COMPLETE,
        true,
        false,
        true))
    } while (it.hasNext)
    when(tierPartitionState.metadata(ArgumentMatchers.anyLong())).thenAnswer(new Answer[Optional[TierObjectMetadata]]{
      override def answer(invocation: InvocationOnMock): Optional[TierObjectMetadata] = {
        val offset = invocation.getArgument(0).asInstanceOf[Long]
        Optional.ofNullable(offsetToMetadata.get(offset))
      }
    })
    when(tierPartitionState.segments(0L, Long.MaxValue)).thenAnswer(new Answer[CloseableIterator[TierObjectMetadata]]{
      override def answer(invocation: InvocationOnMock): CloseableIterator[TierObjectMetadata] = {
        CloseableIterator.wrap[TierObjectMetadata](offsetToMetadata.values().iterator())
      }
    })
    // delete local segments up to one segment after the last tiered segment
    log.localLog.deleteSegments(log.localLogSegments(from = 0L, to = lastTieredOffset + 2))
    assert(log.localLogStartOffset > lastTieredOffset + 1)
    // invoke recovery on local log and check log boundaries
    log.recoverLocalLogAfterUncleanLeaderElection(trustedTierState)
    assertEquals(s"Unexpected LogStartOffset after recovery", 0L, log.logStartOffset)
    assertEquals(s"Unexpected LogEndOffset after recovery", lastTieredOffset + 1, log.logEndOffset)
    assertEquals(s"Unexpected LocalLogStartOffset after recovery", lastTieredOffset + 1, log.localLogStartOffset)
    assertEquals(s"Unexpected LocalLogEndOffset after recovery", lastTieredOffset + 1, log.localLogEndOffset)
  }

  @Test
  def testSizeOfLogWithOverlap(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
    val numTieredSegments = 30
    val numLocalSegments = 50
    val numOverlapSegments = 10
    val log = createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlapSegments, logConfig)

    val segmentSize = log.localLogSegments.head.size
    val expectedLogSize = (numTieredSegments + numOverlapSegments + numLocalSegments - 1) * segmentSize + log.activeSegment.size
    assertEquals(expectedLogSize, log.size)
    log.close()
  }

  @Test
  def testTierableSegments(): Unit = {
    val noopScheduler = new Scheduler { // noopScheduler allows us to roll segments without scheduling a background flush
      override def startup(): Unit = ()
      override def shutdown(): Unit = ()
      override def isStarted: Boolean = true
      override def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit): ScheduledFuture[_] = {
        MockTask(name, fun, mockTime.milliseconds + delay, period = period, mockTime)
      }
    }

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createMergedLog(logConfig, scheduler = noopScheduler)
    val messagesToWrite = 10
    for (_ <- 0 until messagesToWrite) {
      val segmentStr = "foo"
      val messageStr = "bar"
      def createRecords = TestUtils.singletonRecords(("test" + segmentStr + messageStr).getBytes)
      log.appendAsLeader(createRecords, 0)
      log.roll()
    }

    val tierPartitionState = log.tierPartitionState
    val epoch = 0
    val tieredSegments = log.localLogSegments.take(2)

    // append an init message
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    // append metadata for tiered segments
    tieredSegments.foreach { segment =>
      val appendResult = TierTestUtils.uploadWithMetadata(tierPartitionState,
        topicIdPartition,
        epoch,
        UUID.randomUUID,
        segment.baseOffset,
        segment.readNextOffset - 1,
        segment.largestTimestamp,
        segment.lastModified,
        segment.size,
        false,
        true)
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }

    // no segments should be tierable yet, as recovery point and highwatermark have not moved
    assertEquals(0, log.tierableLogSegments.size)

    // no segments are tierable after recovery point and highwatermark move to the end of first tiered segment
    log.updateHighWatermark(tieredSegments.head.readNextOffset - 1)
    log.flush(1)
    assertEquals(0, log.tierableLogSegments.size)

    // all non tiered segments become tierable after recovery point and highwatermark move to the end of the log
    log.updateHighWatermark(log.logEndOffset)
    log.flush(log.logEndOffset)
    assertEquals(log.localLogSegments.size - tieredSegments.size - 1, log.tierableLogSegments.size)
    log.close()
  }

  @Test
  def testTierableSegmentsOffsetForTimestamp(): Unit = {
    val noopScheduler = new Scheduler { // noopScheduler allows us to roll segments without scheduling a background flush
      override def startup(): Unit = ()
      override def shutdown(): Unit = ()
      override def isStarted: Boolean = true
      override def schedule(name: String, fun: () => Unit, delay: Long, period: Long, unit: TimeUnit): ScheduledFuture[_] = {
        MockTask(name, fun, mockTime.milliseconds + delay, period = period, mockTime)
      }
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

    val tierPartitionState = log.tierPartitionState
    val epoch = 0
    val tieredSegments = log.localLogSegments.take(2)

    // append an init message
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    // append metadata for tiered segments
    tieredSegments.foreach { segment =>
      val appendResult = TierTestUtils.uploadWithMetadata(tierPartitionState,
        topicIdPartition,
        epoch,
        UUID.randomUUID,
        segment.baseOffset,
        segment.readNextOffset - 1,
        segment.largestTimestamp,
        segment.lastModified,
        segment.size,
        false,
        true)
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }
    tierPartitionState.flush()

    // no segments should be tierable yet, as recovery point and highwatermark have not moved
    assertEquals(0, log.tierableLogSegments.size)

    // no segments are tierable after recovery point and highwatermark move to the end of first tiered segment
    log.updateHighWatermark(tieredSegments.head.readNextOffset - 1)
    log.flush(1)
    assertEquals(0, log.tierableLogSegments.size)

    // all non tiered segments become tierable after recovery point and highwatermark move to the end of the log
    log.updateHighWatermark(log.logEndOffset)
    log.flush(log.logEndOffset)
    assertEquals(log.localLogSegments.size - tieredSegments.size - 1, log.tierableLogSegments.size)

    val metadata = tierPartitionState.metadata(0).get
    log.deleteOldSegments()
    val firstTimestamp = metadata.maxTimestamp()
    assertEquals(Some(new TierTimestampAndOffset(firstTimestamp, new TierObjectStore.ObjectMetadata(metadata), metadata.size)),
      log.fetchOffsetByTimestamp(firstTimestamp))
    log.close()
  }

  @Test // Test that log recovery is successful with an empty segment file and a non-empty producer state.
  def testSuccessfulLogRecoveryWithEmptySegment(): Unit = {
    val logConfig = LogTest.createLogConfig(retentionMs = 999)
    val log = createMergedLog(logConfig)

    val pid = 137L
    val epoch = 5.toShort
    val seq = 0

    val records_1 = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord(mockTime.milliseconds - 1000, "foo".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "bar".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "baz".getBytes))

    val records_2 = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid + 1, epoch, seq,
      new SimpleRecord(mockTime.milliseconds - 1000, "foo".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "bar".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "baz".getBytes))

    log.appendAsLeader(records_1, leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(records_2, leaderEpoch = 0)
    log.updateHighWatermark(log.logEndOffset)
    assertEquals("expected two active producers", 2, log.producerStateManager.activeProducers.size)

    val numDeleted = log.deleteOldSegments()
    assertEquals(2, numDeleted)

    log.close()
    val mergedLog = Try(createMergedLog(logConfig))
    assertTrue("expected log recovery to succeed", mergedLog.isSuccess)
    mergedLog.foreach(mergedLog => {
      assertEquals("expected non-expired producers to be retained", 2,
        mergedLog.producerStateManager.activeProducers.size)
    })
  }

  @Test // Test the first unstable offset is correctly set after truncation
  def testSuccessfulLogRecoveryWithNonEmptySegment(): Unit = {
    val logConfig = LogTest.createLogConfig(retentionMs = 999)
    val log = createMergedLog(logConfig)

    val pid = 137L
    val epoch = 5.toShort
    val seq = 0

    val records_1 = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid, epoch, seq,
      new SimpleRecord(mockTime.milliseconds - 1000, "foo".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "bar".getBytes),
      new SimpleRecord(mockTime.milliseconds - 1000, "baz".getBytes))

    val records_2 = MemoryRecords.withTransactionalRecords(CompressionType.NONE, pid + 1, epoch, seq,
      new SimpleRecord(mockTime.milliseconds, "foo".getBytes),
      new SimpleRecord(mockTime.milliseconds, "bar".getBytes),
      new SimpleRecord(mockTime.milliseconds, "baz".getBytes))

    log.appendAsLeader(records_1, leaderEpoch = 0)
    log.roll()
    log.appendAsLeader(records_2, leaderEpoch = 0)
    log.updateHighWatermark(log.logEndOffset)
    assertEquals("expected two active producers", 2, log.producerStateManager.activeProducers.size)

    val numDeleted = log.deleteOldSegments()
    assertEquals(1, numDeleted)

    log.close()
    val mergedLog = Try(createMergedLog(logConfig))
    assertTrue("expected log recovery to succeed", mergedLog.isSuccess)
    mergedLog.foreach(mergedLog => {
      assertEquals("expected non-expired producers to be retained", 2,
        mergedLog.producerStateManager.activeProducers.size)
      assertEquals("expected the first unstable offset to be correctly" +
        " set to the base offset of the batch remaining after truncation",
        mergedLog.localLog.logSegments.head.baseOffset,
        mergedLog.firstUnstableOffset.get)
    })
  }

  @Test
  def testUniqueLogSegmentsPartialOverlapWithFirstSegment(): Unit = {
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true)
    val log = createMergedLog(logConfig)

    val epoch = 0
    val tierPartitionState = log.tierPartitionState
    initializeTierPartitionState(tierPartitionState, epoch)

    // Append a record at offset 110. This record will be appended to the segment with baseOffset = 0.
    val records = TestUtils.records(
      List(new SimpleRecord(mockTime.milliseconds, "k1".getBytes, "v1".getBytes)),
      baseOffset = 110L, partitionLeaderEpoch = 0)
    log.appendAsFollower(records)
    log.updateHighWatermark(111)  // Set high watermark greater than the start offsets that we will use below
    log.maybeIncrementLogStartOffset(110L)
    assertEquals(0L, log.localLogSegments.head.baseOffset)
    assertEquals(110L, log.logStartOffset)
    assertEquals(1, log.localLogSegments.size)

    // Tier a segment at offset 100
    val result = TierTestUtils.uploadWithMetadata(tierPartitionState,
      topicIdPartition,
      epoch,
      UUID.randomUUID,
      100,
      200,
      mockTime.milliseconds,
      mockTime.milliseconds,
      100,
      false,
      true)
    assertEquals(AppendResult.ACCEPTED, result)
    tierPartitionState.flush()

    log.uniqueLogSegments(log.logStartOffset, Long.MaxValue) match {
      case (tieredSegments, localSegments) =>
        assertEquals(0, tieredSegments.asScala.size)
        assertEquals(1, localSegments.size)
        assertEquals(0, localSegments.head.baseOffset)
    }
  }

  @Test
  def testTierConsumerRegistrationForDeletedLog(): Unit = {
    val deletedDir = new File(Log.logDeleteDirName(Log.parseTopicPartitionName(logDir)))
    Files.createDirectory(deletedDir.toPath)
    val logConfig = LogTest.createLogConfig(tierEnable = true)
    val log = createMergedLog(logConfig, logDir = deletedDir)

    try {
      log.assignTopicId(UUID.randomUUID)
    } finally {
      log.close()
      Utils.delete(deletedDir)
    }

    verifyNoInteractions(tierTopicConsumer)
  }

  @Test
  def testFullTruncationLogicIsTieringAgnostic(): Unit = {
    val numTiered = 30
    val numOverlap = 3
    val numLocal = 15 // includes active segment

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, tierEnable = true, tierLocalHotsetBytes = 1)
    val log = createLogWithOverlap(numTiered, numLocal, numOverlap, logConfig)

    val newOffset = 10
    assertTrue(log.tierPartitionState.startOffset.isPresent)
    assertEquals(0.asInstanceOf[Long], log.tierPartitionState.startOffset.get)
    log.truncateFullyAndStartAt(newOffset)

    assertEquals(newOffset, log.firstOffsetMetadata().messageOffset);
    assertEquals(newOffset, log.recoveryPoint)
    assertEquals(newOffset, log.logEndOffset)
    assertEquals(newOffset, log.logStartOffset)
    assertTrue(log.producerStateManager.isEmpty)
  }

  private def makeCleaner(capacity: Int, throttler: Throttler, time: Time, checkDone: TopicPartition => Unit = _ => (), maxMessageSize: Int = 64*1024) =
    new Cleaner(id = 0,
      offsetMap = new FakeOffsetMap(capacity),
      ioBufferSize = maxMessageSize,
      maxIoBufferSize = maxMessageSize,
      dupBufferLoadFactor = 0.75,
      throttler = throttler,
      time = time,
      checkDone = checkDone)

  private def record(key: Int, value: Array[Byte]): MemoryRecords =
    TestUtils.singletonRecords(key = key.toString.getBytes, value = value)

  private def tombstoneRecord(key: Int): MemoryRecords = record(key, null)

  private def initializeTierPartitionState(tierPartitionState: TierPartitionState, epoch: Int): Unit = {
    // append an init message
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionState.onCatchUpComplete()
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      epoch, java.util.UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
  }

  private def logRanges(log: MergedLog): LogRanges = {
    val tierPartitionState = log.tierPartitionState

    val firstTieredOffset = log.logStartOffset
    val lastTieredOffset = tierPartitionState.segments.asScala.toList.last.baseOffset()
    val firstLocalOffset = log.localLogSegments.head.baseOffset
    val lastLocalOffset = log.logEndOffset

    if (firstLocalOffset <= lastTieredOffset)
      LogRanges(firstTieredOffset, lastTieredOffset, firstLocalOffset, lastLocalOffset, Some(firstLocalOffset), Some(lastTieredOffset))
    else
      LogRanges(firstTieredOffset, lastTieredOffset, firstLocalOffset, lastLocalOffset, None, None)
  }

  private def createMergedLog(config: LogConfig,
                              scheduler: Scheduler = mockTime.scheduler,
                              logStartOffset: Long = 0L,
                              logDir: File = logDir): MergedLog = {
    MergedLogTest.createMergedLog(tierLogComponents, logDir, config, brokerTopicStats, scheduler, mockTime, logStartOffset)
  }

  private def createLogWithOverlap(numTieredSegments: Int,
                                   numLocalSegments: Int,
                                   numOverlap: Int,
                                   config: LogConfig,
                                   segmentMaxTimestampCbk: () => Long = () => RecordBatch.NO_TIMESTAMP): MergedLog = {
    MergedLogTest.createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlap, tierLogComponents, logDir,
      config, brokerTopicStats, mockTime.scheduler, mockTime, topicIdPartition, segmentMaxTimestampCbk = segmentMaxTimestampCbk)
  }

  private def metricValue(name: String): Long = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter(_._1.getName == name).values.headOption.get.asInstanceOf[Gauge[Long]].value()
  }

  private def tieredLogSegmentsList(log: AbstractLog): List[TierLogSegment] = {
    val iterator = log.tieredLogSegments
    try {
      iterator.asScala.toList
    } finally {
      iterator.close()
    }
  }
}

object MergedLogTest {
  def createRecords(segmentIdx: Int = 0, messageIdx: Int = 0, timestamp: Long = RecordBatch.NO_TIMESTAMP): MemoryRecords = {
    val segmentStr = "%06d".format(segmentIdx)
    val messageStr = "%06d".format(messageIdx)
    TestUtils.singletonRecords(("test" + segmentStr + messageStr).getBytes, timestamp = timestamp)
  }

  /**
    * @param numTieredSegments Number of tiered segments to create.
    * @param numLocalSegments Number of local segments to create. The active segment is included in this count and will be empty.
    * @param numOverlap Number of overlap segments to create; totalSegments = numTieredSegments + numLocalSegments + numOverlap
    */
  def createLogWithOverlap(numTieredSegments: Int,
                           numLocalSegments: Int,
                           numOverlap: Int,
                           tierLogComponents: TierLogComponents,
                           dir: File,
                           logConfig: LogConfig,
                           brokerTopicStats: BrokerTopicStats,
                           scheduler: Scheduler,
                           time: Time,
                           topicIdPartition: TopicIdPartition,
                           logStartOffset: Long = 0L,
                           recoveryPoint: Long = 0L,
                           maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
                           producerIdExpirationCheckIntervalMs: Int = LogManager.ProducerIdExpirationCheckIntervalMs,
                           segmentMaxTimestampCbk: () => Long = () => RecordBatch.NO_TIMESTAMP): MergedLog = {
    val tempConfig = LogTest.createLogConfig(retentionBytes = 1,
      tierEnable = true,
      segmentBytes = logConfig.segmentSize,
      segmentMs = logConfig.segmentMs,
      tierLocalHotsetBytes = 1)
    var log = createMergedLog(tierLogComponents, dir, tempConfig, brokerTopicStats, scheduler, time, logStartOffset,
      recoveryPoint, maxProducerIdExpirationMs, producerIdExpirationCheckIntervalMs)
    var tierPartitionState = log.tierPartitionState
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionState.onCatchUpComplete()

    val epoch = 0

    // create all segments as local initially
    for (segment <- 0 until (numTieredSegments + numLocalSegments + numOverlap - 1)) {
      val currentNumSegments = log.localLogSegments.size
      val timestamp = segmentMaxTimestampCbk()
      var message = 0
      while (log.localLogSegments.size == currentNumSegments) {
        log.appendAsLeader(createRecords(segment, message, timestamp), leaderEpoch = 0)
        message += 1
      }
    }
    log.updateHighWatermark(log.logEndOffset)
    assertEquals(numTieredSegments + numLocalSegments + numOverlap, log.localLogSegments.size)

    // append an init message
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition,
      epoch,
      java.util.UUID.randomUUID(),
      0), TierTestUtils.nextTierTopicOffsetAndEpoch())

    // initialize metadata for tiered segments
    val segmentsToTier = log.localLogSegments.take(numTieredSegments + numOverlap)
    segmentsToTier.foreach { segment =>
      val appendResult = TierTestUtils.uploadWithMetadata(tierPartitionState,
        topicIdPartition,
        epoch,
        UUID.randomUUID,
        segment.baseOffset,
        segment.readNextOffset - 1,
        segment.largestTimestamp,
        segment.lastModified,
        segment.size,
        false,
        true)
      tierPartitionState.flush()
      assertEquals(AppendResult.ACCEPTED, appendResult)
    }

    val localSegmentsToDelete = segmentsToTier.take(numTieredSegments)
    log.localLog.deleteOldSegments(Some(localSegmentsToDelete.last.readNextOffset), maxNumSegmentsToDelete = Int.MaxValue, retentionType = HotsetRetention)

    // close the log
    log.close()

    // reopen
    log = createMergedLog(tierLogComponents, dir, logConfig, brokerTopicStats, scheduler, time, logStartOffset,
      recoveryPoint, maxProducerIdExpirationMs, producerIdExpirationCheckIntervalMs)
    log.updateHighWatermark(log.logEndOffset)
    tierPartitionState = log.tierPartitionState

    // assert number of segments
    assertEquals(numLocalSegments + numOverlap, log.localLogSegments.size)
    assertEquals(numTieredSegments + numOverlap, tierPartitionState.segments.asScala.size)

    log.uniqueLogSegments match {
      case (tierLogSegments, localLogSegments) =>
        assertEquals(numTieredSegments, tierLogSegments.asScala.size)
        assertEquals(numLocalSegments + numOverlap, localLogSegments.size)
    }
    log
  }

  def createMergedLog(tierLogComponents: TierLogComponents,
                      dir: File,
                      config: LogConfig,
                      brokerTopicStats: BrokerTopicStats,
                      scheduler: Scheduler,
                      time: Time,
                      logStartOffset: Long = 0L,
                      recoveryPoint: Long = 0L,
                      maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
                      producerIdExpirationCheckIntervalMs: Int = LogManager.ProducerIdExpirationCheckIntervalMs): MergedLog = {
    MergedLog(dir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = recoveryPoint,
      scheduler = scheduler,
      brokerTopicStats = brokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10),
      tierLogComponents = tierLogComponents)
  }

  case class LogRanges(firstTieredOffset: Long,
                       lastTieredOffset: Long,
                       firstLocalOffset: Long,
                       lastLocalOffset: Long,
                       firstOverlapOffset: Option[Long],
                       lastOverlapOffset: Option[Long])

  private def deleteOldLogSegments(log: MergedLog, expectedNumDeleted: Int): Unit = {
    val maxNumSegmentsToDeletePerRun = 1
    var numDeleted = 0
    for (i <- 1 to expectedNumDeleted) {
      numDeleted += log.deleteOldSegments(maxNumSegmentsToDeletePerRun)
      assertEquals(i, numDeleted)
    }
    assertEquals(expectedNumDeleted, numDeleted)
    // attempting deletion again is a NOOP
    assertEquals(0, log.deleteOldSegments())
  }
}
