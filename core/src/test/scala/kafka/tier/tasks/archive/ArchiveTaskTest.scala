/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tasks.archive

import java.io.File
import java.nio.ByteBuffer
import java.nio.file.NoSuchFileException
import java.util.Collections
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.UUID

import kafka.log.{AbstractLog, LogSegment, OffsetIndex, ProducerStateManager, TimeIndex, UploadableSegment}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.ReplicaManager
import kafka.server.epoch.LeaderEpochFileCache
import kafka.tier.domain.{TierObjectMetadata, TierSegmentUploadComplete, TierSegmentUploadInitiate}
import kafka.tier.exceptions.{TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.TopicIdPartition
import kafka.tier.tasks.CompletableFutureUtil
import kafka.tier.tasks.archive.ArchiveTask.SegmentDeletedException
import kafka.tier.topic.TierTopicManager
import kafka.utils.TestUtils
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.{MockTime, Time}
import org.junit.{After, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, mock, reset, times, verify, when}
import org.scalatest.Assertions.assertThrows

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ArchiveTaskTest extends KafkaMetricsGroup {
  val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("cbf4eaed-cc00-47dc-b08c-f1f5685f085d"), 0)
  var ctx: CancellationContext = CancellationContext.newContext()
  var tierTopicManager: TierTopicManager = mock(classOf[TierTopicManager])
  var tierObjectStore: TierObjectStore = mock(classOf[TierObjectStore])
  var replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  var time: Time = new MockTime()
  val tmpFile = TestUtils.tempFile()

  @After
  def tearDown(): Unit = {
    tmpFile.delete()
    ctx.cancel()
    reset(tierTopicManager, tierObjectStore, replicaManager)
  }

  @Test
  def testEstablishingLeadership(): Unit = {
    val leaderEpoch = 0

    when(tierTopicManager.becomeArchiver(topicIdPartition, leaderEpoch))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))
    val nextState = Await.result(ArchiveTask.establishLeadership(BeforeLeader(0), topicIdPartition, tierTopicManager), 50 millis)
    assertEquals("Expected task to establish leadership", BeforeUpload(leaderEpoch), nextState)

    when(tierTopicManager.becomeArchiver(topicIdPartition, leaderEpoch))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.NOT_TIERABLE))
    val illegal = Await.ready(ArchiveTask.establishLeadership(BeforeLeader(0), topicIdPartition, tierTopicManager), 50 millis)
    assertTrue("Expected establishing leadership to fail", illegal.value.get.isFailure)

    when(tierTopicManager.becomeArchiver(topicIdPartition, leaderEpoch))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FENCED))
    val fenced = Await.ready(ArchiveTask.establishLeadership(BeforeLeader(0), topicIdPartition, tierTopicManager), 50 millis)
    assertTrue("Expected establishing leadership to fail", fenced.value.get.isFailure)

    when(tierTopicManager.becomeArchiver(topicIdPartition, leaderEpoch))
      .thenReturn(CompletableFutureUtil.completed(new Object()).asInstanceOf[CompletableFuture[AppendResult]])
    val unknown = Await.ready(ArchiveTask.establishLeadership(BeforeLeader(0), topicIdPartition, tierTopicManager), 50 millis)
    assertTrue("Expected establishing leadership to fail", unknown.value.get.isFailure)

    verify(tierTopicManager, times(4)).becomeArchiver(topicIdPartition, leaderEpoch)
  }

  @Test
  def testTierSegmentInvalidEpoch(): Unit = {
    val leaderEpoch = 0

    val tps = when(mock(classOf[TierPartitionState]).tierEpoch())
      .thenReturn(leaderEpoch + 1)
      .getMock[TierPartitionState]()
    val logSegment = mockLogSegment(tmpFile)
    val log = mockAbstractLog(logSegment)
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(log.tierPartitionState).thenReturn(tps)

    val nextState = ArchiveTask.maybeInitiateUpload(
      BeforeUpload(leaderEpoch),
      topicIdPartition,
      time,
      tierTopicManager,
      tierObjectStore,
      replicaManager)

    assertTrue("Expected segment tiering to fail due to fencing",
      Await.ready(nextState, 50 millis).value.get.isFailure)
  }

  @Test
  def testMetadataSizeDuringUpload(): Unit = {
    val leaderEpoch = 0
    val logSegment = mockLogSegment(tmpFile)
    val log = mockAbstractLog(logSegment)
    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition,
      leaderEpoch,
      UUID.randomUUID,
      logSegment.baseOffset,
      logSegment.readNextOffset - 1,
      logSegment.maxTimestampSoFar,
      logSegment.size,
      true,
      true,
      true)

    val epochStateSize = 1000000000L
    val producerStateSize = 2000000000L
    val abortedTxnsLimit = 150
    val abortedTxnsPos = 50
    val expectedSize = logSegment.size + epochStateSize + producerStateSize + (abortedTxnsLimit - abortedTxnsPos)

    val epochState = mock(classOf[File])
    val producerState = mock(classOf[File])
    val abortedTxns = ByteBuffer.wrap(new Array[Byte](abortedTxnsLimit))
    abortedTxns.limit(abortedTxnsLimit)
    abortedTxns.position(abortedTxnsPos)

    val epochStateOpt = Some(epochState)
    val producerStateOpt = Some(producerState)
    val abortedTxnsOpt = Some(abortedTxns)

    when(epochState.exists).thenReturn(true)
    when(producerState.exists).thenReturn(true)
    when(epochState.length).thenReturn(epochStateSize)
    when(producerState.length).thenReturn(producerStateSize)

    doNothing().when(tierObjectStore).putSegment(any(), any(), any(), any(), any(), any(), any())

    val uploadableSegment = UploadableSegment(log, logSegment, logSegment.readNextOffset, producerStateOpt, epochStateOpt, abortedTxnsOpt)
    val upload = Upload(leaderEpoch, uploadInitiate, uploadableSegment)

    val uploadResult = ArchiveTask.upload(upload, topicIdPartition, time, tierObjectStore)
    val afterUpload = Await.result(uploadResult, 1 second)

    assertEquals("metadata size of AfterUpload object is incorrect value",
      expectedSize, afterUpload.uploadedSize)

    assertTrue("metadata size of AfterUpload object is negative and overflowed",
      afterUpload.uploadedSize > 0)
  }

  @Test
  def testMetadataSizeAfterUpload(): Unit = {
    val testUploadSize = 400
    val metricName = "BytesPerSec"

    removeMetric(metricName)
    val byteRate = newMeter(metricName, "bytes per second", TimeUnit.SECONDS)

    val leaderEpoch = 0
    val logSegment = mockLogSegment(tmpFile)
    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition,
      leaderEpoch,
      UUID.randomUUID,
      logSegment.baseOffset,
      logSegment.readNextOffset - 1,
      logSegment.maxTimestampSoFar,
      logSegment.size,
      true,
      true,
      true)

    val afterUpload = AfterUpload(0, uploadInitiate, testUploadSize)

    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadInitiate]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadComplete]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    val afterUploadResult = ArchiveTask.finalizeUpload(afterUpload, topicIdPartition, time, tierTopicManager, Some(byteRate))
    Await.result(afterUploadResult, 1 second)

    assertEquals("tier archiver mean rate shows no data uploaded to tiered storage",
      testUploadSize, byteRate.count())
  }

  @Test
  def testExceptionDuringInitiateUpload(): Unit = {
    val nextState = testExceptionHandlingDuringInitiateUpload(new IllegalStateException("illegal state"))
    assertThrows[IllegalStateException] {
      Await.result(nextState, 1 second)
    }
  }

  @Test
  def testSegmentDeletedDuringUpload(): Unit = {
    val nextState = testExceptionHandlingDuringUpload(new NoSuchFileException("segment deleted"), deleteSegment = true)
    assertThrows[SegmentDeletedException] {
      Await.result(nextState, 1 second)
    }
  }

  @Test
  def testUnknownExceptionDuringUpload(): Unit = {
    val nextState = testExceptionHandlingDuringUpload(new IllegalStateException("illegal state"), deleteSegment = false)
    assertThrows[IllegalStateException] {
      Await.result(nextState, 1 second)
    }
  }

  @Test
  def testTierSegmentNoSegments(): Unit = {
    val leaderEpoch = 0

    val tierPartitionState = when(mock(classOf[TierPartitionState]).tierEpoch())
      .thenReturn(leaderEpoch)
      .getMock[TierPartitionState]()

    val emptyLog = mock(classOf[AbstractLog])
      when(emptyLog.tierableLogSegments)
        .thenReturn(Collections.emptyList().asScala)
        .getMock[AbstractLog]()

    when(emptyLog.tierPartitionState).thenReturn(tierPartitionState)
    when(replicaManager.getLog(topicIdPartition.topicPartition))
      .thenReturn(Some(emptyLog))

    val nextState = ArchiveTask.maybeInitiateUpload(
      BeforeUpload(leaderEpoch),
      topicIdPartition,
      time,
      tierTopicManager,
      tierObjectStore,
      replicaManager)

    val result = Await.result(nextState, 50 millis)
    assertTrue("Expected segment tiering to complete successfully, but not progress to AfterUpload",
      result.isInstanceOf[BeforeUpload])
  }

  @Test
  def testTierSegmentWithoutLeaderEpochState(): Unit = {
    val leaderEpoch = 0
    val tierPartitionState = mockTierPartitionState(leaderEpoch)

    val logSegment = mockLogSegment(tmpFile)

    val mockProducerStateManager = mock(classOf[ProducerStateManager])
    when(mockProducerStateManager.snapshotFileForOffset(ArgumentMatchers.any(classOf[Long]))).thenReturn(None)

    val log = mockAbstractLog(logSegment)
    when(log.tierPartitionState).thenReturn(tierPartitionState)
    when(log.leaderEpochCache).thenReturn(None)
    when(log.producerStateManager).thenReturn(mockProducerStateManager)
    when(log.collectAbortedTransactions(ArgumentMatchers.any(classOf[Long]), ArgumentMatchers.any(classOf[Long])))
      .thenReturn(List())
    val uploadableSegment = UploadableSegment(log, logSegment, logSegment.readNextOffset, None, None, None)
    when(log.createUploadableSegment(logSegment)).thenReturn(uploadableSegment)


    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadInitiate]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadComplete]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    val metadata = tierSegment(log, leaderEpoch)
    assertEquals("expected start offset to be 0", metadata.baseOffset, 0)
    assertEquals("expected end offset to be 9", metadata.endOffset, 9)
  }

  @Test
  def testTierSegmentWithLeaderEpochState(): Unit = {
    val leaderEpoch = 0
    val tierPartitionState = mockTierPartitionState(leaderEpoch)

    val logSegment = mockLogSegment(tmpFile)

    val mockLeaderEpochCache = mock(classOf[LeaderEpochFileCache])
    when(mockLeaderEpochCache.clone(ArgumentMatchers.any())).thenReturn(mockLeaderEpochCache)
    when(mockLeaderEpochCache.file).thenReturn(tmpFile)

    val nextOffset = logSegment.readNextOffset

    doNothing().when(mockLeaderEpochCache).truncateFromEnd(nextOffset)

    val log = mockAbstractLog(logSegment)
    when(log.tierPartitionState).thenReturn(tierPartitionState)
    when(log.leaderEpochCache).thenReturn(Some(mockLeaderEpochCache))
    when(log.collectAbortedTransactions(ArgumentMatchers.any(classOf[Long]), ArgumentMatchers.any(classOf[Long])))
      .thenReturn(List())

    val mockProducerStateManager = mock(classOf[ProducerStateManager])
    when(log.producerStateManager).thenReturn(mockProducerStateManager)
    when(mockProducerStateManager.snapshotFileForOffset(ArgumentMatchers.any(classOf[Long]))).thenReturn(None)

    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadInitiate]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadComplete]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    val uploadableSegment = UploadableSegment(log, logSegment, logSegment.readNextOffset, None, Option(mockLeaderEpochCache.file), None)
    when(log.createUploadableSegment(logSegment)).thenReturn(uploadableSegment)

    val metadata = tierSegment(log, leaderEpoch)

    assertTrue("expected metadata to claim to have epoch state", metadata.hasEpochState)
    verify(tierObjectStore, times(1)).putSegment(
      ArgumentMatchers.notNull(),
      ArgumentMatchers.notNull(),
      ArgumentMatchers.notNull(),
      ArgumentMatchers.notNull(),
      ArgumentMatchers.notNull(),
      ArgumentMatchers.notNull(),
      ArgumentMatchers.notNull())
  }

  @Test
  def testArchiverTaskSetsPauseOnRetry(): Unit = {
    val task = new ArchiveTask(ctx, topicIdPartition, BeforeLeader(0), ArchiverMetrics(None, None))

    when(tierTopicManager.becomeArchiver(topicIdPartition, 0)).thenThrow(
      new TierMetadataRetriableException("something"),
      new TierObjectStoreRetriableException("foo", new RuntimeException("foo")))
    val result = Await.result(task.transition(time, tierTopicManager, tierObjectStore, replicaManager), 1 second)
    assertTrue("expected the task to be paused", result.pausedUntil.isDefined)
    assertFalse("expected the task to not be canceled", ctx.isCancelled)
    val pauseTime = result.pausedUntil.get

    time.sleep(100)

    val result2 = Await.result(task.transition(time, tierTopicManager, tierObjectStore, replicaManager), 1 second)
    assertTrue("expected the task to be paused", result2.pausedUntil.isDefined)
    assertFalse("expected the task to not be canceled", ctx.isCancelled)
    val pauseTime2 = result2.pausedUntil.get

    assertTrue("expected the second pause time to be larger than the first", pauseTime2.isAfter(pauseTime))
  }

  @Test
  def testCancelledArchiveTaskDoesNotProgress(): Unit = {
    val task = new ArchiveTask(ctx, topicIdPartition, BeforeLeader(0), ArchiverMetrics(None, None))
    ctx.cancel()
    val result = Await.result(task.transition(time, tierTopicManager, tierObjectStore, replicaManager), 1 second)
    assertTrue("expected task to remain in BeforeLeader", result.state.isInstanceOf[BeforeLeader])
  }

  @Test
  def testHandleSegmentDeletedException(): Unit = {
    val exception = new SegmentDeletedException("segment deleted", new Exception())
    val beforeLeader = BeforeLeader(0)
    val beforeUpload = BeforeUpload(0)
    val upload = Upload(0, mock(classOf[TierSegmentUploadInitiate]), mock(classOf[UploadableSegment]))
    val afterUpload = AfterUpload(0, mock(classOf[TierSegmentUploadInitiate]), 0L)

    assertThrows[SegmentDeletedException] {
      beforeLeader.handleSegmentDeletedException(exception)
    }

    assertEquals(classOf[BeforeUpload], beforeUpload.handleSegmentDeletedException(exception).getClass)
    assertEquals(classOf[BeforeUpload], upload.handleSegmentDeletedException(exception).getClass)

    assertThrows[SegmentDeletedException] {
      afterUpload.handleSegmentDeletedException(exception)
    }
  }

  @Test
  def testHandlingForSegmentDeletedExceptionDuringTransition(): Unit = {
    val logSegment = mockLogSegment(tmpFile)
    val log = mockAbstractLog(logSegment)
    val exception = new SegmentDeletedException("segment deleted", new Exception)
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(log.tierPartitionState).thenThrow(exception)
    val beforeUpload = BeforeUpload(42)
    val task = new ArchiveTask(ctx, topicIdPartition, beforeUpload, ArchiverMetrics(None, None))

    val result = Await.result(task.transition(time, tierTopicManager, tierObjectStore, replicaManager), 1 second)
    assertEquals(result.state, beforeUpload)
    assertEquals(result.retryCount, 1)
  }

  private def testExceptionHandlingDuringInitiateUpload(e: Exception): Future[ArchiveTaskState] = {
    val leaderEpoch = 0
    val tierPartitionState = mock(classOf[TierPartitionState])
    val logSegment = mockLogSegment(tmpFile)
    val log = mockAbstractLog(logSegment)
    val mockProducerStateManager = mock(classOf[ProducerStateManager])

    when(log.tierPartitionState).thenReturn(tierPartitionState)
    when(tierPartitionState.tierEpoch).thenReturn(leaderEpoch)
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(log.tierableLogSegments).thenReturn(Seq(logSegment))
    when(log.collectAbortedTransactions(any(), any())).thenThrow(e)
    when(log.leaderEpochCache).thenReturn(None)
    when(log.producerStateManager).thenReturn(mockProducerStateManager)
    when(mockProducerStateManager.snapshotFileForOffset(any())).thenReturn(None)
    when(log.createUploadableSegment(logSegment)).thenThrow(e)

    ArchiveTask.maybeInitiateUpload(
      BeforeUpload(leaderEpoch),
      topicIdPartition,
      time,
      tierTopicManager,
      tierObjectStore,
      replicaManager)
  }

  private def testExceptionHandlingDuringUpload(e: Exception, deleteSegment: Boolean): Future[ArchiveTaskState] = {
    val leaderEpoch = 0

    var fileToUse : File = tmpFile
    if (deleteSegment) {
      fileToUse = mock(classOf[File])
      when(fileToUse.exists).thenReturn(false)
    }

    val logSegment = mockLogSegment(fileToUse)
    val log = mockAbstractLog(logSegment)

    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition,
      leaderEpoch,
      UUID.randomUUID,
      logSegment.baseOffset,
      logSegment.readNextOffset - 1,
      logSegment.maxTimestampSoFar,
      logSegment.size,
      false,
      false,
      false)

    val uploadableSegment = UploadableSegment(log, logSegment, logSegment.readNextOffset, None, None, None)
    val upload = Upload(leaderEpoch, uploadInitiate, uploadableSegment)

    when(tierObjectStore.putSegment(any(), any(), any(), any(), any(), any(), any())).thenThrow(e)

    ArchiveTask.upload(upload, topicIdPartition, time, tierObjectStore)
  }

  /**
    * Creates a mock log segment backed by a tmpFile with 9 records
    */
  private def mockLogSegment(tmpFile: File): LogSegment = {

    val offsetIndex = mock(classOf[OffsetIndex])
    when(offsetIndex.file).thenReturn(tmpFile)

    val timeIndex = mock(classOf[TimeIndex])
    when(timeIndex.file).thenReturn(tmpFile)

    val fileRecords = mock(classOf[FileRecords])
    when(fileRecords.file()).thenReturn(tmpFile)

    val logSegment = mock(classOf[LogSegment])
    when(logSegment.readNextOffset).thenReturn(10)
    when(logSegment.baseOffset).thenReturn(0)
    when(logSegment.largestTimestamp).thenReturn(0)
    when(logSegment.size).thenReturn(1000)
    when(logSegment.log).thenReturn(fileRecords)
    when(logSegment.offsetIndex).thenReturn(offsetIndex)
    when(logSegment.timeIndex).thenReturn(timeIndex)

    logSegment
  }

  private def mockTierPartitionState(leaderEpoch: Int): TierPartitionState = {
    when(mock(classOf[TierPartitionState]).tierEpoch())
      .thenReturn(leaderEpoch)
      .getMock[TierPartitionState]()
  }

  private def mockAbstractLog(logSegment: LogSegment): AbstractLog = {
    val log = mock(classOf[AbstractLog])
    when(log.tierableLogSegments).thenReturn(Seq(logSegment))
    log
  }

  private def tierSegment(log: AbstractLog, leaderEpoch: Int): TierObjectMetadata = {
    val beforeUploadResult = ArchiveTask.maybeInitiateUpload(
      BeforeUpload(leaderEpoch),
      topicIdPartition,
      time,
      tierTopicManager,
      tierObjectStore,
      replicaManager)
    val upload = Await.result(beforeUploadResult, 1 second).asInstanceOf[Upload]

    val uploadResult = ArchiveTask.upload(upload, topicIdPartition, time, tierObjectStore)
    val afterUpload = Await.result(uploadResult, 1 second)

    val afterUploadResult = ArchiveTask.finalizeUpload(afterUpload, topicIdPartition, time, tierTopicManager, None)
    Await.result(afterUploadResult, 1 second)

    val uploadInitiate = upload.uploadInitiate
    new TierObjectMetadata(uploadInitiate.topicIdPartition, uploadInitiate.tierEpoch, uploadInitiate.objectId,
      uploadInitiate.baseOffset, uploadInitiate.endOffset, uploadInitiate.maxTimestamp, uploadInitiate.size,
      TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE, uploadInitiate.hasEpochState, uploadInitiate.hasAbortedTxns,
      uploadInitiate.hasProducerState)
  }
}
