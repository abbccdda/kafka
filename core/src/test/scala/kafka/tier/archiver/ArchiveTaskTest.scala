/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.File
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.UUID

import kafka.log.{AbstractLog, LogSegment, OffsetIndex, ProducerStateManager, TimeIndex}
import kafka.server.ReplicaManager
import kafka.server.epoch.LeaderEpochFileCache
import kafka.tier.TierTopicManager
import kafka.tier.domain.{TierObjectMetadata, TierSegmentUploadComplete, TierSegmentUploadInitiate}
import kafka.tier.exceptions.{TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.TopicIdPartition
import kafka.utils.TestUtils
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.{MockTime, Time}
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, mock, reset, times, verify, when}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._

class ArchiveTaskTest {
  val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("cbf4eaed-cc00-47dc-b08c-f1f5685f085d"), 0)
  var ctx: CancellationContext = _
  var tierTopicManager: TierTopicManager = _
  var tierObjectStore: TierObjectStore = _
  var replicaManager: ReplicaManager = _
  var time: Time = _

  @Before
  def setup(): Unit = {
    ctx = CancellationContext.newContext()
    tierTopicManager = mock(classOf[TierTopicManager])
    tierObjectStore = mock(classOf[TierObjectStore])
    replicaManager = mock(classOf[ReplicaManager])
    time = new MockTime()
  }

  @After
  def tearDown(): Unit = {
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
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ILLEGAL))
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
    when(tierTopicManager.partitionState(topicIdPartition)).thenReturn(tps)

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
  def testTierSegmentNoSegments(): Unit = {
    val leaderEpoch = 0

    val tierPartitionState = when(mock(classOf[TierPartitionState]).tierEpoch())
      .thenReturn(leaderEpoch)
      .getMock[TierPartitionState]()

    when(tierTopicManager.partitionState(topicIdPartition)).thenReturn(tierPartitionState)

    val emptyLog = mock(classOf[AbstractLog])
      when(emptyLog.tierableLogSegments)
        .thenReturn(Collections.emptyList().asScala)
        .getMock[AbstractLog]()

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

  @Test
  def testTierSegmentWithoutLeaderEpochState(): Unit = {
    val tmpFile = TestUtils.tempFile()
    val leaderEpoch = 0
    val tierPartitionState = mockTierPartitionState(leaderEpoch)
    when(tierTopicManager.partitionState(topicIdPartition)).thenReturn(tierPartitionState)

    val logSegment = mockLogSegment(tmpFile)

    val mockProducerStateManager = mock(classOf[ProducerStateManager])
    when(mockProducerStateManager.snapshotFileForOffset(ArgumentMatchers.any(classOf[Long]))).thenReturn(None)

    val log = mockAbstractLog(logSegment)
    when(log.leaderEpochCache).thenReturn(None)
    when(log.producerStateManager).thenReturn(mockProducerStateManager)

    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadInitiate]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadComplete]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    val metadata = tierSegment(log, leaderEpoch)
    assertEquals("expected start offset to be 0", metadata.baseOffset, 0)
    assertEquals("expected end offset to be 9", metadata.endOffset, 9)
  }

  @Test
  def testTierSegmentWithLeaderEpochState(): Unit = {
    val tmpFile = TestUtils.tempFile()
    val leaderEpoch = 0
    val tierPartitionState = mockTierPartitionState(leaderEpoch)
    when(tierTopicManager.partitionState(topicIdPartition)).thenReturn(tierPartitionState)

    val logSegment = mockLogSegment(tmpFile)

    val mockLeaderEpochCache = mock(classOf[LeaderEpochFileCache])
    when(mockLeaderEpochCache.clone(ArgumentMatchers.any())).thenReturn(mockLeaderEpochCache)
    when(mockLeaderEpochCache.file).thenReturn(tmpFile)

    val nextOffset = logSegment.readNextOffset

    doNothing().when(mockLeaderEpochCache).truncateFromEnd(nextOffset)

    val log = mockAbstractLog(logSegment)
    when(log.leaderEpochCache).thenReturn(Some(mockLeaderEpochCache))

    val mockProducerStateManager = mock(classOf[ProducerStateManager])
    when(log.producerStateManager).thenReturn(mockProducerStateManager)
    when(mockProducerStateManager.snapshotFileForOffset(ArgumentMatchers.any(classOf[Long]))).thenReturn(None)

    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadInitiate]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))
    when(tierTopicManager.addMetadata(any(classOf[TierSegmentUploadComplete]))).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

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
    val task = new ArchiveTask(ctx, topicIdPartition, BeforeLeader(0))

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
    val task = new ArchiveTask(ctx, topicIdPartition, BeforeLeader(0))
    ctx.cancel()
    val result = Await.result(task.transition(time, tierTopicManager, tierObjectStore, replicaManager), 1 second)
    assertTrue("expected task to remain in BeforeLeader", result.state.isInstanceOf[BeforeLeader])
  }
}
