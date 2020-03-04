/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import java.io.File
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.{Properties, UUID}

import kafka.log._
import kafka.server.ReplicaManager
import kafka.tier.domain.{TierObjectMetadata, TierSegmentDeleteComplete, TierSegmentDeleteInitiate}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.delete.DeletionTask._
import kafka.tier.topic.TierTopicManager
import kafka.tier.TopicIdPartition
import kafka.tier.exceptions.TierObjectStoreRetriableException
import kafka.utils.TestUtils
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class DeletionTaskTest {
  val topic1 = "foo-0"
  val topicIdPartition_1 = new TopicIdPartition(topic1, UUID.randomUUID, 0)
  var tierPartitionState_1 = mockTierPartitionState(0)

  val topic2 = "foo-1"
  val topicIdPartition_2 = new TopicIdPartition(topic2, UUID.randomUUID, 0)
  var tierPartitionState_2 = mockTierPartitionState(0)

  val topic3 = "foo-2"
  val topicIdPartition_3 = new TopicIdPartition(topic3, UUID.randomUUID, 0)
  val fencedSegments =  List(new TierObjectMetadata(topicIdPartition_3, 0, UUID.randomUUID, 100L, 3252334L, 1000L,
      102, TierObjectMetadata.State.SEGMENT_FENCED, true, false, false),
    new TierObjectMetadata(topicIdPartition_3, 0, UUID.randomUUID, 4252334L, 5252334L, 5000L,
      102, TierObjectMetadata.State.SEGMENT_FENCED, true, false, false))
  var tierPartitionState_3 = mockTierPartitionState(0, fencedSegments)

  val ctx = CancellationContext.newContext()
  val tierTopicManager = mock(classOf[TierTopicManager])
  val tierObjectStore = mock(classOf[TierObjectStore])
  val exceptionalTierObjectStore = mock(classOf[TierObjectStore])
  val replicaManager = mock(classOf[ReplicaManager])
  val time = new MockTime()
  val tmpFile = TestUtils.tempFile()
  var logWithTieredSegments: AbstractLog = _
  var emptyLog: AbstractLog = _
  var emptyLogWithTierFencedSegments: AbstractLog = _
  @Before
  def setup(): Unit = {
    var baseOffset = 100L
    val tieredSegments = mutable.ListBuffer[TierLogSegment]()
    val localSegments = mutable.ListBuffer[LogSegment]()

    for (_ <- 0 to 15) {
      val segment = tieredLogSegment(topicIdPartition_1, baseOffset, baseOffset + 50)
      baseOffset += 51
      tieredSegments += segment
      time.sleep(50)
    }

    for (_ <- 0 to 5) {
      val segment = mockLogSegment(tmpFile, baseOffset, baseOffset + 50)
      baseOffset += 51
      localSegments += segment
    }

    val properties = new Properties()
    properties.put(LogConfig.RetentionMsProp, "300")
    val logConfig = LogConfig(properties)

    logWithTieredSegments = mockAbstractLog(tieredSegments.toList, localSegments.toList)
    when(logWithTieredSegments.config).thenReturn(logConfig)
    when(logWithTieredSegments.logStartOffset).thenReturn(0L)
    doAnswer(new Answer[Any] {
      override def answer(invocation: InvocationOnMock): Any = {
        val newStartOffset = invocation.getArgument(0).asInstanceOf[Long]
        if (logWithTieredSegments.logStartOffset < newStartOffset)
          when(logWithTieredSegments.logStartOffset).thenReturn(newStartOffset)
      }
    }).when(logWithTieredSegments).maybeIncrementLogStartOffset(any())

    emptyLog = mockAbstractLog(List.empty, List.empty)
    when(emptyLog.config).thenReturn(logConfig)

    emptyLogWithTierFencedSegments = mockAbstractLog(List.empty, List.empty)
    when(emptyLogWithTierFencedSegments.config).thenReturn(logConfig)

    when(replicaManager.getLog(topicIdPartition_1.topicPartition)).thenReturn(Some(logWithTieredSegments))
    when(replicaManager.getLog(topicIdPartition_2.topicPartition)).thenReturn(Some(emptyLog))
    when(replicaManager.getLog(topicIdPartition_3.topicPartition)).thenReturn(Some(emptyLogWithTierFencedSegments))

    when(logWithTieredSegments.tierPartitionState).thenReturn(tierPartitionState_1)
    when(emptyLog.tierPartitionState).thenReturn(tierPartitionState_2)
    when(emptyLogWithTierFencedSegments.tierPartitionState).thenReturn(tierPartitionState_3)

    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    when(exceptionalTierObjectStore.deleteSegment(any())).thenThrow(
      new TierObjectStoreRetriableException("test exception", new Throwable("test exception from object store")))
  }

  @After
  def teardown(): Unit = {
    tmpFile.delete()
  }

  @Test
  def testCollectDeletableSegments(): Unit = {
    val state = CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0))
    val future = state.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[InitiateDelete], nextState.getClass)

    val initiateDelete = nextState.asInstanceOf[InitiateDelete]
    assertTrue(initiateDelete.toDelete.nonEmpty)
  }

  @Test
  def testCollectFencedDeletableSegments(): Unit = {
    val state = CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0))
    val future = state.transition(topicIdPartition_3, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[InitiateDelete], nextState.getClass)

    val initiateDelete = nextState.asInstanceOf[InitiateDelete]
    assertTrue(initiateDelete.toDelete.nonEmpty)
    val expectedObjectIds = fencedSegments.map(fs => fs.objectId()).toSet
    val deletableObjectIds = initiateDelete.toDelete.map(e => e.objectMetadata.objectId()).toSet
    assertEquals(expectedObjectIds, deletableObjectIds)
  }

  @Test
  def testCollectDeletableSegmentsEmptyLog(): Unit = {
    val state = CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0))
    val future = state.transition(topicIdPartition_2, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[CollectDeletableSegments], nextState.getClass)
  }

  @Test
  def testInitiateDelete(): Unit = {
    val toDelete = logWithTieredSegments.tieredLogSegments.take(3).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val initiateDelete = InitiateDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])
    assertEquals(0, initiateDelete.getNextSegmentDelay(time.hiResClockMs()))

    val future = initiateDelete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[Delete], nextState.getClass)

    val delete = nextState.asInstanceOf[Delete]
    assertEquals(toDelete.size, delete.toDelete.size)

    val firstSegment = toDelete.head
    val expected = new TierSegmentDeleteInitiate(topicIdPartition_1, 0, firstSegment.objectMetadata.objectId)
    verify(tierTopicManager, times(1)).addMetadata(expected)
  }

  @Test
  def testInitiateDeleteGetNextSegmentDelay(): Unit = {
    val nowMs = time.hiResClockMs()
    val anyFenceDelayMs = 10
    val timeToDelete = Math.addExact(nowMs, anyFenceDelayMs)
    val toDelete = fencedSegments.map(new TierObjectStore.ObjectMetadata(_)).map(DeleteObjectMetadata(_, Some(timeToDelete)))
    val initiateDelete = InitiateDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])

    time.sleep(4)
    var elapsedTime = 4
    var expectedDelay = Math.subtractExact(anyFenceDelayMs, elapsedTime)
    assertEquals(expectedDelay, initiateDelete.getNextSegmentDelay(time.hiResClockMs()))

    time.sleep(8)
    elapsedTime += 8
    expectedDelay = Math.subtractExact(anyFenceDelayMs, elapsedTime)
    assertEquals(expectedDelay, initiateDelete.getNextSegmentDelay(time.hiResClockMs()))
  }

  @Test
  def testInitiateDeleteWithDelay(): Unit = {
    val task = new DeletionTask(ctx.subContext(), topicIdPartition_3, logCleanupIntervalMs = 5,
      CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0)))

    val timeBeforeTransitionMs = time.hiResClockMs()
    val futureTask = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result = Await.ready(futureTask, 1 second)
    val nextTask = result.value.get.get

    assertEquals(classOf[InitiateDelete], nextTask.state.getClass)
    assertTrue(task.pausedUntil.isDefined)
    assertEquals(Instant.ofEpochMilli(timeBeforeTransitionMs).plusMillis(DeletionTask.FencedSegmentDeleteDelayMs), task.pausedUntil.get)
  }

  @Test
  def testDelete(): Unit = {
    val toDelete = logWithTieredSegments.tieredLogSegments.take(3).map(_.metadata).map(DeleteObjectMetadata(_, Some(time.milliseconds())))
    val delete = Delete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])
    val future = delete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[CompleteDelete], nextState.getClass)

    val completeDelete = nextState.asInstanceOf[CompleteDelete]
    assertEquals(toDelete.size, completeDelete.toDelete.size)

    verify(tierObjectStore, times(1)).deleteSegment(toDelete.head.objectMetadata)
  }

  @Test
  def testDeleteSegmentTierObjectStoreThrows(): Unit = {
    val task = new DeletionTask(ctx.subContext(), topicIdPartition_1, logCleanupIntervalMs = 5,
      CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0)))

    var futureTask = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    var result = Await.ready(futureTask, 1 second)
    var nextTask = result.value.get.get
    assertEquals(classOf[InitiateDelete], nextTask.state.getClass)

    futureTask = nextTask.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    result = Await.ready(futureTask, 1 second)
    nextTask = result.value.get.get
    assertEquals(classOf[Delete], nextTask.state.getClass)

    // Allow task retry when TierObjectStoreRetriableException is thrown on object delete
    futureTask = nextTask.transition(time, tierTopicManager, exceptionalTierObjectStore, replicaManager)
    result = Await.ready(futureTask, 1 second)
    nextTask = result.value.get.get
    assertEquals(classOf[Delete], nextTask.state.getClass)

    // Delete completes when object delete success from TierObjectStore
    futureTask = nextTask.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    result = Await.ready(futureTask, 1 second)
    nextTask = result.value.get.get
    assertEquals(classOf[CompleteDelete], nextTask.state.getClass)
  }

  @Test
  def testFencedDeleteSegmentTierObjectStoreThrows(): Unit = {
    val task = new DeletionTask(ctx.subContext(), topicIdPartition_3, logCleanupIntervalMs = 5,
      CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0)))

    var futureTask = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    var result = Await.ready(futureTask, 1 second)
    var nextTask = result.value.get.get
    assertEquals(classOf[InitiateDelete], nextTask.state.getClass)

    futureTask = nextTask.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    result = Await.ready(futureTask, 1 second)
    nextTask = result.value.get.get
    assertEquals(classOf[Delete], nextTask.state.getClass)

    // Allow task retry when TierObjectStoreRetriableException is thrown on object delete
    futureTask = nextTask.transition(time, tierTopicManager, exceptionalTierObjectStore, replicaManager)
    result = Await.ready(futureTask, 1 second)
    nextTask = result.value.get.get
    assertEquals(classOf[Delete], nextTask.state.getClass)

    // Delete completes when object delete success from TierObjectStore
    futureTask = nextTask.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    result = Await.ready(futureTask, 1 second)
    nextTask = result.value.get.get
    assertEquals(classOf[CompleteDelete], nextTask.state.getClass)
  }

  @Test
  def testCompleteDelete(): Unit = {
    val toDelete = logWithTieredSegments.tieredLogSegments.take(3).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val completeDelete = CompleteDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])
    val future = completeDelete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[InitiateDelete], nextState.getClass)

    val initiateDelete = nextState.asInstanceOf[InitiateDelete]
    assertEquals(toDelete.tail, initiateDelete.toDelete)

    val firstSegment = toDelete.head
    val expected = new TierSegmentDeleteComplete(topicIdPartition_1, 0, firstSegment.objectMetadata.objectId)
    verify(tierTopicManager, times(1)).addMetadata(expected)
  }

  @Test
  def testCompleteAllDeletes(): Unit = {
    val toDelete = logWithTieredSegments.tieredLogSegments.take(1).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val completeDelete = CompleteDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])
    val future = completeDelete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[CollectDeletableSegments], nextState.getClass)
  }

  @Test
  def testDeletePartitionWithNoTieredSegments(): Unit = {
    val collectDeletableSegments = CollectDeletableSegments(DeletedPartitionMetadata(List.empty))
    val future = collectDeletableSegments.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[PartitionDeleteComplete], nextState.getClass)
  }

  @Test
  def testDeleteTaskHandleUnknownExceptionDuringStateTransition(): Unit = {
    val state = mock(classOf[State])
    val ftr: Future[State] = Future.failed(new Throwable("test exception"))
    when(state.transition(topicIdPartition_3, replicaManager, tierTopicManager, tierObjectStore, time)).thenReturn(ftr)

    val task = new DeletionTask(ctx.subContext(), topicIdPartition_3, logCleanupIntervalMs = 5, state)
    val futureTask = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result = Await.ready(futureTask, 1 second)
    val nextTask = result.value.get.get

    assertEquals(task, nextTask)
    assertTrue(nextTask.isErrorState)
    assertTrue(nextTask.ctx.isCancelled)
  }


  private def tieredLogSegment(topicIdPartition: TopicIdPartition, baseOffset: Long, endOffset: Long): TierLogSegment = {
    val segment = mock(classOf[TierLogSegment])
    val size = 200
    val state = TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE
    val metadata = new TierObjectMetadata(topicIdPartition, 0, UUID.randomUUID,
      baseOffset, endOffset, time.milliseconds(), size, state, true, false, true)

    when(segment.startOffset).thenReturn(baseOffset)
    when(segment.endOffset).thenReturn(endOffset)
    when(segment.nextOffset).thenReturn(endOffset + 1)
    when(segment.metadata).thenReturn(new TierObjectStore.ObjectMetadata(metadata))
    when(segment.maxTimestamp).thenReturn(time.milliseconds())

    segment
  }

  private def mockLogSegment(tmpFile: File, baseOffset: Long, endOffset: Long): LogSegment = {
    val offsetIndex = mock(classOf[OffsetIndex])
    when(offsetIndex.file).thenReturn(tmpFile)

    val timeIndex = mock(classOf[TimeIndex])
    when(timeIndex.file).thenReturn(tmpFile)

    val fileRecords = mock(classOf[FileRecords])
    when(fileRecords.file).thenReturn(tmpFile)

    val logSegment = mock(classOf[LogSegment])
    when(logSegment.readNextOffset).thenReturn(endOffset + 1)
    when(logSegment.baseOffset).thenReturn(baseOffset)
    when(logSegment.largestTimestamp).thenReturn(time.milliseconds())
    when(logSegment.size).thenReturn(1000)
    when(logSegment.log).thenReturn(fileRecords)
    when(logSegment.offsetIndex).thenReturn(offsetIndex)
    when(logSegment.timeIndex).thenReturn(timeIndex)

    logSegment
  }

  private def mockTierPartitionState(leaderEpoch: Int, fencedSegments: List[TierObjectMetadata] = List.empty[TierObjectMetadata]): TierPartitionState = {
    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.tierEpoch).thenReturn(leaderEpoch)
    when(tierPartitionState.fencedSegments()).thenReturn(fencedSegments.asJava)
    tierPartitionState
  }

  private def mockAbstractLog(tieredSegments: List[TierLogSegment], localSegments: List[LogSegment]): AbstractLog = {
    val log = mock(classOf[AbstractLog])
    when(log.tieredLogSegments).thenReturn(tieredSegments)
    when(log.tierableLogSegments).thenReturn(localSegments)
    log
  }
}
