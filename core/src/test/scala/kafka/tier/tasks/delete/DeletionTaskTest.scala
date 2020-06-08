/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import java.io.File
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.{Properties, UUID}
import java.util.Optional

import kafka.cluster.Partition
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
import kafka.tier.state.OffsetAndEpoch
import kafka.tier.state.TierPartitionStatus
import kafka.tier.store.TierObjectStore.ObjectMetadata
import kafka.tier.exceptions.{TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.utils.TestUtils
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.CloseableIterator
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class DeletionTaskTest {
  val topic1 = "foo-0"
  val topicIdPartition_1 = new TopicIdPartition(topic1, UUID.randomUUID, 0)
  var tierPartitionState_1 = mockTierPartitionState(0)
  val partition_1 = mock(classOf[Partition])

  val topic2 = "foo-1"
  val topicIdPartition_2 = new TopicIdPartition(topic2, UUID.randomUUID, 0)
  var tierPartitionState_2 = mockTierPartitionState(0)
  val partition_2 = mock(classOf[Partition])

  val topic3 = "foo-2"
  val topicIdPartition_3 = new TopicIdPartition(topic3, UUID.randomUUID, 0)
  val fencedSegments =  List(new TierObjectMetadata(topicIdPartition_3, 0, UUID.randomUUID, 100L, 3252334L, 1000L,
      102, TierObjectMetadata.State.SEGMENT_FENCED, true, false, false),
    new TierObjectMetadata(topicIdPartition_3, 0, UUID.randomUUID, 4252334L, 5252334L, 5000L,
      102, TierObjectMetadata.State.SEGMENT_FENCED, true, false, false))
  var tierPartitionState_3 = mockTierPartitionState(0, fencedSegments)
  val partition_3 = mock(classOf[Partition])

  val topic4 = "foo-3"
  val topicIdPartition_4 = new TopicIdPartition(topic4, UUID.randomUUID, 0)
  var tierPartitionState_4 = mockTierPartitionState(0)
  val partition_4 = mock(classOf[Partition])

  val ctx = CancellationContext.newContext()
  val tierTopicManager = mock(classOf[TierTopicManager])
  val tierObjectStore = mock(classOf[TierObjectStore])
  val exceptionalTierObjectStore = mock(classOf[TierObjectStore])
  val replicaManager = mock(classOf[ReplicaManager])
  val time = new MockTime()
  val tmpFile = TestUtils.tempFile()
  var logWithTieredSegments: AbstractLog = _
  var logWithTieredSegments_2: AbstractLog = _
  var emptyLog: AbstractLog = _
  var emptyLogWithTierFencedSegments: AbstractLog = _
  @Before
  def setup(): Unit = {
    var baseOffset = 100L
    val tieredSegments = mutable.ListBuffer[TierLogSegment]()
    val tieredSegments_2 = mutable.ListBuffer[TierLogSegment]()
    val localSegments = mutable.ListBuffer[LogSegment]()

    for (_ <- 0 to 15) {
      val segment = tieredLogSegment(topicIdPartition_1, baseOffset, baseOffset + 50)
      val segment_2 = tieredLogSegment(topicIdPartition_4, baseOffset, baseOffset + 50)
      baseOffset += 51
      tieredSegments += segment
      tieredSegments_2 += segment_2
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
    logWithTieredSegments_2 = mockAbstractLog(tieredSegments_2.toList, localSegments.toList)
    when(logWithTieredSegments.config).thenReturn(logConfig)
    when(logWithTieredSegments_2.config).thenReturn(logConfig)
    when(logWithTieredSegments.logStartOffset).thenReturn(0L)
    when(logWithTieredSegments_2.logStartOffset).thenReturn(0L)
    doAnswer(new Answer[Any] {
      override def answer(invocation: InvocationOnMock): Any = {
        val newStartOffset = invocation.getArgument(0).asInstanceOf[Long]
        if (logWithTieredSegments.logStartOffset < newStartOffset)
          when(logWithTieredSegments.logStartOffset).thenReturn(newStartOffset)
      }
    }).when(logWithTieredSegments).maybeIncrementLogStartOffset(any(), any())

    when(logWithTieredSegments.tieredLogSegments).thenAnswer(new Answer[CloseableIterator[TierLogSegment]] {
      override def answer(invocation: InvocationOnMock): CloseableIterator[TierLogSegment] = {
        CloseableIterator.wrap[TierLogSegment](tieredSegments.iterator.asJava)
      }
    })

    emptyLog = mockAbstractLog(List.empty, List.empty)
    when(emptyLog.config).thenReturn(logConfig)
    when(emptyLog.tieredLogSegments).thenAnswer(new Answer[CloseableIterator[TierLogSegment]] {
      override def answer(invocation: InvocationOnMock): CloseableIterator[TierLogSegment] = {
        CloseableIterator.wrap[TierLogSegment](List.empty[TierLogSegment].iterator.asJava)
      }
    })

    emptyLogWithTierFencedSegments = mockAbstractLog(List.empty, List.empty)
    when(emptyLogWithTierFencedSegments.config).thenReturn(logConfig)
    when(emptyLogWithTierFencedSegments.tieredLogSegments).thenAnswer(new Answer[CloseableIterator[TierLogSegment]] {
      override def answer(invocation: InvocationOnMock): CloseableIterator[TierLogSegment] = {
        CloseableIterator.wrap[TierLogSegment](List.empty[TierLogSegment].iterator.asJava)
      }
    })

    when(replicaManager.getLog(topicIdPartition_1.topicPartition)).thenReturn(Some(logWithTieredSegments))
    when(replicaManager.getLog(topicIdPartition_2.topicPartition)).thenReturn(Some(emptyLog))
    when(replicaManager.getLog(topicIdPartition_3.topicPartition)).thenReturn(Some(emptyLogWithTierFencedSegments))
    when(replicaManager.getLog(topicIdPartition_4.topicPartition)).thenReturn(Some(logWithTieredSegments_2))

    when(replicaManager.getPartitionOrError(topicIdPartition_1.topicPartition(), expectLeader = true)).thenReturn(Right(partition_1))
    when(replicaManager.getPartitionOrError(topicIdPartition_2.topicPartition(), expectLeader = true)).thenReturn(Right(partition_2))
    when(replicaManager.getPartitionOrError(topicIdPartition_3.topicPartition(), expectLeader = true)).thenReturn(Right(partition_3))
    when(replicaManager.getPartitionOrError(topicIdPartition_4.topicPartition(), expectLeader = true)).thenReturn(Right(partition_4))

    when(partition_1.log).thenReturn(Some(logWithTieredSegments))
    when(partition_2.log).thenReturn(Some(emptyLog))
    when(partition_3.log).thenReturn(Some(emptyLogWithTierFencedSegments))
    when(partition_4.log).thenReturn(Some(logWithTieredSegments_2))

    when(partition_1.getIsUncleanLeader).thenReturn(false)
    when(partition_2.getIsUncleanLeader).thenReturn(false)
    when(partition_3.getIsUncleanLeader).thenReturn(false)
    when(partition_4.getIsUncleanLeader).thenReturn(true)

    when(logWithTieredSegments.tierPartitionState).thenReturn(tierPartitionState_1)
    when(emptyLog.tierPartitionState).thenReturn(tierPartitionState_2)
    when(emptyLogWithTierFencedSegments.tierPartitionState).thenReturn(tierPartitionState_3)
    when(logWithTieredSegments_2.tierPartitionState).thenReturn(tierPartitionState_4)

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
    when(tierPartitionState_1.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(99L, Optional.empty()))
    val state = CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0))
    val future = state.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get
    assertEquals(classOf[InitiateDelete], nextState.getClass)

    val initiateDelete = nextState.asInstanceOf[InitiateDelete]
    assertTrue(initiateDelete.toDelete.nonEmpty)
  }

  @Test
  def testCollectDeletableSegmentsWithUncleanLeaderFlag(): Unit = {
    val state = CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0))
    var future = state.transition(topicIdPartition_4, replicaManager, tierTopicManager, tierObjectStore, time)
    future.onComplete {
      case Success(nextState) =>
        assert(assertion = false, s"Unexpected new state returned ${nextState.toString}. State transition should return a " +
          s"retriable exception for unclean partitions.")
      case Failure(e) =>
        e match {
          case e: TierMetadataRetriableException =>
            assertEquals(s"Unexpected cause for exception", true, e.getMessage.contains("requires recovery post"))
          case e: Exception =>
            assert(assertion = false, s"Unexpected exception returned by state transition for unclean partition " + e)
        }
    }
    // mark the partition clean. state transition must succeed now
    when(partition_4.getIsUncleanLeader).thenReturn(false)
    future = state.transition(topicIdPartition_4, replicaManager, tierTopicManager, tierObjectStore, time)
    future.onComplete {
      case Success(nextState) =>
        assertEquals(s"Unexpected new state returned ${nextState.toString}", classOf[InitiateDelete], nextState.getClass)
        val initiateDelete = nextState.asInstanceOf[InitiateDelete]
        assertTrue(initiateDelete.toDelete.nonEmpty)
      case Failure(e) =>
        assert(assertion = false, s"Unexpected exception returned by state transition for unclean partition " + e)
    }
  }

  @Test
  def testCollectFencedDeletableSegments(): Unit = {
    when(tierPartitionState_3.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(99L, Optional.empty()))
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
    val toDelete = tieredLogSegmentsList(logWithTieredSegments).take(3).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val initiateDelete = InitiateDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), Optional.of(new OffsetAndEpoch(99, Optional.of(1))), mutable.Queue.empty ++ toDelete)
    assertEquals(0, initiateDelete.getNextSegmentDelay(time.hiResClockMs()))

    val future = initiateDelete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[Delete], nextState.getClass)

    val delete = nextState.asInstanceOf[Delete]
    assertEquals(toDelete.size, delete.toDelete.size)
    assertEquals(99L, delete.stateOffsetAndEpoch.get().offset())

    val firstSegment = toDelete.head
    val expected = new TierSegmentDeleteInitiate(topicIdPartition_1, 0, firstSegment.objectMetadata.objectId,
      new OffsetAndEpoch(99L, Optional.of(1)))
    verify(tierTopicManager, times(1)).addMetadata(expected)
  }

  @Test
  def testInitiateDeleteGetNextSegmentDelay(): Unit = {
    val nowMs = time.hiResClockMs()
    val anyFenceDelayMs = 10
    val timeToDelete = Math.addExact(nowMs, anyFenceDelayMs)
    val toDelete = fencedSegments.map(new TierObjectStore.ObjectMetadata(_)).map(DeleteObjectMetadata(_, Some(timeToDelete)))
    val initiateDelete = InitiateDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), Optional.of(new OffsetAndEpoch(1, Optional.of(0))),
      mutable.Queue.empty ++= toDelete)

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

    when(tierPartitionState_3.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(33L, Optional.empty()))

    val timeBeforeTransitionMs = time.hiResClockMs()
    val futureTask = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result = Await.ready(futureTask, 1 second)
    val nextTask = result.value.get.get

    assertEquals(classOf[InitiateDelete], nextTask.state.getClass)
    assertEquals(33L, nextTask.state.asInstanceOf[InitiateDelete].stateOffsetAndEpoch.get().offset())
    assertTrue(task.pausedUntil.isDefined)
    assertEquals(Instant.ofEpochMilli(timeBeforeTransitionMs).plusMillis(DeletionTask.FencedSegmentDeleteDelayMs), task.pausedUntil.get)
  }

  @Test
  def testDelete(): Unit = {
    val toDelete = tieredLogSegmentsList(logWithTieredSegments).take(3).map(_.metadata).map(DeleteObjectMetadata(_, Some(time.milliseconds())))
    val delete = Delete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), Optional.of(new OffsetAndEpoch(1, Optional.of(0))), mutable.Queue.empty ++ toDelete)
    val future = delete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[CompleteDelete], nextState.getClass)
    assertEquals(1L, nextState.asInstanceOf[CompleteDelete].stateOffset.get().offset())

    val completeDelete = nextState.asInstanceOf[CompleteDelete]
    assertEquals(toDelete.size, completeDelete.toDelete.size)

    verify(tierObjectStore, times(1)).deleteSegment(toDelete.head.objectMetadata)
  }

  @Test
  def testDeleteSegmentTierObjectStoreThrows(): Unit = {
    when(tierPartitionState_1.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(99L, Optional.empty()))
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
    assertEquals(99L, nextTask.state.asInstanceOf[CompleteDelete].stateOffset.get().offset())
  }

  @Test
  def testFencedDeleteSegmentTierObjectStoreThrows(): Unit = {
    when(tierPartitionState_3.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(99L, Optional.empty()))

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
    val toDelete = tieredLogSegmentsList(logWithTieredSegments).take(3).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val completeDelete = CompleteDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), Optional.of(new OffsetAndEpoch(99L, Optional.empty())), mutable.Queue.empty ++ toDelete)
    val future = completeDelete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[InitiateDelete], nextState.getClass)

    val initiateDelete = nextState.asInstanceOf[InitiateDelete]
    assertEquals(toDelete.tail, initiateDelete.toDelete)

    assertEquals(99L, nextState.asInstanceOf[InitiateDelete].stateOffsetAndEpoch.get().offset())

    val firstSegment = toDelete.head
    val expected = new TierSegmentDeleteComplete(topicIdPartition_1, 0, firstSegment.objectMetadata.objectId, new OffsetAndEpoch(99L, Optional.empty()))
    verify(tierTopicManager, times(1)).addMetadata(expected)
  }

  @Test
  def testCompleteAllDeletes(): Unit = {
    val toDelete = tieredLogSegmentsList(logWithTieredSegments).take(1).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val completeDelete = CompleteDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), Optional.of(new OffsetAndEpoch(1, Optional.of(0))),
      mutable.Queue.empty ++= toDelete)
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

  @Test
  def testDeleteTaskHandleFailedAwaitCorrectEpoch(): Unit = {
    val toDelete = tieredLogSegmentsList(logWithTieredSegments).take(3).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val initiateDelete = InitiateDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 1), Optional.of(new OffsetAndEpoch(99, Optional.of(1))), mutable.Queue.empty ++ toDelete)
    assertEquals(0, initiateDelete.getNextSegmentDelay(time.hiResClockMs()))
    val task = new DeletionTask(ctx.subContext(), topicIdPartition_3, logCleanupIntervalMs = 5, initiateDelete)

    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.FAILED))

    val future = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result = Await.result(future, 1 second)
    assertTrue(result.state.isInstanceOf[FailedState])

    when(tierPartitionState_3.status).thenReturn(TierPartitionStatus.ONLINE)
    // stale epoch, we will need to wait for updated epoch
    when(tierPartitionState_3.tierEpoch).thenReturn(0)

    val futureTask2 = result.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result2 = Await.result(futureTask2, 1 second)
    assertFalse(result2.isErrorState)
    assertFalse(result2.ctx.isCancelled)
    assertTrue(result2.state.isInstanceOf[FailedState])
  }

  @Test
  def testDeleteTaskHandleFailedNewLeader(): Unit = {
    val toDelete = tieredLogSegmentsList(logWithTieredSegments).take(3).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val initiateDelete = InitiateDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 1), Optional.of(new OffsetAndEpoch(99, Optional.of(1))), mutable.Queue.empty ++ toDelete)
    assertEquals(0, initiateDelete.getNextSegmentDelay(time.hiResClockMs()))
    val task = new DeletionTask(ctx.subContext(), topicIdPartition_3, logCleanupIntervalMs = 5, initiateDelete)

    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.FAILED))

    val future = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result = Await.result(future, 1 second)
    assertTrue(result.state.isInstanceOf[FailedState])

    when(tierPartitionState_3.status).thenReturn(TierPartitionStatus.ONLINE)
    // later epoch, task should be cancelled
    when(tierPartitionState_3.tierEpoch).thenReturn(2)

    val futureTask2 = result.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result2 = Await.result(futureTask2, 1 second)
    assertFalse(result2.isErrorState)
    assertTrue(result2.state.isInstanceOf[FailedState])
    assertTrue("task should now be cancelled", result2.ctx.isCancelled)
  }

  @Test
  def testDeleteTaskHandleFailedExceptionAndRecovery(): Unit = {
    val toDelete = tieredLogSegmentsList(logWithTieredSegments).take(3).map(_.metadata).map(DeleteObjectMetadata(_, None))
    val initiateDelete = InitiateDelete(DeleteAsLeaderMetadata(replicaManager, leaderEpoch = 0), Optional.of(new OffsetAndEpoch(99, Optional.of(1))), mutable.Queue.empty ++ toDelete)
    assertEquals(0, initiateDelete.getNextSegmentDelay(time.hiResClockMs()))
    val task = new DeletionTask(ctx.subContext(), topicIdPartition_3, logCleanupIntervalMs = 5, initiateDelete)

    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.FAILED))

    val future = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result = Await.result(future, 1 second)
    assertTrue(result.state.isInstanceOf[FailedState])

    when(tierPartitionState_3.status).thenReturn(TierPartitionStatus.ONLINE)
    when(tierPartitionState_3.tierEpoch).thenReturn(0)
    when(tierPartitionState_3.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(99L, Optional.empty()))

    val futureTask2 = result.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result2 = Await.result(futureTask2, 1 second)
    assertFalse(result2.isErrorState)
    assertFalse(result2.ctx.isCancelled)
    assertTrue(result2.state.isInstanceOf[CollectDeletableSegments])

    val futureTask3 = result2.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result3 = Await.result(futureTask3, 1 second)
    assertFalse(result3.isErrorState)
    assertFalse(result3.ctx.isCancelled)
    assertTrue(result3.state.isInstanceOf[InitiateDelete])
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
    when(segment.metadata).thenReturn(new ObjectMetadata(metadata))
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
  private def tieredLogSegmentsList(log: AbstractLog): List[TierLogSegment] = {
    val iterator = log.tieredLogSegments
    try {
      iterator.asScala.toList
    } finally {
      iterator.close()
    }
  }

  private def mockAbstractLog(tieredSegments: List[TierLogSegment], localSegments: List[LogSegment]): AbstractLog = {
    val log = mock(classOf[AbstractLog])
    when(log.tierableLogSegments).thenReturn(localSegments)
    log
  }
}
