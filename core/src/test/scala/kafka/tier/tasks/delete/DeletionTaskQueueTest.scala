/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.{Properties, UUID}

import kafka.log.{LogConfig, MergedLog, TierLogSegment}
import kafka.server.ReplicaManager
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.StartLeadership
import kafka.tier.tasks.delete.DeletionTask._
import kafka.tier.topic.TierTopicAppender
import kafka.tier.TopicIdPartition
import kafka.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DeletionTaskQueueTest {
  private val ctx = CancellationContext.newContext()
  private val time: Time = new MockTime()
  private val maxTasks = 3
  val replicaManager = mock(classOf[ReplicaManager])
  private val deletionTaskQueue = new DeletionTaskQueue(ctx, maxTasks, logCleanupIntervalMs = 5, time, replicaManager)

  @Test
  def testPollTaskOrdering(): Unit = {
    val partition_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val partition_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)
    val partition_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 0)
    val partition_4 = new TopicIdPartition("foo-4", UUID.randomUUID, 0)

    deletionTaskQueue.maybeAddTask(StartLeadership(partition_1, 0))
    deletionTaskQueue.maybeAddTask(StartLeadership(partition_2, 0))
    deletionTaskQueue.maybeAddTask(StartLeadership(partition_3, 0))
    deletionTaskQueue.maybeAddTask(StartLeadership(partition_4, 0))

    updateLastProcessedMs(partition_1, 5)
    updateLastProcessedMs(partition_2, 10)
    updateLastProcessedMs(partition_3, 1)
    updateLastProcessedMs(partition_4, 15)

    val tasks = deletionTaskQueue.poll().get
    assertEquals(sortedTasks.take(maxTasks), tasks.map(_.topicIdPartition).toList)
  }

  @Test
  def testSortTasks(): Unit = {
    val partition_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val partition_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)
    val partition_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 0)
    val partition_4 = new TopicIdPartition("foo-4", UUID.randomUUID, 0)
    val partition_5 = new TopicIdPartition("foo-5", UUID.randomUUID, 0)

    val task_1 = new DeletionTask(ctx.subContext(), partition_1, logCleanupIntervalMs = 5, CollectDeletableSegments(retentionMetadata(leaderEpoch = 0)))
    val task_2 = new DeletionTask(ctx.subContext(), partition_2, logCleanupIntervalMs = 5, CollectDeletableSegments(retentionMetadata(leaderEpoch = 1)))
    val task_3 = new DeletionTask(ctx.subContext(), partition_3, logCleanupIntervalMs = 5, Delete(retentionMetadata(leaderEpoch = 0), mock(classOf[mutable.Queue[DeleteObjectMetadata]])))
    val task_4 = new DeletionTask(ctx.subContext(), partition_4, logCleanupIntervalMs = 5, CompleteDelete(retentionMetadata(leaderEpoch = 0), mock(classOf[mutable.Queue[DeleteObjectMetadata]])))
    val task_5 = new DeletionTask(ctx.subContext(), partition_5, logCleanupIntervalMs = 5, InitiateDelete(retentionMetadata(leaderEpoch = 0), mock(classOf[mutable.Queue[DeleteObjectMetadata]])))

    task_1.lastProcessedMs = Some(time.hiResClockMs() - 100)
    task_2.lastProcessedMs = Some(time.hiResClockMs() - 300)
    task_3.lastProcessedMs = Some(time.hiResClockMs() + 100)
    task_4.lastProcessedMs = Some(time.hiResClockMs() - 200)
    task_5.lastProcessedMs = Some(time.hiResClockMs() + 200)

    val taskList = List(task_1, task_2, task_3, task_4, task_5)
    val sortedTasks = deletionTaskQueue.sortTasks(taskList)
    assertEquals(taskList.size, sortedTasks.size)

    // States other than `CollectDeletableSegments` are always prioritized first, i.e. task_3, task_4 and task_5
    assertEquals(Set(task_3, task_4, task_5), sortedTasks.take(3).toSet)

    // Tasks in `CollectDeletableSegments` are prioritized by `lastProcessedMs`, i.e. task_2 and then task_1
    assertEquals(task_2, sortedTasks(3))
    assertEquals(task_1, sortedTasks(4))
  }

  @Test
  def testStateTransitions(): Unit = {
    // The goal of this test is to step through state transitions, and in particular assert that the queue does not
    // return tasks until `fileDeleteDelayMs` time is elapsed before deleting the object from tiered storage.
    val tierTopicAppender = mock(classOf[TierTopicAppender])
    val tierObjectStore = mock(classOf[TierObjectStore])
    val tierObjectMetadata = mock(classOf[TierObjectStore.ObjectMetadata])

    val partition = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val tierPartitionState = mock(classOf[TierPartitionState])
    val log = mock(classOf[MergedLog])
    val tieredLogSegment = mock(classOf[TierLogSegment])
    val fileDeleteDelayMs = 19 * 1000L

    val properties = new Properties()
    properties.put(LogConfig.RetentionMsProp, "1")
    properties.put(LogConfig.FileDeleteDelayMsProp, fileDeleteDelayMs.toString)
    val logConfig = LogConfig(properties)

    // mock tier topic appender
    when(tierTopicAppender.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    // mock replica manager
    when(replicaManager.getLog(partition.topicPartition)).thenReturn(Some(log))

    // mock Log
    when(log.tieredLogSegments).thenReturn(Seq(tieredLogSegment))
    when(log.config).thenReturn(logConfig)
    when(log.logStartOffset).thenReturn(100)
    when(log.tierPartitionState).thenReturn(tierPartitionState)

    // mock segment
    when(tieredLogSegment.maxTimestamp).thenReturn(time.hiResClockMs())
    when(tieredLogSegment.size).thenReturn(100)
    when(tieredLogSegment.endOffset).thenReturn(50)
    when(tieredLogSegment.metadata).thenReturn(tierObjectMetadata)

    // mock tier partition state
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierPartitionState.fencedSegments()).thenReturn(List(
      new TierObjectMetadata(partition, 0, UUID.randomUUID, 100L, 3252334L, 1000L,
        102, TierObjectMetadata.State.SEGMENT_FENCED, true, false, false)
    ).asJava)

    // mock tier object metadata
    when(tierObjectMetadata.topicIdPartition).thenReturn(partition)
    when(tierObjectMetadata.objectId).thenReturn(UUID.randomUUID)

    deletionTaskQueue.maybeAddTask(StartLeadership(partition, 0))

    // 1. `CollectDeletableSegments`
    val nowMs = time.hiResClockMs()
    var task = deletionTaskQueue.poll().get.head
    assertEquals(classOf[CollectDeletableSegments], task.state.getClass)
    assertEquals(None, task.pausedUntil)

    // 2. Transition to `InitiateDelete`
    var future = task.transition(time, tierTopicAppender, tierObjectStore, replicaManager)
    task = Await.result(future, 1.seconds)
    assertEquals(classOf[InitiateDelete], task.state.getClass)
    // No delay for non-fenced segment delete so assert that the pauseUntil as same as current time
    assertEquals(Some(Instant.ofEpochMilli(nowMs)), task.pausedUntil)
    deletionTaskQueue.done(task)
    // move the time beyond now
    time.sleep(1)

    // 3. Transition to `Delete`. The task should be paused for configured log file delete delay.
    var timeBeforeTransitionMs = time.hiResClockMs()
    task = deletionTaskQueue.poll().get.head
    future = task.transition(time, tierTopicAppender, tierObjectStore, replicaManager)
    task = Await.result(future, 1.seconds)
    assertEquals(classOf[Delete], task.state.getClass)
    assertTrue(task.pausedUntil.isDefined)
    assertEquals(Instant.ofEpochMilli(timeBeforeTransitionMs).plusMillis(fileDeleteDelayMs), task.pausedUntil.get)
    deletionTaskQueue.done(task)

    // 4. polling would not return any tasks until file delete delay elapses
    assertTrue(deletionTaskQueue.poll().isEmpty)
    assertEquals(1, deletionTaskQueue.taskCount)
    time.sleep(fileDeleteDelayMs + 1)

    // 5. Transition to `CompleteDelete`
    timeBeforeTransitionMs = time.hiResClockMs()
    task = deletionTaskQueue.poll().get.head
    future = task.transition(time, tierTopicAppender, tierObjectStore, replicaManager)
    task = Await.result(future, 1.seconds)
    assertEquals(classOf[CompleteDelete], task.state.getClass)
    assertTrue(task.pausedUntil.isEmpty)
    deletionTaskQueue.done(task)

    // 6. Transition to `InitiateDelete` for fenced segment
    task = deletionTaskQueue.poll().get.head
    future = task.transition(time, tierTopicAppender, tierObjectStore, replicaManager)
    task = Await.result(future, 1.seconds)
    assertEquals(classOf[InitiateDelete], task.state.getClass)
    assertTrue(task.pausedUntil.isDefined)
    assertEquals(Some(Instant.ofEpochMilli(Math.addExact(nowMs, DeletionTask.FencedSegmentDeleteDelayMs))), task.pausedUntil)
    deletionTaskQueue.done(task)

    // 7. polling would not return any tasks until DeletionTask.FencedSegmentDeleteDelayMs elapses
    assertTrue(deletionTaskQueue.poll().isEmpty)
    assertEquals(1, deletionTaskQueue.taskCount)
    // Elapse remaining time to meet the DeletionTask.FencedSegmentDeleteDelayMs + 1, note that "fileDeleteDelayMs + 1"
    // was already spent to delete the non-fenced delete segment #4.
    time.sleep(DeletionTask.FencedSegmentDeleteDelayMs - (fileDeleteDelayMs + 1) + 1)

    // 8. Transition to `Delete`. The task should be paused for configured log file delete delay.
    timeBeforeTransitionMs = time.hiResClockMs()
    task = deletionTaskQueue.poll().get.head
    future = task.transition(time, tierTopicAppender, tierObjectStore, replicaManager)
    task = Await.result(future, 1.seconds)
    assertEquals(classOf[Delete], task.state.getClass)
    assertTrue(task.pausedUntil.isDefined)
    assertEquals(Instant.ofEpochMilli(timeBeforeTransitionMs).plusMillis(fileDeleteDelayMs), task.pausedUntil.get)
    deletionTaskQueue.done(task)

    // 9. polling would not return any tasks until file delete delay elapses
    assertTrue(deletionTaskQueue.poll().isEmpty)
    assertEquals(1, deletionTaskQueue.taskCount)
    time.sleep(fileDeleteDelayMs + 1)

    // 10. Transition to `CompleteDelete`
    timeBeforeTransitionMs = time.hiResClockMs()
    task = deletionTaskQueue.poll().get.head
    future = task.transition(time, tierTopicAppender, tierObjectStore, replicaManager)
    task = Await.result(future, 1.seconds)
    assertEquals(classOf[CompleteDelete], task.state.getClass)
    assertTrue(task.pausedUntil.isEmpty)
    deletionTaskQueue.done(task)
  }

  private def sortedTasks: List[TopicIdPartition] = {
    deletionTaskQueue.withAllTasks { tasks =>
      tasks.toList.sortBy(_.lastProcessedMs).map(_.topicIdPartition)
    }
  }

  private def updateLastProcessedMs(topicIdPartition: TopicIdPartition, lastProcessedMs: Long): Unit = {
    deletionTaskQueue.withAllTasks { tasks =>
      tasks.find(_.topicIdPartition == topicIdPartition).get.lastProcessedMs = Some(lastProcessedMs)
    }
  }

  private def retentionMetadata(leaderEpoch: Int): DeleteAsLeaderMetadata = {
    DeleteAsLeaderMetadata(replicaManager, leaderEpoch)
  }
}
