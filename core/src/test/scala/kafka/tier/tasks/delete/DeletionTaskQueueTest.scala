/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.{Properties, UUID}

import kafka.log.{LogConfig, MergedLog, TierLogSegment}
import kafka.server.ReplicaManager
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.delete.DeletionTask.{CollectDeletableSegments, CompleteDelete, Delete, InitiateDelete}
import kafka.tier.topic.TierTopicAppender
import kafka.tier.{TierMetadataManager, TopicIdPartition}
import kafka.utils.MockTime
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.compat.java8.OptionConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DeletionTaskQueueTest {
  private val ctx = CancellationContext.newContext()
  private val time: Time = new MockTime()
  private val maxTasks = 3
  private val deletionTaskQueue = new DeletionTaskQueue(ctx, maxTasks, logCleanupIntervalMs = 5, time)

  @Test
  def testPollTaskOrdering(): Unit = {
    val partition_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val partition_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)
    val partition_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 0)
    val partition_4 = new TopicIdPartition("foo-4", UUID.randomUUID, 0)

    deletionTaskQueue.addTask(partition_1, 0)
    deletionTaskQueue.addTask(partition_2, 0)
    deletionTaskQueue.addTask(partition_3, 0)
    deletionTaskQueue.addTask(partition_4, 0)

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

    val task_1 = new DeletionTask(ctx.subContext(), partition_1, logCleanupIntervalMs = 5, CollectDeletableSegments(leaderEpoch = 0))
    val task_2 = new DeletionTask(ctx.subContext(), partition_2, logCleanupIntervalMs = 5, CollectDeletableSegments(leaderEpoch = 1))
    val task_3 = new DeletionTask(ctx.subContext(), partition_3, logCleanupIntervalMs = 5, Delete(leaderEpoch = 0, mock(classOf[mutable.Queue[TierObjectStore.ObjectMetadata]])))
    val task_4 = new DeletionTask(ctx.subContext(), partition_4, logCleanupIntervalMs = 5, CompleteDelete(leaderEpoch = 0, mock(classOf[mutable.Queue[TierObjectStore.ObjectMetadata]])))
    val task_5 = new DeletionTask(ctx.subContext(), partition_5, logCleanupIntervalMs = 5, InitiateDelete(leaderEpoch = 0, mock(classOf[mutable.Queue[TierObjectStore.ObjectMetadata]])))

    task_1.lastProcessedMs = Some(time.hiResClockMs() - 100)
    task_2.lastProcessedMs = Some(time.hiResClockMs() - 300)
    task_3.lastProcessedMs = Some(time.hiResClockMs() + 100)
    task_4.lastProcessedMs = Some(time.hiResClockMs() - 200)
    task_5.lastProcessedMs = Some(time.hiResClockMs() + 200)

    val taskList = ListSet(task_1, task_2, task_3, task_4, task_5)
    val sortedTasks = deletionTaskQueue.sortTasks(taskList).toList
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
    val replicaManager = mock(classOf[ReplicaManager])
    val tierMetadataManager = mock(classOf[TierMetadataManager])
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
    when(replicaManager.tierMetadataManager).thenReturn(tierMetadataManager)
    when(replicaManager.getLog(partition.topicPartition)).thenReturn(Some(log))

    // mock tier metadata manager
    when(tierMetadataManager.tierPartitionState(partition)).thenReturn(Some(tierPartitionState).asJava)

    // mock Log
    when(log.tieredLogSegments).thenReturn(Seq(tieredLogSegment))
    when(log.config).thenReturn(logConfig)
    when(log.logStartOffset).thenReturn(100)

    // mock segment
    when(tieredLogSegment.maxTimestamp).thenReturn(time.hiResClockMs())
    when(tieredLogSegment.size).thenReturn(100)
    when(tieredLogSegment.endOffset).thenReturn(50)
    when(tieredLogSegment.metadata).thenReturn(tierObjectMetadata)

    // mock tier partition state
    when(tierPartitionState.tierEpoch).thenReturn(0)

    // mock tier object metadata
    when(tierObjectMetadata.topicIdPartition).thenReturn(partition)
    when(tierObjectMetadata.objectId).thenReturn(UUID.randomUUID)

    deletionTaskQueue.addTask(partition, 0)

    // 1. `CollectDeletableSegments`
    var task = deletionTaskQueue.poll().get.head
    assertEquals(classOf[CollectDeletableSegments], task.state.getClass)
    assertEquals(None, task.pausedUntil)

    // 2. Transition to `InitiateDelete`
    var future = task.transition(time, tierTopicAppender, tierObjectStore, replicaManager)
    task = Await.result(future, 1.seconds)
    assertEquals(classOf[InitiateDelete], task.state.getClass)
    assertEquals(None, task.pausedUntil)
    deletionTaskQueue.done(task)

    // 3. Transition to `Delete`. The task should be paused for configured log file delete delay.
    val timeBeforeTransitionMs = time.hiResClockMs()
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
}
