/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.archive

import java.util.UUID

import kafka.tier.TopicIdPartition
import kafka.tier.fetcher.CancellationContext
import kafka.tier.tasks.{StartLeadership, StopLeadership}
import kafka.utils.MockTime
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class ArchiverTaskQueueTest {
  private val ctx = CancellationContext.newContext()
  private val time = new MockTime()
  private val lagMap = mutable.Map[TopicIdPartition, Option[Long]]()
  private val maxTasks = 3
  private val archiverTaskQueue = new ArchiverTaskQueue(ctx, maxTasks, time, lagFn, ArchiverMetrics(None, None))

  @Test
  def testRemoveTask(): Unit = {
    val partition_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val partition_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)
    val partition_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 0)
    val partition_4 = new TopicIdPartition("foo-4", UUID.randomUUID, 0)

    addTask(partition_1, 0)
    addTask(partition_2, 0)
    addTask(partition_3, 0)
    addTask(partition_4, 0)

    removeTask(partition_1)
    archiverTaskQueue.withAllTasks { tasks =>
      assertEquals(Set(partition_2, partition_3, partition_4), tasks.map(_.topicIdPartition))
    }

    removeTask(partition_3)
    archiverTaskQueue.withAllTasks { tasks =>
      assertEquals(Set(partition_2, partition_4), tasks.map(_.topicIdPartition))
    }
  }

  @Test
  def testPollTaskOrdering(): Unit = {
    val partition_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val partition_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)
    val partition_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 0)
    val partition_4 = new TopicIdPartition("foo-4", UUID.randomUUID, 0)

    updateLag(partition_1, 5)
    updateLag(partition_2, 10)
    updateLag(partition_3, 1)
    updateLag(partition_4, 15)

    addTask(partition_1, 0)
    addTask(partition_2, 0)
    addTask(partition_3, 0)
    addTask(partition_4, 0)

    val tasks = archiverTaskQueue.poll().get
    assertEquals(lagSortedEligibleTasks.take(maxTasks), tasks.map(_.topicIdPartition).toList)
  }

  @Test
  def testPollTasksLessThanMax(): Unit = {
    val partition_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val partition_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)

    updateLag(partition_1, 5)
    updateLag(partition_2, 0)

    addTask(partition_1, 0)
    addTask(partition_2, 0)

    assertEquals(Set(partition_1), archiverTaskQueue.poll().get.map(_.topicIdPartition))
  }

  @Test
  def testZeroLag(): Unit = {
    val partition_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val partition_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)
    val partition_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 0)
    val partition_4 = new TopicIdPartition("foo-4", UUID.randomUUID, 0)

    updateLag(partition_1, 0)
    updateLag(partition_2, 0)
    updateLag(partition_3, 0)
    updateLag(partition_4, 0)

    addTask(partition_1, 0)
    addTask(partition_2, 0)
    addTask(partition_3, 0)
    addTask(partition_4, 0)

    assertEquals(None, archiverTaskQueue.poll())
  }

  @Test
  def testAddReplacesExistingTask(): Unit = {
    val partition_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val partition_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)
    val partition_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 0)
    val partition_4 = new TopicIdPartition("foo-4", UUID.randomUUID, 0)

    addTask(partition_1, 0)
    addTask(partition_2, 0)
    addTask(partition_3, 0)
    addTask(partition_4, 0)

    // add partition_1 with epoch=1 and partition_3 with epoch=2
    addTask(partition_1, 1)
    addTask(partition_3, 2)

    archiverTaskQueue.withAllTasks { tasks =>
      assertEquals(1, tasks.find(_.topicIdPartition == partition_1).get.state.leaderEpoch)
      assertEquals(0, tasks.find(_.topicIdPartition == partition_2).get.state.leaderEpoch)
      assertEquals(2, tasks.find(_.topicIdPartition == partition_3).get.state.leaderEpoch)
      assertEquals(0, tasks.find(_.topicIdPartition == partition_4).get.state.leaderEpoch)
      assertEquals(4, tasks.size)
    }
  }

  private def lagFn(task: ArchiveTask): Option[Long] = {
    lagMap(task.topicIdPartition)
  }

  private def updateLag(topicIdPartition: TopicIdPartition, lag: Long): Unit = {
    lagMap(topicIdPartition) = Some(lag)
  }

  private def lagSortedEligibleTasks: List[TopicIdPartition] = {
    lagMap.toList
      .map { case (topicPartition, lag) =>
        (topicPartition, lag.getOrElse(0L))
      }.sortBy { case (_, lag) =>
        lag
      }.filter { case (_, lag) =>
        lag != 0L
      }.map { case (topicIdPartition, _) =>
        topicIdPartition
      }
  }

  private def addTask(topicIdPartition: TopicIdPartition, leaderEpoch: Int): Unit = {
    archiverTaskQueue.maybeAddTask(StartLeadership(topicIdPartition, leaderEpoch))
  }

  private def removeTask(topicIdPartition: TopicIdPartition): Unit = {
    archiverTaskQueue.maybeRemoveTask(StopLeadership(topicIdPartition))
  }
}
