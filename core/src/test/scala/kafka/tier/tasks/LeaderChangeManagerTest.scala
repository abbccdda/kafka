/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tasks

import java.util.UUID
import java.util.concurrent.TimeUnit

import kafka.server.ReplicaManager
import kafka.tier.fetcher.CancellationContext
import kafka.tier.store.TierObjectStore
import kafka.tier.TopicIdPartition
import kafka.tier.topic.TierTopicAppender
import org.apache.kafka.common.utils.{MockTime, Time}
import org.junit.Assert._
import org.junit.{After, Test}

import scala.collection.immutable.ListSet
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

case class MockTask(ctx: CancellationContext,
                    topicIdPartition: TopicIdPartition,
                    leaderEpoch: Int) extends TierTask[MockTask](retryRateOpt = None) {
  override def transition(time: Time,
                          tierTopicAppender: TierTopicAppender,
                          tierObjectStore: TierObjectStore,
                          replicaManager: ReplicaManager,
                          maxRetryBackoffMs: Option[Int])(implicit ec: ExecutionContext): Future[MockTask] = {
    Future.successful(this)
  }
}

class LeaderChangeManagerTest {
  private val ctx = CancellationContext.newContext()
  private val time: Time = new MockTime()
  private val maxTasks = 3

  private val queue = new TierTaskQueue[MockTask](ctx.subContext(), maxTasks, time) {
    override protected[tasks] def sortTasks(tasks: ListSet[MockTask]): ListSet[MockTask] = {
      tasks
    }

    override protected[tasks] def newTask(topicIdPartition: TopicIdPartition, epoch: Int): MockTask = {
      MockTask(ctx.subContext(), topicIdPartition, epoch)
    }
  }
  private val leaderChangeManager = new LeaderChangeManager(ctx.subContext(), Seq(queue), time)

  @After
  def teardown(): Unit = {
    ctx.cancel()
  }

  @Test
  def testLeadershipOverrides(): Unit = {
    val tp1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val tp2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)

    // tp1 becomes leader with epoch=0
    leaderChangeManager.onBecomeLeader(tp1, 0)
    leaderChangeManager.process()

    // tp1 becomes leader with epoch=1; tp2 becomes leader with epoch=0 and then with epoch=1
    leaderChangeManager.onBecomeLeader(tp1, 1)
    leaderChangeManager.onBecomeLeader(tp2, 0)
    leaderChangeManager.onBecomeLeader(tp2, 1)
    leaderChangeManager.process()

    val task = queue.poll().get
    assertEquals(2, task.size)
    assertEquals(Set(tp1, tp2), task.map(_.topicIdPartition))
    assertEquals(List(1, 1), task.toList.map(_.leaderEpoch))
  }

  @Test
  def testLeadershipChangesCancelTasks(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    val tp1 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 1)
    leaderChangeManager.onBecomeLeader(tp0, 0)
    leaderChangeManager.onBecomeLeader(tp1, 0)
    leaderChangeManager.process()

    val tasks = queue.poll().get
    assertEquals(Set(tp0, tp1), tasks.map(_.topicIdPartition))
    val tp0Task = tasks.find(_.topicIdPartition == tp0).get
    val tp1Task = tasks.find(_.topicIdPartition == tp1).get

    // when tp0 becomes follower, the corresponding task should be cancelled
    leaderChangeManager.onBecomeFollower(tp0)
    leaderChangeManager.process()
    assertTrue("expected tp0 task to be cancelled due to become follower", tp0Task.ctx.isCancelled)

    // when tp1 becomes leader with a new epoch, the original task should be cancelled
    leaderChangeManager.onBecomeLeader(tp1, 1)
    leaderChangeManager.process()
    assertTrue("expected tp1 task to be cancelled due to new leadership", tp1Task.ctx.isCancelled)
  }

  @Test
  def testExactlyOnceTaskProcessing(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    leaderChangeManager.onBecomeLeader(tp0, 0)
    leaderChangeManager.process()

    var tp0Task = queue.poll().get.head
    assertEquals(tp0, tp0Task.topicIdPartition)
    assertTrue("expected no other tasks to be available", queue.poll().isEmpty)

    queue.done(tp0Task)
    tp0Task = queue.poll().get.head
    assertEquals(tp0, tp0Task.topicIdPartition)
  }

  @Test
  def testLeadershipChangeDuringTaskExecution(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    leaderChangeManager.onBecomeLeader(tp0, 0)
    leaderChangeManager.process()

    val tp0Task = queue.poll().get.head
    assertEquals(tp0, tp0Task.topicIdPartition)

    leaderChangeManager.onBecomeLeader(tp0, 1)
    leaderChangeManager.process()

    queue.done(tp0Task)
    assertTrue("expected task to be immediately canceled since a new onBecomeLeader event occurred", tp0Task.ctx.isCancelled)

    val tasks = queue.poll().head
    assertEquals(1, tasks.size)
    assertEquals(1, tasks.head.leaderEpoch)

    assertTrue("expected to find nothing else in the queue", queue.poll().isEmpty)
  }

  @Test
  def testLossOfLeadershipRemovesTask(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    val tp1 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 1)
    leaderChangeManager.onBecomeLeader(tp0, 0)
    leaderChangeManager.onBecomeLeader(tp1, 0)
    leaderChangeManager.process()

    val tasks = queue.poll().get
    val tp0Task = tasks.find(_.topicIdPartition == tp0).get
    val tp1Task = tasks.find(_.topicIdPartition == tp1).get

    // Test scenario where leadership changes after a task is done
    queue.done(tp0Task)
    leaderChangeManager.onBecomeFollower(tp0)
    leaderChangeManager.process()

    assertTrue("expected task to be cancelled due to leadership change", tp0Task.ctx.isCancelled)
    assertEquals("expected task to be removed from the queue due to leadership change", 1, queue.taskCount)

    // Test scenario where leadership changes before a task is done
    leaderChangeManager.onBecomeFollower(tp1)
    leaderChangeManager.process()
    assertTrue("expected task to be cancelled due to leadership change", tp1Task.ctx.isCancelled)
    assertEquals("expected queue to be empty", queue.taskCount, 0)
  }

  @Test
  def testTimeDelay(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    leaderChangeManager.onBecomeLeader(tp0, 0)
    leaderChangeManager.process()
    val tp0Task = queue.poll().get.head
    assertEquals(tp0Task.topicIdPartition, tp0)

    // Set the task delay to 5 seconds in the future
    tp0Task.retryTaskLater(5000, time.hiResClockMs(), new Throwable)
    queue.done(tp0Task)

    // Polling from the queue returns no elements, as the only task in the queue is paused
    assertTrue(queue.poll().isEmpty)

    // sleep for some time; polling will still not return any tasks
    time.sleep(200)
    assertTrue(queue.poll().isEmpty)

    // sleep for more than 5 seconds in total; we should now see the task returned
    time.sleep(4801)
    assertEquals(tp0, queue.poll().get.head.topicIdPartition)
  }

  @Test
  def testErrorState(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    leaderChangeManager.onBecomeLeader(tp0, 0)
    leaderChangeManager.process()
    val tp0Task = queue.poll().get.head
    assertEquals(tp0Task.topicIdPartition, tp0)

    tp0Task.cancelAndSetErrorState(new Throwable("failed"))
    queue.done(tp0Task)
    assertEquals(1, queue.errorPartitionCount())

    // Polling from the queue returns no elements, as the only TopicIdPartition in the queue is in error state
    assertTrue(queue.poll().isEmpty)
    time.sleep(200)
    assertTrue(queue.poll().isEmpty)

    // Polling from the queue returns no elements after reimmigration as the only TopicIdPartition in the queue is still in error state
    leaderChangeManager.onBecomeLeader(tp0, 1)
    leaderChangeManager.process()
    assertTrue(queue.poll().isEmpty)

    // add a new partition, check that it does get read correctly even though tp0 is filtered out
    val tp1 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 1)
    leaderChangeManager.onBecomeLeader(tp1, 0)
    leaderChangeManager.process()
    val tp1Task = queue.poll().get.head
    assertEquals(tp1Task.topicIdPartition, tp1)
    assertEquals(1, queue.errorPartitionCount())
  }

  @Test
  def testDelayWithMultipleTasks(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    val tp1 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 1)

    leaderChangeManager.onBecomeLeader(tp0, 0)
    leaderChangeManager.onBecomeLeader(tp1, 0)
    leaderChangeManager.process()

    val tasks = queue.poll().get

    assertEquals(2, tasks.size)
    val tp0Task = tasks.find(_.topicIdPartition == tp0).get
    val tp1Task = tasks.find(_.topicIdPartition == tp1).get

    // Set delay for tp0Task to 5 seconds in the future (this is the maximum backoff)
    tp0Task.retryTaskLater(5000, time.hiResClockMs(), new Throwable)
    queue.done(tp0Task)

    // Complete task tp1
    queue.done(tp1Task)

    // Polling from the queue returns tp1Task only
    assertEquals(Set(tp1), queue.poll().get.map(_.topicIdPartition))

    // sleep for some time; polling will not return any tasks as tp0 is still ineligible for execution
    time.sleep(200)
    assertTrue(queue.poll().isEmpty)

    // sleep for more than 5 seconds in total; we should now see the task returned
    time.sleep(4801)
    assertEquals(Set(tp0), queue.poll().get.map(_.topicIdPartition))

    assertEquals(2, queue.taskCount)
    assertEquals(None, queue.poll())
  }

  @Test
  def testMaxTasksPoll(): Unit = {
    val numTasks = 2 * maxTasks

    for (i <- 0 until numTasks) {
      val tp = new TopicIdPartition(s"foo-$i", UUID.randomUUID, 0)
      leaderChangeManager.onBecomeLeader(tp, 0)
    }

    leaderChangeManager.process()

    // poll should not return more than `maxTasks` tasks
    val tasks = queue.poll().get
    assertEquals(maxTasks, tasks.size)
    assertEquals(maxTasks, tasks.map(_.topicIdPartition).size)

    // polling again should not return any tasks
    assertTrue(queue.poll().isEmpty)

    // complete one task
    val doneTask = tasks.head
    queue.done(doneTask)
    assertEquals(1, queue.poll().get.size)
  }

  @Test
  def testTopicDeletionStopsTask(): Unit = {
    val tp = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    leaderChangeManager.onBecomeLeader(tp, 0)
    leaderChangeManager.process()

    val task = queue.poll().get.head
    leaderChangeManager.onDelete(tp)
    leaderChangeManager.process()
    queue.done(task)

    assertTrue("expected task to be canceled", task.ctx.isCancelled)
    assertEquals("expected no tasks to be present", 0, queue.taskCount)
  }

  @Test
  def testCancellationRemovesTaskFromQueue(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)
    leaderChangeManager.onBecomeLeader(tp0, 0)
    leaderChangeManager.process()

    val task = queue.poll().get.head
    assertFalse("expected task not to be canceled", task.ctx.isCancelled)

    queue.withAllTasks(allTasks => {
      assertTrue("expected there to be at least one task in the queue", allTasks.nonEmpty)
    })

    task.ctx.cancel() // Cancel task, re-adding it should cause removal from the task set.
    queue.done(task)

    queue.withAllTasks(allTasks => {
      assertTrue("expected all tasks to have been removed", allTasks.isEmpty)
    })
  }

  @Test
  def testTaskDone(): Unit = {
    val numTasks = maxTasks

    for (i <- 0 until numTasks) {
      val tp = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), i)
      leaderChangeManager.onBecomeLeader(tp, 0)
    }
    leaderChangeManager.process()

    val polledTasks = queue.poll().get.toList
    assertEquals(maxTasks, polledTasks.size)

    // complete couple of tasks and check that a subsequent poll returns them
    queue.done(polledTasks(0))
    queue.done(polledTasks(2))

    assertEquals(Set(polledTasks(0), polledTasks(2)), queue.poll().get)
  }

  @Test
  def testProcessReturnsOnQueueChange(): Unit = {
    val tp0 = new TopicIdPartition("foo", UUID.fromString("3036601f-dfb2-46e0-a809-69b710e0944a"), 0)

    val future = Future(blocking(leaderChangeManager.processAtLeastOne()))
    assertFalse(future.isCompleted)

    leaderChangeManager.onBecomeLeader(tp0, 0)

    Await.ready(future, Duration.create(200, TimeUnit.MILLISECONDS))
    assertTrue(future.isCompleted)
    assertTrue(future.value.get.isSuccess)
    assertEquals(1, queue.taskCount)
  }

  @Test
  def testProcessReturnsOnClose(): Unit = {
    val future = Future(blocking(leaderChangeManager.processAtLeastOne()))
    assertFalse(future.isCompleted)

    // close the queue
    leaderChangeManager.close()

    Await.ready(future, Duration.create(200, TimeUnit.MILLISECONDS))
    assertTrue(future.isCompleted)
    assertTrue(future.value.get.isFailure)
    assertEquals(0, queue.taskCount)
  }
}