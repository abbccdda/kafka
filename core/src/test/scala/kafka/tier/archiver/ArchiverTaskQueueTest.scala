package kafka.tier.archiver


import java.time.Instant
import java.util.concurrent.TimeUnit

import kafka.tier.fetcher.CancellationContext
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.{MockTime, Time}
import org.junit.{After, Assert, Before, Test}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, blocking}

case class MockArchiverTaskQueueTask(ctx: CancellationContext,
                                     topicPartition: TopicPartition,
                                     leaderEpoch: Int) extends ArchiverTaskQueueTask {

  @volatile var pauseTime: Option[Instant] = None
  override def pausedUntil: Option[Instant] = pauseTime
}

final class ArchiverTaskQueueTest {
  var lagMapping: mutable.Map[TopicPartition, Long] = _
  var ctx: CancellationContext = _
  var queue: ArchiverTaskQueue[MockArchiverTaskQueueTask] = _
  var time: Time = _

  @Before
  def setup(): Unit = {
    time = new MockTime()
    lagMapping = mutable.Map()
    ctx = CancellationContext.newContext()
    queue = new ArchiverTaskQueue[MockArchiverTaskQueueTask](
      ctx, time, getLag,
      (ctx, tp, leaderEpoch) => MockArchiverTaskQueueTask(ctx, tp, leaderEpoch))
  }

  @After
  def teardown(): Unit = {
    ctx.cancel()
  }

  private def getLag(task: MockArchiverTaskQueueTask): Option[Long] = {
    lagMapping.synchronized {
      lagMapping.get(task.topicPartition)
    }
  }

  private def updateLag(topicPartition: TopicPartition, lag: Long): Unit = {
    lagMapping.synchronized {
      lagMapping += (topicPartition -> lag)
    }
  }

  @Test
  def testLeadershipOverrides(): Unit = {
    val tp = new TopicPartition("foo", 0)
    queue.onBecomeLeader(tp, 0)
    queue.onBecomeLeader(tp, 1)
    updateLag(tp, 1)
    val task = queue.poll()
    Assert.assertEquals("expected subsequent onBecomeLeader requests to take precedence",
      task.asInstanceOf[MockArchiverTaskQueueTask].leaderEpoch, 1)
  }

  @Test
  def testMinLagPrioritization(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    queue.onBecomeLeader(tp0, 0)
    queue.onBecomeLeader(tp1, 0)
    updateLag(tp0, 50)
    updateLag(tp1, 100)
    Assert.assertEquals("expected the topic partition with the least lag to be polled first",
      queue.poll().topicPartition, tp0)
    Assert.assertEquals("expected the topic partition with the second least lag to be polled next",
      queue.poll().topicPartition, tp1)
  }

  @Test
  def testLeadershipChangesCancelTasks(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    queue.onBecomeLeader(tp0, 0)
    queue.onBecomeLeader(tp1, 0)
    updateLag(tp0, 1)
    updateLag(tp1, 2)

    val tp0Task = queue.poll()
    val tp1Task = queue.poll()

    Assert.assertEquals(tp0Task.topicPartition, tp0)
    Assert.assertEquals(tp1Task.topicPartition, tp1)

    queue.onBecomeFollower(tp0)
    queue.processLeadershipQueue(50, TimeUnit.MILLISECONDS)
    Assert.assertTrue("expected tp0 task to be cancelled due to become follower",
      tp0Task.ctx.isCancelled)

    queue.onBecomeLeader(tp1, 0)
    queue.processLeadershipQueue(50, TimeUnit.MILLISECONDS)
    Assert.assertTrue("expected tp1 task to be cancelled due to new leadership",
      tp1Task.ctx.isCancelled)
  }

  @Test
  def testExactlyOnceTaskProcessing(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    queue.onBecomeLeader(tp0, 0)
    updateLag(tp0, 50)

    val tp0Task = queue.poll()
    Assert.assertEquals(tp0Task.topicPartition, tp0)
    Assert.assertTrue("expected no other tasks to be available", queue.poll(50, TimeUnit.MILLISECONDS).isEmpty)

    queue.done(tp0Task)

    Assert.assertEquals("expected to be able to retrieve task after returning it to the queue",
      queue.poll().topicPartition, tp0)
  }

  @Test
  def testLeadershipChangeDuringTaskExecution(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    queue.onBecomeLeader(tp0, 0)
    updateLag(tp0, 50)

    val tp0Task = queue.poll()
    Assert.assertEquals(tp0Task.topicPartition, tp0)

    queue.onBecomeLeader(tp0, 1)
    queue.processLeadershipQueue(10, TimeUnit.MILLISECONDS)

    queue.done(tp0Task)
    Assert.assertTrue("expected task to be immediately canceled since a new " +
      "onBecomeLeader event occured", tp0Task.ctx.isCancelled)

    Assert.assertEquals("expected to find the new task created by onBecomeLeader in the queue",
      queue.poll().leaderEpoch, 1)

    Assert.assertTrue("expected to find nothing else in the queue",
      queue.poll(10, TimeUnit.MILLISECONDS).isEmpty)
  }

  @Test
  def testLossOfLeadershipRemovesTask(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    queue.onBecomeLeader(tp0, 0)
    queue.onBecomeLeader(tp1, 0)
    updateLag(tp0, 1)
    updateLag(tp1, 2)

    val tp0Task = queue.poll()
    val tp1Task = queue.poll()
    Assert.assertEquals(tp0Task.topicPartition, tp0)
    Assert.assertEquals(tp1Task.topicPartition, tp1)

    // Test scenario where leadership changes after a task is done
    queue.done(tp0Task)
    queue.onBecomeFollower(tp0)
    queue.processLeadershipQueue(50, TimeUnit.MILLISECONDS)
    Assert.assertTrue("expected task to be cancelled due to leadership change", tp0Task.ctx.isCancelled)
    Assert.assertEquals("expected task to be removed from the queue due to leadership change",
      queue.taskCount(), 1)

    // Test scenario where leadership changes before a task is done
    queue.onBecomeFollower(tp1)
    queue.processLeadershipQueue(50, TimeUnit.MILLISECONDS)
    Assert.assertTrue("expected task to be cancelled due to leadership change", tp1Task.ctx.isCancelled)
    Assert.assertEquals("expected queue to be empty", queue.taskCount(), 0)
  }

  @Test
  def testTimeDelay(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    queue.onBecomeLeader(tp0, 0)
    updateLag(tp0, 1)
    val tp0Task = queue.poll()
    Assert.assertEquals(tp0Task.topicPartition, tp0)
    // Set the task delay to 5 seconds in the future
    tp0Task.pauseTime = Some(Instant.ofEpochMilli(time.milliseconds()).plusSeconds(5))
    queue.done(tp0Task)
    // Start up a poller which will block until something is available in the queue
    val fut = Future(blocking(queue.poll()))
    Assert.assertFalse("expected future to still be blocked due to time delay", fut.isCompleted)
    // sleep for half the time, future should still be blocked
    time.sleep(3000)
    Assert.assertFalse("expected future to still be blocked due to time delay", fut.isCompleted)
    // sleep for more than 5 seconds in total, future should be unblocked
    time.sleep(3000)
    Assert.assertTrue("expected task pause to expire and future to complete", Await.ready(fut, 100 milliseconds).isCompleted)
  }

  @Test
  def testLagChangesUnblockPollers(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    queue.onBecomeLeader(tp0, 0)
    updateLag(tp0, 0)

    val fut = Future(blocking(queue.poll()))

    Assert.assertFalse("expected poll to still be blocked", fut.isCompleted)
    updateLag(tp0, 30)
    val task = Await.result(fut, 100 milliseconds)
    Assert.assertEquals("expected updating lag to unblock the queue",
      task.topicPartition, tp0)
  }

  @Test
  def testMultiplePollersGetUniqueTasks(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    queue.onBecomeLeader(tp0, 0)
    queue.onBecomeLeader(tp1, 0)
    updateLag(tp0, 0)
    updateLag(tp1, 0)

    val fut1 = Future(blocking(queue.poll()))
    val fut2 = Future(blocking(queue.poll()))

    updateLag(tp0, 50)
    updateLag(tp1, 100)

    Assert.assertNotEquals("expected unique tasks between pollers",
      Await.result(fut1, 100 milliseconds).topicPartition,
      Await.result(fut2, 100 milliseconds).topicPartition
    )
  }

  @Test
  def testTopicDeletionStopsTask(): Unit = {
    val tp = new TopicPartition("foo", 0)
    queue.onBecomeLeader(tp, 0)
    updateLag(tp, 1)
    val task = queue.poll()
    queue.onDelete(tp)
    queue.processLeadershipQueue(50, TimeUnit.MILLISECONDS)
    queue.done(task)
    Assert.assertTrue("expected task to be canceled", task.ctx.isCancelled)
    Assert.assertEquals("expected no tasks to be present", queue.taskCount(), 0)
  }

  @Test
  def testCancellationRemovesTaskFromQueue(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    queue.onBecomeLeader(tp0, 0)
    updateLag(tp0, 1)
    val task = queue.poll()
    Assert.assertFalse("expected task not to be canceled", task.ctx.isCancelled)

    queue.withAllTasks(allTasks => {
      Assert.assertTrue("expected there to be at least one task in the queue", allTasks.nonEmpty)
    })

    task.ctx.cancel() // Cancel task, re-adding it should cause removal from the task set.
    queue.done(task)

    queue.withAllTasks(allTasks => {
      Assert.assertTrue("expected all tasks to have been removed", allTasks.isEmpty)
    })
  }

  @Test
  def testClosingQueueUnblocksPollers(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    queue.onBecomeLeader(tp0, 0)
    queue.onBecomeLeader(tp1, 0)
    updateLag(tp0, 0)
    updateLag(tp1, 0)

    val fut1 = Future(blocking(queue.poll()))
    val fut2 = Future(blocking(queue.poll()))

    Assert.assertFalse(fut1.isCompleted)
    Assert.assertFalse(fut2.isCompleted)

    ctx.cancel()

    Assert.assertTrue("expected future to be completed", Await.ready(fut1, 100 millis).isCompleted)
    Assert.assertTrue("expected future to be completed", Await.ready(fut2, 100 millis).isCompleted)
    Assert.assertTrue("expected future to fail", fut1.value.get.isFailure)
    Assert.assertTrue("expected future to fail", fut2.value.get.isFailure)
  }

  private def schedulingLoop(queue: ArchiverTaskQueue[MockArchiverTaskQueueTask]): Future[Unit] = {
    Future {
      for (i <- 0 to 10) {
        val task = blocking(queue.poll())
        updateLag(task.topicPartition, 10 - i)
        queue.done(task)
      }
    }
  }

  @Test
  def testLagDrivenToZero(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("foo", 2)
    queue.onBecomeLeader(tp0, 0)
    queue.onBecomeLeader(tp1, 0)
    queue.onBecomeLeader(tp2, 0)
    updateLag(tp0, 1)
    updateLag(tp1, 1)
    updateLag(tp2, 1)
    // Schedule 3 futures which will pop from the queue and decrement
    // the lag before putting back on the queue. They will each do this
    // 10 times before exiting.
    // This tests that we do not lose track of any tasks.
    Await.ready(for {
       _ <- schedulingLoop(queue)
       _ <- schedulingLoop(queue)
       _ <- schedulingLoop(queue)
    } yield (), 5 seconds)

    Assert.assertEquals("expected 3 tasks to be present", queue.taskCount(), 3)
    Assert.assertTrue(queue.withAllTasks(tasks => tasks.forall(getLag(_).get == 0)))
  }

  @Test
  def testMultiThreadedLeadershipChanges(): Unit = {
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    val tp2 = new TopicPartition("foo", 2)

    val fut0 = schedulingLoop(queue)
    val fut1 = schedulingLoop(queue)
    val fut2 = schedulingLoop(queue)

    // Unblock the futures
    queue.onBecomeLeader(tp0, 0)
    queue.onBecomeLeader(tp1, 0)
    queue.onBecomeLeader(tp2, 0)

    // set lag to non-zero to allow them to make progress
    updateLag(tp0, 10)
    updateLag(tp1, 10)
    updateLag(tp2, 10)

    // wait for futures to complete
    Await.ready(for {
      _ <- fut0
      _ <- fut1
      _ <- fut2
    } yield (), 5 seconds)

    Assert.assertEquals("expected 3 tasks to be present", queue.taskCount(), 3)

    Assert.assertTrue(queue.withAllTasks(tasks => tasks.forall(getLag(_).get == 0)))
  }

  @Test
  def testLeadershipQueueDrainsFully(): Unit = {
    // Verify that the leadership change queue is drained fully on every poll.
    val tp0 = new TopicPartition("foo", 0)
    val tp1 = new TopicPartition("foo", 1)
    queue.onBecomeLeader(tp0, 0)
    queue.onBecomeLeader(tp1, 0)
    updateLag(tp0, 0)
    updateLag(tp1, 0)

    Assert.assertTrue("Expected leadership changes to be processed", queue.processLeadershipQueue(1000, TimeUnit.MILLISECONDS))
    // After processing the leadership queue, we expect that both tp0 and tp1 are available as "tasks"
    queue.withAllTasks(tasks => {
      Assert.assertTrue("expected to find a task for TP0", tasks.exists(_.topicPartition == tp0))
      Assert.assertTrue("expected to find a task for TP1", tasks.exists(_.topicPartition == tp1))
    })
  }
}