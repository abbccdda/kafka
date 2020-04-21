/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.concurrent.{Exchanger, ExecutionException, TimeUnit}

import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.junit.{After, Before, Test}
import org.junit.Assert._

class ClusterLinkSchedulerTest {

  val scheduler = new ClusterLinkScheduler()
  var task: ClusterLinkScheduler.PeriodicTask = null

  private val exchanger = new Exchanger[Integer]()
  private def exchangeExpect(value: Integer, expect: Integer): Unit =
    assertEquals(expect, exchanger.exchange(value, 1, TimeUnit.SECONDS))
  private def testNotify(value: Integer): Unit = exchangeExpect(value, null)
  private def testWait(value: Integer): Unit = exchangeExpect(null, value)

  @Before
  def setUp(): Unit = {
    scheduler.startup()
  }

  @After
  def tearDown(): Unit = {
    if (task != null)
      task.shutdown()
    scheduler.shutdown()
  }

  @Test
  def testScheduleWhenComplete(): Unit = {
    val future1 = new KafkaFutureImpl[Void]()
    scheduler.scheduleWhenComplete("test", future1, () => testNotify(1))
    Thread.sleep(10)
    future1.complete(null)
    testWait(1)

    val future2 = new KafkaFutureImpl[Void]()
    scheduler.scheduleWhenComplete("test", future2, () => testNotify(2))
    Thread.sleep(10)
    future2.completeExceptionally(new InvalidRequestException(""))
    testWait(2)
  }

  @Test
  def testPeriodicTaskPeriod(): Unit = {
    val rescheduleDelayMs = 10

    task = new ClusterLinkScheduler.PeriodicTask(scheduler, "TestTask", rescheduleDelayMs) {
      private var first = true

      override def run(): Boolean = {
        if (first) {
          first = false
          testNotify(1)
        } else {
          testNotify(2)
        }
        true
      }
    }

    val start = System.nanoTime()
    task.startup()
    testWait(1)
    testWait(2)
    val diffMs = (System.nanoTime() - start) / 1e6
    assertTrue(diffMs >= rescheduleDelayMs * 2)
  }

  @Test
  def testPeriodicTaskSchedule(): Unit = {
    val rescheduleDelayMs = 10

    val future = new KafkaFutureImpl[Void]()

    task = new ClusterLinkScheduler.PeriodicTask(scheduler, "TestTask", rescheduleDelayMs) {
      private var running = false
      private var done = false

      override def run(): Boolean = {
        assertFalse(running)

        if (done) {
          testNotify(6)
          true
        } else {
          running = true

          testNotify(1)
          testWait(2)
          scheduleOnce(() => next())
          false
        }
      }

      def next(): Boolean = {
        assertTrue(running)
        assertFalse(done)

        testNotify(3)
        testWait(4)
        scheduleWhenComplete(future, () => finish())
        false
      }

      def finish(): Boolean = {
        assertTrue(running)
        assertFalse(done)

        testNotify(5)
        running = false
        done = true
        true
      }

    }

    task.startup()
    testWait(1)
    testNotify(2)
    testWait(3)
    testNotify(4)
    Thread.sleep(rescheduleDelayMs)
    future.complete(null)
    testWait(5)
    testWait(6)
  }

  @Test
  def testPeriodicTaskException(): Unit = {
    task = new ClusterLinkScheduler.PeriodicTask(scheduler, "TestTask", rescheduleDelayMs = 10) {
      private var first = true

      override def run(): Boolean = {
        if (first) {
          first = false
          testNotify(1)
          throw new InvalidRequestException("")
        } else {
          testNotify(2)
          true
        }
      }

    }

    task.startup()
    testWait(1)
    testWait(2)
  }

  @Test
  def testRunOnce(): Unit = {
    task = new ClusterLinkScheduler.PeriodicTask(scheduler, "TestTask", rescheduleDelayMs = 10) {
      private var first = true

      override def run(): Boolean = {
        if (first) {
          first = false
          testNotify(1)
          true
        } else {
          throw new InvalidRequestException("")
        }
      }
    }

    val result = task.runOnce()
    testWait(1)
    result.get
  }

  @Test(expected = classOf[InvalidRequestException])
  def testRunOnceException(): Unit = {
    task = new ClusterLinkScheduler.PeriodicTask(scheduler, "TestTask", rescheduleDelayMs = 10) {
      override def run(): Boolean = {
        throw new InvalidRequestException("")
      }
    }

    try {
      task.runOnce().get
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }
}
