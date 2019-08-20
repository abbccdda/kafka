package kafka.tier.tasks

import java.util.UUID

import kafka.tier.TopicIdPartition
import kafka.utils.TestUtils
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.Assertions.assertThrows

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{CancellationException, Future}

case class QueueEntry(topicIdPartition: TopicIdPartition) extends UpdatableQueueEntry {
  override type Key = TopicIdPartition
  override def key: Key= topicIdPartition
}

class UpdatableQueueTest {
  private val queue = new UpdatableQueue[QueueEntry]

  @Test
  def testPushUnblocksTake(): Unit = {
    val topicPartition = new TopicIdPartition("foo", UUID.randomUUID, 0)

    val future = Future {
      queue.take()
    }

    queue.push(QueueEntry(topicPartition))
    TestUtils.waitUntilTrue(() => future.isCompleted, "Timed out waiting for future to complete")

    assertEquals(topicPartition, future.value.get.get.topicIdPartition)
  }

  @Test
  def testCloseUnblocksTake(): Unit = {
    val future = Future {
      queue.take()
    }

    queue.close()
    TestUtils.waitUntilTrue(() => future.isCompleted, "Timed out waiting for future to complete")

    assertThrows[CancellationException] {
      future.value.get.get
    }
  }
}
