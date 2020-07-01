/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.topic

import java.util.concurrent.{ArrayBlockingQueue}

import org.junit.Test
import kafka.durability.DurabilityTestUtils
import kafka.durability.events.OffsetChangeEvent
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

class DurabilityTopicManagerTest {
  val topic = "test-topic"
  val partition: Int = 50

  val config = DurabilityTestUtils.getDurabilityConfig()

  /**
   * Basic test to verify start stop functionality. Also, validates that DurabilityTopicManager also controls the consumer
   * thread's start/stop operation.
   */
  @Test
  def BasicDurabilityTopicManagerTest(): Unit = {
    val manager = DurabilityTestUtils.getDurabilityTopicManager("test-topic", 50)
    assertFalse(manager.isReady())
    manager.start()
    assertTrue(manager.consumerProvider.isReady)
    assertTrue(manager.isReady())
    manager.shutdown()
    assertTrue(manager.isShutdown)
    assertFalse(manager.consumerProvider.isReady)
  }

  /**
   * End-to-end test which basically validates that via manager events can be produced and in background consumer will poll
   * for the event and fetch it.
   */
  @Test
  def TopicManagerProducerConsumerTest(): Unit = {
    val tpid = new TopicPartition("test", 0)
    val epoch = 0
    val version = 1

    val resultQueue = new ArrayBlockingQueue[ConsumerRecords[Array[Byte], Array[Byte]]](32)
    // consumer's processRecords method has been overridden to generate the fetched records in the resultQueue.
    val manager = DurabilityTestUtils.getDurabilityTopicManager("test-topic", 50, resultQueue = Some(resultQueue))

    manager.start()
    manager.addDurabilityEvent(OffsetChangeEvent(tpid, version, epoch, 100, 0))
    assertEquals(resultQueue.size(), 1)
  }
}
