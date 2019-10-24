/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic

import java.time.Duration
import java.util
import java.util.function.Supplier

import kafka.tier.client.{MockConsumerSupplier, MockProducerSupplier}
import kafka.tier.{TierTestUtils, TopicIdPartition}
import kafka.zk.AdminZkClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Mockito.{mock, when}
import org.scalatest.Assertions.assertThrows

import scala.collection.JavaConverters._

class TierCatchupConsumerTest {
  private val topicIdPartitions = TierTestUtils.randomTopicIdPartitions(3, 5)

  private val numPartitions = 5.toShort
  private val adminZkClientSupplier = new Supplier[AdminZkClient] {
    override def get(): AdminZkClient = {
      mock(classOf[AdminZkClient])
    }
  }
  private val tierTopic = new TierTopic("namespace", adminZkClientSupplier)

  private val producerSupplier = new MockProducerSupplier[Array[Byte], Array[Byte]]()
  private val consumerSupplier = new MockConsumerSupplier[Array[Byte], Array[Byte]]("catchup",
    TierTopicManager.partitions(tierTopic.topicName, numPartitions),
    producerSupplier.producer)

  private val catchupConsumer = new TierCatchupConsumer(consumerSupplier)

  @Before
  def setup(): Unit = {
    tierTopic.ensureTopic(numPartitions, 1)
  }

  @Test
  def testStartConsumer(): Unit = {
    val tierTopicPartitions = assignment(topicIdPartitions)
    catchupConsumer.doStart(tierTopicPartitions)
    assertTrue(catchupConsumer.active)
    assertEquals(underlyingCatchupConsumer.assignment, tierTopicPartitions)

    // doStart should fail while there is an active consumer
    val newTopicPartitions = TierTestUtils.randomTopicIdPartitions(3, 2)

    assertThrows[IllegalStateException] {
      catchupConsumer.doStart(assignment(newTopicPartitions))
    }

    assertEquals(underlyingCatchupConsumer.assignment, tierTopicPartitions)
    assertTrue(catchupConsumer.active)
  }

  @Test
  def testTryComplete(): Unit = {
    val currentAssignment = assignment(topicIdPartitions)
    val primaryConsumer = mock(classOf[Consumer[Array[Byte], Array[Byte]]])

    catchupConsumer.doStart(currentAssignment)
    currentAssignment.asScala.foreach { topicPartition =>
      underlyingCatchupConsumer.seek(topicPartition, 10)
      when(primaryConsumer.position(topicPartition)).thenReturn(20)
    }

    val completed_1 = catchupConsumer.tryComplete(primaryConsumer)
    assertFalse(completed_1)
    assertTrue(catchupConsumer.active)

    currentAssignment.asScala.foreach { topicPartition =>
      underlyingCatchupConsumer.seek(topicPartition, 20)
      when(primaryConsumer.position(topicPartition)).thenReturn(20)
    }

    val completed_2 = catchupConsumer.tryComplete(primaryConsumer)
    assertTrue(completed_2)
    assertFalse(catchupConsumer.active)
  }

  @Test
  def testPoll(): Unit = {
    val currentAssignment = assignment(topicIdPartitions)
    val primaryConsumer = mock(classOf[Consumer[Array[Byte], Array[Byte]]])

    assertEquals(null, catchupConsumer.poll(Duration.ZERO))

    catchupConsumer.doStart(currentAssignment)
    val records = catchupConsumer.poll(Duration.ZERO)
    assertEquals(0, records.count)

    currentAssignment.asScala.foreach { topicPartition =>
      underlyingCatchupConsumer.seek(topicPartition, 20)
      when(primaryConsumer.position(topicPartition)).thenReturn(20)
    }
    val completed = catchupConsumer.tryComplete(primaryConsumer)
    assertTrue(completed)

    assertEquals(null, catchupConsumer.poll(Duration.ZERO))
  }

  private def assignment(topicIdPartitions: Set[TopicIdPartition]): util.Set[TopicPartition] = {
    tierTopic.toTierTopicPartitions(topicIdPartitions.asJava)
  }

  private def underlyingCatchupConsumer = catchupConsumer.consumer
}
