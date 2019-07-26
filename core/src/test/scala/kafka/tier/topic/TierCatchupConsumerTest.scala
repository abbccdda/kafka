package kafka.tier.topic

import java.time.Duration
import java.util
import java.util.{Collections, Optional}
import java.util.function.Supplier

import kafka.tier.{TierMetadataManager, TierTestUtils, TopicIdPartition}
import kafka.tier.client.{MockConsumerSupplier, MockProducerSupplier}
import kafka.tier.state.{TierPartitionState, TierPartitionStatus}
import org.apache.kafka.clients.admin.{AdminClient, MockAdminClient}
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.{Node, TopicPartition}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.Assertions.assertThrows

import scala.collection.JavaConverters._

class TierCatchupConsumerTest {
  private val tierMetadataManager = mock(classOf[TierMetadataManager])
  private val topicIdPartitions = TierTestUtils.randomTopicIdPartitions(3, 5)
  private val topicPartitionStates = setupTopicPartitionStates(topicIdPartitions, TierPartitionStatus.CATCHUP)

  private val numPartitions = 5.toShort
  private val adminClientSupplier = new Supplier[AdminClient] {
    override def get(): AdminClient = {
      val node = new Node(1, "localhost", 9092)
      new MockAdminClient(Collections.singletonList(node), node)
    }
  }
  private val tierTopic = new TierTopic("namespace", adminClientSupplier)

  private val producerSupplier = new MockProducerSupplier[Array[Byte], Array[Byte]]()
  private val consumerSupplier = new MockConsumerSupplier[Array[Byte], Array[Byte]]("catchup",
    TierTopicManager.partitions(tierTopic.topicName, numPartitions),
    producerSupplier.producer)

  private val catchupConsumer = new TierCatchupConsumer(tierMetadataManager, consumerSupplier)

  @Before
  def setup(): Unit = {
    tierTopic.ensureTopic(numPartitions, 1)
  }

  @Test
  def testStartConsumer(): Unit = {
    val expectedAssignment = assignment(topicIdPartitions)

    val started_1 = catchupConsumer.maybeStartConsumer(tierTopic, topicPartitionStates.asJava)
    assertTrue(started_1)
    assertTrue(catchupConsumer.active)
    assertEquals(underlyingCatchupConsumer.assignment, expectedAssignment)
    topicPartitionStates.foreach { state =>
      verify(state, times(1)).beginCatchup()
    }

    // maybeStart should fail while there is an active consumer
    val newTopicPartitions = TierTestUtils.randomTopicIdPartitions(3, 2)
    val newPartitionStates = setupTopicPartitionStates(newTopicPartitions, TierPartitionStatus.CATCHUP)
    val started_2 = catchupConsumer.maybeStartConsumer(tierTopic, newPartitionStates.asJava)
    assertFalse(started_2)
    newPartitionStates.foreach { state =>
      verify(state, times(0)).beginCatchup()
    }

    // doStart should raise an exception while the consumer is active
    assertThrows[IllegalStateException] {
      catchupConsumer.doStart(tierTopic, newPartitionStates.asJava)
    }

    assertEquals(underlyingCatchupConsumer.assignment, expectedAssignment)
    assertTrue(catchupConsumer.active)
  }

  @Test
  def testTryComplete(): Unit = {
    val currentAssignment = assignment(topicIdPartitions)
    val primaryConsumer = mock(classOf[Consumer[Array[Byte], Array[Byte]]])

    val started = catchupConsumer.maybeStartConsumer(tierTopic, topicPartitionStates.asJava)
    assertTrue(started)

    currentAssignment.asScala.foreach { topicPartition =>
      underlyingCatchupConsumer.seek(topicPartition, 10)
      when(primaryConsumer.position(topicPartition)).thenReturn(20)
    }

    val completed_1 = catchupConsumer.tryComplete(primaryConsumer)
    assertFalse(completed_1)
    assertTrue(catchupConsumer.active)
    topicPartitionStates.foreach { state =>
      verify(state, times(0)).onCatchUpComplete()
    }

    currentAssignment.asScala.foreach { topicPartition =>
      underlyingCatchupConsumer.seek(topicPartition, 20)
      when(primaryConsumer.position(topicPartition)).thenReturn(20)
    }

    val completed_2 = catchupConsumer.tryComplete(primaryConsumer)
    assertTrue(completed_2)
    assertFalse(catchupConsumer.active)
    topicPartitionStates.foreach { state =>
      verify(state, times(1)).onCatchUpComplete()
    }
  }

  @Test
  def testPoll(): Unit = {
    val currentAssignment = assignment(topicIdPartitions)
    val primaryConsumer = mock(classOf[Consumer[Array[Byte], Array[Byte]]])

    assertEquals(null, catchupConsumer.poll(Duration.ZERO))

    val started = catchupConsumer.maybeStartConsumer(tierTopic, topicPartitionStates.asJava)
    assertTrue(started)

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

  private def setupTopicPartitionStates(topicIdPartitions: Set[TopicIdPartition], status: TierPartitionStatus): Set[TierPartitionState] = {
    topicIdPartitions.map { topicIdPartition =>
      val tierPartitionState = mock(classOf[TierPartitionState])
      when(tierPartitionState.topicIdPartition).thenReturn(Optional.of(topicIdPartition))
      when(tierPartitionState.topicPartition).thenReturn(topicIdPartition.topicPartition)
      when(tierPartitionState.status).thenReturn(status)
      when(tierMetadataManager.tierPartitionState(topicIdPartition)).thenReturn(Optional.of(tierPartitionState))
      tierPartitionState
    }
  }

  private def underlyingCatchupConsumer = catchupConsumer.consumer
}
