package kafka.tier.topic

import java.util
import java.util.{OptionalInt, UUID}
import java.util.function.Supplier

import kafka.tier.{TierTopicManagerCommitter, TopicIdPartition}
import kafka.tier.client.{MockConsumerSupplier, MockProducerSupplier}
import kafka.tier.state.TierPartitionStatus
import kafka.tier.topic.TierTopicConsumer.ClientCtx
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._

class TierTopicConsumerTest {
  private type ConsumerSupplier = MockConsumerSupplier[Array[Byte], Array[Byte]]
  private type ProducerSupplier = MockProducerSupplier[Array[Byte], Array[Byte]]

  private val clusterId = "mycluster"
  private val tempDir = TestUtils.tempDir()
  private val logDir = tempDir.getAbsolutePath
  private val logDirs = new util.ArrayList(util.Collections.singleton(logDir))

  private val bootstrapSupplier = new Supplier[String] {
    override def get: String = "bootstrap-server"
  }

  private val tierTopicNumPartitions = 7.toShort
  private val tierTopicPartitioner = new TierTopicPartitioner(tierTopicNumPartitions)
  private val tierTopicManagerConfig = new TierTopicManagerConfig(bootstrapSupplier, "", tierTopicNumPartitions, 1.toShort, 3, clusterId, 5L, 30000, 500, logDirs)
  private val tierTopicName = TierTopic.topicName("")
  private val tierTopicPartitions = TierTopicManager.partitions(tierTopicName, tierTopicNumPartitions)
  private val tierTopic = mock(classOf[InitializedTierTopic])

  private val producerSupplier = new ProducerSupplier()
  private val primaryConsumerSupplier = new ConsumerSupplier("primary",
    tierTopicPartitions,
    producerSupplier.producer)
  private val catchupConsumerSupplier = new ConsumerSupplier("catchup",
    tierTopicPartitions,
    producerSupplier.producer)
  private val tierTopicManagerCommitter = mock(classOf[TierTopicManagerCommitter])
  private val tierTopicConsumer = new TierTopicConsumer(tierTopicManagerConfig,
    primaryConsumerSupplier,
    catchupConsumerSupplier,
    tierTopicManagerCommitter)

  @Before
  def setup(): Unit = {
    when(tierTopic.numPartitions).thenReturn(OptionalInt.of(tierTopicNumPartitions))
    when(tierTopic.topicName).thenReturn(tierTopicName)
    when(tierTopic.toTierTopicPartition(any())).thenAnswer(new Answer[TopicPartition] {
      override def answer(invocation: InvocationOnMock): TopicPartition = {
        TierTopic.toTierTopicPartition(invocation.getArgument(0), tierTopicName, tierTopicPartitioner)
      }
    })
    when(tierTopic.toTierTopicPartitions(any())).thenAnswer(new Answer[util.Set[TopicPartition]] {
      override def answer(invocation: InvocationOnMock): util.Set[TopicPartition] = {
        TierTopic.toTierTopicPartitions(invocation.getArgument(0), tierTopicName, tierTopicPartitioner)
      }
    })
    when(tierTopicManagerCommitter.positionFor(any())).thenReturn(null)
  }

  @Test
  def testRegisterPartitions(): Unit = {
    val tp_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val ctx_1 = mock(classOf[ClientCtx])
    when(ctx_1.status()).thenReturn(TierPartitionStatus.ONLINE)

    val tp_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 0)
    val ctx_2 = mock(classOf[ClientCtx])
    when(ctx_2.status()).thenReturn(TierPartitionStatus.ONLINE)

    val tp_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 0)
    val ctx_3 = mock(classOf[ClientCtx])
    when(ctx_3.status()).thenReturn(TierPartitionStatus.INIT)

    val tp_4 = new TopicIdPartition("foo-4", UUID.randomUUID, 0)
    val ctx_4 = mock(classOf[ClientCtx])
    when(ctx_4.status()).thenReturn(TierPartitionStatus.CATCHUP)

    // register all partitions
    tierTopicConsumer.register(tp_1, ctx_1)
    tierTopicConsumer.register(tp_2, ctx_2)
    tierTopicConsumer.register(tp_3, ctx_3)
    tierTopicConsumer.register(tp_4, ctx_4)
    assertEquals(Set(tp_1, tp_2, tp_3, tp_4), tierTopicConsumer.immigratingPartitions.keySet.asScala)

    // tp_1 and tp_2 must be online; tp_3 and tp_4 must be in catchup state
    tierTopicConsumer.startConsume(false, tierTopic)
    tierTopicConsumer.doWork()
    assertEquals(Set(tp_1, tp_2), tierTopicConsumer.onlinePartitions.keySet.asScala)
    assertEquals(Set(tp_3, tp_4), tierTopicConsumer.catchingUpPartitions.keySet.asScala)
    assertEquals(Set(), tierTopicConsumer.immigratingPartitions.keySet.asScala)

    verify(ctx_3, times(1)).beginCatchup()
    verify(ctx_4, times(1)).beginCatchup()

    verify(ctx_1, times(1)).status()
    verify(ctx_2, times(1)).status()
    verify(ctx_3, times(1)).status()
    verify(ctx_4, times(1)).status()

    verifyNoMoreInteractions(ctx_1)
    verifyNoMoreInteractions(ctx_2)
    verifyNoMoreInteractions(ctx_3)
    verifyNoMoreInteractions(ctx_4)

    // verify catchup consumer assignment
    assertEquals(tierTopic.toTierTopicPartitions(Set(tp_3, tp_4).asJava), catchupConsumerSupplier.consumers.get(0).assignment)

    // verify primary consumer assignment
    assertEquals(tierTopicPartitions, primaryConsumerSupplier.consumers.get(0).assignment)
  }

  @Test
  def testPrimaryConsumerSeeksToLastCommittedOffsetOnStartup(): Unit = {
    val committedOffsetMap = tierTopicPartitions.asScala.map { tierTopicPartition =>
      tierTopicPartition -> (tierTopicPartition.partition + 100L)
    }

    // setup committer with committed positions
    committedOffsetMap.foreach { case (tierTopicPartition, offset) =>
      when(tierTopicManagerCommitter.positionFor(tierTopicPartition.partition)).thenReturn(offset)
    }

    tierTopicConsumer.startConsume(false, tierTopic)

    val primaryConsumer = primaryConsumerSupplier.consumers.get(0)
    assertEquals(tierTopicPartitions, primaryConsumer.assignment)
    committedOffsetMap.foreach { case (tierTopicPartition, offset) =>
      assertEquals(offset, primaryConsumer.position(tierTopicPartition))
    }
  }

  @Test
  def testCatchupComplete(): Unit = {
    val tp_1 = new TopicIdPartition("foo-1", UUID.fromString("26081828-71a4-453f-ab1b-6ea27c5f65fb"), 0)
    val ctx_1 = mock(classOf[ClientCtx])
    when(ctx_1.status()).thenReturn(TierPartitionStatus.INIT)

    val tp_2 = new TopicIdPartition("foo-2", UUID.fromString("36081828-71a4-453f-ab1b-6ea27c5f65fb"), 0)
    val ctx_2 = mock(classOf[ClientCtx])
    when(ctx_2.status()).thenReturn(TierPartitionStatus.CATCHUP)

    // setup initial position for primary consumer
    val committedOffsetMap = tierTopicPartitions.asScala.map { tierTopicPartition =>
      tierTopicPartition -> 100L
    }
    committedOffsetMap.foreach { case (tierTopicPartition, offset) =>
      when(tierTopicManagerCommitter.positionFor(tierTopicPartition.partition)).thenReturn(offset)
    }

    tierTopicConsumer.startConsume(false, tierTopic)
    tierTopicConsumer.doWork()

    tierTopicConsumer.register(tp_1, ctx_1)
    tierTopicConsumer.register(tp_2, ctx_2)
    tierTopicConsumer.doWork()

    // partitions must be registered as catching up
    assertEquals(Set(tp_1, tp_2), tierTopicConsumer.catchingUpPartitions.keySet.asScala)

    val catchupConsumer = catchupConsumerSupplier.consumers.get(0)
    val assignment = catchupConsumer.assignment.asScala
    assertEquals(2, assignment.size)

    // advance catchup consumer to offset 50; should continue being in catchup state
    assignment.foreach { assignedPartition =>
      catchupConsumer.seek(assignedPartition, 50L)
    }
    tierTopicConsumer.doWork()
    assertEquals(Set(tp_1, tp_2), tierTopicConsumer.catchingUpPartitions.keySet.asScala)

    // advance one of the partitions to 100; should continue being in catchup state
    catchupConsumer.seek(assignment.head, 100L)
    tierTopicConsumer.doWork()
    assertEquals(Set(tp_1, tp_2), tierTopicConsumer.catchingUpPartitions.keySet.asScala)
    verify(ctx_1, times(0)).completeCatchup()
    verify(ctx_2, times(0)).completeCatchup()

    // advance first partition to 150 and second to 100; should transition to online state as the catchup consumer
    // positions are now >= positions of the primary consumer.
    catchupConsumer.seek(assignment.head, 100L)
    catchupConsumer.seek(assignment.last, 150L)
    tierTopicConsumer.doWork()
    assertEquals(Set(), tierTopicConsumer.catchingUpPartitions.keySet.asScala)
    assertEquals(Set(tp_1, tp_2), tierTopicConsumer.onlinePartitions.keySet.asScala)
    verify(ctx_1, times(1)).completeCatchup()
    verify(ctx_2, times(1)).completeCatchup()
  }
}
