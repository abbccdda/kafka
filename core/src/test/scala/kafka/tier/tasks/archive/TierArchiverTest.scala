/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.archive

import com.yammer.metrics.core.Gauge
import kafka.cluster.Partition
import kafka.log.{AbstractLog, LogSegment}
import kafka.metrics.KafkaYammerMetrics
import kafka.server.ReplicaManager
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState
import kafka.tier.state.TierPartitionStatus
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.TierTasksConfig
import kafka.tier.topic.TierTopicManager
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class TierArchiverTest {
  @Test
  def testLagCalculationWithReset(): Unit = {
    val topicPartition = new TopicPartition("mytopic-1", 0)
    val replicaManager = mock(classOf[ReplicaManager])

    val segment = mock(classOf[LogSegment])
    when(segment.size).thenReturn(20)

    val log = mock(classOf[AbstractLog])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    when(log.tierableLogSegments).thenReturn(List(segment))
    when(log.topicPartition).thenReturn(topicPartition)

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.isTieringEnabled).thenReturn(true)
    when(tierPartitionState.status).thenReturn(TierPartitionStatus.ONLINE)
    when(log.tierPartitionState).thenReturn(tierPartitionState)

    val partition = mock(classOf[Partition])
    when(partition.log).thenReturn(Some(log))

    when(replicaManager.leaderPartitionsIterator).thenAnswer(new Answer[Iterator[Partition]] {
      override def answer(invocation: InvocationOnMock): Iterator[Partition] = {
        List(partition).iterator
      }
    })
    val config = TierTasksConfig(numThreads = 2)
    TestUtils.clearYammerMetrics()
    val archiver = new TierArchiver(
      config,
      replicaManager,
      mock(classOf[TierTopicManager]),
      mock(classOf[TierObjectStore]),
      CancellationContext.newContext(),
      config.numThreads,
      new MockTime())

    def checkLag(expectedLag: Int) = {
      val laggingPartitions = archiver.partitionLagInfo
      val expectedPartitions = if (expectedLag > 0) 1 else 0
      assertEquals(expectedPartitions, laggingPartitions.size)
      if (laggingPartitions.size == 1) {
        assertEquals(
          (topicPartition, TierPartitionStatus.ONLINE, expectedLag), laggingPartitions(0))
      }

      archiver.logPartitionLagInfo()

      assertEquals(expectedLag, metricValue[Long]("TotalLag"))
      assertEquals(expectedLag, metricValue[Long]("TotalLagWithoutErrorPartitions"))
      assertEquals(expectedLag, metricValue[Long]("PartitionLagMaxValue"))
      assertEquals(expectedPartitions, metricValue[Int]("LaggingPartitionsCount"))
    }

    // 1. First call to logPartitionLagInfo() should log the lagging partition.
    checkLag(20)

    // 2. Second call to logPartitionLagInfo() should reset metrics to 0, since, the partition is
    // not lagging now.
    when(replicaManager.leaderPartitionsIterator).thenAnswer(new Answer[Iterator[Partition]] {
      override def answer(invocation: InvocationOnMock): Iterator[Partition] = {
        List().iterator
      }
    })
    checkLag(0)
  }

  @Test
  def testLagCalculation(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])

    val partition1 = new TopicPartition("mytopic-1", 0)
    val partition2 = new TopicPartition("mytopic-2", 0)
    val partition3 = new TopicPartition("mytopic-3", 0)
    val partition4 = new TopicPartition("mytopic-4", 0)
    val partition5 = new TopicPartition("mytopic-5", 0)
    val partition6 = new TopicPartition("mytopic-6", 0)

    // Map of TopicPartition -> (PerSegmentSize, TierPartitionStatus)
    val topicIdPartitionsMap = Map(
      partition1 -> (20, TierPartitionStatus.ONLINE),
      partition2 -> (10, TierPartitionStatus.ONLINE),
      partition3 -> (30, TierPartitionStatus.ONLINE),
      partition4 -> (20, TierPartitionStatus.ERROR),
      partition5 -> (10, TierPartitionStatus.ERROR),
      partition6 -> (30, TierPartitionStatus.ERROR),
    )

    val partitions =
      topicIdPartitionsMap.map { case (topicPartition, partitionInfo) =>
        val segmentSize = partitionInfo._1
        val status = partitionInfo._2

        val segment = mock(classOf[LogSegment])
        when(segment.size).thenReturn(segmentSize)

        val log = mock(classOf[AbstractLog])
        when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

        // 4 segments, each with the same segment size
        when(log.tierableLogSegments).thenReturn(List(segment, segment, segment, segment))
        when(log.topicPartition).thenReturn(topicPartition)

        val tierPartitionState = mock(classOf[TierPartitionState])
        when(tierPartitionState.isTieringEnabled).thenReturn(true)
        when(tierPartitionState.status).thenReturn(status)
        when(log.tierPartitionState).thenReturn(tierPartitionState)

        val partition = mock(classOf[Partition])
        when(partition.log).thenReturn(Some(log))
        partition
      }

    when(replicaManager.leaderPartitionsIterator).thenAnswer(new Answer[Iterator[Partition]] {
      override def answer(invocation: InvocationOnMock): Iterator[Partition] = {
        partitions.iterator
      }
    })
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = mock(classOf[TierObjectStore])
    val time = new MockTime()
    val config = TierTasksConfig(numThreads = 2)
    TestUtils.clearYammerMetrics()
    val archiver = new TierArchiver(
      config,
      replicaManager,
      tierTopicManager,
      tierObjectStore,
      CancellationContext.newContext(),
      config.numThreads,
      time)

    val laggingPartitions = archiver.partitionLagInfo
    assertEquals(6, laggingPartitions.size)
    assertEquals((partition6, TierPartitionStatus.ERROR, 120), laggingPartitions(0))
    assertEquals((partition3, TierPartitionStatus.ONLINE, 120), laggingPartitions(1))
    assertEquals((partition1, TierPartitionStatus.ONLINE, 80), laggingPartitions(2))
    assertEquals((partition4, TierPartitionStatus.ERROR, 80), laggingPartitions(3))
    assertEquals((partition5, TierPartitionStatus.ERROR, 40), laggingPartitions(4))
    assertEquals((partition2, TierPartitionStatus.ONLINE, 40), laggingPartitions(5))

    archiver.logPartitionLagInfo()
    // Expect the total lag to match that of: num_logs x num_segments_per_log x per_segment_size
    assertEquals(480L, metricValue[Long]("TotalLag"))
    assertEquals(240L, metricValue[Long]("TotalLagWithoutErrorPartitions"))
    assertEquals(120, metricValue[Long]("PartitionLagMaxValue"))
    assertEquals(6, metricValue[Int]("LaggingPartitionsCount"))
  }

  private def metricValue[T](name: String): T = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filterKeys(_.getName == name).values.headOption.get.asInstanceOf[Gauge[T]].value()
  }
}
