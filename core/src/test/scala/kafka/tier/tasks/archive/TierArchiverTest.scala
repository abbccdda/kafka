/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.archive

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import kafka.cluster.Partition
import kafka.log.{AbstractLog, LogSegment}
import kafka.server.ReplicaManager
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.TierTasksConfig
import kafka.tier.topic.TierTopicManager
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class TierArchiverTest {
  @Test
  def testLagCalculation(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])

    val partition1 = new TopicPartition("mytopic-1", 0)
    val partition2 = new TopicPartition("mytopic-2", 0)
    val partition3 = new TopicPartition("mytopic-3", 0)

    // Map of TopicPartition -> PerSegmentSize
    val topicIdPartitionsMap = Map(
      partition1 -> 20,
      partition2 -> 10,
      partition3 -> 30
    )

    val partitions =
      topicIdPartitionsMap.map { case (topicPartition, segmentSize) =>
        val segment = mock(classOf[LogSegment])
        when(segment.size).thenReturn(segmentSize)

        val log = mock(classOf[AbstractLog])
        when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

        // 4 segments, each with the same segment size
        when(log.tierableLogSegments).thenReturn(List(segment, segment, segment, segment))
        when(log.topicPartition).thenReturn(topicPartition)

        val tierPartitionState = mock(classOf[TierPartitionState])
        when(tierPartitionState.isTieringEnabled).thenReturn(true)
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
    assertEquals(3, laggingPartitions.size)
    assertEquals((partition3, 120), laggingPartitions(0))
    assertEquals((partition1, 80), laggingPartitions(1))
    assertEquals((partition2, 40), laggingPartitions(2))

    archiver.logPartitionLagInfo()
    // Expect the total lag to match that of: num_logs x num_segments_per_log x per_segment_size
    assertEquals(240L, metricValue[Long]("TotalLagValue"))
    assertEquals(120, metricValue[Long]("PartitionLagMaxValue"))
    assertEquals(3, metricValue[Int]("LaggingPartitionsCount"))
  }

  private def metricValue[T](name: String): T = {
    Metrics.defaultRegistry.allMetrics.asScala.filterKeys(_.getName == name).values.headOption.get.asInstanceOf[Gauge[T]].value()
  }
}
