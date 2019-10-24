/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.archive

import java.util.UUID

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
import kafka.tier.TopicIdPartition
import kafka.utils.{MockTime, TestUtils}
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
    val topicIdPartitions = List(new TopicIdPartition("mytopic-1", UUID.randomUUID, 0),
      new TopicIdPartition("mytopic-2", UUID.randomUUID, 0))

    val partitions =
      topicIdPartitions.map { topicIdPartition =>
        val segment = mock(classOf[LogSegment])
        when(segment.size).thenReturn(Integer.MAX_VALUE)

        val log = mock(classOf[AbstractLog])
        when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))

        // 4 segments, each with of Integer.MAX_VALUE size
        when(log.tierableLogSegments).thenReturn(List(segment, segment, segment, segment))

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

    // two logs * 4 segments * MAX_VALUE
    assertEquals(17179869176L, TierArchiver.totalLag(replicaManager))

    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = mock(classOf[TierObjectStore])
    val time = new MockTime()
    val config = TierTasksConfig(numThreads = 2)

    TestUtils.clearYammerMetrics()
    new TierArchiver(config, replicaManager, tierTopicManager, tierObjectStore, CancellationContext.newContext(),
      config.numThreads, time)
    assertEquals(17179869176L, metricValue("TotalLag"))
  }

  private def metricValue(name: String): Long = {
    Metrics.defaultRegistry.allMetrics.asScala.filterKeys(_.getName == name).values.headOption.get.asInstanceOf[Gauge[Long]].value()
  }
}
