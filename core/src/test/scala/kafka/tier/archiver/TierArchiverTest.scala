/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.archiver

import java.util
import java.util.UUID

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import kafka.log.{AbstractLog, LogSegment}
import kafka.server.ReplicaManager
import kafka.tier.state.TierPartitionState
import kafka.tier.store.TierObjectStore
import kafka.tier.topic.TierTopicManager
import kafka.tier.{TierMetadataManager, TopicIdPartition}
import kafka.utils.{MockTime, TestUtils}
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._

class TierArchiverTest {
  @Test
  def testLagCalculation(): Unit = {
    val topicIdPartition1: TopicIdPartition = new TopicIdPartition("mytopic-1", UUID.randomUUID, 0)
    val topicIdPartition2: TopicIdPartition = new TopicIdPartition("mytopic-2", UUID.randomUUID, 0)

    val replicaManager = mock(classOf[ReplicaManager])
    val log = mock(classOf[AbstractLog])
    val segment = mock(classOf[LogSegment])
    val tierMetadataManager = mock(classOf[TierMetadataManager])
    val tierPartitionState1 = mock(classOf[TierPartitionState])
    val tierPartitionState2 = mock(classOf[TierPartitionState])

    when(replicaManager.getLog(topicIdPartition1.topicPartition)).thenReturn(Some(log))
    when(replicaManager.getLog(topicIdPartition2.topicPartition)).thenReturn(Some(log))
    when(segment.size).thenReturn(Integer.MAX_VALUE)
    when(log.tierableLogSegments).thenReturn(List(segment, segment, segment, segment))
    when(tierPartitionState1.tieringEnabled).thenReturn(true)
    when(tierPartitionState2.tieringEnabled).thenReturn(true)
    when(tierPartitionState1.topicPartition).thenReturn(topicIdPartition1.topicPartition)
    when(tierPartitionState2.topicPartition).thenReturn(topicIdPartition2.topicPartition)
    when(tierMetadataManager.tierEnabledLeaderPartitionStateIterator).thenAnswer(new Answer[util.Iterator[TierPartitionState]] {
      override def answer(invocation: InvocationOnMock): util.Iterator[TierPartitionState] = {
        List(tierPartitionState1, tierPartitionState2).iterator.asJava
      }
    })

    // two logs * 4 segments * MAX_VALUE
    assertEquals(17179869176L, TierArchiver.totalLag(replicaManager, tierMetadataManager))

    val tierTopicManager: TierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore: TierObjectStore = mock(classOf[TierObjectStore])
    val time = new MockTime()

    TestUtils.clearYammerMetrics()
    new TierArchiver(TierArchiverConfig(), replicaManager, tierMetadataManager,
      tierTopicManager, tierObjectStore, time)
    assertEquals(17179869176L, metricValue("TotalLag"))
  }

  private def metricValue(name: String): Long = {
    Metrics.defaultRegistry.allMetrics.asScala.filterKeys(_.getName == name).values.headOption.get.asInstanceOf[Gauge[Long]].value()
  }
}
