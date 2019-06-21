/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.archiver

import java.util.UUID

import kafka.log.{AbstractLog, LogSegment}
import kafka.server.ReplicaManager
import kafka.tier.state.TierPartitionState
import kafka.tier.{TierMetadataManager, TopicIdPartition}
import org.easymock.EasyMock
import org.junit.Assert.assertEquals
import org.junit.Test
import scala.collection.JavaConverters._

class TierArchiverTest {
  @Test
  def testLagCalculation(): Unit = {
    val topicIdPartition1: TopicIdPartition = new TopicIdPartition("mytopic-1", UUID.randomUUID, 0)
    val topicIdPartition2: TopicIdPartition = new TopicIdPartition("mytopic-2", UUID.randomUUID, 0)

    val replicaManager: ReplicaManager = EasyMock.mock(classOf[ReplicaManager])
    val log: AbstractLog = EasyMock.mock(classOf[AbstractLog])
    val segment: LogSegment = EasyMock.mock(classOf[LogSegment])
    val tierMetadataManager: TierMetadataManager = EasyMock.mock(classOf[TierMetadataManager])
    val tierPartitionState1: TierPartitionState = EasyMock.mock(classOf[TierPartitionState])
    val tierPartitionState2: TierPartitionState = EasyMock.mock(classOf[TierPartitionState])

    EasyMock.expect(replicaManager.getLog(topicIdPartition1.topicPartition)).andReturn(Some(log))
    EasyMock.expect(replicaManager.getLog(topicIdPartition2.topicPartition)).andReturn(Some(log))
    EasyMock.expect(segment.size).andStubReturn(Integer.MAX_VALUE)
    EasyMock.expect(log.tierableLogSegments).andStubReturn(List(segment, segment, segment, segment))
    EasyMock.expect(tierPartitionState1.tieringEnabled).andReturn(true)
    EasyMock.expect(tierPartitionState2.tieringEnabled).andReturn(true)
    EasyMock.expect(tierPartitionState1.topicPartition).andReturn(topicIdPartition1.topicPartition)
    EasyMock.expect(tierPartitionState2.topicPartition).andReturn(topicIdPartition2.topicPartition)
    EasyMock.expect(tierMetadataManager.tierEnabledLeaderPartitionStateIterator).andReturn(List(tierPartitionState1, tierPartitionState2).iterator.asJava)

    EasyMock.replay(replicaManager, log, segment, tierMetadataManager, tierPartitionState1, tierPartitionState2)
    // two logs * 4 segments * MAX_VALUE
    assertEquals(17179869176L, TierArchiver.totalLag(replicaManager, tierMetadataManager))
  }
}
