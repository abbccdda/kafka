/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier

import java.util.{Optional, UUID}

import kafka.tier.TierReplicaManager.ChangeListener
import kafka.tier.state.FileTierPartitionState
import org.junit.{Before, Test}
import org.mockito.Mockito._

class TierReplicaManagerTest {
  private val listener = mock(classOf[ChangeListener])
  private val tierEnabledPartitionState = mock(classOf[FileTierPartitionState])
  private val tierDisabledPartitionState = mock(classOf[FileTierPartitionState])
  private val tierReplicaManager = new TierReplicaManager()

  @Before
  def setup(): Unit = {
    when(tierEnabledPartitionState.isTieringEnabled).thenReturn(true)
    when(tierEnabledPartitionState.topicIdPartition).thenReturn(Optional.of(new TopicIdPartition("foo", UUID.randomUUID, 0)))

    when(tierDisabledPartitionState.isTieringEnabled).thenReturn(false)
    when(tierDisabledPartitionState.topicIdPartition).thenReturn(Optional.of(new TopicIdPartition("bar", UUID.randomUUID, 0)))

    tierReplicaManager.addListener(listener)
  }

  @Test
  def testBecomeLeaderPropagation(): Unit = {
    tierReplicaManager.becomeLeader(tierEnabledPartitionState, 10)
    tierReplicaManager.becomeLeader(tierDisabledPartitionState, 10)
    verify(listener, times(1)).onBecomeLeader(tierEnabledPartitionState.topicIdPartition.get, 10)
    verifyNoMoreInteractions(listener)
  }

  @Test
  def testBecomeFollowerPropagation(): Unit = {
    tierReplicaManager.becomeFollower(tierEnabledPartitionState)
    tierReplicaManager.becomeFollower(tierDisabledPartitionState)
    verify(listener, times(1)).onBecomeFollower(tierEnabledPartitionState.topicIdPartition.get)
    verifyNoMoreInteractions(listener)
  }

  @Test
  def testDeletePropagation(): Unit = {
    tierReplicaManager.delete(tierEnabledPartitionState.topicIdPartition.get)
    tierReplicaManager.delete(tierDisabledPartitionState.topicIdPartition.get)
    verify(listener, times(1)).onDelete(tierEnabledPartitionState.topicIdPartition.get)

    // We currently propagate deletion regardless of whether tiering was enabled, mainly for two reasons - it does not
    // have much of an overhead and we can simplify some of the implementation during StopReplicaRequest handling.
    verify(listener, times(1)).onDelete(tierDisabledPartitionState.topicIdPartition.get)
    verifyNoMoreInteractions(listener)
  }
}
