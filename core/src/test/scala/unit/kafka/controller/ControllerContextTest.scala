/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package unit.kafka.controller

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import kafka.controller.ReplicaAssignment.Assignment
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.controller.{ControllerContext, ReplicaAssignment}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.{Before, Test}
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.assertFalse


class ControllerContextTest {

  var context: ControllerContext = null
  val brokers: Seq[Int] = Seq(1, 2, 3)
  val tp1 = new TopicPartition("A", 0)
  val tp2 = new TopicPartition("A", 1)
  val tp3 = new TopicPartition("B", 0)

  @Before
  def setUp(): Unit = {
    context = new ControllerContext

    val brokerEpochs = Seq(1,2,3).map { brokerId =>
      val endpoint = new EndPoint("localhost", 9900 + brokerId, new ListenerName("PLAINTEXT"),
        SecurityProtocol.PLAINTEXT)
      Broker(brokerId, Seq(endpoint), rack = None) -> 1L
    }.toMap

    context.setLiveBrokers(brokerEpochs)

    // Simple round-robin replica assignment
    var leaderIndex = 0
    Seq(tp1, tp2, tp3).foreach { partition =>
      val replicas = brokers.indices.map { i =>
        brokers((i + leaderIndex) % brokers.size)
      }
      context.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(replicas, Seq.empty))
      leaderIndex += 1
    }
  }

  @Test
  def testUpdatePartitionReplicaAssignmentUpdatesReplicaAssignmentOnly(): Unit = {
    val expectedReplicas = Seq(4)
    context.updatePartitionFullReplicaAssignment(
      tp1,
      ReplicaAssignment(expectedReplicas, Seq.empty)
    )
    val assignment = context.partitionReplicaAssignment(tp1)
    val fullAssignment = context.partitionFullReplicaAssignment(tp1)

    assertEquals(expectedReplicas, assignment)
    assertEquals(expectedReplicas, fullAssignment.replicas)
    assertEquals(Seq(), fullAssignment.addingReplicas)
    assertEquals(Seq(), fullAssignment.removingReplicas)
  }

  @Test
  def testUpdatePartitionFullReplicaAssignmentUpdatesReplicaAssignment(): Unit = {
    val initialReplicas = Seq(4)
    context.updatePartitionFullReplicaAssignment(
      tp1,
      ReplicaAssignment(initialReplicas, Seq.empty)
    ) // update only the replicas
    val fullAssignment = context.partitionFullReplicaAssignment(tp1)
    assertEquals(initialReplicas, fullAssignment.replicas)
    assertEquals(Seq(), fullAssignment.addingReplicas)
    assertEquals(Seq(), fullAssignment.removingReplicas)

    val expectedFullAssignment = ReplicaAssignment(Seq(3), Seq(1), Seq(2), Seq.empty, Some(Seq.empty))
    context.updatePartitionFullReplicaAssignment(tp1, expectedFullAssignment)
    val updatedFullAssignment = context.partitionFullReplicaAssignment(tp1)
    assertEquals(expectedFullAssignment.replicas, updatedFullAssignment.replicas)
    assertEquals(expectedFullAssignment.addingReplicas, updatedFullAssignment.addingReplicas)
    assertEquals(expectedFullAssignment.removingReplicas, updatedFullAssignment.removingReplicas)
  }

  @Test
  def testPartitionReplicaAssignmentReturnsEmptySeqIfTopicOrPartitionDoesNotExist(): Unit = {
    val noTopicReplicas = context.partitionReplicaAssignment(new TopicPartition("NONEXISTENT", 0))
    assertEquals(Seq.empty, noTopicReplicas)
    val noPartitionReplicas = context.partitionReplicaAssignment(new TopicPartition("A", 100))
    assertEquals(Seq.empty, noPartitionReplicas)
  }

  @Test
  def testPartitionFullReplicaAssignmentReturnsEmptyAssignmentIfTopicOrPartitionDoesNotExist(): Unit = {
    val expectedEmptyAssignment = ReplicaAssignment.empty

    val noTopicAssignment = context.partitionFullReplicaAssignment(new TopicPartition("NONEXISTENT", 0))
    assertEquals(expectedEmptyAssignment, noTopicAssignment)
    val noPartitionAssignment = context.partitionFullReplicaAssignment(new TopicPartition("A", 100))
    assertEquals(expectedEmptyAssignment, noPartitionAssignment)
  }

  @Test
  def testPartitionReplicaAssignmentForTopicReturnsEmptyMapIfTopicDoesNotExist(): Unit = {
    assertEquals(Map.empty, context.partitionReplicaAssignmentForTopic("NONEXISTENT"))
  }

  @Test
  def testPartitionReplicaAssignmentForTopicReturnsExpectedReplicaAssignments(): Unit = {
    val expectedAssignments = Map(
      tp1 -> context.partitionReplicaAssignment(tp1),
      tp2 -> context.partitionReplicaAssignment(tp2)
    )
    val receivedAssignments = context.partitionReplicaAssignmentForTopic("A")
    assertEquals(expectedAssignments, receivedAssignments)
  }

  @Test
  def testPartitionReplicaAssignment(): Unit = {
    val reassigningPartition = ReplicaAssignment(
      List(1, 2, 3, 4, 5, 6), List(2, 3, 4), List(1, 5, 6), Seq.empty, Some(Seq.empty)
    )
    assertTrue(reassigningPartition.isBeingReassigned)
    assertEquals(
      Some(ReplicaAssignment.Assignment(List(2, 3, 4), Seq.empty)), reassigningPartition.targetAssignment
    )

    val reassigningPartition2 = ReplicaAssignment(
      List(1, 2, 3, 4), List(), List(1, 4), Seq.empty, Some(Seq.empty)
    )
    assertTrue(reassigningPartition2.isBeingReassigned)
    assertEquals(Some(ReplicaAssignment.Assignment(List(2, 3), Seq.empty)), reassigningPartition2.targetAssignment)

    val reassigningPartition3 = ReplicaAssignment(List(1, 2, 3, 4), List(4), List(2), Seq.empty, Some(Seq.empty))
    assertTrue(reassigningPartition3.isBeingReassigned)
    assertEquals(
      Some(ReplicaAssignment.Assignment(List(1, 3, 4), Seq.empty)), reassigningPartition3.targetAssignment
    )

    val partition = ReplicaAssignment(List(1, 2, 3, 4, 5, 6), Seq.empty)
    assertFalse(partition.isBeingReassigned)
    assertEquals(ReplicaAssignment(List(1, 2, 3, 4, 5, 6), Seq.empty), partition.targetReplicaAssignment)

    val reassigningPartition4 =
      ReplicaAssignment(List(1, 2, 3, 4), Seq.empty)
        .reassignTo(ReplicaAssignment.Assignment(List(4, 2, 5, 3), Seq.empty))
    assertEquals(List(4, 2, 5, 3, 1), reassigningPartition4.replicas)
    assertEquals(
      Some(ReplicaAssignment.Assignment(List(4, 2, 5, 3), Seq.empty)), reassigningPartition4.targetAssignment
    )
    assertEquals(List(5), reassigningPartition4.addingReplicas)
    assertEquals(List(1), reassigningPartition4.removingReplicas)
    assertTrue(reassigningPartition4.isBeingReassigned)

    val reassigningPartition5 =
      ReplicaAssignment(List(1, 2, 3), Seq.empty)
        .reassignTo(ReplicaAssignment.Assignment(List(4, 5, 6), Seq.empty))
    assertEquals(List(4, 5, 6, 1, 2, 3), reassigningPartition5.replicas)
    assertEquals(
      Some(ReplicaAssignment.Assignment(List(4, 5, 6), Seq.empty)), reassigningPartition5.targetAssignment
    )
    assertEquals(List(4, 5, 6), reassigningPartition5.addingReplicas)
    assertEquals(List(1, 2, 3), reassigningPartition5.removingReplicas)
    assertTrue(reassigningPartition5.isBeingReassigned)

    val nonReassigningPartition = ReplicaAssignment(List(1, 2, 3), Seq.empty)
      .reassignTo(ReplicaAssignment.Assignment(List(3, 1, 2), Seq.empty))
    assertEquals(List(3, 1, 2), nonReassigningPartition.replicas)
    assertEquals(
      ReplicaAssignment(List(3, 1, 2), Seq.empty), nonReassigningPartition.targetReplicaAssignment
    )
    assertEquals(List(), nonReassigningPartition.addingReplicas)
    assertEquals(List(), nonReassigningPartition.removingReplicas)
    assertFalse(nonReassigningPartition.isBeingReassigned)
  }

  @Test
  def testReassignmentFromObserverToSyncReplica(): Unit = {
    val initialAssignment = ReplicaAssignment(List(1, 2, 3), List(3))
    val newAssignment = Assignment(List(1, 2, 3), List())
    val reassignment = initialAssignment.reassignTo(newAssignment)
    assertEquals(List(1, 2, 3), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(), reassignment.removingReplicas)
    assertEquals(List(), reassignment.effectiveObservers)
  }

  @Test
  def testRemovalOfSyncReplica(): Unit = {
    val initialAssignment = ReplicaAssignment(List(1, 2, 3, 4), List(2, 3))
    val newAssignment = Assignment(List(1, 2, 3), List(2, 3))
    val reassignment = initialAssignment.reassignTo(newAssignment)
    assertEquals(List(1, 4, 2, 3), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(4), reassignment.removingReplicas)
    assertEquals(List(2, 3), reassignment.effectiveObservers)
  }

  @Test
  def testRemovalOfObserver(): Unit = {
    val initialAssignment = ReplicaAssignment(List(1, 2, 3, 4), List(4, 3))
    val newAssignment = Assignment(List(1, 2, 3), List(3))
    val reassignment = initialAssignment.reassignTo(newAssignment)
    assertEquals(List(1, 2, 3, 4), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(4), reassignment.removingReplicas)
    assertEquals(List(3, 4), reassignment.effectiveObservers)
  }

  @Test
  def testRemovalOfObserverAndMakeSyncReplicaIntoObserver(): Unit = {
    val initialAssignment = ReplicaAssignment(List(1, 2, 3, 4), List(4, 3))
    val newAssignment = Assignment(List(1, 2, 3), List(2, 3))
    val reassignment = initialAssignment.reassignTo(newAssignment)
    assertEquals(List(1, 2, 3, 4), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(4), reassignment.removingReplicas)
    assertEquals(List(2, 3, 4), reassignment.effectiveObservers)
  }

  @Test
  def testRemovalOfSyncReplicaAndObserverChange(): Unit = {
    val initialAssignment = ReplicaAssignment(List(1, 2, 3, 4), List(4, 3))
    val newAssignment = Assignment(List(4, 2, 3), List(2, 3))
    val reassignment = initialAssignment.reassignTo(newAssignment)
    assertEquals(List(4, 1, 2, 3), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(1), reassignment.removingReplicas)
    assertEquals(List(2, 3), reassignment.effectiveObservers)
  }

  @Test
  def testReassignmentSwapsObserversAndSyncReplicas(): Unit = {
    val initialAssignment = ReplicaAssignment(List(1, 2, 3, 4), List(3, 4))
    val newAssignment = Assignment(List(3, 4, 1, 2), List(1, 2))
    val reassignment = initialAssignment.reassignTo(newAssignment)

    assertEquals(Seq(3, 4, 1, 2), reassignment.replicas)
    assertEquals(Seq(1, 2), reassignment.effectiveObservers)
    assertEquals(Seq(), reassignment.addingReplicas)
    assertEquals(Seq(), reassignment.removingReplicas)
  }

  @Test
  def testReassignTo(): Unit = {
    val assignment = ReplicaAssignment(Seq(1, 2, 3), Seq.empty)
    val firstReassign = assignment.reassignTo(ReplicaAssignment.Assignment(Seq(4, 5, 6), Seq.empty))

    assertEquals(
      ReplicaAssignment(Seq(4, 5, 6, 1, 2, 3), Seq(4, 5, 6), Seq(1, 2, 3), Seq.empty, Some(Seq.empty)),
      firstReassign
    )
    assertEquals(
      ReplicaAssignment(Seq(7, 8, 9, 1, 2, 3), Seq(7, 8, 9), Seq(1, 2, 3), Seq.empty, Some(Seq.empty)),
      firstReassign.reassignTo(ReplicaAssignment.Assignment(Seq(7, 8, 9), Seq.empty))
    )
    assertEquals(
      ReplicaAssignment(Seq(7, 8, 9, 1, 2, 3), Seq(7, 8, 9), Seq(1, 2, 3), Seq.empty, Some(Seq.empty)),
      assignment.reassignTo(ReplicaAssignment.Assignment(Seq(7, 8, 9), Seq.empty))
    )
    assertEquals(assignment, firstReassign.reassignTo(ReplicaAssignment.Assignment(Seq(1,2,3), Seq.empty)))
  }

  @Test
  def testPreferredReplicaImbalanceMetric(): Unit = {
    context.updatePartitionFullReplicaAssignment(tp1, ReplicaAssignment(Seq(1, 2, 3), Seq.empty))
    context.updatePartitionFullReplicaAssignment(tp2, ReplicaAssignment(Seq(1, 2, 3), Seq.empty))
    context.updatePartitionFullReplicaAssignment(tp3, ReplicaAssignment(Seq(1, 2, 3), Seq.empty))
    assertEquals(0, context.preferredReplicaImbalanceCount)

    context.putPartitionLeadershipInfo(tp1, LeaderIsrAndControllerEpoch(LeaderAndIsr(1, List(1, 2, 3), false), 0))
    assertEquals(0, context.preferredReplicaImbalanceCount)

    context.putPartitionLeadershipInfo(tp2, LeaderIsrAndControllerEpoch(LeaderAndIsr(2, List(2, 3, 1), false), 0))
    assertEquals(1, context.preferredReplicaImbalanceCount)

    context.putPartitionLeadershipInfo(tp3, LeaderIsrAndControllerEpoch(LeaderAndIsr(3, List(3, 1, 2), false), 0))
    assertEquals(2, context.preferredReplicaImbalanceCount)

    context.updatePartitionFullReplicaAssignment(tp1, ReplicaAssignment(Seq(2, 3, 1), Seq.empty))
    context.updatePartitionFullReplicaAssignment(tp2, ReplicaAssignment(Seq(2, 3, 1), Seq.empty))
    assertEquals(2, context.preferredReplicaImbalanceCount)

    context.queueTopicDeletion(Set(tp3.topic))
    assertEquals(1, context.preferredReplicaImbalanceCount)

    context.putPartitionLeadershipInfo(tp3, LeaderIsrAndControllerEpoch(LeaderAndIsr(1, List(3, 1, 2), false), 0))
    assertEquals(1, context.preferredReplicaImbalanceCount)

    context.removeTopic(tp1.topic)
    context.removeTopic(tp2.topic)
    context.removeTopic(tp3.topic)
    assertEquals(0, context.preferredReplicaImbalanceCount)
  }
}
