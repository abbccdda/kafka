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

import kafka.cluster.{Broker, EndPoint}
import kafka.controller.PartitionReplicaAssignment.Assignment
import kafka.controller.{ControllerContext, PartitionReplicaAssignment}
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

    context.setLiveBrokerAndEpochs(brokerEpochs)

    // Simple round-robin replica assignment
    var leaderIndex = 0
    Seq(tp1, tp2, tp3).foreach {
      partition =>
        val replicas = brokers.indices.map { i =>
          val replica = brokers((i + leaderIndex) % brokers.size)
          replica
        }
        context.updatePartitionFullReplicaAssignment(
          partition,
          PartitionReplicaAssignment.fromCreate(replicas, Seq.empty)
        )
        leaderIndex += 1
    }
  }

  @Test
  def testUpdatePartitionReplicaAssignmentUpdatesReplicaAssignmentOnly(): Unit = {
    val expectedReplicas = Seq(4)
    context.updatePartitionFullReplicaAssignment(
      tp1,
      PartitionReplicaAssignment.fromCreate(expectedReplicas, Seq.empty)
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
      PartitionReplicaAssignment.fromCreate(initialReplicas, Seq.empty)
    ) // update only the replicas
    val fullAssignment = context.partitionFullReplicaAssignment(tp1)
    assertEquals(initialReplicas, fullAssignment.replicas)
    assertEquals(Seq(), fullAssignment.addingReplicas)
    assertEquals(Seq(), fullAssignment.removingReplicas)

    val expectedFullAssignment = PartitionReplicaAssignment(Seq(3), Seq(1), Seq(2), Seq.empty, Some(Seq.empty))
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
    val expectedEmptyAssignment = PartitionReplicaAssignment.empty

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
    val reassigningPartition = PartitionReplicaAssignment(
      List(1, 2, 3, 4, 5, 6), List(2, 3, 4), List(1, 5, 6), Seq.empty, Some(Seq.empty)
    )
    assertTrue(reassigningPartition.isBeingReassigned)
    assertEquals(List(2, 3, 4), reassigningPartition.targetReplicas)

    val reassigningPartition2 = PartitionReplicaAssignment(
      List(1, 2, 3, 4), List(), List(1, 4), Seq.empty, Some(Seq.empty)
    )
    assertTrue(reassigningPartition2.isBeingReassigned)
    assertEquals(List(2, 3), reassigningPartition2.targetReplicas)

    val reassigningPartition3 = PartitionReplicaAssignment(List(1, 2, 3, 4), List(4), List(2), Seq.empty, Some(Seq.empty))
    assertTrue(reassigningPartition3.isBeingReassigned)
    assertEquals(List(1, 3, 4), reassigningPartition3.targetReplicas)

    val partition = PartitionReplicaAssignment.fromCreate(List(1, 2, 3, 4, 5, 6), Seq.empty)
    assertFalse(partition.isBeingReassigned)
    assertEquals(List(1, 2, 3, 4, 5, 6), partition.targetReplicas)

    val reassigningPartition4 = PartitionReplicaAssignment.fromOriginalAndTarget(
      PartitionReplicaAssignment.Assignment(List(1, 2, 3, 4), Seq.empty),
      PartitionReplicaAssignment.Assignment(List(4, 2, 5, 3), Seq.empty)
    )
    assertEquals(List(4, 2, 5, 3, 1), reassigningPartition4.replicas)
    assertEquals(List(4, 2, 5, 3), reassigningPartition4.targetReplicas)
    assertEquals(List(5), reassigningPartition4.addingReplicas)
    assertEquals(List(1), reassigningPartition4.removingReplicas)
    assertTrue(reassigningPartition4.isBeingReassigned)

    val reassigningPartition5 = PartitionReplicaAssignment.fromOriginalAndTarget(
      PartitionReplicaAssignment.Assignment(List(1, 2, 3), Seq.empty),
      PartitionReplicaAssignment.Assignment(List(4, 5, 6), Seq.empty)
    )
    assertEquals(List(4, 5, 6, 1, 2, 3), reassigningPartition5.replicas)
    assertEquals(List(4, 5, 6), reassigningPartition5.targetReplicas)
    assertEquals(List(4, 5, 6), reassigningPartition5.addingReplicas)
    assertEquals(List(1, 2, 3), reassigningPartition5.removingReplicas)
    assertTrue(reassigningPartition5.isBeingReassigned)

    val nonReassigningPartition = PartitionReplicaAssignment.fromOriginalAndTarget(
      PartitionReplicaAssignment.Assignment(List(1, 2, 3), Seq.empty),
      PartitionReplicaAssignment.Assignment(List(3, 1, 2), Seq.empty)
    )
    assertEquals(List(3, 1, 2), nonReassigningPartition.replicas)
    assertEquals(List(3, 1, 2), nonReassigningPartition.targetReplicas)
    assertEquals(List(), nonReassigningPartition.addingReplicas)
    assertEquals(List(), nonReassigningPartition.removingReplicas)
    assertFalse(nonReassigningPartition.isBeingReassigned)
  }

  @Test
  def testReassignmentFromObserverToSyncReplica(): Unit = {
    val initialAssignment = Assignment(List(1, 2, 3), List(3))
    val newAssignment = Assignment(List(1, 2, 3), List())
    val reassignment = PartitionReplicaAssignment.fromOriginalAndTarget(initialAssignment, newAssignment)
    assertEquals(List(1, 2, 3), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(), reassignment.removingReplicas)
    assertEquals(List(), reassignment.effectiveObservers)
  }

  @Test
  def testRemovalOfSyncReplica(): Unit = {
    val initialAssignment = Assignment(List(1, 2, 3, 4), List(2, 3))
    val newAssignment = Assignment(List(1, 2, 3), List(2, 3))
    val reassignment = PartitionReplicaAssignment.fromOriginalAndTarget(initialAssignment, newAssignment)
    assertEquals(List(1, 4, 2, 3), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(4), reassignment.removingReplicas)
    assertEquals(List(2, 3), reassignment.effectiveObservers)
  }

  @Test
  def testRemovalOfObserver(): Unit = {
    val initialAssignment = Assignment(List(1, 2, 3, 4), List(4, 3))
    val newAssignment = Assignment(List(1, 2, 3), List(3))
    val reassignment = PartitionReplicaAssignment.fromOriginalAndTarget(initialAssignment, newAssignment)
    assertEquals(List(1, 2, 3, 4), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(4), reassignment.removingReplicas)
    assertEquals(List(3, 4), reassignment.effectiveObservers)
  }

  @Test
  def testRemovalOfObserverAndMakeSyncReplicaIntoObserver(): Unit = {
    val initialAssignment = Assignment(List(1, 2, 3, 4), List(4, 3))
    val newAssignment = Assignment(List(1, 2, 3), List(2, 3))
    val reassignment = PartitionReplicaAssignment.fromOriginalAndTarget(initialAssignment, newAssignment)
    assertEquals(List(1, 2, 3, 4), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(4), reassignment.removingReplicas)
    assertEquals(List(2, 3, 4), reassignment.effectiveObservers)
  }

  @Test
  def testRemovalOfSyncReplicaAndObserverChange(): Unit = {
    val initialAssignment = Assignment(List(1, 2, 3, 4), List(4, 3))
    val newAssignment = Assignment(List(4, 2, 3), List(2, 3))
    val reassignment = PartitionReplicaAssignment.fromOriginalAndTarget(initialAssignment, newAssignment)
    assertEquals(List(4, 1, 2, 3), reassignment.replicas)
    assertEquals(List(), reassignment.addingReplicas)
    assertEquals(List(1), reassignment.removingReplicas)
    assertEquals(List(2, 3), reassignment.effectiveObservers)
  }

  @Test
  def testReassignmentSwapsObserversAndSyncReplicas(): Unit = {
    val initialAssignment = Assignment(List(1, 2, 3, 4), List(3, 4))
    val newAssignment = Assignment(List(3, 4, 1, 2), List(1, 2))
    val reassignment = PartitionReplicaAssignment.fromOriginalAndTarget(initialAssignment, newAssignment)

    assertEquals(Seq(3, 4, 1, 2), reassignment.replicas)
    assertEquals(Seq(1, 2), reassignment.effectiveObservers)
    assertEquals(Seq(), reassignment.addingReplicas)
    assertEquals(Seq(), reassignment.removingReplicas)
  }

}
