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

package kafka.admin

import java.util.Optional

import kafka.controller.ReplicaAssignment
import kafka.server.BaseRequestTest
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata
import org.apache.kafka.common.requests.{MetadataRequest, MetadataResponse}
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class AddPartitionsTest extends BaseRequestTest {

  override def brokerCount: Int = 4

  val partitionId = 0

  val topic1 = "new-topic1"
  val topic1Assignment = Map(0 -> ReplicaAssignment(Seq(0,1), Seq.empty))
  val topic2 = "new-topic2"
  val topic2Assignment = Map(0 -> ReplicaAssignment(Seq(1,2), Seq.empty))
  val topic3 = "new-topic3"
  val topic3Assignment = Map(0 -> ReplicaAssignment(Seq(2,3,0,1), Seq.empty))
  val topic4 = "new-topic4"
  val topic4Assignment = Map(0 -> ReplicaAssignment(Seq(0,3), Seq.empty))
  val topic5 = "new-topic5"
  val topic5Assignment = Map(1 -> ReplicaAssignment(Seq(0,1), Seq.empty))

  @Before
  override def setUp(): Unit = {
    super.setUp()

    createTopic(topic1, partitionReplicaAssignment = topic1Assignment.mapValues(_.replicas).toMap)
    createTopic(topic2, partitionReplicaAssignment = topic2Assignment.mapValues(_.replicas).toMap)
    createTopic(topic3, partitionReplicaAssignment = topic3Assignment.mapValues(_.replicas).toMap)
    createTopic(topic4, partitionReplicaAssignment = topic4Assignment.mapValues(_.replicas).toMap)
  }

  @Test
  def testWrongReplicaCount(): Unit = {
    try {
      adminZkClient.addPartitions(
        topic1, topic1Assignment, adminZkClient.getBrokerMetadatas(), 2,
        Some(
          Map(
            0 -> ReplicaAssignment(Seq(0, 1), Seq.empty),
            1 -> ReplicaAssignment(Seq(0, 1, 2), Seq.empty)
          )
        )
      )
      fail("Add partitions should fail")
    } catch {
      case _: InvalidReplicaAssignmentException => //this is good
    }
  }

  @Test
  def testMissingPartition0(): Unit = {
    try {
      adminZkClient.addPartitions(
        topic5, topic5Assignment, adminZkClient.getBrokerMetadatas(), 2,
        Some(
          Map(
            1 -> ReplicaAssignment(Seq(0, 1), Seq.empty),
            2 -> ReplicaAssignment(Seq(0, 1, 2), Seq.empty)
          )
        )
      )
      fail("Add partitions should fail")
    } catch {
      case e: AdminOperationException => //this is good
        assertTrue(e.getMessage.contains("Unexpected existing replica assignment for topic 'new-topic5', partition id 0 is missing"))
    }
  }

  @Test
  def testIncrementPartitions(): Unit = {
    adminZkClient.addPartitions(topic1, topic1Assignment, adminZkClient.getBrokerMetadatas(), 3)
    // wait until leader is elected
    val leader1 = waitUntilLeaderIsElectedOrChanged(zkClient, topic1, 1)
    val leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic1, 2)
    val leader1FromZk = zkClient.getLeaderForPartition(new TopicPartition(topic1, 1)).get
    val leader2FromZk = zkClient.getLeaderForPartition(new TopicPartition(topic1, 2)).get
    assertEquals(leader1, leader1FromZk)
    assertEquals(leader2, leader2FromZk)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic1, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic1, 2)
    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topic1).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val partitions = response.topicMetadata.asScala.head.partitionMetadata.asScala.sortBy(_.partition)
    assertEquals(partitions.size, 3)
    assertEquals(1, partitions(1).partition)
    assertEquals(2, partitions(2).partition)

    for (partition <- partitions) {
      val replicas = partition.replicaIds
      assertEquals(2, replicas.size)
      assertTrue(partition.leaderId.isPresent)
      val leaderId = partition.leaderId.get
      assertTrue(replicas.contains(leaderId))
    }
  }

  @Test
  def testManualAssignmentOfReplicas(): Unit = {
    // Add 2 partitions
    adminZkClient.addPartitions(
      topic2, topic2Assignment, adminZkClient.getBrokerMetadatas(), 3,
      Some(
        Map(
          0 -> ReplicaAssignment(Seq(1, 2), Seq.empty),
          1 -> ReplicaAssignment(Seq(0, 1), Seq.empty),
          2 -> ReplicaAssignment(Seq(2, 3), Seq.empty)
        )
      )
    )
    // wait until leader is elected
    val leader1 = waitUntilLeaderIsElectedOrChanged(zkClient, topic2, 1)
    val leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic2, 2)
    val leader1FromZk = zkClient.getLeaderForPartition(new TopicPartition(topic2, 1)).get
    val leader2FromZk = zkClient.getLeaderForPartition(new TopicPartition(topic2, 2)).get
    assertEquals(leader1, leader1FromZk)
    assertEquals(leader2, leader2FromZk)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 2)
    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topic2).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val topicMetadata = response.topicMetadata.asScala.head
    val partitionMetadata = topicMetadata.partitionMetadata.asScala.sortBy(_.partition)
    assertEquals(3, topicMetadata.partitionMetadata.size)
    assertEquals(0, partitionMetadata(0).partition)
    assertEquals(1, partitionMetadata(1).partition)
    assertEquals(2, partitionMetadata(2).partition)
    val replicas = partitionMetadata(1).replicaIds
    assertEquals(2, replicas.size)
    assertEquals(Set(0, 1), replicas.asScala.toSet)
  }

  @Test
  def testReplicaPlacementAllServers(): Unit = {
    adminZkClient.addPartitions(topic3, topic3Assignment, adminZkClient.getBrokerMetadatas(), 7)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 2)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 3)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 4)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 5)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 6)

    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topic3).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val topicMetadata = response.topicMetadata.asScala.head
    validateLeaderAndReplicas(topicMetadata, 0, 2, Set(2, 3, 0, 1))
    validateLeaderAndReplicas(topicMetadata, 1, 3, Set(3, 2, 0, 1))
    validateLeaderAndReplicas(topicMetadata, 2, 0, Set(0, 3, 1, 2))
    validateLeaderAndReplicas(topicMetadata, 3, 1, Set(1, 0, 2, 3))
    validateLeaderAndReplicas(topicMetadata, 4, 2, Set(2, 3, 0, 1))
    validateLeaderAndReplicas(topicMetadata, 5, 3, Set(3, 0, 1, 2))
    validateLeaderAndReplicas(topicMetadata, 6, 0, Set(0, 1, 2, 3))
  }

  @Test
  def testReplicaPlacementPartialServers(): Unit = {
    adminZkClient.addPartitions(topic2, topic2Assignment, adminZkClient.getBrokerMetadatas(), 3)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 2)

    val response = connectAndReceive[MetadataResponse](
      new MetadataRequest.Builder(Seq(topic2).asJava, false).build)
    assertEquals(1, response.topicMetadata.size)
    val topicMetadata = response.topicMetadata.asScala.head
    validateLeaderAndReplicas(topicMetadata, 0, 1, Set(1, 2))
    validateLeaderAndReplicas(topicMetadata, 1, 2, Set(0, 2))
    validateLeaderAndReplicas(topicMetadata, 2, 3, Set(1, 3))
  }

  def validateLeaderAndReplicas(metadata: TopicMetadata, partitionId: Int, expectedLeaderId: Int,
                                expectedReplicas: Set[Int]): Unit = {
    val partitionOpt = metadata.partitionMetadata.asScala.find(_.partition == partitionId)
    assertTrue(s"Partition $partitionId should exist", partitionOpt.isDefined)
    val partition = partitionOpt.get

    assertEquals("Partition leader id should match", Optional.of(expectedLeaderId), partition.leaderId)
    assertEquals("Replica set should match", expectedReplicas, partition.replicaIds.asScala.toSet)
  }

}
