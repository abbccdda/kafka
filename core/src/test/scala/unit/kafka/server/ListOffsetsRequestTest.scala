/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util.{Optional, Properties}

import kafka.utils.TestUtils
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ListOffsetRequest, ListOffsetResponse}
import org.apache.kafka.common.{IsolationLevel, TopicPartition}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class ListOffsetsRequestTest extends BaseRequestTest {

  @Test
  def testListOffsetsRequestForNonTieredPartition(): Unit = {
    val topicName: String = "test-topic"
    val props = new Properties
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "16384")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    val partitionToLeaderMap = createTopic(topicName, 1, 1, props)

    val numMessages = 3000
    TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)

    val topicPartition = new TopicPartition(topicName, 0)
    val leaderId = partitionToLeaderMap(topicPartition.partition())
    val server = serverForId(leaderId).get
    val log = server.logManager.getLog(topicPartition).get
    TestUtils.waitUntilTrue(() =>
      (log.logEndOffset == numMessages),
      "Timeout waiting for all records to be produced", 180000)

    makeListOffsetsRequestAndValidateResponse(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP, leaderId, log, 0)
    // advance the log start offset and run again
    log.maybeIncrementHighWatermark(LogOffsetMetadata(log.logStartOffset + 100))
    log.maybeIncrementLogStartOffset(log.logStartOffset + 100)
    makeListOffsetsRequestAndValidateResponse(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP, leaderId, log, 0)
    makeListOffsetsRequestAndValidateResponse(topicPartition, ListOffsetRequest.EARLIEST_TIMESTAMP, leaderId, log, 0)
  }

  @Test
  def testListOffsetsErrorCodes(): Unit = {
    val topic = "topic"
    val partition = new TopicPartition(topic, 0)
    val targetTimes = Map(partition -> new ListOffsetRequest.PartitionData(
      ListOffsetRequest.EARLIEST_TIMESTAMP, Optional.of[Integer](0))).asJava

    val consumerRequest = ListOffsetRequest.Builder
      .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
      .setTargetTimes(targetTimes)
      .build()

    val replicaRequest = ListOffsetRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, servers.head.config.brokerId)
      .setTargetTimes(targetTimes)
      .build()

    val debugReplicaRequest = ListOffsetRequest.Builder
      .forReplica(ApiKeys.LIST_OFFSETS.latestVersion, ListOffsetRequest.DEBUGGING_REPLICA_ID)
      .setTargetTimes(targetTimes)
      .build()

    // Unknown topic
    val randomBrokerId = servers.head.config.brokerId
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, consumerRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, replicaRequest)
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, debugReplicaRequest)

    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 2, servers)
    val replicas = zkClient.getReplicasForPartition(partition).toSet
    val leader = partitionToLeader(partition.partition)
    val follower = replicas.find(_ != leader).get
    val nonReplica = servers.map(_.config.brokerId).find(!replicas.contains(_)).get

    // Follower
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, follower, consumerRequest)
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, follower, replicaRequest)
    assertResponseError(Errors.NONE, follower, debugReplicaRequest)

    // Non-replica
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, nonReplica, consumerRequest)
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, nonReplica, replicaRequest)
    assertResponseError(Errors.REPLICA_NOT_AVAILABLE, nonReplica, debugReplicaRequest)
  }

  @Test
  def testCurrentEpochValidation(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    def assertResponseErrorForEpoch(error: Errors, brokerId: Int, currentLeaderEpoch: Optional[Integer]): Unit = {
      val targetTimes = Map(topicPartition -> new ListOffsetRequest.PartitionData(
        ListOffsetRequest.EARLIEST_TIMESTAMP, currentLeaderEpoch)).asJava
      val request = ListOffsetRequest.Builder
        .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
        .setTargetTimes(targetTimes)
        .build()
      assertResponseError(error, brokerId, request)
    }

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    // Check leader error codes
    val secondLeaderId = TestUtils.awaitLeaderChange(servers, topicPartition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, servers)
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch + 1))

    // Check follower error codes
    val followerId = TestUtils.findFollowerId(topicPartition, servers)
    assertResponseErrorForEpoch(Errors.NOT_LEADER_FOR_PARTITION, followerId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NOT_LEADER_FOR_PARTITION, followerId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch + 1))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch - 1))
  }

  @Test
  def testResponseIncludesLeaderEpoch(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    TestUtils.generateAndProduceMessages(servers, topic, 10)

    def fetchOffsetAndEpoch(serverId: Int,
                            timestamp: Long): (Long, Optional[Integer]) = {
      val targetTimes = Map(topicPartition -> new ListOffsetRequest.PartitionData(
        timestamp, Optional.empty[Integer]())).asJava

      val request = ListOffsetRequest.Builder
        .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
        .setTargetTimes(targetTimes)
        .build()

      val response = sendRequest(serverId, request)
      val partitionData = response.responseData.get(topicPartition)
      val epochOpt = partitionData.leaderEpoch

      (partitionData.offset, epochOpt)
    }

    assertEquals((0L, Optional.of(0)), fetchOffsetAndEpoch(firstLeaderId, 0L))
    assertEquals((0L, Optional.of(0)), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.EARLIEST_TIMESTAMP))
    assertEquals((10L, Optional.of(0)), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.LATEST_TIMESTAMP))

    assertEquals((0L, Optional.empty()), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.LOCAL_START_OFFSET))
    assertEquals((10L, Optional.empty()), fetchOffsetAndEpoch(firstLeaderId, ListOffsetRequest.LOCAL_END_OFFSET))

    // Kill the first leader so that we can verify the epoch change when fetching the latest offset
    killBroker(firstLeaderId)
    val secondLeaderId = TestUtils.awaitLeaderChange(servers, topicPartition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, servers)

    // No changes to written data
    assertEquals((0L, Optional.of(0)), fetchOffsetAndEpoch(secondLeaderId, 0L))
    assertEquals((0L, Optional.of(0)), fetchOffsetAndEpoch(secondLeaderId, ListOffsetRequest.EARLIEST_TIMESTAMP))

    // The latest offset reflects the updated epoch
    assertEquals((10L, Optional.of(secondLeaderEpoch)), fetchOffsetAndEpoch(secondLeaderId, ListOffsetRequest.LATEST_TIMESTAMP))
  }

  private def assertResponseError(error: Errors, brokerId: Int, request: ListOffsetRequest): Unit = {
    val response = sendRequest(brokerId, request)
    assertEquals(request.partitionTimestamps.size, response.responseData.size)
    response.responseData.asScala.values.foreach { partitionData =>
      assertEquals(error, partitionData.error)
    }
  }

  private def sendRequest(leaderId: Int, request: ListOffsetRequest): ListOffsetResponse = {
    connectAndReceive[ListOffsetResponse](request, destination = brokerSocketServer(leaderId))
  }

}
