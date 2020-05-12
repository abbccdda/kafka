/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsRequestData.{DeleteRecordsPartition, DeleteRecordsTopic}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{DeleteRecordsRequest, DeleteRecordsResponse}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import scala.jdk.CollectionConverters._

class DeleteRecordsRequestTest extends BaseRequestTest {

  @Test
  def testDeleteRecordsRequestDuringPartitionRecoveryAfterUncleanLeaderElection(): Unit = {
    // DeleteRecordsRequest will be blocked when a partition is marked unclean upon receiving LeaderAndIsr
    // request that indicates election of an unclean leader. The request must be unblocked when partition
    // has undergone recovery.
    val topic = "test-topic"
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 2, servers)
    val topicPartition = new TopicPartition(topic, 0)
    val leader = partitionToLeader(topicPartition.partition())
    val replicas = zkClient.getReplicasForPartition(topicPartition).toSet
    val follower = replicas.find(_ != leader).get
    val nonReplica = servers.map(_.config.brokerId).find(!replicas.contains(_)).get
    val partition = servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true)
    // produce some records
    TestUtils.generateAndProduceMessages(servers, topic, 500, -1)
    // Generates delete records request for all possible versions and validates the response
    def deleteRecordsAndValidateResponse(brokerId: Int, offset: java.lang.Long, errorCode: Int): Unit = {
      for (ver <- ApiKeys.DELETE_RECORDS.oldestVersion to ApiKeys.DELETE_RECORDS.latestVersion()) {
        val deleteRecordsPartition = new DeleteRecordsPartition()
          .setPartitionIndex(0)
          .setOffset(offset)
        val deleteRecordsTopic = new DeleteRecordsTopic()
          .setName(topic)
          .setPartitions(List(deleteRecordsPartition).asJava)
        val deleteRecordsRequestData = new DeleteRecordsRequestData()
          .setTopics(List(deleteRecordsTopic).asJava)
          .setTimeoutMs(15000)
        val request = new DeleteRecordsRequest.Builder(deleteRecordsRequestData).build(ver.toShort)
        val deleteRecordsResponse = sendDeleteRecordsRequest(brokerId, request)
        assertEquals( s"Response for unexpected number of topics",
          1, deleteRecordsResponse.data().topics().size())
        assertEquals(s"Response for expected topic not found",
          topic, deleteRecordsResponse.data().topics().find(topic).name())
        assertEquals(s"Response for unexpected number of partitions",
          1, deleteRecordsResponse.data().topics().find(topic).partitions().size())
        assertTrue(s"Response for expected partition not found",
          deleteRecordsResponse.data().topics().find(topic).partitions().find(0) != null)
        assertEquals(s"Unexpected error for tested partition",
          errorCode, deleteRecordsResponse.data().topics().find(topic).partitions().find(0).errorCode())
      }
    }
    // Toggle the isUnclean boolean flag at partition object to mark the partition clean/unclean
    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).setUncleanLeaderFlagTo(false)
    deleteRecordsAndValidateResponse(leader, partition.logStartOffset + 10, Errors.NONE.code())
    deleteRecordsAndValidateResponse(follower, partition.logStartOffset + 10, Errors.NOT_LEADER_FOR_PARTITION.code())
    deleteRecordsAndValidateResponse(nonReplica, partition.logStartOffset + 10, Errors.NOT_LEADER_FOR_PARTITION.code())
    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).setUncleanLeaderFlagTo(true)
    deleteRecordsAndValidateResponse(leader, partition.logStartOffset + 10, Errors.LEADER_NOT_AVAILABLE.code())
    deleteRecordsAndValidateResponse(follower, partition.logStartOffset + 10, Errors.NOT_LEADER_FOR_PARTITION.code())
    deleteRecordsAndValidateResponse(nonReplica, partition.logStartOffset + 10, Errors.NOT_LEADER_FOR_PARTITION.code())
    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).setUncleanLeaderFlagTo(false)
    deleteRecordsAndValidateResponse(leader, partition.logStartOffset + 10, Errors.NONE.code())
    deleteRecordsAndValidateResponse(follower, partition.logStartOffset + 10, Errors.NOT_LEADER_FOR_PARTITION.code())
    deleteRecordsAndValidateResponse(nonReplica, partition.logStartOffset + 10, Errors.NOT_LEADER_FOR_PARTITION.code())
  }

  private def sendDeleteRecordsRequest(brokerId: Int, request: DeleteRecordsRequest): DeleteRecordsResponse = {
    connectAndReceive[DeleteRecordsResponse](request, destination = brokerSocketServer(brokerId))
  }
}