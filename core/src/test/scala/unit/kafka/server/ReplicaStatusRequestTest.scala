/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ReplicaStatusRequest, ReplicaStatusResponse}
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.jdk.CollectionConverters._

class ReplicaStatusRequestTest extends BaseRequestTest {

  @Test
  def testReplicaStatusRequestDuringPartitionRecoveryAfterUncleanLeaderElection(): Unit = {
    // ReplicaStatusRequest will be blocked when a partition is marked unclean upon receiving LeaderAndIsr
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
    TestUtils.generateAndProduceMessages(servers, topic, 10, -1)
    // Generates replica status request for all possible versions and validates the response
    def sendReplicaStatusRequestAndValidateResponse(brokerId: Int, error: Errors): Unit = {
      for (ver <- ApiKeys.REPLICA_STATUS.oldestVersion to ApiKeys.REPLICA_STATUS.latestVersion()) {
        val request = new ReplicaStatusRequest.Builder(Set(topicPartition).asJava, false).build(ver.toShort)
        val replicaStatusResponse = connectAndReceive[ReplicaStatusResponse](request, destination = brokerSocketServer(brokerId))
        assertEquals(s"Unexpected number of topics in response", 1, replicaStatusResponse.data().topics().size())
        assertEquals(s"Unexpected topic in response", topic, replicaStatusResponse.data().topics().get(0).name())
        assertEquals(s"Unexpected number of partitions in response", 1, replicaStatusResponse.data().topics().get(0).partitions().size())
        assertEquals(s"Unexpected partition index", 0, replicaStatusResponse.data().topics().get(0).partitions().get(0).partitionIndex())
        assertEquals(s"Unexpected error code for partition", error.code(), replicaStatusResponse.data().topics().get(0).partitions().get(0).errorCode())
        if (error == Errors.NONE) {
          assertEquals(s"Unexpected number of replicas", 2, replicaStatusResponse.data().topics().get(0).partitions().get(0).replicas().size())
          for (replica <- replicaStatusResponse.data().topics().get(0).partitions().get(0).replicas().asScala) {
            if (replica.isLeader) {
              assertEquals(s"Unexpected log start offset at leader", partition.logStartOffset, replica.logStartOffset())
              assertEquals(s"Unexpected log end offset at leader", partition.log.get.logEndOffset, replica.logEndOffset())
            }
          }
        } else {
          assertEquals(s"Unexpected number of replicas", null, replicaStatusResponse.data().topics().get(0).partitions().get(0).replicas())
        }
      }
    }
    // Toggle the isUnclean boolean flag at partition object to mark the partition clean/unclean
    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).setUncleanLeaderFlagTo(false)
    sendReplicaStatusRequestAndValidateResponse(leader, Errors.NONE)
    sendReplicaStatusRequestAndValidateResponse(follower, Errors.NOT_LEADER_FOR_PARTITION)
    sendReplicaStatusRequestAndValidateResponse(nonReplica, Errors.NOT_LEADER_FOR_PARTITION)
    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).setUncleanLeaderFlagTo(true)
    sendReplicaStatusRequestAndValidateResponse(leader, Errors.LEADER_NOT_AVAILABLE)
    sendReplicaStatusRequestAndValidateResponse(follower, Errors.NOT_LEADER_FOR_PARTITION)
    sendReplicaStatusRequestAndValidateResponse(nonReplica, Errors.NOT_LEADER_FOR_PARTITION)
    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(topicPartition, expectLeader = true).setUncleanLeaderFlagTo(false)
    sendReplicaStatusRequestAndValidateResponse(leader, Errors.NONE)
    sendReplicaStatusRequestAndValidateResponse(follower, Errors.NOT_LEADER_FOR_PARTITION)
    sendReplicaStatusRequestAndValidateResponse(nonReplica, Errors.NOT_LEADER_FOR_PARTITION)
  }
}
