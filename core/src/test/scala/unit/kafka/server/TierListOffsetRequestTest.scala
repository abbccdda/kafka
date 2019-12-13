package unit.kafka.server

import java.util.{Collections, Optional}

import kafka.utils.TestUtils
import kafka.server.BaseRequestTest
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.TierListOffsetRequestData
import org.apache.kafka.common.message.TierListOffsetRequestData.{TierListOffsetPartition, TierListOffsetTopic}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.TierListOffsetRequest.OffsetType
import org.apache.kafka.common.requests.{TierListOffsetRequest, TierListOffsetResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class TierListOffsetRequestTest extends BaseRequestTest {

  @Test
  def testTierListOffsetErrorCodes(): Unit = {
    val topicPartition = new TopicPartition("topic", 0)
    val tierListOffsetTopic = new TierListOffsetTopic()
      .setName(topicPartition.topic)
      .setPartitions(Collections.singletonList(new TierListOffsetPartition()
        .setPartitionIndex(topicPartition.partition)
        .setOffsetType(OffsetType.toId(OffsetType.LOCAL_START_OFFSET))
        .setCurrentLeaderEpoch(0)))
    val randomBrokerId = servers.head.config.brokerId

    // (1) Broker request, unknown topic
    val replicaRequest = new TierListOffsetRequest.Builder(new TierListOffsetRequestData()
        .setReplicaId(randomBrokerId)
        .setTopics(Collections.singletonList(tierListOffsetTopic)))
      .build()
    assertResponseError(Errors.UNKNOWN_TOPIC_OR_PARTITION, randomBrokerId, replicaRequest)

    val partitionToLeader = TestUtils.createTopic(zkClient, topicPartition.topic, numPartitions = 1, replicationFactor = 2, servers)
    val replicas = zkClient.getReplicasForPartition(topicPartition).toSet
    val leader = partitionToLeader(topicPartition.partition)
    val follower = replicas.find(_ != leader).get
    val nonReplica = servers.map(_.config.brokerId).find(!replicas.contains(_)).get

    // (2) Broker request to follower
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, follower, replicaRequest)

    // (3) Broker request to non-replica
    assertResponseError(Errors.NOT_LEADER_FOR_PARTITION, nonReplica, replicaRequest)
  }

  @Test
  def testCurrentEpochValidation(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    def assertResponseErrorForEpoch(error: Errors, brokerId: Int, currentLeaderEpoch: Optional[Integer]): Unit = {
      val tierListOffsetTopic = new TierListOffsetTopic()
        .setName(topicPartition.topic)
        .setPartitions(Collections.singletonList(new TierListOffsetPartition()
          .setPartitionIndex(topicPartition.partition)
          .setOffsetType(OffsetType.toId(OffsetType.LOCAL_START_OFFSET))
          .setCurrentLeaderEpoch(currentLeaderEpoch.orElse(-1))))
      val request = new TierListOffsetRequest.Builder(new TierListOffsetRequestData()
        .setReplicaId(brokerId)
        .setTopics(Collections.singletonList(tierListOffsetTopic)))
        .build()
      assertResponseError(error, brokerId, request)
    }

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and -1 is treated as having no epoch at all
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

  private def assertResponseError(error: Errors, brokerId: Int, request: TierListOffsetRequest): Unit = {
    val response = sendRequest(brokerId, request)
    assertEquals(request.data.topics.size, response.data.topics.size)
    response.data.topics.asScala.foreach { topicResponse =>
      topicResponse.partitions.asScala.foreach { partitionResponse =>
        assertEquals(error.code, partitionResponse.errorCode)
      }
    }
  }

  private def sendRequest(leaderId: Int, request: TierListOffsetRequest): TierListOffsetResponse = {
    val socketServer = brokerSocketServer(leaderId)
    connectAndReceive[TierListOffsetResponse](request, destination = socketServer)
  }
}
