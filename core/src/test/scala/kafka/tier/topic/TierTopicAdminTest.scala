/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic

import java.util
import java.util.Collections

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.DescribeTopicsResult
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.junit.Test
import org.junit.Assert._
import org.mockito.Mockito.{mock, when}
import org.mockito.ArgumentMatchers.any
import org.scalatest.Assertions.assertThrows


class TierTopicAdminTest {
  val tierTopicName = "_confluent-tier-state"
  val clusterId = "mycluster"
  val partitions: Integer = 2
  val replication: Short = 3

  @Test
  def testEnsureTopicCreated(): Unit = {
    val adminClient = mock(classOf[AdminClient])

    // topic does not exist for initial check
    val describeTopicsFut = new KafkaFutureImpl[TopicDescription]()
    describeTopicsFut.completeExceptionally(new UnknownTopicOrPartitionException("Topic does not exist"))
    val describeTopicsResult: DescribeTopicsResult = mock(classOf[DescribeTopicsResult])
    val describeTopicsValues = Collections.singletonMap[String, KafkaFuture[TopicDescription]](tierTopicName, describeTopicsFut)
    when(describeTopicsResult.values()).thenReturn(describeTopicsValues)
    when(adminClient.describeTopics(Collections.singleton(tierTopicName)))
      .thenReturn(describeTopicsResult)

    // allow topic creation to take place
    val topicCreatedFut = new KafkaFutureImpl[Void]()
    topicCreatedFut.complete(null)
    val createTopicsResult: CreateTopicsResult = mock(classOf[CreateTopicsResult])
    val values = Collections.singletonMap[String, KafkaFuture[Void]](tierTopicName, topicCreatedFut)
    when(createTopicsResult.values()).thenReturn(values)
    when(adminClient.createTopics(any()))
      .thenReturn(createTopicsResult)

    assertTrue(TierTopicAdmin.ensureTopicCreated(adminClient, tierTopicName, partitions, replication))
  }

  @Test
  def testTopicMismatchedPartitions(): Unit = {
    val adminClient = mock(classOf[AdminClient])

    val topicPartition1 = new TopicPartitionInfo(0, new Node(0, "", 0), new util.ArrayList[Node](), new util.ArrayList[Node]())
    val topicPartition2 = new TopicPartitionInfo(1, new Node(1, "", 0), new util.ArrayList[Node](), new util.ArrayList[Node]())
    val topicPartition3 = new TopicPartitionInfo(2, new Node(2, "", 0), new util.ArrayList[Node](), new util.ArrayList[Node]())

    // topic does exist, but differs in number of partitions
    val describeTopicsFut = new KafkaFutureImpl[TopicDescription]()
    describeTopicsFut.complete(new TopicDescription(tierTopicName, true, util.Arrays.asList(topicPartition1, topicPartition2, topicPartition3)))
    val describeTopicsResult: DescribeTopicsResult = mock(classOf[DescribeTopicsResult])
    val describeTopicsValues = Collections.singletonMap[String, KafkaFuture[TopicDescription]](tierTopicName, describeTopicsFut)
    when(describeTopicsResult.values()).thenReturn(describeTopicsValues)
    when(adminClient.describeTopics(Collections.singleton(tierTopicName)))
      .thenReturn(describeTopicsResult)

    assertThrows[IllegalArgumentException] {
      TierTopicAdmin.ensureTopicCreated(adminClient, tierTopicName, partitions, replication)
    }
  }

  @Test
  def testTopicAlreadyExists(): Unit = {
    val adminClient = mock(classOf[AdminClient])

    val topicPartition1 = new TopicPartitionInfo(0, new Node(0, "", 0), new util.ArrayList[Node](), new util.ArrayList[Node]())
    val topicPartition2 = new TopicPartitionInfo(1, new Node(1, "", 0), new util.ArrayList[Node](), new util.ArrayList[Node]())

    val describeTopicsFut = new KafkaFutureImpl[TopicDescription]()
    describeTopicsFut.complete(new TopicDescription(tierTopicName, true, util.Arrays.asList(topicPartition1, topicPartition2)))
    val describeTopicsResult: DescribeTopicsResult = mock(classOf[DescribeTopicsResult])
    val describeTopicsValues = Collections.singletonMap[String, KafkaFuture[TopicDescription]](tierTopicName, describeTopicsFut)
    when(describeTopicsResult.values()).thenReturn(describeTopicsValues)
    when(adminClient.describeTopics(Collections.singleton(tierTopicName)))
      .thenReturn(describeTopicsResult)

    assertTrue(TierTopicAdmin.ensureTopicCreated(adminClient, tierTopicName, partitions, replication))
  }
}
