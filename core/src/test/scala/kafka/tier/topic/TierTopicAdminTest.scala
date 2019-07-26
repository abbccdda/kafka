/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic

import kafka.admin.AdminOperationException
import kafka.zk.AdminZkClient
import org.apache.kafka.common.errors.TopicExistsException
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.Assertions.assertThrows

class TierTopicAdminTest {
  private val adminZkClient = mock(classOf[AdminZkClient])
  private val topicName = "testTopic"
  private val configuredReplicationFactor = 3.toShort

  @Test
  def testTopicCreate(): Unit = {
    val configuredNumPartitions = 5
    val createdPartitions = TierTopicAdmin.ensureTopic(adminZkClient, topicName, configuredNumPartitions, configuredReplicationFactor)
    assertEquals(configuredNumPartitions, createdPartitions)
    verify(adminZkClient, times(1)).createTopic(topicName, configuredNumPartitions,
      configuredReplicationFactor, TierTopicAdmin.topicConfig)
  }

  @Test
  def testTopicExists(): Unit = {
    val numPartitionsConfigured = 10
    val numPartitionsExist = 5

    when(adminZkClient.createTopic(topicName, numPartitionsConfigured, configuredReplicationFactor, TierTopicAdmin.topicConfig))
        .thenThrow(new TopicExistsException("topic exists"))
    when(adminZkClient.numPartitions(Set(topicName))).thenReturn(Map(topicName -> numPartitionsExist))

    val numPartitions = TierTopicAdmin.ensureTopic(adminZkClient, topicName, numPartitionsConfigured, configuredReplicationFactor)
    assertEquals(numPartitionsExist, numPartitions)
  }

  @Test
  def testUnknownException(): Unit = {
    val numPartitionsConfigured = 10

    when(adminZkClient.createTopic(topicName, numPartitionsConfigured, configuredReplicationFactor, TierTopicAdmin.topicConfig))
      .thenThrow(new AdminOperationException("admin exception"))

    assertThrows[AdminOperationException] {
      TierTopicAdmin.ensureTopic(adminZkClient, topicName, numPartitionsConfigured, configuredReplicationFactor)
    }
  }
}
