/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.Properties

import kafka.cluster.Partition
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ClusterLinkInUseException, ClusterLinkNotFoundException}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.MockTime
import org.easymock.EasyMock.{createNiceMock, expect, mock, replay, reset}
import org.junit.{After, Before, Test}
import org.junit.Assert._

import org.scalatest.Assertions.intercept

class ClusterLinkReplicaManagerTest {

  private val brokerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
  private val metrics = new Metrics
  private val time = new MockTime
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private var clusterLinkReplicaManager: ClusterLinkReplicaManager = _

  @Before
  def setUp(): Unit = {
    clusterLinkReplicaManager = new ClusterLinkReplicaManager(
      brokerConfig,
      replicaManager,
      UnboundedQuota,
      metrics,
      time,
      tierStateFetcher = None)
  }

  @After
  def tearDown(): Unit = {
    metrics.close()
  }

  @Test
  def testClusterLinks(): Unit = {
    val linkName = "testLink"
    val topic = "testTopic"
    val tp0 = new TopicPartition(topic, 0)
    val partition0: Partition = createNiceMock(classOf[Partition])
    assertEquals(None, clusterLinkReplicaManager.fetcherManager(linkName))
    assertEquals(None, clusterLinkReplicaManager.clientManager(linkName))

    setupMock(partition0, tp0, None)
    clusterLinkReplicaManager.addPartitions(Set(partition0))

    setupMock(partition0, tp0, Some(linkName))
    intercept[ClusterLinkNotFoundException] {
      clusterLinkReplicaManager.addPartitions(Set(partition0))
    }

    setupMock(partition0, tp0, Some(linkName))
    clusterLinkReplicaManager.addClusterLink(linkName, clusterLinkConfig(linkName))
    assertNotEquals(None, clusterLinkReplicaManager.fetcherManager(linkName))
    assertNotEquals(None, clusterLinkReplicaManager.clientManager(linkName))
    val fetcherManager = clusterLinkReplicaManager.fetcherManager(linkName).get
    val clientManager = clusterLinkReplicaManager.clientManager(linkName).get

    clusterLinkReplicaManager.addPartitions(Set(partition0))
    assertTrue("Topic not added to metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertTrue("Topic not added to client manager", clientManager.getTopics.contains(topic))

    intercept[ClusterLinkInUseException] {
      clusterLinkReplicaManager.removeClusterLink(linkName)
    }

    val partitionState: LeaderAndIsrPartitionState = mock(classOf[LeaderAndIsrPartitionState])
    expect(partitionState.clusterLink()).andReturn(linkName).anyTimes()
    replay(partitionState)
    clusterLinkReplicaManager.removePartitions(Map(partition0 -> partitionState))
    assertTrue("Topic removed from metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic not removed from client manager", clientManager.getTopics.contains(topic))

    reset(partitionState)
    expect(partitionState.clusterLink()).andReturn(null).anyTimes()
    replay(partitionState)
    clusterLinkReplicaManager.removePartitions(Map(partition0 -> partitionState))
    assertFalse("Topic not removed from metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be in client manager", clientManager.getTopics.contains(topic))

    val tp1 = new TopicPartition(topic, 1)
    val partition1: Partition = createNiceMock(classOf[Partition])
    setupMock(partition1, tp1, Some(linkName))

    clusterLinkReplicaManager.addPartitions(Set(partition1))
    assertTrue("Topic not added to metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be added to client manager", clientManager.getTopics.contains(topic))

    clusterLinkReplicaManager.removePartitionsAndMetadata(Set(tp1))
    assertFalse("Topic not removed from metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be in to client manager", clientManager.getTopics.contains(topic))

    assertTrue("Unexpected fetcher manager", clusterLinkReplicaManager.fetcherManager(linkName).get == fetcherManager)
    assertTrue("Unexpected client manager", clusterLinkReplicaManager.clientManager(linkName).get == clientManager)

    clusterLinkReplicaManager.removeClusterLink(linkName)
    assertEquals(None, clusterLinkReplicaManager.fetcherManager(linkName))
    assertEquals(None, clusterLinkReplicaManager.clientManager(linkName))
  }

  private def clusterLinkConfig(linkName: String): ClusterLinkConfig = {
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    new ClusterLinkConfig(props)
  }

  private def setupMock(partition: Partition, tp: TopicPartition, linkName: Option[String]): Unit = {
    reset(partition)
    expect(partition.topicPartition).andReturn(tp).anyTimes()
    expect(partition.getClusterLink).andReturn(linkName).anyTimes()
    expect(partition.isLinkDestination).andReturn(linkName.nonEmpty).anyTimes()
    replay(partition)
  }
}
