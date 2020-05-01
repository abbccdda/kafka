/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util
import java.util.{Collections, Properties}

import kafka.cluster.Partition
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.{KafkaConfig, MetadataCache, ReplicaManager}
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

class ClusterLinkManagerTest {

  private val brokerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
  private val metrics = new Metrics
  private val time = new MockTime
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val metadataCache: MetadataCache = new MetadataCache(0)
  private var clusterLinkManager: ClusterLinkManager = _

  @Before
  def setUp(): Unit = {
    expect(replicaManager.metadataCache).andReturn(metadataCache).anyTimes()
    replay(replicaManager)
    clusterLinkManager = new ClusterLinkManager(
      brokerConfig,
      "clusterId",
      UnboundedQuota,
      zkClient = null,
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
    assertEquals(None, clusterLinkManager.fetcherManager(linkName))
    assertEquals(None, clusterLinkManager.clientManager(linkName))

    setupMock(partition0, tp0, None)
    clusterLinkManager.addPartitions(Set(partition0))

    setupMock(partition0, tp0, Some(linkName))
    intercept[ClusterLinkNotFoundException] {
      clusterLinkManager.addPartitions(Set(partition0))
    }

    setupMock(partition0, tp0, Some(linkName))
    clusterLinkManager.addClusterLink(linkName, clusterLinkProps(linkName))
    assertNotEquals(None, clusterLinkManager.fetcherManager(linkName))
    assertNotEquals(None, clusterLinkManager.clientManager(linkName))
    val fetcherManager = clusterLinkManager.fetcherManager(linkName).get
    val clientManager = clusterLinkManager.clientManager(linkName).get

    clusterLinkManager.addPartitions(Set(partition0))
    assertTrue("Topic not added to metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertTrue("Topic not added to client manager", clientManager.getTopics.contains(topic))

    intercept[ClusterLinkInUseException] {
      clusterLinkManager.removeClusterLink(linkName)
    }

    val partitionState: LeaderAndIsrPartitionState = mock(classOf[LeaderAndIsrPartitionState])
    expect(partitionState.clusterLink()).andReturn(linkName).anyTimes()
    replay(partitionState)
    clusterLinkManager.removePartitions(Map(partition0 -> partitionState))
    assertTrue("Topic removed from metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic not removed from client manager", clientManager.getTopics.contains(topic))

    reset(partitionState)
    expect(partitionState.clusterLink()).andReturn(null).anyTimes()
    replay(partitionState)
    clusterLinkManager.removePartitions(Map(partition0 -> partitionState))
    assertFalse("Topic not removed from metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be in client manager", clientManager.getTopics.contains(topic))

    val tp1 = new TopicPartition(topic, 1)
    val partition1: Partition = createNiceMock(classOf[Partition])
    setupMock(partition1, tp1, Some(linkName))

    clusterLinkManager.addPartitions(Set(partition1))
    assertTrue("Topic not added to metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be added to client manager", clientManager.getTopics.contains(topic))

    clusterLinkManager.removePartitionsAndMetadata(Set(tp1))
    assertFalse("Topic not removed from metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be in to client manager", clientManager.getTopics.contains(topic))

    assertTrue("Unexpected fetcher manager", clusterLinkManager.fetcherManager(linkName).get == fetcherManager)
    assertTrue("Unexpected client manager", clusterLinkManager.clientManager(linkName).get == clientManager)

    clusterLinkManager.removeClusterLink(linkName)
    assertEquals(None, clusterLinkManager.fetcherManager(linkName))
    assertEquals(None, clusterLinkManager.clientManager(linkName))
  }

  @Test
  def testReconfigure(): Unit = {
    val linkName = "testLink"

    assertEquals(None, clusterLinkManager.fetcherManager(linkName))
    val linkProps = clusterLinkProps(linkName)
    clusterLinkManager.addClusterLink(linkName, linkProps)
    val fetcherManager = clusterLinkManager.fetcherManager(linkName).get
    assertEquals(Collections.singletonList("localhost:1234"), fetcherManager.currentConfig.bootstrapServers)

    val newProps = new util.HashMap[String, String]()
    newProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:5678")
    clusterLinkManager.reconfigureClusterLink(linkName, newProps)
    assertEquals(Collections.singletonList("localhost:5678"), fetcherManager.currentConfig.bootstrapServers)
  }

  private def clusterLinkProps(linkName: String): Properties = {
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    props
  }

  private def setupMock(partition: Partition, tp: TopicPartition, linkName: Option[String]): Unit = {
    reset(partition)
    expect(partition.topicPartition).andReturn(tp).anyTimes()
    expect(partition.getClusterLink).andReturn(linkName).anyTimes()
    expect(partition.isLinkDestination).andReturn(linkName.nonEmpty).anyTimes()
    expect(partition.getLinkedLeaderEpoch).andReturn(Some(1)).anyTimes()
    replay(partition)
  }
}
