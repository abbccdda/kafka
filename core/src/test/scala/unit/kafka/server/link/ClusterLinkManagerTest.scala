/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.{Collections, Properties}

import kafka.cluster.Partition
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.{KafkaConfig, MetadataCache, ReplicaManager}
import kafka.utils.TestUtils
import kafka.zk.{ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.{Endpoint, TopicPartition}
import org.apache.kafka.common.errors.{ClusterLinkInUseException, ClusterLinkNotFoundException}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.easymock.EasyMock.{createNiceMock, expect, mock, replay, reset}
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.scalatest.Assertions.intercept

class ClusterLinkManagerTest {

  private val brokerConfig = createBrokerConfig()
  private val metrics = new Metrics
  private val time = new MockTime
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val zkClient: KafkaZkClient = createNiceMock(classOf[KafkaZkClient])
  private val metadataCache: MetadataCache = new MetadataCache(0)
  private var clusterLinkManager: ClusterLinkManager = _

  @Before
  def setUp(): Unit = {
    expect(replicaManager.metadataCache).andReturn(metadataCache).anyTimes()
    expect(replicaManager.zkClient).andReturn(zkClient).anyTimes()
    replay(replicaManager)
    clusterLinkManager = createClusterLinkManager(brokerConfig)
  }

  @After
  def tearDown(): Unit = {
    clusterLinkManager.shutdown()
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
    clusterLinkManager.addClusterLink(linkName, clusterLinkProps)
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
    expect(partitionState.clusterLinkTopicState()).andReturn("Mirror").anyTimes()
    expect(partitionState.linkedLeaderEpoch()).andReturn(1).anyTimes()
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

    reset(partitionState)
    expect(partitionState.clusterLink()).andReturn(linkName).anyTimes()
    expect(partitionState.clusterLinkTopicState()).andReturn("FailedMirror").anyTimes()
    expect(partitionState.linkedLeaderEpoch()).andReturn(-1).anyTimes()
    replay(partitionState)
    clusterLinkManager.addPartitions(Set(partition0))
    clusterLinkManager.removePartitions(Map(partition0 -> partitionState))
    assertFalse("Topic not removed from metadata for failed mirror",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be in client manager for failed mirror", clientManager.getTopics.contains(topic))

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
    clusterLinkManager.addClusterLink(linkName, clusterLinkProps)
    val fetcherManager = clusterLinkManager.fetcherManager(linkName).get
    assertEquals(Collections.singletonList("localhost:1234"), fetcherManager.currentConfig.bootstrapServers)

    val newProps = new Properties
    newProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:5678")
    expect(zkClient.getClusterLinks(Set(linkName)))
      .andReturn(Map(linkName -> ClusterLinkData(linkName, null, None)))
      .anyTimes()
    replay(zkClient)
    clusterLinkManager.processClusterLinkChanges(linkName, newProps)
    assertEquals(Collections.singletonList("localhost:5678"), fetcherManager.currentConfig.bootstrapServers)
  }

  private def createBrokerConfig(): KafkaConfig = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.put(KafkaConfig.ClusterLinkEnableProp, "true")
    KafkaConfig.fromProps(props)
  }

  private def clusterLinkProps: ClusterLinkProps = {
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    ClusterLinkProps(new ClusterLinkConfig(props), None)
  }

  private def setupMock(partition: Partition, tp: TopicPartition, linkName: Option[String]): Unit = {
    reset(partition)
    expect(partition.topicPartition).andReturn(tp).anyTimes()
    expect(partition.getClusterLink).andReturn(linkName).anyTimes()
    expect(partition.isActiveLinkDestinationLeader).andReturn(linkName.nonEmpty).anyTimes()
    expect(partition.getLinkedLeaderEpoch).andReturn(Some(1)).anyTimes()
    replay(partition)
  }

  private def createClusterLinkManager(brokerConfig: KafkaConfig): ClusterLinkManager = {
    val manager = ClusterLinkFactory.createLinkManager(
      brokerConfig,
      "clusterId",
      UnboundedQuota,
      zkClient,
      metrics,
      time,
      tierStateFetcher = None)
    val brokerEndpoint = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 1234)
    manager.startup(brokerEndpoint, null, null, null, None)
    manager.asInstanceOf[ClusterLinkManager]
  }
}
