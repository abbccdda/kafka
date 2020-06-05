/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.{Collections, Properties, UUID}

import kafka.cluster.Partition
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.{ConfigType, KafkaConfig, MetadataCache, ReplicaManager}
import kafka.utils.TestUtils
import kafka.zk.{ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.{Endpoint, TopicPartition}
import org.apache.kafka.common.errors.{ClusterLinkExistsException, ClusterLinkNotFoundException}
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
    val linkId = UUID.randomUUID()
    val clusterId = "testClusterId"
    val clusterLinkData = ClusterLinkData(linkName, linkId, Some(clusterId), None, false)
    val topic = "testTopic"
    val tp0 = new TopicPartition(topic, 0)
    val partition0: Partition = createNiceMock(classOf[Partition])
    assertEquals(None, clusterLinkManager.fetcherManager(linkId))
    assertEquals(None, clusterLinkManager.clientManager(linkId))
    assertEquals(None, clusterLinkManager.resolveLinkId(linkName))
    intercept[ClusterLinkNotFoundException] {
      clusterLinkManager.resolveLinkIdOrThrow(linkName)
    }
    clusterLinkManager.ensureLinkNameDoesntExist(linkName)
    assertEquals(Seq.empty, clusterLinkManager.listClusterLinks())

    setupMock(partition0, tp0, None)
    clusterLinkManager.addPartitions(Set(partition0))

    setupMock(partition0, tp0, Some(linkId))
    intercept[ClusterLinkNotFoundException] {
      clusterLinkManager.addPartitions(Set(partition0))
    }

    setupMock(partition0, tp0, Some(linkId))
    expect(zkClient.clusterLinkExists(linkId)).andReturn(false).times(1)
    replay(zkClient)
    clusterLinkManager.createClusterLink(clusterLinkData, clusterLinkConfig, clusterLinkPersistentProps)
    assertNotEquals(None, clusterLinkManager.fetcherManager(linkId))
    assertNotEquals(None, clusterLinkManager.clientManager(linkId))
    assertEquals(Some(linkId), clusterLinkManager.resolveLinkId(linkName))
    assertEquals(Seq(clusterLinkData), clusterLinkManager.listClusterLinks())
    val fetcherManager = clusterLinkManager.fetcherManager(linkId).get
    val clientManager = clusterLinkManager.clientManager(linkId).get

    intercept[ClusterLinkExistsException] {
      clusterLinkManager.createClusterLink(ClusterLinkData(linkName, UUID.randomUUID(), Some(clusterId), None, false),
        clusterLinkConfig, clusterLinkPersistentProps)
    }

    clusterLinkManager.addPartitions(Set(partition0))
    assertTrue("Topic not added to metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertTrue("Topic not added to client manager", clientManager.getTopics.contains(topic))
    assertFalse("Fetcher not recording active topic", fetcherManager.isEmpty)

    val partitionState: LeaderAndIsrPartitionState = mock(classOf[LeaderAndIsrPartitionState])
    expect(partitionState.clusterLinkId()).andReturn(linkId.toString).anyTimes()
    expect(partitionState.clusterLinkTopicState()).andReturn("Mirror").anyTimes()
    expect(partitionState.linkedLeaderEpoch()).andReturn(1).anyTimes()
    replay(partitionState)
    clusterLinkManager.removePartitions(Map(partition0 -> partitionState))
    assertTrue("Topic removed from metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic not removed from client manager", clientManager.getTopics.contains(topic))

    reset(partitionState)
    expect(partitionState.clusterLinkId()).andReturn(null).anyTimes()
    replay(partitionState)
    clusterLinkManager.removePartitions(Map(partition0 -> partitionState))
    assertFalse("Topic not removed from metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be in client manager", clientManager.getTopics.contains(topic))

    reset(partitionState)
    expect(partitionState.clusterLinkId()).andReturn(linkId.toString).anyTimes()
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
    setupMock(partition1, tp1, Some(linkId))

    clusterLinkManager.addPartitions(Set(partition1))
    assertTrue("Topic not added to metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be added to client manager", clientManager.getTopics.contains(topic))

    clusterLinkManager.removePartitionsAndMetadata(Set(tp1))
    assertFalse("Topic not removed from metadata",
      fetcherManager.currentMetadata.retainTopic(topic, isInternal = false, time.milliseconds))
    assertFalse("Topic should not be in to client manager", clientManager.getTopics.contains(topic))

    assertTrue("Unexpected fetcher manager", clusterLinkManager.fetcherManager(linkId).get == fetcherManager)
    assertTrue("Unexpected client manager", clusterLinkManager.clientManager(linkId).get == clientManager)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true).times(1)
    expect(zkClient.setClusterLink(ClusterLinkData(linkName, linkId, Some(clusterId), None, true)))
    replay(zkClient)
    clusterLinkManager.deleteClusterLink(linkName, linkId)
    assertEquals(None, clusterLinkManager.fetcherManager(linkId))
    assertEquals(None, clusterLinkManager.clientManager(linkId))
    assertEquals(None, clusterLinkManager.resolveLinkId(linkName))

    intercept[ClusterLinkNotFoundException] {
      clusterLinkManager.deleteClusterLink(linkName, linkId)
    }
  }

  @Test
  def testReconfigure(): Unit = {
    val linkName = "testLink"
    val linkId = UUID.randomUUID()

    intercept[ClusterLinkNotFoundException] {
      clusterLinkManager.updateClusterLinkConfig(linkName, (props: Properties) => false)
    }

    expect(zkClient.clusterLinkExists(linkId)).andReturn(false).times(1)
    expect(zkClient.getClusterLinks(Set(linkId)))
      .andReturn(Map(linkId -> ClusterLinkData(linkName, linkId, None, None, false)))
      .anyTimes()
    replay(zkClient)

    assertEquals(None, clusterLinkManager.fetcherManager(linkId))
    clusterLinkManager.createClusterLink(ClusterLinkData(linkName, linkId, None, None, false),
      clusterLinkConfig, clusterLinkPersistentProps)
    val fetcherManager = clusterLinkManager.fetcherManager(linkId).get
    assertEquals(Collections.singletonList("localhost:1234"), fetcherManager.currentConfig.bootstrapServers)

    val newProps = new Properties
    newProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:5678")
    clusterLinkManager.processClusterLinkChanges(linkId, newProps)
    assertEquals(Collections.singletonList("localhost:5678"), fetcherManager.currentConfig.bootstrapServers)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true).times(1)
    expect(zkClient.getEntityConfigs(ConfigType.ClusterLink, linkId.toString)).andReturn(newProps).times(1)
    replay(zkClient)
    clusterLinkManager.updateClusterLinkConfig(linkName, (props: Properties) => {
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
      false
    })
    assertEquals(Collections.singletonList("localhost:5678"), fetcherManager.currentConfig.bootstrapServers)

    reset(zkClient)
    expect(zkClient.clusterLinkExists(linkId)).andReturn(true).times(1)
    expect(zkClient.getEntityConfigs(ConfigType.ClusterLink, linkId.toString)).andReturn(newProps).times(1)
    replay(zkClient)
    clusterLinkManager.updateClusterLinkConfig(linkName, (props: Properties) => {
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
      true
    })
    assertEquals(Collections.singletonList("localhost:1234"), fetcherManager.currentConfig.bootstrapServers)
  }

  private def createBrokerConfig(): KafkaConfig = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.put(KafkaConfig.ClusterLinkEnableProp, "true")
    KafkaConfig.fromProps(props)
  }

  private def clusterLinkPersistentProps: Properties = {
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    props
  }

  private def clusterLinkConfig: ClusterLinkConfig =
    new ClusterLinkConfig(clusterLinkPersistentProps)

  private def setupMock(partition: Partition, tp: TopicPartition, linkId: Option[UUID]): Unit = {
    reset(partition)
    expect(partition.topicPartition).andReturn(tp).anyTimes()
    expect(partition.getClusterLinkId).andReturn(linkId).anyTimes()
    expect(partition.isActiveLinkDestinationLeader).andReturn(linkId.nonEmpty).anyTimes()
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
