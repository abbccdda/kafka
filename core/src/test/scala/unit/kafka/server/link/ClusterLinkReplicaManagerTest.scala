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
    val tp = new TopicPartition(topic, 0)
    val partition: Partition = createNiceMock(classOf[Partition])
    assertEquals(None, clusterLinkReplicaManager.fetcherManager(linkName))

    setupMock(partition, tp, None)
    clusterLinkReplicaManager.addLinkedFetcherForPartitions(Set(partition))

    setupMock(partition, tp, Some(linkName))
    intercept[InvalidClusterLinkException] {
      clusterLinkReplicaManager.addLinkedFetcherForPartitions(Set(partition))
    }

    setupMock(partition, tp, Some(linkName))
    clusterLinkReplicaManager.addClusterLink(linkName, clusterLinkProps(linkName))
    assertNotEquals(None, clusterLinkReplicaManager.fetcherManager(linkName))
    val fetcherManager = clusterLinkReplicaManager.fetcherManager(linkName).get

    clusterLinkReplicaManager.addLinkedFetcherForPartitions(Set(partition))
    assertTrue("Topic not added to metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))

    intercept[IllegalStateException] {
      clusterLinkReplicaManager.removeClusterLink(linkName)
    }

    val partitionState: LeaderAndIsrPartitionState = mock(classOf[LeaderAndIsrPartitionState])
    expect(partitionState.clusterLink()).andReturn(linkName).anyTimes()
    replay(partitionState)
    clusterLinkReplicaManager.removeLinkedFetcherForPartitions(Map(partition -> partitionState))
    assertTrue("Topic removed from metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))

    reset(partitionState)
    expect(partitionState.clusterLink()).andReturn(null).anyTimes()
    replay(partitionState)
    clusterLinkReplicaManager.removeLinkedFetcherForPartitions(Map(partition -> partitionState))
    assertFalse("Topic not removed from metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))

    clusterLinkReplicaManager.addLinkedFetcherForPartitions(Set(partition))
    assertTrue("Topic not added to metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))

    clusterLinkReplicaManager.removeLinkedFetcherAndMetadataForPartitions(Set(tp))
    assertFalse("Topic not removed from metadata",
      fetcherManager.metadata.retainTopic(topic, isInternal = false, time.milliseconds))

    assertTrue("Unexpected Fetcher manager", clusterLinkReplicaManager.fetcherManager(linkName).get == fetcherManager)

    clusterLinkReplicaManager.removeClusterLink(linkName)
    assertEquals(None, clusterLinkReplicaManager.fetcherManager(linkName))
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
    replay(partition)
  }
}
