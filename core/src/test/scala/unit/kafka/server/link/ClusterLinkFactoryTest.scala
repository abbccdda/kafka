/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.{Properties, UUID}

import kafka.cluster.Partition
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server.{KafkaConfig, MetadataCache, ReplicaManager}
import kafka.utils.TestUtils
import kafka.zk.{ClusterLinkData, KafkaZkClient}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.AlterMirrorsRequest.StopTopicMirrorOp
import org.apache.kafka.common.requests.{AlterMirrorsRequest, NewClusterLink}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.common.{Endpoint, TopicPartition}
import org.easymock.EasyMock._
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.junit.function.ThrowingRunnable

import scala.collection.Map
import scala.jdk.CollectionConverters._


class ClusterLinkFactoryTest {

  private val metrics = new Metrics
  private val time = new MockTime
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val zkClient: KafkaZkClient = createNiceMock(classOf[KafkaZkClient])
  private val metadataCache: MetadataCache = new MetadataCache(0)
  private var clusterLinkManager: ClusterLinkFactory.LinkManager = _

  @Before
  def setUp(): Unit = {
    expect(replicaManager.metadataCache).andReturn(metadataCache).anyTimes()
    expect(replicaManager.zkClient).andReturn(zkClient).anyTimes()
    replay(replicaManager)
  }

  @After
  def tearDown(): Unit = {
    clusterLinkManager.shutdown()
    metrics.close()
  }

  @Test
  def testLinkManagerWithClusterLinkDisabled(): Unit = {
    val linkName = "testLink"
    val linkId = UUID.randomUUID()
    val brokerConfig = createBrokerConfig(enableClusterLink = false)
    clusterLinkManager = createClusterLinkManager(brokerConfig)
    assertSame(ClusterLinkDisabled.LinkManager, clusterLinkManager)
    assertSame(ClusterLinkDisabled.AdminManager, clusterLinkManager.admin)

    verifyClusterLinkDisabled(() => clusterLinkManager.configEncoder)
    verifyClusterLinkDisabled(() => clusterLinkManager.createClusterLink(
      ClusterLinkData(linkName, linkId, None),
      ClusterLinkProps(new ClusterLinkConfig(Map.empty.asJava), None),
      new Properties()))
    verifyClusterLinkDisabled(() => clusterLinkManager.listClusterLinks())
    verifyClusterLinkDisabled(() => clusterLinkManager.updateClusterLinkConfig(linkName, (props: Properties) => false))
    verifyClusterLinkDisabled(() => clusterLinkManager.deleteClusterLink(linkName, linkId))
    verifyClusterLinkDisabled(() => clusterLinkManager.fetcherManager(linkId))
    verifyClusterLinkDisabled(() => clusterLinkManager.clientManager(linkId))
    verifyClusterLinkDisabled(() => clusterLinkManager.resolveLinkId(linkName))
    verifyClusterLinkDisabled(() => clusterLinkManager.resolveLinkIdOrThrow(linkName))
    verifyClusterLinkDisabled(() => clusterLinkManager.ensureLinkNameDoesntExist(linkName))

    // Verify that cluster links in ZK created when cluster links were enabled don't
    // throw exceptions when cluster linking is disabled.
    clusterLinkManager.processClusterLinkChanges(linkId, new Properties)
    verifyClusterLinkDisabled(() => clusterLinkManager.fetcherManager(linkId))

    // Verify that partitions with cluster links don't throw exceptions when cluster links are disabled.
    val tp0 = new TopicPartition("topic", 0)
    val partition0: Partition = createNiceMock(classOf[Partition])
    setupMock(partition0, tp0, Some(linkId))
    clusterLinkManager.addPartitions(Set(partition0))
    verifyClusterLinkDisabled(() => clusterLinkManager.fetcherManager(linkId))

    val partitionState: LeaderAndIsrPartitionState = mock(classOf[LeaderAndIsrPartitionState])
    clusterLinkManager.removePartitions(Map(partition0 -> partitionState))
    clusterLinkManager.removePartitionsAndMetadata(Set(tp0))

    // Verify start/stop don't throw exceptions
    clusterLinkManager.startup(null, null, null, null, null)
    clusterLinkManager.shutdownIdleFetcherThreads()
    clusterLinkManager.shutdown()
  }

  @Test
  def testAdminManagerWithClusterLinkDisabled(): Unit = {
    val linkName = "testLink"
    val brokerConfig = createBrokerConfig(enableClusterLink = false)
    clusterLinkManager = createClusterLinkManager(brokerConfig)

    val admin = clusterLinkManager.admin
    assertSame(ClusterLinkDisabled.AdminManager, clusterLinkManager.admin)
    verifyClusterLinkDisabled(() => admin.purgatory)

    val newClusterLink = new NewClusterLink(linkName, "cluster1",
      Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> "localhost:1234").asJava)
    verifyClusterLinkDisabled(() => admin.createClusterLink(newClusterLink, tenantPrefix = None,
      validateOnly = true, validateLink = false, timeoutMs = 10000))
    verifyClusterLinkDisabled(() => admin.deleteClusterLink(linkName, validateOnly = true, force = true))
    verifyClusterLinkDisabled(() => admin.listClusterLinks())

    val stopMirror: AlterMirrorsRequest.Op = new StopTopicMirrorOp("topic")
    verifyClusterLinkDisabled(() => admin.alterMirror(stopMirror, validateOnly = true))
  }

  private def createBrokerConfig(enableClusterLink: Boolean): KafkaConfig = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.put(KafkaConfig.ClusterLinkEnableProp, enableClusterLink.toString)
    KafkaConfig.fromProps(props)
  }

  private def setupMock(partition: Partition, tp: TopicPartition, linkId: Option[UUID]): Unit = {
    reset(partition)
    expect(partition.topicPartition).andReturn(tp).anyTimes()
    expect(partition.getClusterLinkId).andReturn(linkId).anyTimes()
    expect(partition.isActiveLinkDestinationLeader).andReturn(linkId.nonEmpty).anyTimes()
    expect(partition.getLinkedLeaderEpoch).andReturn(Some(1)).anyTimes()
    replay(partition)
  }

  private def createClusterLinkManager(brokerConfig: KafkaConfig): ClusterLinkFactory.LinkManager = {
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
    manager
  }

  private def verifyClusterLinkDisabled(runnable: ThrowingRunnable): Unit = {
    assertThrows(classOf[ClusterAuthorizationException], runnable)
  }
}
