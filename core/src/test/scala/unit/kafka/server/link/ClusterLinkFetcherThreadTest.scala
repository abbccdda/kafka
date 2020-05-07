/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.{Collections, Properties}

import kafka.api.{ApiVersion, LeaderAndIsr}
import kafka.cluster.{BrokerEndPoint, DelayedOperations, Partition, PartitionStateStore}
import kafka.log.{AbstractLog, LogManager}
import kafka.controller.KafkaController
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server._
import kafka.tier.fetcher.TierStateFetcher
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidClusterLinkException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.{LogContext, SystemTime, Time}
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.easymock.EasyMock._
import org.junit.Assert._
import org.easymock.EasyMock.{anyObject, expect, mock, replay}
import org.junit.Test

class ClusterLinkFetcherThreadTest extends ReplicaFetcherThreadTest {

  private val clusterLinkName = "testCluster"
  private var fetcherThread: ClusterLinkFetcherThread = _

  override protected def createReplicaFetcherThread(name: String,
                                                    fetcherId: Int,
                                                    sourceBroker: BrokerEndPoint,
                                                    brokerConfig: KafkaConfig,
                                                    failedPartitions: FailedPartitions,
                                                    replicaMgr: ReplicaManager,
                                                    metrics: Metrics,
                                                    time: Time,
                                                    quota: ReplicaQuota,
                                                    tierStateFetcher: Option[TierStateFetcher],
                                                    leaderEndpointBlockingSend: Option[BlockingSend],
                                                    logContextOpt: Option[LogContext]): ReplicaFetcherThread = {
    val fetcherManager: ClusterLinkFetcherManager = mock(classOf[ClusterLinkFetcherManager])
    expect(fetcherManager.partition(anyObject(classOf[TopicPartition]))).andReturn(None).anyTimes()
    expect(fetcherManager.clearPartitionLinkFailure(anyObject(classOf[TopicPartition]), anyString())).anyTimes()
    replay(fetcherManager)
    new ClusterLinkFetcherThread(
      name,
      fetcherId = 0,
      brokerConfig,
      new ClusterLinkConfig(clusterLinkProps),
      new ClusterLinkMetadata(brokerConfig, clusterLinkName, 100, 60000),
      fetcherManager,
      brokerEndPoint,
      failedPartitions,
      replicaMgr,
      UnboundedQuota,
      new Metrics,
      new SystemTime,
      tierStateFetcher = None,
      mock(classOf[ClusterLinkNetworkClient]),
      if (leaderEndpointBlockingSend.isDefined) leaderEndpointBlockingSend.get else mock(classOf[BlockingSend]))
  }

  override def cleanup(): Unit = {
    if (fetcherThread != null)
      fetcherThread.shutdown()
    super.cleanup()
  }

  private def clusterLinkProps : Properties = {
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, s"${brokerEndPoint.host}:${brokerEndPoint.port}")
    props
  }

  /*
   * We don't support cluster linking with versions less than 2.3, so using this test to ensure
   * we dont't create linked fetchers with older versions
   */
  @Test(expected = classOf[InvalidClusterLinkException])
  override def shouldUseLeaderEndOffsetIfInterBrokerVersionBelow20(): Unit = {
    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    props.put(KafkaConfig.InterBrokerProtocolVersionProp, "0.11.0")
    val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
    val authorizer: Option[Authorizer] = Some(createNiceMock(classOf[Authorizer]))
    val controller: KafkaController = createNiceMock(classOf[KafkaController])
    val clusterLinkManager = new ClusterLinkManager(KafkaConfig.fromProps(props), "clusterId", quota = UnboundedQuota, zkClient = null, new Metrics, new SystemTime, tierStateFetcher = None)
    clusterLinkManager.startup(replicaManager, adminManager = null, controller, authorizer)
    clusterLinkManager.addClusterLink(clusterLinkName, clusterLinkProps)
  }

  @Test
  def testSourceOffsetsPendingState(): Unit = {
    val time = new MockTime
    val tp = new TopicPartition("topic", 0)
    val stateStore: PartitionStateStore = createNiceMock(classOf[PartitionStateStore])
    val logManager: LogManager = createNiceMock(classOf[LogManager])
    val partition = new Partition(tp,
                                  replicaLagTimeMaxMs = 10000,
                                  ApiVersion.latestVersion,
                                  localBrokerId = 0,
                                  time,
                                  stateStore,
                                  createNiceMock(classOf[DelayedOperations]),
                                  new MetadataCache(0),
                                  logManager,
                                  tierReplicaManagerOpt = None)
    expect(stateStore.updateClusterLinkState(anyInt(), anyObject(classOf[LeaderAndIsr]))).andReturn(Some(1)).anyTimes()
    val log: AbstractLog = createNiceMock(classOf[AbstractLog])
    partition.log = Some(log)

    val props = TestUtils.createBrokerConfig(1, "localhost:1234")
    val brokerConfig = KafkaConfig.fromProps(props)
    val replicaManager: ReplicaManager = createNiceMock(classOf[ReplicaManager])
    expect(replicaManager.brokerTopicStats).andReturn(mock(classOf[BrokerTopicStats])).anyTimes()
    expect(replicaManager.localLogOrException(tp)).andReturn(log).anyTimes()
    val blockingSend: BlockingSend = createNiceMock(classOf[BlockingSend])
    expect(blockingSend.close()).once()
    replay(replicaManager, stateStore, logManager, log, blockingSend)
    val clusterLinkConfig = new ClusterLinkConfig(clusterLinkProps)

    val fetcherManager = new ClusterLinkFetcherManager(clusterLinkName,
                                                       clusterLinkConfig,
                                                       brokerConfig,
                                                       replicaManager,
                                                       adminManager = null,
                                                       UnboundedQuota,
                                                       new Metrics,
                                                       time) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ClusterLinkFetcherThread = {
        fetcherThread = new ClusterLinkFetcherThread(name,
                                                     fetcherId = 0,
                                                     brokerConfig,
                                                     clusterLinkConfig,
                                                     new ClusterLinkMetadata(brokerConfig, clusterLinkName, 100, 60000),
                                                     this,
                                                     brokerEndPoint,
                                                     failedPartitions,
                                                     replicaManager,
                                                     UnboundedQuota,
                                                     new Metrics,
                                                     new SystemTime,
                                                     tierStateFetcher = None,
                                                     createNiceMock(classOf[ClusterLinkNetworkClient]),
                                                     blockingSend) {
          override def truncate(tp: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {}

          override def latestEpoch(topicPartition: TopicPartition): Option[Int] = Some(1)
        }
        fetcherThread
      }
      override protected def partitionCount(topic: String): Int = 1
    }

    def sourceLeaderEpoch(p: TopicPartition): Integer = 1
    def offsetsPending: Boolean = JTestUtils.fieldValue(partition, classOf[Partition], "needsLinkedLeaderOffsets")

    JTestUtils.setFieldValue(partition, "leaderEpoch", 2)
    fetcherManager.addLinkedFetcherForPartitions(Set(partition))
    assertNull("Fetcher thread created without metadata", fetcherThread)
    val metadataResponse = JTestUtils.metadataUpdateWith("cluster", 1,
      Collections.singletonMap("topic", Errors.NONE),
      Collections.singletonMap("topic", 1),
      sourceLeaderEpoch)
    fetcherManager.currentMetadata.update(1, metadataResponse, false, time.milliseconds)
    fetcherManager.onNewMetadata(fetcherManager.currentMetadata.fetch())
    assertNotNull("Fetcher thread not created", fetcherThread)
    assertTrue("State reset before fetching offsets", offsetsPending)

    fetcherThread.updateFetchOffsetAndMaybeMarkTruncationComplete(Map.empty)
    assertTrue("State reset before source offsets available", offsetsPending)

    fetcherThread.updateFetchOffsetAndMaybeMarkTruncationComplete(
      Map(tp -> OffsetTruncationState(10, truncationCompleted = false)))
    assertTrue("State reset before truncation", offsetsPending)

    fetcherThread.updateFetchOffsetAndMaybeMarkTruncationComplete(
      Map(tp -> OffsetTruncationState(10, truncationCompleted = true)))
    assertFalse("State not reset after truncation", offsetsPending)
  }
}
