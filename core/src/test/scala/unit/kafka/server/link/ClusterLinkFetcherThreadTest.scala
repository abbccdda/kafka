/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.Properties

import kafka.cluster.BrokerEndPoint
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server._
import kafka.tier.fetcher.TierStateFetcher
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, SystemTime, Time}
import org.easymock.EasyMock.{anyObject, expect, mock, replay}
import org.junit.Test

class ClusterLinkFetcherThreadTest extends ReplicaFetcherThreadTest {

  val clusterLinkName = "testCluster"

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
    replay(fetcherManager)
    new ClusterLinkFetcherThread(
      name,
      fetcherId = 0,
      brokerConfig,
      new ClusterLinkConfig(clusterLinkConfigs),
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

  private def clusterLinkConfigs : Properties = {
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
    val clusterLinkReplicaManager = new ClusterLinkReplicaManager(
      KafkaConfig.fromProps(props),
      replicaManager,
      quota = UnboundedQuota,
      new Metrics,
      new SystemTime,
      tierStateFetcher = None)
    clusterLinkReplicaManager.addClusterLink(clusterLinkName, clusterLinkConfigs)
  }
}
