/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Properties}

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.{CommonClientConfigs, KafkaClient, MockClient}
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.utils.{LogContext, MockTime}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.{After, Before, Test}
import org.junit.Assert._

import scala.collection.Map

class ClusterLinkMetadataThreadTest {

  private val linkName = "testLink"
  private val brokerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
  private val metadata = new ClusterLinkMetadata(brokerConfig, linkName, 100, 60000)
  private val metrics = new Metrics
  private val time = new MockTime
  private val mockClient = new MockClient(time, metadata)
  private var metadataThread: ClusterLinkMetadataThread = _

  @Before
  def setUp(): Unit = {
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    val clusterLinkConfig = new ClusterLinkConfig(props)
    metadataThread = new ClusterLinkMetadataThread(clusterLinkConfig, metadata, metrics, time) {
      override protected def createNetworkClient(clusterLinkConfig: ClusterLinkConfig, clusterLinkMetadata: ClusterLinkMetadata): ClusterLinkNetworkClient = {
        new ClusterLinkNetworkClient(
          clusterLinkConfig,
          clusterLinkMetadata.throttleTimeSensorName,
          Some(metadata),
          metadataUpdater = None,
          metrics,
          Map("link-name" -> clusterLinkMetadata.linkName),
          time,
          s"cluster-link-metadata-${clusterLinkMetadata.linkName}-broker",
          new LogContext()) {
          override protected def createNetworkClient(selector: Selector): KafkaClient = {
            mockClient
          }
        }
      }
    }
    mockClient.updateMetadata(JTestUtils.metadataUpdateWith(1, Collections.emptyMap[String, Integer]))
  }

  @After
  def tearDown(): Unit = {
    metadataThread.shutdown()
    metadata.close()
    metrics.close()
  }

  @Test
  def testMetadataThread(): Unit = {
    assertTrue(metadataThread.isRunning)
    @volatile var cluster: Cluster = null
    val updateCount = new AtomicInteger()
    val metadataListener = new MetadataListener {
      override def onNewMetadata(newCluster: Cluster): Unit = {
        cluster = newCluster
        updateCount.incrementAndGet
      }
    }
    metadataThread.addListener(metadataListener)
    metadataThread.start()

    assertEquals(0, updateCount.get)
    metadata.requestUpdate()
    TestUtils.waitUntilTrue(() => updateCount.get > 0, "Metadata listener not invoked")
    assertEquals(1, updateCount.get)
    assertEquals(Collections.emptySet, cluster.topics)

    val metadataResponse = JTestUtils.metadataUpdateWith(1, Collections.singletonMap("testTopic", 2))
    mockClient.prepareMetadataUpdate(metadataResponse)
    metadata.setTopics(Set("testTopic"))

    TestUtils.waitUntilTrue(() => updateCount.get > 1, "MClusterLinkMetadataThreadTestetadata listener not invoked")
    assertEquals(2, updateCount.get)
    assertEquals(Collections.singleton("testTopic"), cluster.topics)
    assertEquals(2, cluster.partitionCountForTopic("testTopic"))

    mockClient.prepareMetadataUpdate(metadataResponse)
    metadata.requestUpdate()
    TestUtils.waitUntilTrue(() => updateCount.get > 2, "Metadata listener not invoked")
    assertEquals(3, updateCount.get)

    mockClient.prepareMetadataUpdate(JTestUtils.metadataUpdateWith(1, Collections.singletonMap("testTopic2", 3)))
    metadata.setTopics(Set("testTopic2"))
    TestUtils.waitUntilTrue(() => updateCount.get > 3, "Metadata listener not invoked")
    assertEquals(4, updateCount.get)
    assertEquals(Collections.singleton("testTopic2"), cluster.topics)
    assertEquals(3, cluster.partitionCountForTopic("testTopic2"))

    metadataThread.shutdown()
    assertFalse(mockClient.active)
  }
}
