/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util
import java.util.{Collections, Properties}

import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.log.AbstractLog
import kafka.server._
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, CreatePartitionsResult, NewPartitions}
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.easymock.{Capture, CaptureType}
import org.easymock.EasyMock._
import org.junit.{After, Before, Test}
import org.junit.Assert._

import scala.collection.Map
import scala.jdk.CollectionConverters._

class ClusterLinkFetcherManagerTest {

  private val linkName = "testLink"
  private val brokerConfig = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, "localhost:1234"))
  private val metrics = new Metrics
  private val time = new MockTime
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val log: AbstractLog = createNiceMock(classOf[AbstractLog])
  private var fetcherManager: ClusterLinkFetcherManager = _
  private var destAdminClient: Admin = _
  private var numPartitions = 2


  @Before
  def setUp(): Unit = {
    val props = new Properties
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
    val clusterLinkConfig = new ClusterLinkConfig(props)

    destAdminClient = createNiceMock(classOf[Admin])
    fetcherManager = new ClusterLinkFetcherManager(
      linkName,
      clusterLinkConfig,
      None,
      brokerConfig,
      replicaManager,
      destAdminClient,
      UnboundedQuota,
      metrics,
      time) {
      override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ClusterLinkFetcherThread = {
        val thread: ClusterLinkFetcherThread = createNiceMock(classOf[ClusterLinkFetcherThread])
        setupFetcherThreadMock(thread)
        thread
      }
      override protected def partitionCount(topic: String): Int = numPartitions
    }
    expect(log.localLogEndOffset).andReturn(0L).anyTimes()
    replay(log)
  }

  @After
  def tearDown(): Unit = {
    if (fetcherManager != null)
      fetcherManager.shutdown()
    metrics.close()
  }

  @Test
  def testMetadataTopics(): Unit = {
    val topic1 = "testTopic1"
    val tp1_0 = new TopicPartition(topic1, 0)
    val partition1_0 : Partition = mock(classOf[Partition])
    setupMock(partition1_0, tp1_0)

    fetcherManager.addLinkedFetcherForPartitions(Set(partition1_0))
    assertEquals(Set(topic1), metadataTopics)
    assertEquals(0, fetcherManager.currentMetadata.timeToNextUpdate(time.milliseconds))

    fetcherManager.removeLinkedFetcherForPartitions(Set(tp1_0), retainMetadata = true)
    assertEquals(Set(topic1), metadataTopics)

    val topic2 = "testTopic2"
    val tp2_4 = new TopicPartition(topic2, 4)
    val partition2_4 : Partition = mock(classOf[Partition])
    setupMock(partition2_4, tp2_4)

    fetcherManager.addLinkedFetcherForPartitions(Set(partition2_4))
    assertEquals(Set(topic1, topic2), metadataTopics)

    fetcherManager.removeLinkedFetcherForPartitions(Set(tp1_0), retainMetadata = false)
    assertEquals(Collections.singletonList(topic2), fetcherManager.currentMetadata.newMetadataRequestBuilder().topics())

    val tp1_1 = new TopicPartition(topic1, 1)
    val partition1_1 : Partition = mock(classOf[Partition])
    setupMock(partition1_1, tp1_1)
    fetcherManager.addLinkedFetcherForPartitions(Set(partition1_1))
    assertEquals(Set(topic1, topic2), metadataTopics)
    fetcherManager.addLinkedFetcherForPartitions(Set(partition1_0))
    assertEquals(2, fetcherManager.currentMetadata.newMetadataRequestBuilder().topics().size)
    assertEquals(Set(topic1, topic2), metadataTopics)
    fetcherManager.removeLinkedFetcherForPartitions(Set(tp1_0), retainMetadata = false)
    assertEquals(Set(topic1, topic2), metadataTopics)
  }

  @Test
  def testFetcherThreads(): Unit = {
    val topic = "testTopic"
    val tp = new TopicPartition(topic, 0)
    val partition : Partition = mock(classOf[Partition])
    setupMock(partition, tp)

    fetcherManager.addLinkedFetcherForPartitions(Set(partition))
    assertEquals(None, fetcherManager.getFetcher(tp))
    assertEquals(Set(topic), metadataTopics)

    val topics: Map[String, Integer] = Map(topic -> 2)
    setupMock(partition, tp, linkedLeaderEpoch=1, numEpochUpdates = 1)
    updateMetadata(topics, linkedLeaderEpoch = 5)
    assertEquals(1, fetcherManager.fetcherThreadMap.size)
    verify(partition)

    setupMock(partition, tp, linkedLeaderEpoch=5, numEpochUpdates = 0)
    updateMetadata(topics, linkedLeaderEpoch = 5)
    assertEquals(1, fetcherManager.fetcherThreadMap.size)
    verify(partition)

    setupMock(partition, tp, linkedLeaderEpoch=5, numEpochUpdates = 1)
    updateMetadata(topics, linkedLeaderEpoch = 6)
    assertEquals(1, fetcherManager.fetcherThreadMap.size)
    verify(partition)

    fetcherManager.removeLinkedFetcherForPartitions(Set(tp), retainMetadata = true)
    assertEquals(Collections.singletonList(topic), fetcherManager.currentMetadata.newMetadataRequestBuilder().topics())
    fetcherManager.shutdownIdleFetcherThreads()
    assertEquals(0, fetcherManager.fetcherThreadMap.size)
    verify(partition)

    fetcherManager.addLinkedFetcherForPartitions(Set(partition))
    setupMock(partition, tp, linkedLeaderEpoch=6, numEpochUpdates = 0)
    updateMetadata(topics, linkedLeaderEpoch = 6)
    assertTrue(fetcherManager.getFetcher(tp).nonEmpty)
    verify(partition)

    fetcherManager.shutdown()
    assertEquals(0, fetcherManager.fetcherThreadMap.size)
  }

  @Test
  def testAddSourcePartitions(): Unit = {
    val topic = "testTopic"
    val tp = new TopicPartition(topic, 0)
    val partition: Partition = mock(classOf[Partition])
    setupMock(partition, tp)

    val createPartitionsResult: CreatePartitionsResult = createNiceMock(classOf[CreatePartitionsResult])
    expect(createPartitionsResult.values())
      .andReturn(Collections.singletonMap(topic, KafkaFuture.completedFuture(null))).anyTimes()
    val capturedRequests: Capture[util.Map[String, NewPartitions]] = newCapture(CaptureType.ALL)
    expect(destAdminClient.createPartitions(capture(capturedRequests)))
      .andReturn(createPartitionsResult)
      .anyTimes()
    replay(replicaManager, destAdminClient, createPartitionsResult)

    numPartitions = 1
    var numSourcePartitions: Integer = 1
    val sourceEpoch = 1
    fetcherManager.addLinkedFetcherForPartitions(Set(partition))
    updateMetadata(Map(topic -> numSourcePartitions), sourceEpoch)
    assertEquals(1, fetcherManager.fetcherThreadMap.size)

    // Increase source partitions and verify that we attempt to increase destination partitions
    numSourcePartitions = 4
    updateMetadata(Map(topic -> numSourcePartitions), sourceEpoch)
    assertEquals(1, capturedRequests.getValues.size)
    val captured1 = capturedRequests.getValues.get(0)
    assertEquals(1, captured1.size)
    assertEquals(4, captured1.get(topic).totalCount)

    // Verify that we retry on next metadata if destination partition count hasn't been updated
    updateMetadata(Map(topic -> numSourcePartitions), sourceEpoch)
    assertEquals(2, capturedRequests.getValues.size)
    val captured2 = capturedRequests.getValues.get(1)
    assertEquals(1, captured2.size)
    assertEquals(4, captured2.get(topic).totalCount)

    // Verify that we dont retry after destination partition count is updated
    numPartitions = 4
    updateMetadata(Map(topic -> numSourcePartitions), sourceEpoch)
    assertEquals(2, capturedRequests.getValues.size)
  }

  @Test
  def testReconfigure(): Unit = {
    val topic = "testTopic"
    val tp = new TopicPartition(topic, 0)
    val partition : Partition = mock(classOf[Partition])
    setupMock(partition, tp)

    fetcherManager.addLinkedFetcherForPartitions(Set(partition))
    assertEquals(None, fetcherManager.getFetcher(tp))
    assertEquals(Set(topic), metadataTopics)

    val topics: Map[String, Integer] = Map(topic -> 2)
    setupMock(partition, tp, linkedLeaderEpoch=2, numEpochUpdates = 1)
    updateMetadata(topics, linkedLeaderEpoch = 2)
    assertEquals(1, fetcherManager.fetcherThreadMap.size)
    val fetcherThread1 = fetcherManager.fetcherThreadMap.values.head
    val metadata1 = fetcherManager.currentMetadata
    val metadataThread1: ClusterLinkMetadataThread =
      JTestUtils.fieldValue(fetcherManager, classOf[ClusterLinkFetcherManager], "metadataRefreshThread")
    val metadataClient1 = metadataThread1.clusterLinkClient

    setupFetcherThreadMock(fetcherThread1, Set(new TopicPartition(topic, 0)))
    val fetcherClient1 = fetcherThread1.clusterLinkClient
    expect(fetcherClient1.reconfigure(anyObject())).times(1)
    replay(fetcherClient1)

    val newDynamicProps = new util.HashMap[String, String]
    newDynamicProps.putAll(fetcherManager.currentConfig.originalsStrings())
    newDynamicProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "truststore.jks")
    fetcherManager.reconfigure(new ClusterLinkConfig(newDynamicProps), Set(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
    assertEquals(1, fetcherManager.fetcherThreadMap.size)
    assertSame(fetcherThread1, fetcherManager.fetcherThreadMap.values.head)
    assertSame(metadata1, fetcherManager.currentMetadata)
    verify(fetcherClient1)

    val newPeriodicMigrationProps = new util.HashMap[String, String]
    newDynamicProps.putAll(fetcherManager.currentConfig.originalsStrings())
    newDynamicProps.put(ClusterLinkConfig.AclSyncMsProp, "120000")
    fetcherManager.reconfigure(new ClusterLinkConfig(newPeriodicMigrationProps), Set(ClusterLinkConfig.AclSyncMsProp))
    assertEquals(1, fetcherManager.fetcherThreadMap.size)
    assertSame(fetcherThread1, fetcherManager.fetcherThreadMap.values.head)
    assertSame(metadata1, fetcherManager.currentMetadata)
    verify(fetcherClient1)

    val newNonDynamicProps = new util.HashMap[String, String]
    newNonDynamicProps.putAll(fetcherManager.currentConfig.originalsStrings())
    newNonDynamicProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:5678")
    reset(fetcherThread1.clusterLinkClient)
    expect(fetcherClient1.close()).once()
    replay(fetcherClient1)
    fetcherManager.reconfigure(new ClusterLinkConfig(newNonDynamicProps), Set(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
    assertEquals(0, fetcherManager.fetcherThreadMap.size)
    assertNotSame(metadata1, fetcherManager.currentMetadata)
    assertEquals(Set(topic), metadataTopics)
    updateMetadata(topics, linkedLeaderEpoch = 2)
    assertNotSame(fetcherThread1, fetcherManager.fetcherThreadMap.values.head)
    assertFalse("Metadata client not closed", metadataClient1.networkClient.active)
    val metadataThread2: ClusterLinkMetadataThread =
      JTestUtils.fieldValue(fetcherManager, classOf[ClusterLinkFetcherManager], "metadataRefreshThread")
    assertNotSame(metadataThread1, metadataThread2)
    assertNotSame(metadataClient1, metadataThread2.clusterLinkClient)
    assertTrue("Metadata client not active", metadataThread2.clusterLinkClient.networkClient.active)
    verify(fetcherClient1)
  }

  private def updateMetadata(topics: Map[String, Integer],
                             linkedLeaderEpoch: Int): Unit = {
    val metadata = fetcherManager.currentMetadata
    val metadataResponse = JTestUtils.metadataUpdateWith("sourceCluster", 1,
      Collections.emptyMap[String, Errors], topics.asJava, _ => linkedLeaderEpoch)
    metadata.update(metadata.updateVersion(), metadataResponse, false, time.milliseconds)
    fetcherManager.onNewMetadata(JTestUtils.clusterWith(1, topics.asJava))
  }

  private def setupMock(partition: Partition, tp: TopicPartition,
                        linkedLeaderEpoch: Int = 1,
                        numEpochUpdates: Int = 0): Unit = {
    reset(partition)
    expect(partition.topicPartition).andReturn(tp).anyTimes()
    expect(partition.isActiveLinkDestinationLeader).andReturn(true).anyTimes()
    expect(partition.getLinkedLeaderEpoch).andReturn(Some(linkedLeaderEpoch)).anyTimes()
    expect(partition.getLeaderEpoch).andReturn(10).anyTimes()
    expect(partition.localLogOrException).andReturn(log).anyTimes()
    if (numEpochUpdates > 0) {
      expect(partition.updateLinkedLeaderEpoch(anyInt())).andReturn(true).times(numEpochUpdates)
      expect(partition.linkedLeaderOffsetsPending(true)).times(numEpochUpdates)
    }
    replay(partition)
  }

  private def setupFetcherThreadMock(fetcherThread: ClusterLinkFetcherThread,
                                     partitions: Set[TopicPartition] = Set.empty): Unit = {
    reset(fetcherThread)
    val initialFetchState: InitialFetchState = createNiceMock(classOf[InitialFetchState])
    val partitionAndOffsets = partitions.map(_ -> initialFetchState).toMap
    expect(fetcherThread.partitionsAndOffsets).andReturn(partitionAndOffsets).anyTimes()
    val fetchState: PartitionFetchState = createNiceMock(classOf[PartitionFetchState])
    expect(fetcherThread.fetchState(anyObject())).andReturn(Some(fetchState)).anyTimes()
    val fetcherClient: ClusterLinkNetworkClient = createNiceMock(classOf[ClusterLinkNetworkClient])
    expect(fetcherThread.clusterLinkClient).andReturn(fetcherClient).anyTimes()
    expect(fetcherThread.shutdown()).andAnswer(() => fetcherClient.close()).anyTimes()
    replay(fetcherThread)
  }

  private def metadataTopics = fetcherManager.currentMetadata.newMetadataRequestBuilder().topics().asScala.toSet
}
