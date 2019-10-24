/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.util.Properties

import kafka.api.IntegrationTestHarness
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.test.IntegrationTest
import org.junit.experimental.categories.Category
import org.junit.{After, Assert, Before, Test}

// This test should be a system test
@Category(Array(classOf[IntegrationTest]))
class TierProducerStateReplicationTest extends IntegrationTestHarness with Logging {
  private val segmentBytes = 1024
  private val topicName = "topic"
  private val replicationFactor = brokerCount
  // onshotProducer is used to produce to a single segment file, we use it to ensure
  // that a new replica sees a full history of producers, even if they don't exist in the
  // local log.
  private var oneshotProducer: KafkaProducer[Array[Byte], Array[Byte]] = _
  // producer is used to fill in log segments and cause a portion of the log to be tiered.
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _
  private var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = _


  override protected def brokerCount: Int = 3

  override def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach { p =>
      p.put(KafkaConfig.ControlledShutdownEnableProp, "false")
      p.put(KafkaConfig.TierBackendProp, "mock")
      p.put(KafkaConfig.TierEnableProp, "true")
      p.put(KafkaConfig.TierFeatureProp, "true")
      p.put(KafkaConfig.TierMetadataReplicationFactorProp, "3")
      p.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
      p.put(KafkaConfig.TierLocalHotsetBytesProp, (segmentBytes * 2).toString)
      p.put(KafkaConfig.LogSegmentBytesProp, segmentBytes.toString)
      p.put(KafkaConfig.LogCleanupIntervalMsProp, "500")
    }
  }

  @Before
  override def setUp() {
    super.setUp()
    val topicProps = new Properties()
    topicProps.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    topicProps.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    topicProps.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    createTopic(topicName, replicationFactor = replicationFactor)

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    oneshotProducer = createProducer(new ByteArraySerializer, new ByteArraySerializer, producerProps)
    producer = createProducer(new ByteArraySerializer(), new ByteArraySerializer(), producerProps)
    consumer = createConsumer()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    producer.flush()
    producer.close()
    oneshotProducer.flush()
    oneshotProducer.close()
    consumer.close()
  }

  private def produceBytes(prod: KafkaProducer[Array[Byte], Array[Byte]], numBytes: Int): RecordMetadata = {
    val bytes: Array[Byte] = TestUtils.randomBytes(numBytes)
    prod.send(new ProducerRecord[Array[Byte], Array[Byte]](topicName, 0, null, bytes)).get()
  }

  private def leader(topicPartition: TopicPartition): KafkaServer = {
    val leaderId = zkClient.getLeaderForPartition(topicPartition).get
    servers.filter(_.config.brokerId == leaderId).head
  }

  // Block on ISR reaching the expected count
  private def awaitISR(topicPartition: TopicPartition, expected: Int): Unit = {
    TestUtils.waitUntilTrue(() => {
      leader(topicPartition).replicaManager.nonOfflinePartition(topicPartition).get.inSyncReplicaIds.size == expected
    }, "timed out waiting for replicas to join the ISR", 120000, 1000)
  }

  @Test
  def testProducerStateRestorationFromTieredStorage(): Unit = {
    val topicPartition = new TopicPartition(topicName, 0)

    // Wait on the tier topic manager being ready for all brokers, this prevents the test from hanging due to issues
    // creating the tier topic after a broker has been killed
    TestUtils.waitUntilTrue(() => servers.forall(_.tierTopicManagerOpt.get.isReady),
      "Wait until the tier topic manager is ready for all brokers", 30000, 1000)

    val killedBroker = killRandomBroker()

    // wait until the killed broker leaves the ISR
    awaitISR(topicPartition, replicationFactor - 1)
    // produce one batch with the oneshotProducer
    produceBytes(oneshotProducer, 512)
    // fill in enough segments to breach the hotset, causing tiering and local log retention.
    for (_ <- 0 to 30)
      produceBytes(producer, 512)

    val (deadServers, livingServers) = servers.partition(_.config.brokerId == killedBroker)
    val deadServer = deadServers.head
    // Wait until the 2 living servers have tiered some data and advanced their log
    // start offset
    TestUtils.waitUntilTrue(() => {
      livingServers.forall(broker => {
        val log = broker.replicaManager.logManager.getLog(topicPartition)
        log.get.localLogStartOffset > 0
      })
    }, "expected to tier some segments and advance the local log start offset", 300000, 1000)

    Assert.assertTrue(
      "expected the shutdown server to have no active producers",
      deadServer.replicaManager.getLog(topicPartition).get.producerStateManager.activeProducers.isEmpty)

    Assert.assertTrue(
      "expected the active brokers to both have materialized some producer state",
      livingServers.forall(_.replicaManager.getLog(topicPartition).get.producerStateManager.activeProducers.nonEmpty)
    )

    // restart the broker that was killed earlier, and wait until it rejoins the ISR.
    restartDeadBrokers()
    awaitISR(topicPartition, replicationFactor)
    Assert.assertEquals(servers.size, replicationFactor)

    TestUtils.waitUntilTrue(() => {
      val endOffsets = servers.map(_.replicaManager.getLog(topicPartition).get.localLogEndOffset)
      endOffsets.forall(_ == endOffsets.head)
    }, "expected all brokers to have matching end offsets", 300000, 1000)

    // Ensure that all brokers have the same number of producer id's.
    // This tests that even though the killed broker was down when we producer with the
    // oneshot producer, it was still able to restore the producer state from tiered storage
    // when it was brought back up.
    val activeProducersAcrossBrokers = servers.map(broker => {
      broker.replicaManager.getLog(topicPartition).get.producerStateManager.activeProducers.keys.toSet
    })

    activeProducersAcrossBrokers.foreach(s => {
      Assert.assertEquals("expected the set of active producers for all brokers to be the same", activeProducersAcrossBrokers.head, s)
    })
  }
}
