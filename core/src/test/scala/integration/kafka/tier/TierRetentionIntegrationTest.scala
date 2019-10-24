package kafka.tier

import java.util.concurrent.atomic.AtomicBoolean

import kafka.api.IntegrationTestHarness
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.utils.Exit.Procedure
import org.junit.{Before, Test}
import org.junit.After
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import scala.collection.JavaConverters._

class TierRetentionIntegrationTest extends IntegrationTestHarness {
  serverConfig.setProperty(KafkaConfig.TierFeatureProp, "true")
  serverConfig.setProperty(KafkaConfig.TierEnableProp, "true")
  serverConfig.setProperty(KafkaConfig.TierMetadataNumPartitionsProp, "2")
  serverConfig.setProperty(KafkaConfig.TierBackendProp, "mock")
  serverConfig.setProperty(KafkaConfig.LogRetentionBytesProp, "10000")
  serverConfig.setProperty(KafkaConfig.LogSegmentBytesProp, "1000")
  serverConfig.setProperty(KafkaConfig.LogFlushSchedulerIntervalMsProp, "10")
  serverConfig.setProperty(KafkaConfig.TierPartitionStateCommitIntervalProp, "10")
  serverConfig.setProperty(KafkaConfig.LogCleanupIntervalMsProp, "10")

  private val topic = "foo"
  private val numPartitions = 1
  private val numReplicas = 3

  override protected def brokerCount: Int = 3
  val exited = new AtomicBoolean(false)

  @Before
  override def setUp(): Unit = {
    Exit.setExitProcedure(new Procedure {
      override def execute(statusCode: Int, message: String): Unit = exited.set(true)
    })
    super.setUp()
    createTopic(topic, numPartitions, numReplicas)
  }

  @After
  override def tearDown() {
    super.tearDown()
    assertFalse(exited.get())
  }

  @Test
  def testBasicRetention(): Unit = {
    val topicPartition = new TopicPartition(topic, 0)
    val record = new ProducerRecord(topic, 0, s"key".getBytes, s"value".getBytes)
    val recordSize = record.key.size + record.value.size
    val producer = createProducer()

    val partitionInfo = producer.partitionsFor(topic).asScala.find(_.partition == 0).get
    val leaderBrokerId = partitionInfo.leader.id
    val followers = partitionInfo.replicas.map(_.id).filter(_ != leaderBrokerId)
    val leader = servers.find(_.config.brokerId == leaderBrokerId).get

    // Produce some messages
    var bytesSent = 0
    while (bytesSent < 1100) {
      producer.send(record).get
      bytesSent += recordSize
    }

    val log = leader.logManager.getLog(topicPartition).get
    val tierPartitionState = log.tierPartitionState

    // Wait for at least one segment to be tiered
    TestUtils.waitUntilTrue(() => tierPartitionState.totalSize > 0, "Timed out waiting for segments to be tiered")
    TestUtils.waitUntilTrue(() => tierPartitionState.committedEndOffset.orElse(0L) > 0, "Timed out waiting for tier partition state to be flushed")
    assertTrue(tierPartitionState.segmentOffsets.size > 0)
    assertEquals(0, log.logStartOffset)
    assertEquals(0L, tierPartitionState.segmentOffsets.first)

    // Kill one of the followers
    awaitISR(topicPartition, numReplicas, leader)
    killBroker(followers(0))

    // Produce some more messages, taking us past the configured retention.bytes
    while (bytesSent < 20000) {
      producer.send(record).get
      bytesSent += recordSize
    }

    // Wait until retention deleted tiered segments on all alive brokers
    servers.foreach { server =>
      val isAlive = alive(servers.indexOf(server))
      if (isAlive) {
        val log = server.logManager.getLog(topicPartition).get
        val tierPartitionState = log.tierPartitionState
        TestUtils.waitUntilTrue(() => log.logStartOffset > 0, "Timed out waiting for retention to kick in")
        TestUtils.waitUntilTrue(() => tierPartitionState.segmentOffsets.size > 0, "Timed out waiting for more segments to be tiered")
        assertTrue(tierPartitionState.segmentOffsets.first > 0)
      }
    }

    // Kill another follower
    killBroker(followers(1))

    // Produce some more messages
    while (bytesSent < 40000) {
      producer.send(record).get
      bytesSent += recordSize
    }

    // Restart both dead followers
    restartDeadBrokers()
    awaitISR(topicPartition, numReplicas, leader)

    // Wait until all brokers have the same log structure
    waitUntilEqualOnAllBrokers(server => server.logManager.getLog(topicPartition).get.logStartOffset.toString, "Timed out waiting for logStartOffset sync")
    waitUntilEqualOnAllBrokers(server => server.logManager.getLog(topicPartition).get.logEndOffset.toString, "Timed out waiting for logEndOffset sync")
    waitUntilEqualOnAllBrokers(server => {
      val tierPartitionState = server.logManager.getLog(topicPartition).get.tierPartitionState
      tierPartitionState.segmentOffsets
    }, "Timed out waiting for tier partition state sync")
  }

  private def waitUntilEqualOnAllBrokers(computeFn: (KafkaServer) => Object, msg: String): Unit = {
    TestUtils.waitUntilTrue(() =>
      servers.map { server =>
        computeFn(server)
      }.toSet.size == 1, "")
  }

  private def awaitISR(tp: TopicPartition, numReplicas: Int, leader: KafkaServer): Unit = {
    TestUtils.waitUntilTrue(() => {
      leader.replicaManager.nonOfflinePartition(tp).get.inSyncReplicaIds.size == numReplicas
    }, "Timed out waiting for replicas to join ISR")
  }
}
