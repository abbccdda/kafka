/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier

import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.tier.store.MockInMemoryTierObjectStore
import kafka.utils.{CoreUtils, TestUtils}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Exit
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.{After, Before, Test}

import scala.jdk.CollectionConverters._

class TierTopicDeletionIntegrationTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 3

  private val topic = "foo"
  private val numPartitions = 4
  private val numReplicas = 2
  private val exited = new AtomicBoolean(false)

  locally {
    serverConfig.setProperty(KafkaConfig.TierFeatureProp, "true")
    serverConfig.setProperty(KafkaConfig.TierEnableProp, "true")
    serverConfig.setProperty(KafkaConfig.TierMetadataNumPartitionsProp, "2")
    serverConfig.setProperty(KafkaConfig.TierMetadataReplicationFactorProp, "2")
    serverConfig.setProperty(KafkaConfig.TierBackendProp, "mock")
    serverConfig.setProperty(KafkaConfig.TierTopicDeleteCheckIntervalMsProp, "10")
    serverConfig.setProperty(KafkaConfig.TierPartitionStateCommitIntervalProp, "10")
    serverConfig.setProperty(KafkaConfig.LogRetentionBytesProp, "10000")
    serverConfig.setProperty(KafkaConfig.LogSegmentBytesProp, "1000")
    serverConfig.setProperty(KafkaConfig.LogFlushSchedulerIntervalMsProp, "10")
  }

  @Before
  override def setUp(): Unit = {
    Exit.setExitProcedure((_, _) => exited.set(true))
    super.setUp()
    createTopic(topic, numPartitions, numReplicas)
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    assertFalse(exited.get())
  }

  @Test
  def testTopicDeletion(): Unit = {
    val records = for (i <- 0 until numPartitions) yield new ProducerRecord(topic, i, s"key".getBytes, s"value".getBytes)
    val recordSize = records.head.key.size + records.head.value.size
    val producer = createProducer()

    // Produce some messages
    var bytesSent = 0
    while (bytesSent < 200) {
      records.foreach { record =>
        producer.send(record).get
      }
      bytesSent += recordSize
    }

    val mockObjectStore = servers.head.tierObjectStoreOpt.get.asInstanceOf[MockInMemoryTierObjectStore]
    var topicId: String = null
    for (i <- 0 until numPartitions) {
      val topicPartition = new TopicPartition(topic, i)

      // find leader of partition
      val partitionInfo = producer.partitionsFor(topic).asScala.find(_.partition == i).get
      val leaderBrokerId = partitionInfo.leader.id
      val leader = servers.find(_.config.brokerId == leaderBrokerId).get

      val log = leader.logManager.getLog(topicPartition).get
      val tierPartitionState = log.tierPartitionState
      TestUtils.waitUntilTrue(() => tierPartitionState.totalSize > 0, "Timed out waiting for segments to be tiered")
      TestUtils.waitUntilTrue(() => tierPartitionState.committedEndOffset > 0, "Timed out waiting for tier partition state to be flushed")

      topicId = CoreUtils.uuidToBase64(tierPartitionState.topicIdPartition.get.topicId)
    }

    // MockInMemoryObjectStore uses a static instance to store tiered objects, and so we could see objects left over
    // by previous tests or tests running in parallel. Here, we filter the objects based on the topic id in the
    // object key, so we only see objects relevant for the specific topic we are interested in.
    def numObjects: Int = mockObjectStore.getStored.keys.asScala.filter(_.contains(topicId)).size

    // at least one segment per partition should have been tiered
    assertTrue(numObjects >= numPartitions)

    // delete topic
    val adminClient = createAdminClient()
    adminClient.deleteTopics(Collections.singleton(topic)).all.get
    TestUtils.waitUntilTrue(() => numObjects == 0, "Timed out waiting for all objects to be deleted")
  }
}
