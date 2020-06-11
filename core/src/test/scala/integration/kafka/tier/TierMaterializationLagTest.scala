/*
 Copyright 2018-2020 Confluent Inc.
 */

package kafka.tier

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}
import java.util.concurrent.atomic.AtomicBoolean

import javax.management.ObjectName
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.NewPartitionReassignment
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.apache.kafka.common.utils.Exit
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.{Before, Test}
import org.junit.After

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

class TierMaterializationLagTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 3

  private def configureMock(): Unit = {
    serverConfig.put(KafkaConfig.TierBackendProp, "mock")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  }

  serverConfig.put(KafkaConfig.TierPartitionStateCommitIntervalProp, "5")
  serverConfig.put(KafkaConfig.TierEnableProp, "false")
  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "3")
  serverConfig.put(KafkaConfig.TierMetadataReplicationFactorProp, "1")
  serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, Int.MaxValue.toString) // disable log cleanup, we will manually trigger retention
  serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  serverConfig.put(KafkaConfig.TierFetcherMemoryPoolSizeBytesProp, (1024 * 1024).toString)
  configureMock()

  private val topic = "test_topic"
  private val partitions: Int = 1

  private def topicPartitions: Seq[TopicPartition] = Range(0, partitions).map(p => new TopicPartition(topic, p))

  val exited = new AtomicBoolean(false)

  @Before
  override def setUp(): Unit = {
    Exit.setExitProcedure((_, _) => exited.set(true))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    assertFalse(exited.get())
  }

  @Test
  def testMaxTierMaterializationLagMetric(): Unit = {
    val props = new Properties
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "10000")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_MS_CONFIG, "-1")
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "1")

    // create topic with customized replica assignment
    val partitionToLeaderMap = createTopic(topic, Map(0 -> List(0, 1)), props)

    val nBatches = 1000
    val recordsPerBatch = 100
    produceRecords(nBatches, recordsPerBatch)
    waitHotsetRetention(partitionToLeaderMap)

    val mBeanServer = ManagementFactory.getPlatformMBeanServer

    // max tier materialization lag should be 0 before replication
    val maxMaterializationLag = mBeanServer
      .getAttributes(new ObjectName("kafka.server:type=TierTopicConsumer"), Array("MaxTierLag"))
      .asList.asScala
      .map { attr => attr.getValue.asInstanceOf[Double] }
      .toList

    assertEquals("Max tier materialization lag shows wrong value with no replication",
      List(0.0), maxMaterializationLag)

    // reassign the partition and trigger replication
    val client = createAdminClient()
    client.alterPartitionReassignments(Map(new TopicPartition(topic, 0) ->
      Optional.of(new NewPartitionReassignment(util.Arrays.asList(2, 0)))).asJava )

    // make sure log manager is defined so that materialization offset can be tracked by the metric
    TestUtils.waitUntilTrue(() =>
      servers(2).logManager.getLog(new TopicPartition(topic, 0)).isDefined,
      "Log manager is not instantiated properly.")

    // max tier materialization lag should be greater than 0 when replication begins
    TestUtils.waitUntilTrue(() => {
      val List(maxMaterializationLag) = mBeanServer
        .getAttributes(new ObjectName("kafka.server:type=TierTopicConsumer"), Array("MaxTierLag"))
        .asList.asScala
        .map { attr => attr.getValue.asInstanceOf[Double] }
        .toList
      maxMaterializationLag > 0.0 }, "Max tier materialization lag stays at 0 after replication.")

    // max tier materialization lag should be 0 after replication is finished
    TestUtils.waitUntilTrue(() => {
      val List(maxMaterializationLag) = mBeanServer
        .getAttributes(new ObjectName("kafka.server:type=TierTopicConsumer"), Array("MaxTierLag"))
        .asList.asScala
        .map { attr => attr.getValue.asInstanceOf[Double] }
        .toList
      maxMaterializationLag == 0.0 }, "Max tier materialization lag fails to return to 0")
  }

  private def produceRecords(nBatches: Int, recordsPerBatch: Int): Unit = {
    val producer = createProducer()
    try {
      for (b <- 0 until nBatches) {
        val timestamp = System.currentTimeMillis
        val producerRecords = (0 until recordsPerBatch).map { i =>
          val m = recordsPerBatch * b + i
          new ProducerRecord(topic, null, timestamp,
            "foo".getBytes(StandardCharsets.UTF_8),
            s"$m".getBytes(StandardCharsets.UTF_8))
        }
        producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))
      }
    } finally {
      producer.close()
    }
  }

  private def getLeaderForTopicPartition(topicPartition: TopicPartition, partitionToLeaderMap: Map[Int, Int]): Int = {
    partitionToLeaderMap(topicPartition.partition)
  }

  /**
   * Wait until retention is triggered and some data is tiered
   */
  private def waitHotsetRetention(partitionToLeaderMap: Map[Int, Int]): Unit = {
    topicPartitions.foreach { tp =>
      val leaderId = getLeaderForTopicPartition(tp, partitionToLeaderMap)
      val server = serverForId(leaderId)
      val log = server.get.replicaManager.logManager.getLog(tp).get

      TestUtils.waitUntilTrue(() => {
        // manually triggers retention
        log.deleteOldSegments()
        log.localLogStartOffset > log.logStartOffset
      }, "timeout waiting for retention is triggered and some tiered data is deleted")
    }
  }
}
