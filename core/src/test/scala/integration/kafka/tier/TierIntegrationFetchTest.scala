/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.{Properties, UUID}
import java.util.Random

import javax.management.ObjectName
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class TierIntegrationFetchTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 1
  serverConfig.put(KafkaConfig.TierPartitionStateCommitIntervalProp, "5")

  private def configureMock(): Unit = {
    serverConfig.put(KafkaConfig.TierBackendProp, "mock")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  }

  private def configureMinio(): Unit = {
    serverConfig.put(KafkaConfig.TierBackendProp, "S3")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
    serverConfig.put(KafkaConfig.TierS3AwsAccessKeyIdProp, "admin")
    serverConfig.put(KafkaConfig.TierS3AwsSecretAccessKeyProp, "password")
    serverConfig.put(KafkaConfig.TierS3RegionProp, "us-east-1")
    serverConfig.put(KafkaConfig.TierS3EndpointOverrideProp, "http://localhost:9000")
    serverConfig.put(KafkaConfig.TierS3SignerOverrideProp, "AWSS3V4SignerType")
    serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  }

  private def configures3GcsCompatMode(): Unit = {
    serverConfig.put(KafkaConfig.TierBackendProp, "S3")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "tiered-storage-gcs-compatibility-testing-lucas")
    serverConfig.put(KafkaConfig.TierS3RegionProp, "us-east-1")
    serverConfig.put(KafkaConfig.TierS3EndpointOverrideProp, "storage.googleapis.com")
    serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  }

  private def configureS3(): Unit = {
    serverConfig.put(KafkaConfig.TierBackendProp, "S3")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "confluent-tier-system-test")
    serverConfig.put(KafkaConfig.TierS3RegionProp, "us-west-2")
    serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  }

  serverConfig.put(KafkaConfig.TierEnableProp, "false")
  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
  serverConfig.put(KafkaConfig.TierMetadataReplicationFactorProp, "1")
  serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, Int.MaxValue.toString) // disable log cleanup, we will manually trigger retention
  serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  configureMock()

  private val topic = UUID.randomUUID().toString
  private val partitions: Int = 1
  private var partitionToLeader: Map[Int, Int] = Map()
  private def topicPartitions: Seq[TopicPartition] = Range(0,partitions).map(p => new TopicPartition(topic, p))

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val props = new Properties
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "10000")
    // Set retention bytes adequately low, to allow us to delete some segments after they have been tiered
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "5000")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")

    partitionToLeader = createTopic(topic, partitions, 1, props)
  }

  private def produceRecords(nBatches: Int, recordsPerBatch: Int): Unit = {
    val producer = createProducer()
    try {
      for (b <- 0 until nBatches) {
        val producerRecords = (0 until recordsPerBatch).map { i =>
          val m = recordsPerBatch * b + i
          val timestamp = Math.abs(new Random().nextLong())
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

  private def getLeaderForTopicPartition(leaderTopicPartition: TopicPartition): Int = {
    partitionToLeader(leaderTopicPartition.partition())
  }

  /**
    * Waits until minNumSegments across all topic partitions are tiered.
    */
  private def waitUntilSegmentsTiered(minNumSegments: Int = 1): Unit = {
    topicPartitions.foreach { tp =>
      val leaderId = getLeaderForTopicPartition(tp)
      val server = serverForId(leaderId)
      val tierPartitionState = server.get.tierMetadataManager.tierPartitionState(tp).get

      TestUtils.waitUntilTrue(() =>
        tierPartitionState.numSegments > minNumSegments &&
          tierPartitionState.endOffset == tierPartitionState.committedEndOffset,
        s"timeout waiting for at least $minNumSegments to be archived and materialized", 60000L)
    }
  }

  /**
    * Delete old (tiered) segments on all brokers.
    */
  private def simulateRetention(): Unit = {
    topicPartitions.foreach { tp =>
      val leaderId = getLeaderForTopicPartition(tp)
      val server = serverForId(leaderId)
      val numDeleted = server.get.replicaManager.logManager.getLog(tp).get.deleteOldSegments()
      assertTrue("tiered segments should have been deleted", numDeleted > 0)
    }
  }

  @Test
  def testArchiveAndFetchSingleTopicPartition(): Unit = {
    val nBatches = 100
    val recordsPerBatch = 100
    produceRecords(nBatches, recordsPerBatch)
    waitUntilSegmentsTiered(10)
    simulateRetention()

    val topicPartition = topicPartitions.head

    val brokerList = TestUtils.bootstrapServers(servers, listenerName)

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50000")
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer")
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")

    val consumer = new KafkaConsumer[String, String](consumerProps)

    try {
      val partitions = new util.ArrayList[TopicPartition]()
      partitions.add(topicPartition)
      partitions.add(new TopicPartition(topic, 1))
      consumer.assign(partitions)
      consumer.seekToBeginning(partitions)
      val valuesRead = new util.ArrayList[Int]()
      val timestampsOffsets = new util.ArrayList[(Long,Long)]()
      while (valuesRead.size() != nBatches * recordsPerBatch) {
        val records = consumer.poll(Duration.ofMillis(1000))
        records.forEach(new Consumer[ConsumerRecord[String, String]]() {
          override def accept(r: ConsumerRecord[String, String]): Unit = {
            valuesRead.add(Integer.parseInt(r.value()))
            timestampsOffsets.add((r.timestamp(), r.offset()))
          }
        })
      }
      val expectedValues = new util.ArrayList[Int](Range(0, nBatches * recordsPerBatch).asJava)
      assertEquals(expectedValues, valuesRead)

      for ((timestamp, offset) <- timestampsOffsets.asScala) {
        val expectedOffset = timestampsOffsets.asScala.find {case (recordTimestamp, recordOffset) => recordTimestamp >= timestamp }.get._2
        assertTimestampForOffsetLookupCorrect(topicPartition, consumer, timestamp, expectedOffset)
      }

      // smallest possible timestamp should return offset of 0
      assertTimestampForOffsetLookupCorrect(topicPartition, consumer, 0, 0)
      // largest possible timestamp should not happen, so offset for times should return null
      assertTimestampForOffsetLookupMissing(topicPartition, consumer, Long.MaxValue)
    } finally {
      consumer.close()
    }

    val mBeanServer = ManagementFactory.getPlatformMBeanServer

    val bean = "kafka.server:type=TierFetcher"
    val attrs = Array("BytesFetchedTotal")
    val List(bytesFetchedTotal) = mBeanServer
      .getAttributes(new ObjectName(bean), attrs)
      .asList.asScala
      .map { attr => attr.getValue.asInstanceOf[Double] }
      .toList

    assertTrue("tier fetch metric shows no data fetched from tiered storage",
      bytesFetchedTotal > 100)

    val List(meanArchiveRate) = mBeanServer
      .getAttributes(new ObjectName("kafka.tier.archiver:type=TierArchiver,name=BytesPerSec"), Array("MeanRate"))
      .asList.asScala
      .map { attr => attr.getValue.asInstanceOf[Double] }
      .toList

    assertTrue("tier archiver mean rate shows no data uploaded to tiered storage",
      meanArchiveRate > 100)
  }

  private def assertTimestampForOffsetLookupCorrect(topicPartition: TopicPartition, consumer: KafkaConsumer[String, String], timestamp: Long, expectedOffset: Long) = {
    val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]()
    timestampsToSearch.put(topicPartition, timestamp)
    assertEquals("timestamp should match offset read",
      consumer.offsetsForTimes(timestampsToSearch).get(topicPartition).offset(), expectedOffset)
  }

  private def assertTimestampForOffsetLookupMissing(topicPartition: TopicPartition, consumer: KafkaConsumer[String,String], timestamp: Long) = {
    val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]()
    timestampsToSearch.put(topicPartition, timestamp)
    assertEquals("offset should not be returned", null, consumer.offsetsForTimes(timestampsToSearch).get(topicPartition))
  }
}
