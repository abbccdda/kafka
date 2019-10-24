/*
 Copyright 2018 Confluent Inc.
 */

package integration.kafka.tier

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.{Properties, UUID}
import java.util.concurrent.atomic.AtomicBoolean

import javax.management.ObjectName
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.utils.Exit.Procedure
import org.junit.{Assert, Before, Test}
import org.junit.After

import scala.collection.JavaConverters._

class TierIntegrationTransactionTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 1
  serverConfig.put(KafkaConfig.TierPartitionStateCommitIntervalProp, "5")

  private def configureMock = {
    serverConfig.put(KafkaConfig.TierBackendProp, "mock")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  }

  serverConfig.put(KafkaConfig.TierEnableProp, "false")
  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
  serverConfig.put(KafkaConfig.TierMetadataReplicationFactorProp, "1")
  serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, Int.MaxValue.toString) // disable log cleanup, we will manually trigger retention
  serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  serverConfig.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
  serverConfig.put(KafkaConfig.TransactionsTopicMinISRProp, "1")

  configureMock

  private val topic = UUID.randomUUID().toString
  private val partitions: Int = 1
  private var partitionToLeader: Map[Int, Int] = Map()
  private def topicPartitions: Seq[TopicPartition] = Range(0, partitions).map(p => new TopicPartition(topic, p))
  val exited = new AtomicBoolean(false)

  @Before
  override def setUp(): Unit = {

    Exit.setExitProcedure(new Procedure {
      override def execute(statusCode: Int, message: String): Unit = exited.set(true)
    })

    super.setUp()
    val props = new Properties
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "10000")
    // Set retention bytes adequately low, to allow us to delete some segments after they have been tiered
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "5000")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    props.put(TopicConfig.RETENTION_MS_CONFIG, "-1")

    partitionToLeader = createTopic(topic, partitions, 1, props)
  }

  @After
  override def tearDown() {
    super.tearDown()
    Assert.assertFalse(exited.get())
  }

  // Produce records, alternating between comitting and aborting batches
  private def produceRecords(nBatches: Int, recordsPerBatch: Int): Unit = {
    val props = new Properties
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "1")
    val producer = createProducer(configOverrides = props)
    producer.initTransactions()

    try {
      for (b <- 0 until nBatches) {
        val abortBatch = b % 2 == 0
        val key = if (abortBatch) "aborted" else "committed"
        val producerRecords = (0 until recordsPerBatch).map(i => {
          val m = recordsPerBatch * b + i
          val timestamp = b + 1L * i
          new ProducerRecord(topic, null, timestamp,
            key.getBytes(StandardCharsets.UTF_8),
            s"$m".getBytes(StandardCharsets.UTF_8)
          )
        })

        producer.beginTransaction()
        producerRecords.foreach(record => {
          producer.send(record).get(10, TimeUnit.SECONDS)
        })
        if (abortBatch)
          producer.abortTransaction()
        else
          producer.commitTransaction()
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
  private def waitUntilSegmentsTiered(minNumSegments: Int): Unit = {
    TestUtils.waitUntilTrue(() => {
      topicPartitions.forall { tp =>
        val leaderId = getLeaderForTopicPartition(tp)
        val server = serverForId(leaderId)
        val tierPartitionState = server.get.logManager.getLog(tp).get.tierPartitionState
        val endOffset = tierPartitionState.endOffset.orElse(0L)
        val committedEndOffset = tierPartitionState.committedEndOffset.orElse(0L)
        endOffset > 0 &&
          committedEndOffset > 0 &&
          endOffset == committedEndOffset &&
          tierPartitionState.numSegments > minNumSegments
      }
    }, s"timeout waiting for at least $minNumSegments to be archived and materialized", 60000L)
  }

  /**
    * Delete old (tiered) segments on all brokers.
    */
  private def simulateRetention(): Unit = {
    topicPartitions.foreach(tp => {
      val leaderId = getLeaderForTopicPartition(tp)
      val server = serverForId(leaderId)
      val numDeleted = server.get.replicaManager.logManager.getLog(tp).get.deleteOldSegments()
      Assert.assertTrue("tiered segments should have been deleted", numDeleted > 0)
    })
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
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "foo")
    consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "foo")

    val consumer = new KafkaConsumer[String, String](consumerProps)

    try {
      val partitions = new java.util.ArrayList[TopicPartition]()
      partitions.add(topicPartition)
      partitions.add(new TopicPartition(topic, 1))
      consumer.assign(partitions)
      consumer.seekToBeginning(partitions)
      val valuesRead = new java.util.ArrayList[Int]()
      // expect half of the batches to be aborted, so stop reading when we've approximately halfway through
      while (valuesRead.size() < ((nBatches * recordsPerBatch) / 2) - recordsPerBatch) {
        val records = consumer.poll(Duration.ofMillis(1000))
        records.forEach(new Consumer[ConsumerRecord[String, String]]() {
          override def accept(r: ConsumerRecord[String, String]): Unit = {
            Assert.assertNotEquals("did not expect to find any aborted records", r.key(), "aborted")
            valuesRead.add(Integer.parseInt(r.value()))
          }
        })
      }

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

    Assert.assertTrue("tier fetch metric shows no data fetched from tiered storage",
      bytesFetchedTotal > 0)

    val List(meanArchiveRate) = mBeanServer
      .getAttributes(new ObjectName("kafka.tier.tasks.archive:type=TierArchiver,name=BytesPerSec"), Array("MeanRate"))
      .asList.asScala
      .map { attr => attr.getValue.asInstanceOf[Double] }
      .toList

    Assert.assertTrue("tier archiver mean rate shows no data uploaded to tiered storage",
      meanArchiveRate > 0)
  }
}

