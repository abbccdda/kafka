/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.link

import java.nio.ByteBuffer
import java.util.Properties

import kafka.server.KafkaConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.ByteBufferOutputStream
import org.apache.kafka.test.IntegrationTest
import org.junit.experimental.categories.Category
import org.junit.{Before, Test}

@Category(Array(classOf[IntegrationTest]))
class CompactedTopicMirrorTest extends AbstractClusterLinkIntegrationTest {

  // Override setUp to avoid super-class setting up clusters since
  // we set up clusters with test-specific configs later.
  @Before
  override def setUp(): Unit = {
  }

  private def setUpClusters(cleanerInterval: Option[Int] = None): Unit = {
    sourceCluster.producerConfig.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "150")
    cleanerInterval.foreach { intervalMs =>
      sourceCluster.serverConfig.setProperty(KafkaConfig.LogCleanupIntervalMsProp, intervalMs.toString)
      destCluster.serverConfig.setProperty(KafkaConfig.LogCleanupIntervalMsProp, intervalMs.toString)
      destCluster.serverConfig.setProperty(KafkaConfig.LogDeleteDelayMsProp, intervalMs.toString)
    }
    super.setUp()

    numPartitions = 2
    val topicProps = new Properties
    topicProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    topicProps.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1000")
    topicProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "5")
    topicProps.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "2000")
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2, topicProps)
  }

  @Test
  def testCompactedTopicMirror(): Unit = {
    setUpClusters()

    produceToSourceCluster(10)

    def appendRecords(offsetInterval: Long): Unit = {
      (0 until numPartitions).foreach { partition =>
        val startOffset = nextOffset(partition)
        (1 to 10).foreach { i =>
          appendAtOffset(partition, startOffset + i * offsetInterval + partition)
        }
      }
    }

    appendRecords(100)
    appendRecords(Int.MaxValue * 2L) // test segment with > 2 billion offset diff

    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    waitForMirror(topic)

    appendRecords(10)
    verifyMirror(topic)
  }

  @Test
  def testCompactedTopicMirrorWithLogCleaning(): Unit = {
    setUpClusters(cleanerInterval = Some(5))
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)

    val producer = sourceCluster.createProducer()
    produceRecords(producer, topic, 500, index => s"key ${index % 5}")
    waitForLogCleaning(sourceCluster, 400)

    waitForMirror(topic)
    waitForLogCleaning(destCluster, 400)
  }

  private def waitForLogCleaning(cluster: ClusterLinkTestHarness, offset: Long): Unit = {
    partitions.foreach { tp =>
      cluster.partitionLeader(tp).logManager.cleaner.awaitCleaned(tp, offset, maxWaitMs = 15000)
    }
  }

  private def appendAtOffset(partition: Int, offset: Long): Unit = {
    val buf = ByteBuffer.allocate(1024)
    val bytesOut = new ByteBufferOutputStream(buf)
    val nowMs = System.currentTimeMillis
    val builder = new MemoryRecordsBuilder(bytesOut, RecordBatch.CURRENT_MAGIC_VALUE,
      CompressionType.NONE, TimestampType.CREATE_TIME, offset,
      nowMs, 0L, 0.asInstanceOf[Short], 0, false, false, 0, buf.capacity)
    val key = s"key-$offset".getBytes
    val value = s"value-$offset".getBytes
    builder.append(nowMs, key, value)
    val records = builder.build()

    // Appending as follower to explicitly set offset of records
    val tp = new TopicPartition(topic, partition)
    val leaderLog = sourceCluster.partitionLeader(tp).logManager.getLog(tp).get
    leaderLog.appendAsFollower(records)
    producedRecords += SourceRecord(topic, partition, key, value, offset)
  }
}