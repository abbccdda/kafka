/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.link

import java.util.Properties

import kafka.log.AbstractLog
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.apache.kafka.test.IntegrationTest
import org.junit.{Before, Test}
import org.junit.experimental.categories.Category

import scala.jdk.CollectionConverters._

@Category(Array(classOf[IntegrationTest]))
class ClusterLinkTierIntegrationTest extends AbstractClusterLinkIntegrationTest {

  @Before
  override def setUp(): Unit = {
    // Set up will be done within each test after setting up cluster configs
  }

  @Test
  def testMirroringWithTieringEnabledOnSource(): Unit = {
    enableTiering(sourceCluster, "sourceBucket")
    super.setUp()
    verifyMirroringWithTieringEnabled(sourceTopicProps, new Properties)
  }

  @Test
  def testMirroringWithTieringEnabledOnSourceAndDest(): Unit = {
    enableTiering(sourceCluster, "sourceBucket")
    enableTiering(destCluster, "destBucket")
    super.setUp()
    verifyMirroringWithTieringEnabled(sourceTopicProps, tierProps)
  }

  private def verifyMirroringWithTieringEnabled(sourceTopicProps: Properties, destTopicProps: Properties): Unit = {
    numPartitions = 2
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2, sourceTopicProps)
    val producer = sourceCluster.createProducer()
    produceAndSimulateRetention(producer)
    consume(sourceCluster, topic) // verify that source consumers are able to consume all records

    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName, destTopicProps.asScala)
    waitForMirror(topic)

    produceAndSimulateRetention(producer)
    verifyMirror(topic)
  }

  private def enableTiering(cluster: ClusterLinkTestHarness, bucket: String): Unit = {
    cluster.serverConfig.put(KafkaConfig.TierPartitionStateCommitIntervalProp, "5")
    cluster.serverConfig.put(KafkaConfig.TierEnableProp, "false")
    cluster.serverConfig.put(KafkaConfig.TierFeatureProp, "true")
    cluster.serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
    cluster.serverConfig.put(KafkaConfig.TierMetadataReplicationFactorProp, "2")
    cluster.serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
    cluster.serverConfig.put(KafkaConfig.TierFetcherMemoryPoolSizeBytesProp, (1024 * 1024).toString)
    cluster.serverConfig.put(KafkaConfig.TierBackendProp, "mock")
    cluster.serverConfig.put(KafkaConfig.TierS3BucketProp, bucket)
    cluster.serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, Int.MaxValue.toString)
  }

  private def sourceTopicProps: Properties = {
    val props = new Properties
    props.putAll(tierProps)
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "2000")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    props
  }

  private def tierProps: Properties = {
    val props = new Properties
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "1000")
    props
  }

  private def produceAndSimulateRetention(producer: KafkaProducer[Array[Byte], Array[Byte]]): Unit = {
    val minTiered = partitions.map { tp =>
      tp -> (leaderLog(sourceCluster, tp).tierPartitionState.numSegments + 3)
    }.toMap
    (0 until 10).foreach { _ => produceRecords(producer, topic, 50) }
    waitUntilSegmentsTiered(sourceCluster, minTiered)
    simulateRetention(sourceCluster)
  }

  private def waitUntilSegmentsTiered(cluster: ClusterLinkTestHarness, minNumSegments: Map[TopicPartition, Int]): Unit = {
    partitions.foreach { tp =>
      val tierState = leaderLog(cluster, tp).tierPartitionState
      val minSegments = minNumSegments(tp)

      TestUtils.waitUntilTrue(() => {
        tierState.numSegments >= minSegments && tierState.endOffset == tierState.committedEndOffset},
        s"Timed out waiting for $minNumSegments to be archived and materialized", 30000L)
    }
  }

  private def simulateRetention(cluster: ClusterLinkTestHarness): Unit = {
    partitions.foreach { tp =>
      TestUtils.waitUntilTrue(() => leaderLog(cluster, tp).deleteOldSegments() > 0,
        "tiered segments should have been deleted")
    }
  }

  private def leaderLog(cluster: ClusterLinkTestHarness, tp: TopicPartition): AbstractLog = {
    cluster.partitionLeader(tp).logManager.getLog(tp).get
  }
}