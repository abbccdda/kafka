/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.link

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutionException, TimeUnit}

import kafka.log.LogConfig
import kafka.server.link.ClusterLinkConfig
import kafka.server.{ConfigType, DynamicConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{Config => _, _}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.test.IntegrationTest
import org.junit.Assert._
import org.junit.experimental.categories.Category
import org.junit.Test
import org.scalatest.Assertions.intercept

import scala.annotation.nowarn
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

@Category(Array(classOf[IntegrationTest]))
class ClusterLinkIntegrationTest extends AbstractClusterLinkIntegrationTest {

  val offsetToCommit = 10L
  val syncPeriod = 10000L
  val consumerGroupFilter =
    """
      |{
      |"groupFilters": [
      |  {
      |     "name": "testGroup",
      |     "patternType": "literal",
      |     "filterType": "whitelist"
      |  }
      |]}
      |""".stripMargin
  val multiConsumerGroupFilter =
    """
      |{
      |"groupFilters": [
      |  {
      |     "name": "testGroup",
      |     "patternType": "literal",
      |     "filterType": "whitelist"
      |  },
      |  {
      |     "name": "testGroup2",
      |     "patternType": "literal",
      |     "filterType": "whitelist"
      |  }
      |]}
      |""".stripMargin

  /**
    * Verifies topic mirroring when mirroring is set up on a source topic that is empty.
    */
  @Test
  def testMirrorNewRecords(): Unit = {
    val numRecords = 20
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    val linkId = destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)

    produceToSourceCluster(numRecords)
    consume(sourceCluster, topic)

    verifyMirror(topic)

    val jaasConfig = destCluster.adminZkClient.fetchClusterLinkConfig(linkId).getProperty(SaslConfigs.SASL_JAAS_CONFIG)
    assertNotNull(jaasConfig)
    assertFalse(s"Password not encrypted: $jaasConfig", jaasConfig.contains("secret-"))

    destCluster.deleteClusterLink(linkName)
  }

  /**
    * Verifies topic mirroring when mirroring is set up on a source topic that contains records.
    */
  @Test
  def testMirrorExistingRecords(): Unit = {
    val numRecords = 20
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    produceToSourceCluster(numRecords)

    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)

    waitForMirror(topic)
    verifyLinkMetrics()
    verifyMirror(topic)
  }

  /**
    * Verifies topic mirroring with source leader changes. Verifies that truncation is performed
    * for unclean leader election.
    */
  @Test
  def testSourceLeaderChanges(): Unit = {
    numPartitions = 1
    val tp = partitions.head
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Produce before and after source epoch change
    produceToSourceCluster(2)
    sourceCluster.bounceLeader(tp)
    produceToSourceCluster(2)

    // Create a topic mirror when source epoch > 0 and verify that records with different epochs are mirrored
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    var destLeaderEpoch = destCluster.waitForLeaderEpochChange(tp, 0, sourceCluster.leaderEpoch(tp))
    waitForMirror(topic)
    produceToSourceCluster(2)
    waitForMirror(topic)

    // Shutdown leader and verify clean leader election. No truncation is expected.
    val (leader1, _) = sourceCluster.shutdownLeader(tp)
    produceToSourceCluster(2)
    destLeaderEpoch = destCluster.waitForLeaderEpochChange(tp, destLeaderEpoch, sourceCluster.leaderEpoch(tp))
    waitForMirror(topic)

    // Trigger unclean leader election in the source cluster and ensure truncation is performed
    // on the leader as well as follower in the destination cluster
    val (leader2, _) = sourceCluster.shutdownLeader(tp)
    sourceCluster.startBroker(leader1)
    truncate(producedRecords, 2)
    produceToSourceCluster(4)
    val (endOffset, _) = TestUtils.computeUntilTrue(logEndOffset(sourceCluster.servers(leader1), tp).get)(_ >= producedRecords.size)
    assertEquals(producedRecords.size, endOffset)
    consume(sourceCluster, topic)
    verifyMirror(topic)
  }

  /**
    * Verifies topic mirroring with destination leader changes. Verifies that truncation is not
    * performed for clean or unclean leader elections.
    */
  @Test
  def testDestLeaderChanges(): Unit = {
    numPartitions = 1
    val tp = partitions.head
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Create a mirror and produce some records.
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    produceToSourceCluster(2)
    waitForMirror(topic)

    // Shutdown destination leader and verify clean leader election. No truncation is expected.
    val (leader1, _) = destCluster.shutdownLeader(tp)
    produceToSourceCluster(2)
    waitForMirror(topic, servers = destCluster.servers.filter(_ != destCluster.servers(leader1)))

    // Trigger unclean leader election in the destination cluster. No truncation is expected.
    // Produce records and ensure that all records are replicated in destination leader and follower
    val (leader2, _) = destCluster.shutdownLeader(tp)
    destCluster.startBroker(leader1)
    produceToSourceCluster(2)
    waitForMirror(topic, servers = destCluster.servers.filter(_ != destCluster.servers(leader2)))
    destCluster.servers(leader2).startup()
    produceToSourceCluster(2)
    verifyMirror(topic)
  }

  /**
    * Scenario:
    *   destBroker1 shutdown
    *   Source cluster: (epoch=0, 0-99) (epoch=1, 100-199) (epoch=2, 200-299) (epoch=3, 300-399)
    *   sourceBroker1 shutdown
    *   sourceBroker2 produces: (epoch=4, 400-499) with only one replica
    *   destBroker2 mirrors: (epoch=0, 0-99) (epoch=1, 100-199) (epoch=2, 200-299) (epoch=3, 300-399) (epoch=4, 400-499)
    *   destBroker2 shutdown
    *   sourceBroker2 shutdown
    *   sourceBroker1 starts up becomes leader, (epoch=4, 400-499) needs truncation in followers
    *   destBroker1 starts up and becomes leader with no records yet and hence no truncation
    *   destBroker2 starts up, becomes follower, gets offsets from destBroker1 and performs truncation
    *   Source cluster: (epoch=0, 0-99) (epoch=1, 100-199) (epoch=2, 200-299) (epoch=3, 300-399) (epoch=6 400-499)
    *   Wait for mirror and verify records in destBroker1 and destBroker2
    */
  @Test
  def testDestFollowerAheadOfLeader(): Unit = {
    numPartitions = 1
    val tp = partitions.head
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)

    val (destBroker1, _) = destCluster.shutdownLeader(tp)
    val destBroker2 = TestUtils.waitUntilLeaderIsElectedOrChanged(destCluster.zkClient, topic, 0, oldLeaderOpt = Some(destBroker1))

    // Produce records with multiple source epochs and wait for destBroker2 to mirror them. The last
    // batch is produced with a single leader and will be truncated due to unclean leader election later.
    produceToSourceCluster(100)
    (0 until 3).foreach { i =>
      sourceCluster.bounceLeader(tp)
      produceToSourceCluster(100)
    }
    val (sourceBroker1, _) = sourceCluster.shutdownLeader(tp)
    produceToSourceCluster(100)
    waitForMirror(topic, Seq(destCluster.servers(destBroker2)))

    // Shutdown destination destBroker2 and trigger unclean leader election in the source cluster
    destCluster.shutdownLeader(tp)
    val (sourceBroker2, _) = sourceCluster.shutdownLeader(tp)
    truncate(producedRecords, 100)
    sourceCluster.startBroker(sourceBroker1)

    // Startup destBroker1 so that destBroker1 with no records becomes the new leader
    destCluster.startBroker(destBroker1)
    val newLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(destCluster.zkClient, topic, 0, oldLeaderOpt = Some(destBroker2))
    assertEquals(destBroker1, newLeader)

    // Restart destBroker2 which was ahead of destBroker1 and verify the mirrored records on leader and follower
    destCluster.startBroker(destBroker2)
    produceToSourceCluster(100)
    verifyMirror(topic)
  }

  @Test
  def testSourceClusterQuota(): Unit = {
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    destCluster.createClusterLink(linkName, sourceCluster, fetchMaxBytes = 100)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    val sourceAdmin = sourceCluster.createAdminClient()

    def setQuota(byteRate: Long): Unit = {
      val quotaUser = new ClientQuotaEntity(Map(ClientQuotaEntity.USER -> destCluster.linkUserName(linkName)).asJava)
      val quotaOp = new ClientQuotaAlteration.Op(DynamicConfig.Client.ConsumerByteRateOverrideProp, byteRate)
      val quota = new ClientQuotaAlteration(quotaUser, Collections.singleton(quotaOp))
      sourceAdmin.alterClientQuotas(Collections.singleton(quota)).all().get(15, TimeUnit.SECONDS)
    }

    def throttled(): Boolean = {
      destCluster.servers.exists { server =>
        kafkaMetricMaxValue(server, "fetch-throttle-time-max", "cluster-link") > 0.0
      }
    }

    verifyQuota(setQuota, throttled, "Source cluster link user quota")
  }

  @Test
  def testDestinationClusterQuota(): Unit = {
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    destCluster.createClusterLink(linkName, sourceCluster)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    val destAdmin = destCluster.createAdminClient()

    def setQuota(byteRate: Long): Unit = {
      val alterOp = new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.ClusterLinkIoMaxBytesPerSecondProp, byteRate.toString), AlterConfigOp.OpType.SET)
      val configs = destCluster.servers.map(_.config.brokerId)
        .map(brokerId => new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString))
        .map(_ -> Set(alterOp).asJavaCollection).toMap.asJava
      destAdmin.incrementalAlterConfigs(configs).all().get()
    }

    def throttled(): Boolean = {
      destCluster.servers.exists { server =>
        yammerMetricMaxValue(s"kafka.server:type=ReplicaManager,name=ThrottledClusterLinkReplicasPerSec", linkOpt = None) > 0.0
      }
    }

    verifyQuota(setQuota, throttled, "Destination cluster link replication quota")
  }

  @Test
  def testAddPartitions(): Unit = {
    numPartitions = 1
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Create a mirror and produce some records.
    destCluster.createClusterLink(linkName, sourceCluster, metadataMaxAgeMs = 1000L)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    produceToSourceCluster(4)
    waitForMirror(topic)

    numPartitions = 4
    sourceCluster.createPartitions(topic, numPartitions)
    produceToSourceCluster(8)

    val (numDestPartitions, _) = TestUtils.computeUntilTrue(destCluster.describeTopic(topic).partitions.size) {
      _ == numPartitions
    }
    assertEquals(numPartitions, numDestPartitions)

    produceToSourceCluster(8)
    verifyMirror(topic)
  }

  @Test
  def testAlterClusterLinkConfigs(): Unit = {
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Create a mirror and produce some records.
    destCluster.createClusterLink(linkName, sourceCluster, metadataMaxAgeMs = 10000L)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    produceToSourceCluster(8)
    waitForMirror(topic)

    // Update non-critical non-dynamic config metadata.max.age.ms
    val metadataMaxAge = "60000"
    destCluster.alterClusterLink(linkName, Map(CommonClientConfigs.METADATA_MAX_AGE_CONFIG -> metadataMaxAge))
    produceToSourceCluster(8)
    waitForMirror(topic)

    // Verify the update.
    assertEquals(metadataMaxAge, destCluster.describeClusterLink(linkName).get(CommonClientConfigs.METADATA_MAX_AGE_CONFIG).value)

    // Update critical non-dynamic config bootstrap.servers. Restart source brokers to ensure
    // new bootstrap servers are required for the test to pass.
    sourceCluster.servers.foreach(_.shutdown())
    sourceCluster.servers.foreach(_.startup())
    sourceCluster.updateBootstrapServers()
    destCluster.alterClusterLink(linkName, Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> sourceCluster.brokerList))
    produceToSourceCluster(8)
    waitForMirror(topic)

    // Update critical dynamic truststore path config
    val oldFile = new File(destCluster.describeClusterLink(linkName).get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG).value)
    val newFile = File.createTempFile("truststore", ".jks")
    Files.copy(oldFile.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    destCluster.alterClusterLink(linkName, Map(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> newFile.getAbsolutePath))
    produceToSourceCluster(8)
    waitForMirror(topic)
    verifyMirror(topic)
  }

  @Test
  def testSourceTopicDelete(): Unit = {
    val numRecords = 10
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    destCluster.createClusterLink(linkName, sourceCluster, retryTimeoutMs = 10000)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)

    produceToSourceCluster(numRecords)
    waitForMirror(topic)
    assertTrue(destCluster.topicLinkState(topic).state.shouldSync)
    sourceCluster.deleteTopic(topic)
    TestUtils.waitUntilTrue(() => !destCluster.topicLinkState(topic).state.shouldSync,
      "Source topic deletion not propagated", waitTimeMs = 20000)

    val topicProps = new Properties
    topicProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100000")
    sourceCluster.createTopic(topic, 1, replicationFactor = 2, topicProps)
    produceToSourceCluster(numRecords)
    truncate(producedRecords, numRecords)

    // Verify that partitions and configs of new source topic are not sync'ed
    assertEquals(numPartitions, destCluster.describeTopic(topic).partitions().size())

    val maxMessageSize = Option(destCluster.describeTopicConfig(topic).get(TopicConfig.MAX_MESSAGE_BYTES_CONFIG))
    assertTrue(maxMessageSize.nonEmpty)
    assertNotEquals("100000", maxMessageSize.get.value)
    verifyMirror(topic)
  }

  /**
   * Verifies offset migration for a for 2 consumer groups added progressively
   */
  @Test
  def testOffsetMigrationWithAddedConsumerGroup(): Unit = {
    val finalOffset = 20L
    val additionalConsumerGroup = "testGroup2"

    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    val linkProps = new Properties()
    linkProps.setProperty(ClusterLinkConfig.ConsumerOffsetSyncEnableProp, "true")
    linkProps.setProperty(ClusterLinkConfig.ConsumerOffsetGroupFiltersProp, consumerGroupFilter)
    linkProps.setProperty(ClusterLinkConfig.ConsumerOffsetSyncMsProp, String.valueOf(syncPeriod))
    destCluster.createClusterLink(linkName, sourceCluster, configOverrides = linkProps)
    destCluster.linkTopic(topic, 2, linkName)

    commitOffsets(sourceCluster, topic, offsetToCommit, consumerGroupFilter)

    verifyOffsetMigration(topic, offsetToCommit, syncPeriod * 4, consumerGroupFilter)

    val updatedProps = Map[String,String] (
      ClusterLinkConfig.ConsumerOffsetSyncEnableProp -> "true",
      ClusterLinkConfig.ConsumerOffsetGroupFiltersProp -> multiConsumerGroupFilter,
      ClusterLinkConfig.ConsumerOffsetSyncMsProp -> String.valueOf(syncPeriod))
    destCluster.alterClusterLink(linkName, updatedProps)

    commitOffsets(sourceCluster, topic, finalOffset, consumerGroupFilter)
    commitOffsets(sourceCluster, topic, finalOffset, additionalConsumerGroup)

    verifyOffsetMigration(topic, finalOffset, syncPeriod * 4, consumerGroupFilter)
    verifyOffsetMigration(topic, finalOffset, syncPeriod * 4, additionalConsumerGroup)

    destCluster.unlinkTopic(topic, linkName)
    destCluster.deleteClusterLink(linkName)
  }

  /**
   * Verifies offset migration for 2 linked topics added progressively
   */
  @Test
  def testOffsetMigrationWithAddedTopic(): Unit = {
    val finalOffset = 20L
    val additionalTopic = "linkedTopic2"

    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
    sourceCluster.createTopic(additionalTopic, numPartitions, replicationFactor = 2)

    val linkProps = new Properties()
    linkProps.setProperty(ClusterLinkConfig.ConsumerOffsetSyncEnableProp, "true")
    linkProps.setProperty(ClusterLinkConfig.ConsumerOffsetGroupFiltersProp, consumerGroupFilter)
    linkProps.setProperty(ClusterLinkConfig.ConsumerOffsetSyncMsProp, String.valueOf(syncPeriod))
    destCluster.createClusterLink(linkName, sourceCluster, configOverrides = linkProps)
    destCluster.linkTopic(topic, 2, linkName)

    commitOffsets(sourceCluster, topic, offsetToCommit, consumerGroupFilter)

    verifyOffsetMigration(topic, offsetToCommit, syncPeriod * 4, consumerGroupFilter)

    destCluster.linkTopic(additionalTopic, 2, linkName)

    commitOffsets(sourceCluster, topic, finalOffset, consumerGroupFilter)
    commitOffsets(sourceCluster, additionalTopic, finalOffset, consumerGroupFilter)

    verifyOffsetMigration(topic, finalOffset, syncPeriod * 4, consumerGroupFilter)
    verifyOffsetMigration(additionalTopic, finalOffset, syncPeriod * 4, consumerGroupFilter)

    destCluster.unlinkTopic(topic, linkName, false)
    destCluster.unlinkTopic(additionalTopic, linkName)
    destCluster.deleteClusterLink(linkName)
  }

  @nowarn("cat=deprecation")
  @Test
  def testDestReadOnly(): Unit = {
    sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)

    // Create a mirror and produce some records.
    destCluster.createClusterLink(linkName, sourceCluster, metadataMaxAgeMs = 10000L)
    destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    produceToSourceCluster(4)
    waitForMirror(topic)

    // Attempt to produce to the mirror.
    val producer = destCluster.createProducer()
    try {
      producer.send(new ProducerRecord(topic, 0, 0, "key".getBytes, "value".getBytes)).get(15, TimeUnit.SECONDS)
    } catch {
      case e: ExecutionException =>
        assertTrue(e.getCause.isInstanceOf[InvalidRequestException])
    } finally {
      producer.close()
    }

    // Attempt to increase the partitions of the mirror, which should fail as an invalid request.
    intercept[InvalidPartitionsException] {
      destCluster.createPartitions(topic, 8)
    }

    destCluster.withAdmin((admin: ConfluentAdmin) => {
      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)

      try {
        // Attempt to alter the topic's configuration via alterTopics(), which is disallowed.
        admin.alterConfigs(Map(resource -> new Config(List.empty.asJavaCollection)).asJava).all().get(20, TimeUnit.SECONDS)
        fail("alterConfigs() on a mirror topic should fail")
      } catch {
        case e: ExecutionException => assertTrue(e.getCause.isInstanceOf[InvalidRequestException])
      }

      // Attempt to alter the topic's configuration, verifying only the mutable parameters can be modified.
      val alterations = Seq(
        LogConfig.UncleanLeaderElectionEnableProp -> Some("true"),
        LogConfig.UncleanLeaderElectionEnableProp -> None,
        LogConfig.CleanupPolicyProp -> Some("compact"),
        LogConfig.CleanupPolicyProp -> None,
      )
      alterations.foreach { case (name, value) =>
        val expectSuccess = (name == LogConfig.UncleanLeaderElectionEnableProp)
        val op = value match {
          case Some(v) => new AlterConfigOp(new ConfigEntry(name, v), AlterConfigOp.OpType.SET)
          case None => new AlterConfigOp(new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE)
        }
        try {
          val ops = Collections.singleton(op).asInstanceOf[util.Collection[AlterConfigOp]]
          admin.incrementalAlterConfigs(Map(resource -> ops).asJava).all.get
          assertTrue(expectSuccess)
        } catch {
          case e: ExecutionException =>
            assertTrue(e.getCause.isInstanceOf[InvalidConfigurationException])
            assertFalse(expectSuccess)
        }
      }
    })

    // Produce more records to the source and verify we see no additional records.
    produceToSourceCluster(4)
    waitForMirror(topic)

    destCluster.unlinkTopic(topic, linkName, false)
    destCluster.deleteClusterLink(linkName)
  }

  @Test
  def testDeleteClusterLinkCleanup(): Unit = {
    val linkId = destCluster.createClusterLink(linkName, sourceCluster, metadataMaxAgeMs = 10000L)

    val topics = { for (idx <- 0 until 5) yield s"topic-$idx" }.toSet
    topics.foreach { topic =>
      sourceCluster.createTopic(topic, numPartitions, replicationFactor = 2)
      destCluster.linkTopic(topic, replicationFactor = 2, linkName)
    }
    assertEquals(topics.size, destCluster.zkClient.getClusterLinkForTopics(topics).size)

    intercept[ClusterLinkInUseException] {
      destCluster.deleteClusterLink(linkName)
    }
    destCluster.deleteClusterLink(linkName, force = true)
    assertTrue(destCluster.zkClient.getClusterLinkForTopics(topics).isEmpty)
    assertTrue(destCluster.zkClient.getEntityConfigs(ConfigType.ClusterLink, linkId.toString).isEmpty)
  }

  private def verifyOffsetMigration(topic: String,offset:Long,timeout: Long, consumerGroup: String): Unit = {

    destCluster.withAdmin((adminClient: ConfluentAdmin) => {
      val (actualOffset, foundOffset) = TestUtils.computeUntilTrue(adminClient.listConsumerGroupOffsets(consumerGroup).partitionsToOffsetAndMetadata.get
        .getOrDefault(new TopicPartition(topic, 0), new OffsetAndMetadata(0, "")).offset(), timeout)(_ != offset)
      assertTrue("expected offset: " + offset + " and got offset: " + actualOffset + " for topic: " + topic + " and group " + consumerGroup, foundOffset)
    })
  }

  private def truncate(records: mutable.Buffer[ProducerRecord[Array[Byte], Array[Byte]]], numRecords: Int): Unit = {
    records.remove(records.size - numRecords, numRecords)
  }

  private def verifyLinkMetrics(): Unit = {

    def verifyKafkaMetric(name: String, group: String, expectNonZero: Boolean = true): Unit = {
     val maxValue = kafkaMetricMaxValue(destCluster.servers.head, name, group)
      if (expectNonZero)
        assertTrue(s"Metric not updated: $group:$name $maxValue", maxValue > 0.0)
    }

    def verifyYammerMetric(prefix: String, expectNonZero: Boolean = true): Unit = {
      val maxValue = yammerMetricMaxValue(prefix)
      if (expectNonZero)
        assertTrue(s"Metric not updated: $prefix $maxValue", maxValue > 0.0)
    }

    verifyKafkaMetric("incoming-byte-total", "cluster-link-metadata-metrics")
    verifyKafkaMetric("incoming-byte-total", "cluster-link-fetcher-metrics")
    verifyKafkaMetric("fetch-throttle-time-max", "cluster-link", expectNonZero = false)
    verifyYammerMetric("kafka.server.link:type=ClusterLinkFetcherManager,name=MaxLag", expectNonZero = false)
    verifyYammerMetric("kafka.server:type=FetcherStats,name=BytesPerSec")
    verifyYammerMetric("kafka.server:type=FetcherLagMetrics,name=ConsumerLag", expectNonZero = false)
  }

  private def verifyQuota(setQuota: Long => Unit,
                          checkQuota: () => Boolean,
                          quotaDesc: String): Unit = {

    val producer = sourceCluster.createProducer()
    setQuota(100)
    produceUntil(producer, checkQuota, s"$quotaDesc not applied")

    setQuota(500000)
    produceRecords(producer, topic, 10)
    waitForMirror(topic, maxWaitMs = 30000)
  }
}