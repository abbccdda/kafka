/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.lang.management.ManagementFactory
import java.util.Properties

import javax.management.ObjectName
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.internals.Topic
import org.junit.Test
import org.junit.Assert.assertTrue

import scala.collection.JavaConverters._

class TierIntegrationTopicMangerSoftFailureTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 1
  serverConfig.put(KafkaConfig.TierPartitionStateCommitIntervalProp, "5")
  serverConfig.put(KafkaConfig.TierBackendProp, "mock")
  serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  serverConfig.put(KafkaConfig.TierEnableProp, "false")
  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
  serverConfig.put(KafkaConfig.TierMetadataReplicationFactorProp, "1")
  serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, Int.MaxValue.toString) // disable log cleanup, we will manually trigger retention
  serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, "10")

  @Test
  def testSoftFailure(): Unit = {
    TestUtils.waitUntilTrue(() => servers.head.tierTopicManager.isReady, s"timeout waiting for TierTopicManager ready")

    val properties = new Properties()
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "__kafka.tiertopicmanager")
    val producer = createProducer(configOverrides = properties)
    try {
      val keyBytes = new Array[Byte](Byte.MaxValue) // bad payload
      val valBytes = new Array[Byte](33)
      // force a deserialization error in the TierTopicManager
      producer.send(new ProducerRecord[Array[Byte],Array[Byte]](Topic.TIER_TOPIC_NAME, 0, 0L, keyBytes, valBytes)).get()
    }
    finally {
      producer.close()
    }

    TestUtils.waitUntilTrue(() => !servers.head.tierTopicManager.isReady, s"timeout waiting for TierTopicManager to no longer be ready to uncaught exception")

    val mBeanServer = ManagementFactory.getPlatformMBeanServer
    val List(retentionCountAfterNotReady) = mBeanServer
      .getAttributes(new ObjectName("kafka.tier.retention:type=TierRetentionManager,name=CyclesPerSec"), Array("Count"))
      .asList.asScala
      .map { attr => attr.getValue.asInstanceOf[Long] }
      .toList

    assertTrue( "retention thread should have checked at least once", retentionCountAfterNotReady > 0)

    val List(archiverCountAfterNotReady) = mBeanServer
      .getAttributes(new ObjectName("kafka.tier.archiver:type=TierArchiver,name=CyclesPerSec"), Array("Count"))
      .asList.asScala
      .map { attr => attr.getValue.asInstanceOf[Long] }
      .toList

    assertTrue("archiver thread should have checked at least once", retentionCountAfterNotReady > 0)

    // sleep for a while to test that retention no longer cycles
    Thread.sleep(1000)

    val List(latestArchiverCount) = mBeanServer
      .getAttributes(new ObjectName("kafka.tier.archiver:type=TierArchiver,name=CyclesPerSec"), Array("Count"))
      .asList.asScala
      .map { attr => attr.getValue.asInstanceOf[Long] }
      .toList

    assertTrue(latestArchiverCount == archiverCountAfterNotReady)

    val List(latestRetentionCount) = mBeanServer
      .getAttributes(new ObjectName("kafka.tier.retention:type=TierRetentionManager,name=CyclesPerSec"), Array("Count"))
      .asList.asScala
      .map { attr => attr.getValue.asInstanceOf[Long] }
      .toList

    assertTrue(latestRetentionCount == retentionCountAfterNotReady)
  }
}
