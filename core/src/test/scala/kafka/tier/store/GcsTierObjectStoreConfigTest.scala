/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.Test
import org.scalatest.Assertions.assertThrows

class GcsTierObjectStoreConfigTest {

  @Test
  def testValidConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    props.put(KafkaConfig.TierGcsBucketProp, "bucket")
    props.put(KafkaConfig.TierGcsRegionProp, "region")
    props.put(KafkaConfig.TierGcsReadChunkSizeProp, "10000")
    props.put(KafkaConfig.TierGcsWriteChunkSizeProp, "10000")
    val kafkaConfig = KafkaConfig.fromProps(props)
    new GcsTierObjectStoreConfig("clusterid", kafkaConfig)
  }

  @Test
  def testInvalidConfigNoRegion(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    props.put(KafkaConfig.TierGcsBucketProp, "bucket")
    props.put(KafkaConfig.TierGcsReadChunkSizeProp, "10000")
    props.put(KafkaConfig.TierGcsWriteChunkSizeProp, "10000")
    val kafkaConfig = KafkaConfig.fromProps(props)
    assertThrows[IllegalArgumentException]{new GcsTierObjectStoreConfig("clusterid", kafkaConfig)}
  }

  @Test
  def testInvalidConfigNoBucket(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    props.put(KafkaConfig.TierGcsRegionProp, "region")
    props.put(KafkaConfig.TierGcsReadChunkSizeProp, "10000")
    props.put(KafkaConfig.TierGcsWriteChunkSizeProp, "10000")
    val kafkaConfig = KafkaConfig.fromProps(props)
    assertThrows[IllegalArgumentException]{new GcsTierObjectStoreConfig("clusterid", kafkaConfig)}
  }
}
