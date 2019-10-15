/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.junit.Test
import org.scalatest.Assertions.assertThrows

class S3TierObjectStoreConfigTest {

  @Test
  def testValidConfigNoAuth(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    props.put(KafkaConfig.TierS3BucketProp, "bucket")
    props.put(KafkaConfig.TierS3RegionProp, "region")
    val kafkaConfig = KafkaConfig.fromProps(props)
    val s3Config = new S3TierObjectStoreConfig("clusterid", kafkaConfig)
    assert(!s3Config.s3AwsAccessKeyId.isPresent)
    assert(!s3Config.s3AwsSecretAccessKey.isPresent)
  }

  @Test
  def testValidConfigWithAuth(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    props.put(KafkaConfig.TierS3BucketProp, "bucket")
    props.put(KafkaConfig.TierS3RegionProp, "region")
    props.put(KafkaConfig.TierS3AwsSecretAccessKeyProp, "secret")
    props.put(KafkaConfig.TierS3AwsAccessKeyIdProp, "keyid")
    val kafkaConfig = KafkaConfig.fromProps(props)
    val s3Config = new S3TierObjectStoreConfig("clusterid", kafkaConfig)
    assert(s3Config.s3AwsAccessKeyId.isPresent)
    assert(s3Config.s3AwsSecretAccessKey.isPresent)
  }

  @Test
  def testValidConfigWithEndpoint(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    props.put(KafkaConfig.TierS3BucketProp, "bucket")
    props.put(KafkaConfig.TierS3RegionProp, "region")
    props.put(KafkaConfig.TierS3EndpointOverrideProp, "endpoint")
    val kafkaConfig = KafkaConfig.fromProps(props)
    val s3Config = new S3TierObjectStoreConfig("clusterid", kafkaConfig)
    assert(s3Config.s3EndpointOverride.isPresent)
  }

  @Test
  def testInvalidConfigNoRegion(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    props.put(KafkaConfig.TierS3BucketProp, "bucket")
    val kafkaConfig = KafkaConfig.fromProps(props)
    assertThrows[IllegalArgumentException]{new S3TierObjectStoreConfig("clusterid", kafkaConfig)}
  }

  @Test
  def testInvalidConfigNoKeyID(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    props.put(KafkaConfig.TierS3BucketProp, "bucket")
    props.put(KafkaConfig.TierS3RegionProp, "region")
    props.put(KafkaConfig.TierS3AwsSecretAccessKeyProp, "secret")
    val kafkaConfig = KafkaConfig.fromProps(props)
    assertThrows[IllegalArgumentException]{new S3TierObjectStoreConfig("clusterid", kafkaConfig)}
  }
}
