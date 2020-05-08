package unit.kafka.server

import kafka.server.{BrokerBackpressureConfig, Defaults, KafkaConfig, QuotaFactory, QuotaType, ReplicationQuotaManagerConfig}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.junit.Test
import org.junit.Assert._

class QuotaFactoryTest {

  @Test
  def testReplicationConfigSetsCorrectRateForLeaderThrottle(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "localhost:2181")
    props.put(KafkaConfig.LeaderReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.AllThrottledReplicasValue)
    props.put(KafkaConfig.FollowerReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.NoThrottledReplicasValue)
    props.put(KafkaConfig.LeaderReplicationThrottledRateProp, "1111")
    props.put(KafkaConfig.FollowerReplicationThrottledRateProp, "2222")

    val config = QuotaFactory.replicationConfig(KafkaConfig.fromProps(props), QuotaType.LeaderReplication)

    assertTrue("Expected all leader replicas to be throttled", config.allReplicasThrottled)
    assertEquals(1111, config.quotaBytesPerSecond)
  }

  @Test
  def testReplicationConfigSetsCorrectRateForFollowerThrottle(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "localhost:2181")
    props.put(KafkaConfig.LeaderReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.NoThrottledReplicasValue)
    props.put(KafkaConfig.FollowerReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.AllThrottledReplicasValue)
    props.put(KafkaConfig.LeaderReplicationThrottledRateProp, "1111")
    props.put(KafkaConfig.FollowerReplicationThrottledRateProp, "2222")

    val config = QuotaFactory.replicationConfig(KafkaConfig.fromProps(props), QuotaType.FollowerReplication)

    assertTrue("Expected all follower replicas to be throttled", config.allReplicasThrottled)
    assertEquals(2222, config.quotaBytesPerSecond)
  }

  @Test
  def testReplicationConfigSetsDefaultForAlterLogDirsReplicationThrottle(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "localhost:2181")
    props.put(KafkaConfig.LeaderReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.NoThrottledReplicasValue)
    props.put(KafkaConfig.FollowerReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.AllThrottledReplicasValue)
    props.put(KafkaConfig.LeaderReplicationThrottledRateProp, "1111")
    props.put(KafkaConfig.FollowerReplicationThrottledRateProp, "2222")

    val config = QuotaFactory.replicationConfig(KafkaConfig.fromProps(props), QuotaType.AlterLogDirsReplication)

    assertFalse("Expected no log dir replicas to be throttled", config.allReplicasThrottled)
    assertEquals(Defaults.QuotaBytesPerSecond, config.quotaBytesPerSecond)
  }

  @Test
  def testClientRequestConfigSetsCorrectBackpressureConfigs(): Unit = {
    val props = TestUtils.createBrokerConfig(0, "localhost:2181")
    props.put(KafkaConfig.QueuedMaxRequestsProp, "500")
    props.put(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG, "request")
    props.put(ConfluentConfigs.MULTITENANT_LISTENER_NAMES_CONFIG, "PLAINTEXT")

    val config = QuotaFactory.clientRequestConfig(KafkaConfig.fromProps(props))

    // backpressure is disabled because there is no tenant callback class
    assertFalse("Expected request backpressure disabled", config.backpressureConfig.backpressureEnabledInConfig)
    assertEquals(Seq("PLAINTEXT"), config.backpressureConfig.tenantEndpointListenerNames)
    assertEquals(500, config.backpressureConfig.maxQueueSize, 0.0)
    assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_DEFAULT.toDouble, config.backpressureConfig.minBrokerRequestQuota, 0.0)
    assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT, config.backpressureConfig.queueSizePercentile)

    // set non-default request backpressure configs
    props.put(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_CONFIG, "100")
    props.put(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_CONFIG, "p99")

    val config2 = QuotaFactory.clientRequestConfig(KafkaConfig.fromProps(props))
    assertEquals(100.0, config2.backpressureConfig.minBrokerRequestQuota, 0.0)
    assertEquals("p99", config2.backpressureConfig.queueSizePercentile)

    // setting an invalid value for min broker quota will set it to BrokerBackpressureConfig.MinBrokerRequestQuota
    // and setting invalid value for queue size percentile will set it back to the default
    props.put(ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_CONFIG, "0")
    props.put(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_CONFIG, "p105")

    val config3 = QuotaFactory.clientRequestConfig(KafkaConfig.fromProps(props))
    assertEquals(BrokerBackpressureConfig.MinBrokerRequestQuota, config3.backpressureConfig.minBrokerRequestQuota, 0.0)
    assertEquals(ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT, config3.backpressureConfig.queueSizePercentile)
  }
}
