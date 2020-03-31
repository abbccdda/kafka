package unit.kafka.server

import kafka.server.{KafkaConfig, QuotaFactory, QuotaType, ReplicationQuotaManagerConfig}
import kafka.utils.TestUtils
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
    assertEquals(ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault, config.quotaBytesPerSecond)
  }
}
