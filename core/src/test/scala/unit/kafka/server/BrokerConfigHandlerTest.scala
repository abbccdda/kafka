package unit.kafka.server

import java.util.Properties

import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.{BrokerConfigHandler, KafkaConfig, QuotaFactory, ReplicationQuotaManagerConfig}
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.MockTime

class BrokerConfigHandlerTest {
  val staticLeaderThrottle: Long = 111
  val staticFollowerThrottle: Long = 222
  val tp: TopicPartition = new TopicPartition("tp", 0)
  var configHandler: BrokerConfigHandler = _
  var quotaManagers: QuotaManagers = _

  @Before
  def setUp(): Unit = {
    val props = TestUtils.createBrokerConfigs(1, "127.0.0.1:9999").head
    props.setProperty(KafkaConfig.LeaderReplicationThrottledRateProp, staticLeaderThrottle.toString)
    props.setProperty(KafkaConfig.FollowerReplicationThrottledRateProp, staticFollowerThrottle.toString)
    props.put(KafkaConfig.LeaderReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.AllThrottledReplicasValue)
    props.put(KafkaConfig.FollowerReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.NoThrottledReplicasValue)
    val config = KafkaConfig.fromProps(props)
    quotaManagers = QuotaFactory.instantiate(config, new Metrics(), new MockTime, "")
    configHandler = new BrokerConfigHandler(config, quotaManagers)
  }

  @Test
  def testOverridesReplicationThrottles(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.LeaderReplicationThrottledRateProp, "333")
    props.put(KafkaConfig.FollowerReplicationThrottledRateProp, "444")
    assertEquals(ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault, quotaManagers.alterLogDirs.upperBound())
    configHandler.processConfigChanges("0", props)
    assertEquals(333, quotaManagers.leader.upperBound())
    assertEquals(444, quotaManagers.follower.upperBound())
    assertEquals(ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault, quotaManagers.alterLogDirs.upperBound())
  }

  @Test
  def testOverridesReplicationThrottledReplicas(): Unit = {
    val props = new Properties
    // static settings are: leader - enabled, follower throttling - disabled
    assertTrue(quotaManagers.leader.isThrottled(tp))
    assertFalse(quotaManagers.follower.isThrottled(tp))

    props.put(KafkaConfig.LeaderReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.NoThrottledReplicasValue)
    props.put(KafkaConfig.FollowerReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.AllThrottledReplicasValue)
    configHandler.processConfigChanges("0", props)

    assertFalse("expected leader throttle to be disabled after dynamic reconfiguration",
      quotaManagers.leader.isThrottled(tp))
    assertTrue("expected follower throttle to be enabled after dynamic reconfiguration",
      quotaManagers.follower.isThrottled(tp))
  }

  @Test
  def testRevertsBackToStaticallySetReplicationThrottles(): Unit = {
    val props = new Properties
    // static settings are: leader - enabled, follower throttling - disabled
    assertTrue(quotaManagers.leader.isThrottled(tp))
    assertFalse(quotaManagers.follower.isThrottled(tp))

    props.put(KafkaConfig.LeaderReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.NoThrottledReplicasValue)
    props.put(KafkaConfig.FollowerReplicationThrottledReplicasProp, ReplicationQuotaManagerConfig.AllThrottledReplicasValue)
    configHandler.processConfigChanges("0", props)

    assertFalse("Dynamic broker throttle setting should override the static one", quotaManagers.leader.isThrottled(tp))
    assertTrue(quotaManagers.follower.isThrottled(tp))

    configHandler.processConfigChanges("0", new Properties())
    assertTrue("expected leader throttle to be enabled after resetting to static config",
      quotaManagers.leader.isThrottled(tp))
    assertFalse("expected follower throttle to be disabled after resetting to static config",
      quotaManagers.follower.isThrottled(tp))
  }

  @Test
  def testRevertsBackToStaticallySetReplicationThrottledReplicas(): Unit = {
    val props = new Properties
    props.put(KafkaConfig.LeaderReplicationThrottledRateProp, "333")
    props.put(KafkaConfig.FollowerReplicationThrottledRateProp, "444")
    configHandler.processConfigChanges("0", props)
    assertEquals(333, quotaManagers.leader.upperBound())
    assertEquals(444, quotaManagers.follower.upperBound())

    configHandler.processConfigChanges("0", new Properties())
    assertEquals(staticLeaderThrottle, quotaManagers.leader.upperBound())
    assertEquals(staticFollowerThrottle, quotaManagers.follower.upperBound())
  }
}
