package kafka.tier

import java.util
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import kafka.api.IntegrationTestHarness
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AlterConfigOp, AlterConfigsResult, ConfigEntry}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.{Assert, Before, Test}
import org.scalatest.Assertions.intercept

import scala.concurrent.ExecutionException

class TierConfigurationTest extends IntegrationTestHarness {
  serverConfig.setProperty(KafkaConfig.TierFeatureProp, "true")
  serverConfig.setProperty(KafkaConfig.TierMetadataNumPartitionsProp, "2")
  serverConfig.setProperty(KafkaConfig.TierBackendProp, "mock")

  private val topicPartition = new TopicPartition("foo", 0)

  override protected val brokerCount: Int = 1

  @Before
  override def setUp(): Unit = {
  }

  @Test
  def testEnableCompactionAfterEnableTierAtBrokerLevel(): Unit = {
    serverConfig.setProperty(KafkaConfig.TierEnableProp, "true")
    super.setUp()
    createTopic(topicPartition.topic)
    assertInvalid(changeTopicConfigUsingAlterConfigs(LogConfig.CleanupPolicyProp, LogConfig.Compact))

    servers.foreach { server =>
      assertTrue(server.logManager.getLog(topicPartition).get.tierPartitionState.isTieringEnabled)
    }
  }

  @Test
  def testEnableCompactionAfterEnableTierAtTopicLevel(): Unit = {
    super.setUp()
    val topicConfig = new Properties()
    topicConfig.setProperty(LogConfig.TierEnableProp, "true")
    createTopic(topicPartition.topic, topicConfig = topicConfig)
    assertInvalid(changeTopicConfigUsingAlterConfigs(LogConfig.CleanupPolicyProp, LogConfig.Compact))

    servers.foreach { server =>
      assertTrue(server.logManager.getLog(topicPartition).get.tierPartitionState.isTieringEnabled)
    }
  }

  @Test
  def testCreateCompactedTopicAfterEnableTierAtBrokerLevel(): Unit = {
    serverConfig.setProperty(KafkaConfig.TierEnableProp, "true")
    super.setUp()

    val topicConfig = new Properties()
    topicConfig.setProperty(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    createTopic(topicPartition.topic, topicConfig = topicConfig)

    // trying to set compact property again is permissible
    changeTopicConfigUsingAlterConfigs(LogConfig.CleanupPolicyProp, LogConfig.Compact).all().get(5, TimeUnit.SECONDS)

    servers.foreach { server =>
      assertFalse(server.logManager.getLog(topicPartition).get.tierPartitionState.isTieringEnabled)
    }
  }

  @Test
  def testEnableTierOnCompactedTopicIsNotAllowed(): Unit = {
    serverConfig.setProperty(KafkaConfig.TierEnableProp, "true")
    super.setUp()
    val topicConfig = new Properties()
    topicConfig.setProperty(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    createTopic(topicPartition.topic, topicConfig = topicConfig)
    // This operation will not throw an exception because (tier.enable = true && compact) is a valid config for a compacted
    // topic created on a tiering enabled broker. But tiering is still not going to be enabled for such topic because of
    // the checks at tierPartitionStateFactory#mayEnabledTiering
    changeTopicConfigUsingAlterConfigs(LogConfig.TierEnableProp, "true").all().get(5, TimeUnit.SECONDS)
    servers.foreach { server =>
      TestUtils.waitUntilTrue(() => !server.logManager.getLog(topicPartition).get.tierPartitionState.isTieringEnabled,
        "Timed out waiting for tiered storage to be disabled", 5000)
    }
  }

  @Test
  def testDisableTieringWhenSetAtTopicLevel(): Unit = {
    super.setUp()
    val topicConfig = new Properties()
    topicConfig.setProperty(LogConfig.TierEnableProp, "true")
    createTopic(topicPartition.topic, topicConfig = topicConfig)
    servers.foreach { server =>
      assertTrue(server.logManager.getLog(topicPartition).get.tierPartitionState.isTieringEnabled)
    }
    changeTopicConfigUsingAlterConfigs(LogConfig.TierEnableProp, "false").all().get(5, TimeUnit.SECONDS)
    servers.foreach { server =>
      TestUtils.waitUntilTrue(() => !server.logManager.getLog(topicPartition).get.tierPartitionState.isTieringEnabled,
        "Timed out waiting for tiered storage to be disabled", 5000)
    }
  }

  @Test
  def testDisableTieringWhenSetAtBrokerLevel(): Unit = {
    serverConfig.setProperty(KafkaConfig.TierEnableProp, "true")
    super.setUp()
    createTopic(topicPartition.topic)
    servers.foreach { server =>
      assertTrue(server.logManager.getLog(topicPartition).get.tierPartitionState.isTieringEnabled)
    }
    changeTopicConfigUsingAlterConfigs(LogConfig.TierEnableProp, "false").all().get(5, TimeUnit.SECONDS)
    servers.foreach { server =>
      TestUtils.waitUntilTrue(() => !server.logManager.getLog(topicPartition).get.tierPartitionState.isTieringEnabled,
        "Timed out waiting for tiered storage to be disabled", 5000)
    }
  }

  @Test
  def testEnableCompactionWhenTierFeatureEnabledIsNotAllowed(): Unit = {
    // We cannot enable compaction on a topic partition using incremental alter configs when tier feature is enabled.
    serverConfig.setProperty(KafkaConfig.TierFeatureProp, "true")
    super.setUp()
    createTopic(topicPartition.topic)

    assertInvalid(changeTopicConfigUsingAlterConfigs(LogConfig.CleanupPolicyProp, LogConfig.Compact))
  }

  private def changeTopicConfigUsingAlterConfigs(propKey: String, propValue: String): AlterConfigsResult = {
    val alterConfigOp = new AlterConfigOp(new ConfigEntry(propKey, propValue), AlterConfigOp.OpType.SET)
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topicPartition.topic), Collections.singletonList(alterConfigOp))

    val adminClient = createAdminClient()
    adminClient.incrementalAlterConfigs(configs)
  }

  private def assertInvalid(result: AlterConfigsResult): Unit = {
    val exception = intercept[ExecutionException] {
      result.all.get
    }
    Assert.assertEquals(classOf[InvalidConfigurationException], exception.getCause.getClass)
  }
}
