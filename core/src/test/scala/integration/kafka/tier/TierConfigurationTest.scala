package kafka.tier

import java.util
import java.util.{Collections, Properties}

import kafka.api.IntegrationTestHarness
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import org.apache.kafka.clients.admin.{AlterConfigOp, AlterConfigsResult, ConfigEntry}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.junit.Assert.{assertEquals, assertThrows, assertTrue}
import org.junit.function.ThrowingRunnable
import org.junit.{Before, Test}

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
    assertInvalid(enableCompaction())

    servers.foreach { server =>
      assertEquals(topicPartition, server.tierMetadataManager.tierEnabledPartitionStateIterator.next().topicPartition)
    }
  }

  @Test
  def testEnableCompactionAfterEnableTierAtTopicLevel(): Unit = {
    super.setUp()
    val topicConfig = new Properties()
    topicConfig.setProperty(LogConfig.TierEnableProp, "true")
    createTopic(topicPartition.topic, topicConfig = topicConfig)
    assertInvalid(enableCompaction())

    servers.foreach { server =>
      assertEquals(topicPartition, server.tierMetadataManager.tierEnabledPartitionStateIterator.next().topicPartition)
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
    enableCompaction().all().get

    servers.foreach { server =>
      assertTrue(!server.tierMetadataManager.tierEnabledPartitionStateIterator.hasNext)
    }
  }

  private def enableCompaction(): AlterConfigsResult = {
    val alterConfigOp = new AlterConfigOp(new ConfigEntry(LogConfig.CleanupPolicyProp, LogConfig.Compact), AlterConfigOp.OpType.SET)
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topicPartition.topic), Collections.singletonList(alterConfigOp))

    val adminClient = createAdminClient()
    adminClient.incrementalAlterConfigs(configs)
  }

  private def assertInvalid(result: AlterConfigsResult): Unit = {
    assertThrows(classOf[InvalidConfigurationException], new ThrowingRunnable {
      override def run(): Unit = try {
        result.all.get
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    })
  }
}
