package kafka.tier.client

import java.util
import java.util.Collections
import java.util.function.Supplier

import kafka.tier.topic.TierTopicManagerConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.junit.Assert._
import org.junit.Test

class TierTopicClientTest {
  private val clusterId = "cluster-id"
  private val brokerId = 3
  private val instanceId = 10

  @Test
  def testConsumerClientId(): Unit = {
    val clientIdSuffix = "suffix"
    val clientId = TierTopicConsumerSupplier.clientId(clusterId, brokerId, instanceId, clientIdSuffix)
    assertTrue(TierTopicClient.isTierTopicClient(clientId))
  }

  @Test
  def testProducerClientId(): Unit = {
    val clientId = TierTopicProducerSupplier.clientId(clusterId, brokerId, instanceId)
    assertTrue(TierTopicClient.isTierTopicClient(clientId))
  }

  @Test
  def testMetricsReporterConfigNotPresent(): Unit = {
    val configsSupplier = new Supplier[util.Map[String, Object]] {
      override def get(): util.Map[String, Object] = {
        val configs = new util.HashMap[String, Object]()
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bootstrap")
        configs.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, "reporter")
        configs
      }
    }

    val tierTopicManagerConfig = new TierTopicManagerConfig(configsSupplier,
      "tierNamespace",
      2.toShort,
      1.toShort,
      0,
      "clusterId",
      100,
      200,
      300,
      Collections.singletonList("logDir"))

    val producerConfigs = TierTopicProducerSupplier.properties(tierTopicManagerConfig, "clientId")
    assertEquals("bootstrap", producerConfigs.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
    assertNull(producerConfigs.getProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG))

    val consumerConfigs = TierTopicConsumerSupplier.properties(tierTopicManagerConfig, "clientId")
    assertEquals("bootstrap", consumerConfigs.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
    assertNull(consumerConfigs.getProperty(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG))
  }
}
