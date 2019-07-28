package kafka.tier.client

import org.junit.Assert.assertTrue
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
}
