package kafka.server

import java.util.{Properties, UUID}

import kafka.integration.KafkaServerTestHarness
import kafka.tier.TierTestUtils
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

/* Temporary integration test until we have a more substantial Tiered Storage integration test with archiving. */
class TierTopicManagerIntegrationTest extends KafkaServerTestHarness {

  val overridingProps = new Properties()
  overridingProps.setProperty(KafkaConfig.TierFeatureProp, "true")
  overridingProps.setProperty(KafkaConfig.TierMetadataNumPartitionsProp, "2")
  overridingProps.setProperty(KafkaConfig.TierMetadataReplicationFactorProp, "1")
  overridingProps.setProperty(KafkaConfig.TierBackendProp, "mock")
  val logDir = TestUtils.tempDir()

  override def generateConfigs =
    TestUtils
      .createBrokerConfigs(1, zkConnect, enableControlledShutdown = false)
      .map {
        KafkaConfig.fromProps(_, overridingProps)
      }

  @Test
  def testTierTopicManager(): Unit = {
    val tierTopicManager = servers.last.tierTopicManager
    val tierMetadataManager = servers.last.tierMetadataManager

    val properties = new Properties()
    properties.put(KafkaConfig.TierEnableProp, "true")

    while (!tierTopicManager.isReady)
      Thread.sleep(5)

    val topic1 = "foo"
    TestUtils.createTopic(this.zkClient, topic1, 2, 1,
      servers, properties)
    val topicPartition = new TopicPartition(topic1, 0)

    TestUtils.waitUntilTrue(() => {
      val partitionState = tierMetadataManager.tierPartitionState(topicPartition)
      partitionState.isPresent && partitionState.get.topicIdPartition.isPresent && partitionState.get.tierEpoch == 0
    }, "Did not become leader for TierPartitionState.")

    val tierPartitionState = tierMetadataManager.tierPartitionState(topicPartition).get
    val topicIdPartition1 = tierPartitionState.topicIdPartition.get
    val result1 = TierTestUtils.uploadWithMetadata(tierTopicManager,
      topicIdPartition1,
      tierEpoch = 0,
      objectId = UUID.randomUUID,
      startOffset = 0,
      endOffset = 1000L,
      maxTimestamp = 15000L,
      lastModifiedTime = 0L,
      size = 100,
      hasAbortedTxnIndex = false,
      hasEpochState = true,
      hasProducerState = false)
    assertEquals(AppendResult.ACCEPTED, result1.get)

    tierPartitionState.flush()
    assertEquals(1000L, tierPartitionState.committedEndOffset.get())
    val result2 = TierTestUtils.uploadWithMetadata(tierTopicManager,
      topicIdPartition1,
      tierEpoch = 0,
      objectId = UUID.randomUUID,
      startOffset = 0L,
      endOffset = 1000L,
      maxTimestamp = 15000L,
      lastModifiedTime = 0L,
      size = 200,
      hasAbortedTxnIndex = false,
      hasEpochState = true,
      hasProducerState = false)
    assertEquals(AppendResult.FENCED, result2.get())

    tierPartitionState.flush()
    assertEquals(1000L, tierPartitionState.committedEndOffset.get())
    assertEquals(1, tierPartitionState.numSegments())

    val topic2 = "topic2"
    val topicPartition2 = new TopicPartition(topic2,0)
    TestUtils.createTopic(this.zkClient, topicPartition2.topic, 1, 1,
      servers, properties)

    TestUtils.waitUntilTrue(() => {
      val partitionState = tierMetadataManager.tierPartitionState(topicPartition2)
      partitionState.isPresent && partitionState.get().topicIdPartition().isPresent && partitionState.get().tierEpoch() == 0
    }, "Did not become leader for TierPartitionState topic2.")

    assertTrue(tierMetadataManager.tierPartitionState(topicPartition2).get().topicIdPartition().isPresent)

    TestUtils.waitUntilTrue(() => {
      !tierTopicManager.catchingUp()
    }, "tierTopicManager consumers catchingUp timed out", 500L)

    val originalState = tierTopicManager.partitionState(topicIdPartition1)
    // original topic1 tier partition state should only have one entry, even after catch up
    // consumer has been seeked backwards.
    assertEquals(1, originalState.numSegments())
  }
}
