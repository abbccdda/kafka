package kafka.server

import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.serdes.State
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

    val properties = new Properties()
    properties.put(KafkaConfig.TierEnableProp, "true")

    while (!tierTopicManager.isReady) {
      Thread.sleep(5)
    }

    val topic1 = "foo"
    TestUtils.createTopic(this.zkClient, topic1, 2, 1,
      servers, properties)
    val topicPartition = new TopicPartition(topic1, 0)

    TestUtils.waitUntilTrue(() => {
      val initialState = tierTopicManager.partitionState(topicPartition)
      initialState.tierEpoch() == 0
    }, "Did not become leader for TierPartitionState.")
    val metaresult1 = tierTopicManager
      .addMetadata(
        new TierObjectMetadata(
          topicPartition,
          0,
          0L,
          1000,
          15000L,
          16000L,
          100,
          true,
          true,
          State.AVAILABLE))
      .get()

    assertEquals(AppendResult.ACCEPTED, metaresult1)

    val tierPartitionState = tierTopicManager.partitionState(topicPartition)
    tierPartitionState.flush()
    assertEquals(1000L, tierPartitionState.committedEndOffset.get())
    val metaresult2 = tierTopicManager.addMetadata(
      new TierObjectMetadata(
        topicPartition,
        0,
        0L,
        1000,
        15000L,
        16000L,
        200,
        true,
        false,
        State.AVAILABLE))

    assertEquals(AppendResult.FENCED, metaresult2.get())

    tierPartitionState.flush()
    assertEquals(1000L, tierPartitionState.committedEndOffset.get())
    assertEquals(1, tierPartitionState.numSegments())

    val topic2 = "topic2"
    TestUtils.createTopic(this.zkClient, topic2, 1, 1,
      servers, properties)

    val topicPartitionTopic2 = new TopicPartition(topic2, 0)
        TestUtils.waitUntilTrue(() => {
      val state = tierTopicManager.partitionState(topicPartitionTopic2)
      state.tierEpoch() == 0
    }, "Did not become leader for TierPartitionState topic2.")


    TestUtils.waitUntilTrue(() => {
      !tierTopicManager.catchingUp()
    }, "tierTopicManager consumers catchingUp timed out", 500L)


    val originalState = tierTopicManager.partitionState(topicPartition)
    // original topic1 tier partition state should only have one entry, even after catch up
    // consumer has been seeked backwards.
    assertEquals(1, originalState.numSegments())
  }
}
