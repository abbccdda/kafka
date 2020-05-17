/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.apache.kafka.common.requests.ListOffsetRequest
import org.junit.Test

class ListOffsetsRequestTieredPartitionTest extends BaseRequestTest {
  serverConfig.put(KafkaConfig.TierEnableProp, "false")
  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierBackendProp, "mock")
  serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
  serverConfig.put(KafkaConfig.TierMetadataReplicationFactorProp, "1")
  serverConfig.put(KafkaConfig.TierPartitionStateCommitIntervalProp, "5")

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    serverConfig.stringPropertyNames().forEach(key => properties.put(key, serverConfig.get(key)))
  }

  @Test
  def testListOffsetsRequestForTieredPartition(): Unit = {
    val topicName: String = "tiered-topic"
    val props = new Properties
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "16384")
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "16384")
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_MS_CONFIG, "-1")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")
    val partitionToLeaderMap = createTopic(topicName, 1, 1, props)
    // produce enough records to have some of the segments tiered
    val numMessages = 3000
    TestUtils.generateAndProduceMessages(servers.toSeq, topicName, numMessages)

    val topicPartition = new TopicPartition(topicName, 0)
    val leaderId = partitionToLeaderMap(topicPartition.partition())
    val server = serverForId(leaderId).get
    val log = server.logManager.getLog(topicPartition).get

    // wait for some segments to tier and get deleted from local disk. we want to test against condition where
    // localLogOffset > mergedLogStartOffset, and make sure that offsets from ListOffsetRequestV0 start at mergedLogStartOffset
    TestUtils.waitUntilTrue(() => {
      val iterator = log.tieredLogSegments
      try {
        (log.logEndOffset == numMessages) && iterator.hasNext &&
          (log.localLogSegments.size < log.numberOfSegments)
      } finally {
        iterator.close()
      }
    },
    "Timeout waiting for some segments to tier and be deleted from local disk", 180000)

    makeListOffsetsRequestAndValidateResponse(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP, leaderId, log, 0)
    // advance the log start offset and run again
    log.maybeIncrementHighWatermark(LogOffsetMetadata(log.logStartOffset + 100))
    log.maybeIncrementLogStartOffset(log.logStartOffset + 100)
    makeListOffsetsRequestAndValidateResponse(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP, leaderId, log, 0)
    makeListOffsetsRequestAndValidateResponse(topicPartition, ListOffsetRequest.EARLIEST_TIMESTAMP, leaderId, log, 0)
  }
}
