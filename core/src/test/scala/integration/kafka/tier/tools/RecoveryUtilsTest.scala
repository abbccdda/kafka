/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tools

import java.time.Duration
import java.util.Arrays;
import java.util.Collections
import java.util.Optional
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ExecutionException

import kafka.api.IntegrationTestHarness
import kafka.tier.TopicIdPartition
import kafka.tier.domain.{AbstractTierMetadata, TierTopicInitLeader}
import kafka.tier.topic.{TierTopic, TierTopicAdmin, TierTopicManager}
import kafka.utils.CoreUtils

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.intercept

class RecoveryUtilsTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 3

  @Test
  def testGetNumPartitionsOnExistingTopic(): Unit = {
    val numPartitions = 31
    val topicName = "test-topic"
    createTopic(topicName, numPartitions, 3)
    assertEquals(numPartitions, RecoveryUtils.getNumPartitions(brokerList, topicName))
  }

  @Test
  def testGetNumPartitionsOnNonExistingTopic(): Unit = {
    val topicName = "nonexisting-test-topic"
    intercept[ExecutionException](
      RecoveryUtils.getNumPartitions(brokerList, topicName)
    ).getCause.isInstanceOf[UnknownTopicOrPartitionException]
  }

  @Test
  def testInjectTierTopicEventOnExistingTopic(): Unit = {
    // 1. Initialize few variables
    val tierTopicNamespace = ""
    val tierTopicName = TierTopic.topicName(tierTopicNamespace)
    val numTierTopicPartitions: Short = 19
    val tierTopicReplicationFactor = 3
    val tieredTopicIdPartition = new TopicIdPartition(
      "dummy",
      UUID.fromString("021516db-7a5f-40ef-adda-b6e2b21a3e83"),
      123
    )

    // 2. Create the TierTopic.
    createTopic(
      tierTopicName,
      numTierTopicPartitions,
      tierTopicReplicationFactor,
      TierTopicAdmin.topicConfig)

    // 3. Create an event to be injected into TierTopic.
    val leaderEpoch = 1000
    val messageId = UUID.randomUUID;
    val brokerId = 1
    val initLeaderEvent = new TierTopicInitLeader(
      tieredTopicIdPartition, leaderEpoch, messageId, brokerId);

    // 4. Inject the initLeaderEvent into TierTopic.
    var mayBeProducer: Option[Producer[Array[Byte], Array[Byte]]] = None
    var mayBeMetadata: Option[RecordMetadata] = None
    try {
      mayBeProducer = Some(RecoveryUtils.createTierTopicProducer(
        brokerList, tierTopicNamespace, numTierTopicPartitions, "test"))
      mayBeMetadata = Some(RecoveryUtils.injectTierTopicEvent(
        mayBeProducer.get, initLeaderEvent, tierTopicName, numTierTopicPartitions))
    } finally {
      mayBeProducer.map(producer => producer.close)
    }
    mayBeMetadata.map(metadata => {
      assertTrue(metadata.hasOffset)
      assertEquals(0, metadata.offset)
      assertTrue(metadata.serializedKeySize > 0)
      assertTrue(metadata.serializedValueSize > 0)
    })

    // 5. Using a dummy consumer, verify that exactly the above event was written to TierTopic.
    var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null
    try {
      val consumerProps = new Properties();
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
      consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps);
      val tierTopicPartitions = TierTopicManager.partitions(tierTopicName, numTierTopicPartitions);
      consumer.assign(tierTopicPartitions);
      tierTopicPartitions.forEach(tp => consumer.seekToBeginning(Collections.singletonList(tp)))
      val records = consumer.poll(Duration.ofMillis(100))
      assertEquals(1, records.count)
      val record = records.iterator.next
      val eventOpt: Optional[AbstractTierMetadata]
        = AbstractTierMetadata.deserialize(record.key, record.value);
      assertTrue(eventOpt.isPresent)
      val retrievedEvent = eventOpt.get.asInstanceOf[TierTopicInitLeader]
      assertEquals(initLeaderEvent, retrievedEvent)
    } finally {
      consumer.close
    }
  }

  @Test
  def testInjectTierTopicEventOnNonExistingTopic(): Unit = {
    // 1. Create an event.
    val initLeaderEvent = new TierTopicInitLeader(
      new TopicIdPartition(
        "dummy",
        UUID.fromString("021516db-7a5f-40ef-adda-b6e2b21a3e83"),
        123
      ),
      1000,
      UUID.randomUUID,
      1);

    // 2. Inject the initLeaderEvent into TierTopic that doesn't exist. This should cause a suitable
    // exception to be raised.
    val numTierTopicPartitions: Short = 1
    var mayBeProducer: Option[Producer[Array[Byte], Array[Byte]]] = None
    try {
      mayBeProducer = Some(RecoveryUtils.createTierTopicProducer(
        brokerList, "", numTierTopicPartitions, "test"))
      intercept[ExecutionException](
        RecoveryUtils.injectTierTopicEvent(
          mayBeProducer.get, initLeaderEvent, "", numTierTopicPartitions)
      ).getCause.isInstanceOf[UnknownTopicOrPartitionException]
    } finally {
      mayBeProducer.map(producer => producer.close)
    }
  }

  @Test
  def testToTopicIdPartitionsWithEmptyTopicName(): Unit = {
    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          CoreUtils.generateUuidAsBase64,
          "",
          "23")));
    }

    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          CoreUtils.generateUuidAsBase64,
          "   ",
          "23")));
    }
  }

  @Test
  def testToTopicIdPartitionsWithBadTopicId(): Unit = {
    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          "",
          "foo",
          "23")));
    }

    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          "  ",
          "foo",
          "23")));
    }

    val badUuid = "badUuid"
    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          badUuid,
          "foo",
          "23")));
    }
  }

  @Test
  def testToTopicIdPartitionsWithBadPartitionNumber(): Unit = {
    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          CoreUtils.generateUuidAsBase64,
          "foo",
          "")));
    }

    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          CoreUtils.generateUuidAsBase64,
          "foo",
          "  ")));
    }

    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          CoreUtils.generateUuidAsBase64,
          "foo",
          "abc")));
    }

    org.scalatest.Assertions.assertThrows[IllegalArgumentException]{
      RecoveryUtils.toTopicIdPartitions(
        Arrays.asList("%s,%s,%s".format(
          CoreUtils.generateUuidAsBase64,
          "foo",
          "-1")));
    }
  }

  @Test
  def testToTopicIdPartitionsWithGoodArgs(): Unit = {
    val topicIdPartition1 = new TopicIdPartition("foo", UUID.randomUUID(), 23)
    val topicIdPartition2 = new TopicIdPartition("bar", UUID.randomUUID(), 97)
    val result = RecoveryUtils.toTopicIdPartitions(
      Arrays.asList(
        "%s,%s,%d".format(
          CoreUtils.uuidToBase64(topicIdPartition1.topicId),
          topicIdPartition1.topic,
          topicIdPartition1.partition),
        "%s,%s,%d".format(
          CoreUtils.uuidToBase64(topicIdPartition2.topicId),
          topicIdPartition2.topic,
          topicIdPartition2.partition)));
    assertEquals(Arrays.asList(topicIdPartition1, topicIdPartition2), result)
  }
}
