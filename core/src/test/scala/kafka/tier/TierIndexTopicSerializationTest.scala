/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.util.UUID

import kafka.tier.domain.{AbstractTierMetadata, TierObjectMetadata, TierTopicInitLeader}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

class TierIndexTopicSerializationTest {
  @Test
  def serializeDeserializeTest(): Unit = {
    roundTrip(new TierTopicInitLeader(new TopicPartition("my-topic33", 0), 0, UUID.randomUUID, 33))
    roundTrip(new TierTopicInitLeader(new TopicPartition("my", 199999), 1, UUID.randomUUID, 99))
    roundTrip(new TierObjectMetadata(new TopicPartition("foo", 0), 0, 0L, 33333, 0L, 99999L, 3333, true, false, false, kafka.tier.serdes.State.AVAILABLE))
  }

  private def roundTrip(v: AbstractTierMetadata): Unit = {
    val key = v.serializeKey()
    val value = v.serializeValue()
    val v2 = AbstractTierMetadata.deserialize(key, value).get()
    assertEquals(v, v2)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def metadataIllegalEpochTest(): Unit = {
    new TierObjectMetadata(new TopicPartition("foo", 0), -1, 0L, 23252334, 0L,
      0L, 102, true, false, false, kafka.tier.serdes.State.AVAILABLE)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def initIllegalEpochTest(): Unit = {
    new TierTopicInitLeader(new TopicPartition("my-topic", 0), -1, UUID.randomUUID, 33)
  }

}
