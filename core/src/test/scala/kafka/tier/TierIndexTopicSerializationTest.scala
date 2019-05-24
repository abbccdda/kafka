/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.util.UUID

import kafka.tier.domain.{AbstractTierMetadata, TierObjectMetadata, TierTopicInitLeader}
import org.junit.Assert._
import org.junit.Test

class TierIndexTopicSerializationTest {
  @Test
  def serializeDeserializeTest(): Unit = {
    // 256 character topic name - the maximum length
    roundTrip(new TierTopicInitLeader(new TopicIdPartition("8BOZItC97ECWLZAXG1twhNjHgq5uiQhMhI5TW9oDdnLsNqPQ8DL85rCWF1lMIkb0RjH37L86WWFiFXb68zEEAgnRSiNRBlfsEMOAbgRJ1J4GyiwjHmHqCEjC9tMfDZCjAnfYC2DxgFygZLacUmr0wi6yK9L8ShR7krsMlxEdTRarNFDZfnihFFmhFc0eHb0aRf4nvg2Gt9zeqSb3FDIdjrNbKtCi2V9VtXZjd014SO28Noi5CwEmuXD0crus1unE", UUID.randomUUID(), 0), 0, UUID.randomUUID, 33))
    roundTrip(new TierTopicInitLeader(new TopicIdPartition("my", UUID.randomUUID(), 199999), 1, UUID.randomUUID, 99))
    roundTrip(new TierObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID(), 0), 0, 0L, 33333, 0L, 99999L, 3333, true, false, false, kafka.tier.serdes.State.AVAILABLE))
  }

  private def roundTrip(v: AbstractTierMetadata): Unit = {
    val key = v.serializeKey()
    val value = v.serializeValue()
    val v2 = AbstractTierMetadata.deserialize(key, value).get()
    assertEquals(v, v2)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def metadataIllegalEpochTest(): Unit = {
    new TierObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID(), 0), -1, 0L, 23252334, 0L,
      0L, 102, true, false, false, kafka.tier.serdes.State.AVAILABLE)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def initIllegalEpochTest(): Unit = {
    new TierTopicInitLeader(new TopicIdPartition("my-topic", UUID.randomUUID(), 0), -1, UUID.randomUUID, 33)
  }

}
