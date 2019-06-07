/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.util.UUID

import kafka.tier.domain.{AbstractTierMetadata, TierObjectMetadata, TierSegmentDeleteComplete, TierSegmentDeleteInitiate, TierTopicInitLeader, TierSegmentUploadComplete, TierSegmentUploadInitiate}
import org.junit.Assert._
import org.junit.Test

class TierTopicSerDeTest {
  @Test
  def serializeDeserializeTest(): Unit = {
    // 256 character topic name - the maximum length
    roundTrip(new TierTopicInitLeader(new TopicIdPartition("8BOZItC97ECWLZAXG1twhNjHgq5uiQhMhI5TW9oDdnLsNqPQ8DL85rCWF1lMIkb0RjH37L86WWFiFXb68zEEAgnRSiNRBlfsEMOAbgRJ1J4GyiwjHmHqCEjC9tMfDZCjAnfYC2DxgFygZLacUmr0wi6yK9L8ShR7krsMlxEdTRarNFDZfnihFFmhFc0eHb0aRf4nvg2Gt9zeqSb3FDIdjrNbKtCi2V9VtXZjd014SO28Noi5CwEmuXD0crus1unE", UUID.randomUUID(), 0), 0, UUID.randomUUID, 33))
    roundTrip(new TierTopicInitLeader(new TopicIdPartition("my", UUID.randomUUID,199999), 1, UUID.randomUUID, 99))
    roundTrip(new TierSegmentUploadInitiate(new TopicIdPartition("foo", UUID.randomUUID,0), 0, UUID.randomUUID, 0L, 33333L, 99999L, 3333, false, true, false))
    roundTrip(new TierSegmentUploadComplete(new TopicIdPartition("foo", UUID.randomUUID,0), 0, UUID.randomUUID))
    roundTrip(new TierSegmentDeleteInitiate(new TopicIdPartition("foo", UUID.randomUUID,0), 0, UUID.randomUUID))
    roundTrip(new TierSegmentDeleteComplete(new TopicIdPartition("foo", UUID.randomUUID,0), 0, UUID.randomUUID))
  }

  private def roundTrip(v: AbstractTierMetadata): Unit = {
    val key = v.serializeKey()
    val value = v.serializeValue()
    val v2 = AbstractTierMetadata.deserialize(key, value).get()
    assertEquals(v, v2)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def metadataIllegalEpochTest(): Unit = {
    new TierObjectMetadata(new TopicIdPartition("foo", UUID.randomUUID,0), -1, UUID.randomUUID, 0L, 23252334L, 0L,
      102, TierObjectMetadata.State.SEGMENT_DELETE_INITIATE, true, false, false)
  }

  @Test (expected = classOf[IllegalArgumentException])
  def initIllegalEpochTest(): Unit = {
    new TierTopicInitLeader(new TopicIdPartition("my-topic", UUID.randomUUID(), 0), -1, UUID.randomUUID, 33)
  }
}
