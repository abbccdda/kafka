/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store

import java.util.UUID

import kafka.tier.domain.TierObjectMetadata
import kafka.tier.TopicIdPartition
import org.junit.Test
import org.junit.Assert._

class TierObjectStoreUtilsTest {
  @Test
  def testKeyPathGeneration(): Unit = {
    val topicId = UUID.fromString("43aeca7f-a684-4b60-bff8-9b3b783691bb")
    val metadata = new TierObjectMetadata(new TopicIdPartition("foo", topicId, 0), 0, UUID.randomUUID,
      0, 100, 100, 1000, TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE, false, false, false)
    assertEquals(s"0/${metadata.objectIdAsBase64}/Q67Kf6aES2C_-Js7eDaRuw/0/00000000000000000000_0_v0.segment",
      TierObjectStoreUtils.keyPath(new TierObjectStore.ObjectMetadata(metadata), TierObjectStore.FileType.SEGMENT))
  }
}
