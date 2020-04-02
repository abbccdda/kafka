/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.state

import java.util.{Optional, UUID}

import com.google.flatbuffers.FlatBufferBuilder
import kafka.tier.serdes.{MaterializationTrackingInfo, TierPartitionStateHeader}
import org.junit.Assert.assertEquals
import org.junit.Test

class HeaderTest {
  @Test
  def testReadHeaderV0(): Unit = {
    val topicId = UUID.randomUUID
    val version = 0: Byte
    val epoch = 0
    val status = TierPartitionStatus.INIT

    val builder = new FlatBufferBuilder(100).forceDefaults(true)
    TierPartitionStateHeader.startTierPartitionStateHeader(builder)
    val topicIdOffset = kafka.tier.serdes.UUID.createUUID(builder, topicId.getMostSignificantBits, topicId.getLeastSignificantBits)
    TierPartitionStateHeader.addTopicId(builder, topicIdOffset)
    TierPartitionStateHeader.addTierEpoch(builder, epoch)
    TierPartitionStateHeader.addVersion(builder, version)
    TierPartitionStateHeader.addStatus(builder, TierPartitionStatus.toByte(status))
    val entryId = kafka.tier.serdes.TierPartitionStateHeader.endTierPartitionStateHeader(builder)
    builder.finish(entryId)

    val header = new Header(TierPartitionStateHeader.getRootAsTierPartitionStateHeader(builder.dataBuffer))

    assertEquals(version, header.version)
    assertEquals(topicId, header.topicId)
    assertEquals(epoch, header.tierEpoch)
    assertEquals(status, header.status)

    // default values are returned for values that were not specified
    assertEquals(-1L, header.endOffset)
    assertEquals(new OffsetAndEpoch(-1, Optional.empty[Integer]), header.localMaterializedOffsetAndEpoch)
    assertEquals(new OffsetAndEpoch(-1, Optional.empty[Integer]), header.globalMaterializedOffsetAndEpoch)
  }

  @Test
  def testReadHeaderV4(): Unit = {
    val topicId = UUID.randomUUID
    val version = 4: Byte
    val epoch = 0
    val status = TierPartitionStatus.INIT
    val endOffset = 100
    val localMaterializedOffsetAndEpoch = new OffsetAndEpoch(50, Optional.of(5))
    val globalMaterializedOffset = new OffsetAndEpoch(20, Optional.of(2))

    val builder = new FlatBufferBuilder(100).forceDefaults(true)
    val materializedInfo = MaterializationTrackingInfo.createMaterializationTrackingInfo(builder,
      globalMaterializedOffset.offset,
      localMaterializedOffsetAndEpoch.offset,
      globalMaterializedOffset.epoch.get,
      localMaterializedOffsetAndEpoch.epoch.get)
    TierPartitionStateHeader.startTierPartitionStateHeader(builder)
    val topicIdOffset = kafka.tier.serdes.UUID.createUUID(builder, topicId.getMostSignificantBits, topicId.getLeastSignificantBits)
    TierPartitionStateHeader.addTopicId(builder, topicIdOffset)
    TierPartitionStateHeader.addTierEpoch(builder, epoch)
    TierPartitionStateHeader.addVersion(builder, version)
    TierPartitionStateHeader.addStatus(builder, TierPartitionStatus.toByte(status))
    TierPartitionStateHeader.addEndOffset(builder, endOffset)
    TierPartitionStateHeader.addMaterializationInfo(builder, materializedInfo)
    val entryId = kafka.tier.serdes.TierPartitionStateHeader.endTierPartitionStateHeader(builder)
    builder.finish(entryId)
    val header = new Header(TierPartitionStateHeader.getRootAsTierPartitionStateHeader(builder.dataBuffer))

    assertEquals(version, header.version)
    assertEquals(topicId, header.topicId)
    assertEquals(epoch, header.tierEpoch)
    assertEquals(status, header.status)
    assertEquals(localMaterializedOffsetAndEpoch, header.localMaterializedOffsetAndEpoch)
    assertEquals(globalMaterializedOffset, header.globalMaterializedOffsetAndEpoch)
  }
}
