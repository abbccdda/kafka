/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.state

import java.util.{Optional, UUID}

import com.google.flatbuffers.FlatBufferBuilder
import kafka.tier.serdes.{MaterializationTrackingInfo, TierPartitionStateHeader}
import kafka.tier.serdes.OffsetAndEpoch.createOffsetAndEpoch
import org.junit.Assert.assertEquals
import org.junit.Test

class HeaderTest {
  @Test
  def testReadHeaderVersions(): Unit = {
    val MAX_TESTED_VERSION = 6
    if (MAX_TESTED_VERSION != FileTierPartitionState.CURRENT_VERSION)
      throw new Exception("FileTierPartitionState version has been bumped." +
        "Please ensure the readability of all versions, including any new fields, and then bump testedVersion.")

    (0 to FileTierPartitionState.CURRENT_VERSION).foreach { version =>
      val topicId = UUID.randomUUID
      val epoch = 0
      val status = TierPartitionStatus.INIT
      val endOffset = 100
      val localMaterializedOffsetAndEpoch = new OffsetAndEpoch(50, Optional.of(5))
      val globalMaterializedOffset = new OffsetAndEpoch(20, Optional.of(2))
      val errorOffsetAndEpoch = new OffsetAndEpoch(30, Optional.of(2))
      val restoreOffsetAndEpoch = new OffsetAndEpoch(35, Optional.of(3))

      val builder = new FlatBufferBuilder(100).forceDefaults(true)
      val materializedInfo = MaterializationTrackingInfo.createMaterializationTrackingInfo(builder,
        globalMaterializedOffset.offset,
        localMaterializedOffsetAndEpoch.offset,
        globalMaterializedOffset.epoch.get,
        localMaterializedOffsetAndEpoch.epoch.get)
      TierPartitionStateHeader.startTierPartitionStateHeader(builder)

      if (version >= 0) {
        val topicIdOffset = kafka.tier.serdes.UUID.createUUID(builder, topicId.getMostSignificantBits, topicId.getLeastSignificantBits)
        TierPartitionStateHeader.addTopicId(builder, topicIdOffset)
        TierPartitionStateHeader.addTierEpoch(builder, epoch)
        TierPartitionStateHeader.addVersion(builder, version.byteValue())
        TierPartitionStateHeader.addStatus(builder, TierPartitionStatus.toByte(status))
      }

      if (version >= 1)
        TierPartitionStateHeader.addEndOffset(builder, endOffset)

      if (version >= 2)
        TierPartitionStateHeader.addMaterializationInfo(builder, materializedInfo)

      if (version >= 5) {
        val errorOffsetAndEpochId = createOffsetAndEpoch(builder, errorOffsetAndEpoch.offset(), errorOffsetAndEpoch.epoch().orElse(-1))
        TierPartitionStateHeader.addErrorOffsetAndEpoch(builder, errorOffsetAndEpochId)
      }

      if (version >= 6) {
        val restoreOffsetAndEpochId = createOffsetAndEpoch(builder, restoreOffsetAndEpoch.offset(), restoreOffsetAndEpoch.epoch().orElse(-1))
        TierPartitionStateHeader.addRestoreOffsetAndEpoch(builder, restoreOffsetAndEpochId)
      }

      val entryId = kafka.tier.serdes.TierPartitionStateHeader.endTierPartitionStateHeader(builder)
      builder.finish(entryId)
      val header = new Header(TierPartitionStateHeader.getRootAsTierPartitionStateHeader(builder.dataBuffer))

      assertEquals(version, header.version)
      assertEquals(topicId, header.topicId)
      assertEquals(epoch, header.tierEpoch)
      assertEquals(status, header.status)

      if (version >= 2) {
        assertEquals(localMaterializedOffsetAndEpoch, header.localMaterializedOffsetAndEpoch)
        assertEquals(globalMaterializedOffset, header.globalMaterializedOffsetAndEpoch)
      } else {
        assertEquals(OffsetAndEpoch.EMPTY, header.localMaterializedOffsetAndEpoch)
        assertEquals(OffsetAndEpoch.EMPTY, header.globalMaterializedOffsetAndEpoch)
      }

      if (version >= 5)
        assertEquals(errorOffsetAndEpoch, header.errorOffsetAndEpoch)
      else
        assertEquals(OffsetAndEpoch.EMPTY, header.errorOffsetAndEpoch)

      if (version >= 6)
        assertEquals(restoreOffsetAndEpoch, header.restoreOffsetAndEpoch)
      else
        assertEquals(OffsetAndEpoch.EMPTY, header.restoreOffsetAndEpoch)
    }
  }
}