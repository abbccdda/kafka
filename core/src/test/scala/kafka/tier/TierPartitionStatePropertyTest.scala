/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.util.UUID

import kafka.tier.domain.{AbstractTierMetadata, TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.serdes.State
import kafka.tier.state.{FileTierPartitionState, MemoryTierPartitionState}
import kafka.utils.ScalaCheckUtils.assertProperty
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalacheck.Test.Parameters.defaultVerbose

class TierPartitionStatePropertyTest {
  val brokerId = 0
  val baseDir = TestUtils.tempDir()
  val topic = UUID.randomUUID().toString
  val partition = 0
  val tp = new TopicPartition(topic, partition)
  val n = 200

  val genObjectMetadata: Gen[TierObjectMetadata] = for {
    epoch <- Gen.posNum[Int]
    startOffset <- Gen.posNum[Long]
    endOffsetDelta <- Gen.posNum[Int]
    lastStableOffset <- Gen.posNum[Long]
    hasTierState <- Gen.oneOf(true, false)
    hasAborts <- Gen.oneOf(true, false)
    maxTimestamp <- Gen.posNum[Long]
    lastModifiedTime <- Gen.posNum[Long]
    size <- Gen.posNum[Int]
  } yield
    new TierObjectMetadata(tp,
                           epoch,
                           startOffset,
                           endOffsetDelta,
                           lastStableOffset,
                           maxTimestamp,
                           size,
                           hasTierState,
                           hasAborts,
                           State.AVAILABLE)

  val genInit: Gen[TierTopicInitLeader] = for {
    epoch <- Gen.posNum[Int]} yield
    new TierTopicInitLeader(tp,
                            epoch,
                            UUID.randomUUID(),
                            brokerId)

  val genMetadata: Gen[AbstractTierMetadata] =
    Gen.oneOf(genObjectMetadata, genInit)

  @Test
  def testSameElementsProperty(): Unit = {
    val prop = forAll(Gen.listOf(genMetadata)) {
      objectMetadatas =>
        val diskstate = new FileTierPartitionState(baseDir, tp, true, false)
        diskstate.beginCatchup()
        diskstate.onCatchUpComplete()
        try {
          val memstate = new MemoryTierPartitionState(baseDir, tp, true)
          memstate.beginCatchup()
          memstate.onCatchUpComplete()
          for (m <- objectMetadatas) {
            memstate.append(m)
            diskstate.append(m)
          }
          diskstate.flush()
          memstate.totalSize == diskstate.totalSize && memstate.endOffset == diskstate.endOffset && memstate.uncommittedEndOffset == diskstate.uncommittedEndOffset
        } finally {
          diskstate.close()
          diskstate.delete()
        }
    }

    assertProperty(prop, defaultVerbose.withMinSuccessfulTests(2000))
  }

  case class OffsetCheck(metadatas: List[AbstractTierMetadata],
                         offset: Long)

  val genOffsetCheck: Gen[OffsetCheck] =
    for {
      numEntries <- Gen.choose(0, 100)
      metadatas <- Gen.listOfN(numEntries, genMetadata)
      offset <- Gen.choose(0, 100000)
    } yield OffsetCheck(metadatas, offset)

  @Test
  def testMetadataForOffsetProperty(): Unit = {
    val prop = forAll(genOffsetCheck) { trial =>
      val diskstate = new FileTierPartitionState(baseDir, tp, true, false)
      diskstate.beginCatchup()
      diskstate.onCatchUpComplete()
      try {
        val memstate = new MemoryTierPartitionState(baseDir, tp, true)
        memstate.beginCatchup()
        memstate.onCatchUpComplete()
        for (m <- trial.metadatas) {
          memstate.append(m)
          diskstate.append(m)
        }

        // flush to make metadata visible
        diskstate.flush()
        val m1 = memstate.metadata(trial.offset)
        val m2 = diskstate.metadata(trial.offset)

        m1.equals(m2)

      } finally {
        diskstate.close()
        diskstate.delete()
      }
    }

    assertProperty(prop, defaultVerbose.withMinSuccessfulTests(2000))
  }
}
