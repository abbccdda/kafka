/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.util.UUID

import kafka.tier.domain.{AbstractTierMetadata, TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.serdes.State
import kafka.tier.state.{FileTierPartitionState, MemoryTierPartitionState}
import kafka.utils.TestUtils
import org.apache.kafka.test.IntegrationTest
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.scalacheck.Checkers

@Category(Array(classOf[IntegrationTest]))
@RunWith(classOf[JUnitRunner])
class TierPartitionStatePropertyTest extends FunSuite with Checkers {
  val brokerId = 0
  val baseDir = TestUtils.tempDir()
  val topic = UUID.randomUUID().toString
  val topicId = UUID.randomUUID
  val partition = 0
  val tpid = new TopicIdPartition(topic, topicId, partition)
  val tp = tpid.topicPartition()
  val n = 200

  val genObjectMetadata: Gen[TierObjectMetadata] = for {
    epoch <- Gen.posNum[Int]
    startOffset <- Gen.posNum[Long]
    endOffsetDelta <- Gen.posNum[Int]
    lastStableOffset <- Gen.posNum[Long]
    hasTierState <- Gen.oneOf(true, false)
    hasProducerState <- Gen.oneOf(true, false)
    hasAborts <- Gen.oneOf(true, false)
    maxTimestamp <- Gen.posNum[Long]
    lastModifiedTime <- Gen.posNum[Long]
    size <- Gen.posNum[Int]
  } yield
    new TierObjectMetadata(tpid,
                           epoch,
                           startOffset,
                           endOffsetDelta,
                           lastStableOffset,
                           maxTimestamp,
                           size,
                           hasTierState,
                           hasProducerState,
                           hasAborts,
                           State.AVAILABLE)

  val genInit: Gen[TierTopicInitLeader] = for {
    epoch <- Gen.posNum[Int]} yield
    new TierTopicInitLeader(tpid,
                            epoch,
                            UUID.randomUUID(),
                            brokerId)

  val genMetadata: Gen[AbstractTierMetadata] =
    Gen.oneOf(genObjectMetadata, genInit)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 2000)

  test("testSameElementsProperty") {
    check {
      forAll(Gen.listOf(genMetadata)) {
        objectMetadatas =>
          val diskstate = new FileTierPartitionState(baseDir, tp, true)
          diskstate.setTopicIdPartition(tpid)
          diskstate.beginCatchup()
          diskstate.onCatchUpComplete()
          try {
            val memstate = new MemoryTierPartitionState(baseDir, tp, true)
            memstate.setTopicIdPartition(tpid)
            memstate.beginCatchup()
            memstate.onCatchUpComplete()
            for (m <- objectMetadatas) {
              memstate.append(m)
              diskstate.append(m)
            }
            memstate.flush()
            diskstate.flush()
            memstate.totalSize == diskstate.totalSize && memstate.committedEndOffset == diskstate.committedEndOffset && memstate.endOffset == diskstate.endOffset
          } finally {
            diskstate.close()
            diskstate.delete()
          }
      }
    }
  }

  case class OffsetCheck(metadatas: List[AbstractTierMetadata],
                         offset: Long)

  val genOffsetCheck: Gen[OffsetCheck] =
    for {
      numEntries <- Gen.choose(0, 100)
      metadatas <- Gen.listOfN(numEntries, genMetadata)
      offset <- Gen.choose(0, 100000)
    } yield OffsetCheck(metadatas, offset)

  test("testMetadataForOffsetProperty") {
    check {
      forAll(genOffsetCheck) { trial =>
        val diskstate = new FileTierPartitionState(baseDir, tp, true)
        diskstate.setTopicIdPartition(tpid)
        diskstate.beginCatchup()
        diskstate.onCatchUpComplete()
        try {
          val memstate = new MemoryTierPartitionState(baseDir, tp, true)
          memstate.setTopicIdPartition(tpid)
          memstate.beginCatchup()
          memstate.onCatchUpComplete()
          for (m <- trial.metadatas) {
            memstate.append(m)
            diskstate.append(m)
          }

          val m1 = memstate.metadata(trial.offset)
          val m2 = diskstate.metadata(trial.offset)
          m1.equals(m2)
        } finally {
          diskstate.close()
          diskstate.delete()
        }
      }
    }
  }
}
