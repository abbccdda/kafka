/*
 Copyright 2018 Confluent Inc.
 */

package integration.kafka.tier

import kafka.log.AbortedTxn
import kafka.tier.tasks.archive.ArchiveTask
import kafka.tier.fetcher.{CancellationContext, TierAbortedTxnReader}
import org.apache.kafka.common.utils.ByteBufferInputStream
import org.apache.kafka.test.IntegrationTest
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.scalacheck.Checkers

import scala.annotation.tailrec
import scala.collection.JavaConverters._

private case class OffsetRange(startOffset: Long, endOffset: Long)

private object Generators {
  def offsetRangesGen(startOffset: Long = 0): Gen[Seq[OffsetRange]] = {
    @tailrec
    def genOffsetRanges(maxCount: Int, delta: Long, seq: Seq[OffsetRange]): Seq[OffsetRange] = {
      if (seq.size == maxCount) seq
      else {
        val lastOffsetRange: OffsetRange = seq.lastOption.getOrElse(OffsetRange(0, 0))
        val newStartOffset: Long = lastOffsetRange.endOffset + 1
        val newRange = OffsetRange(newStartOffset, newStartOffset + delta)
        genOffsetRanges(maxCount, delta,   seq :+ newRange)
      }
    }
    Gen.sized(numRanges => {
      for {
        delta <- Gen.posNum[Long]
      } yield genOffsetRanges(numRanges, delta, Seq())
    })
  }

  def abortedTransactionGen(): Gen[Seq[AbortedTxn]] = {
    for {
      offsetRanges <- offsetRangesGen()
    } yield offsetRanges.map {
      offsetRange: OffsetRange =>
        new AbortedTxn(0, offsetRange.startOffset, offsetRange.endOffset, 0)
    }
  }
}

@Category(Array(classOf[IntegrationTest]))
@RunWith(classOf[JUnitRunner])
class TierAbortedTxnReaderPropertyTest extends FunSuite with Checkers {
  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 10000)

  private implicit val arbOffsetRanges: Arbitrary[Seq[OffsetRange]] = Arbitrary {
    Generators.offsetRangesGen()
  }
  private implicit val arbAbortedTxns: Arbitrary[Seq[AbortedTxn]] = Arbitrary {
    Generators.abortedTransactionGen()
  }

  test("testOffsetsMonotonic") {
    check { // This test verifies the generator is working correctly
      forAll { offsets: Seq[OffsetRange] =>
        var result = passed
        var lastEndOffset: Option[Long] = None
        for (offsetRange <- offsets) {
          lastEndOffset match {
            case Some(previousEndOffset) =>
              if (previousEndOffset > offsetRange.startOffset)
                result = falsified
              else
                lastEndOffset = Some(offsetRange.endOffset)
            case None =>
              lastEndOffset = Some(offsetRange.endOffset)
          }
        }
        result
      }
    }
  }

  private def checkEmptyOrNonEmpty(abortedTxns: Seq[AbortedTxn]): Prop = {
    val abortedTxnsBuf = ArchiveTask.serializeAbortedTransactions(abortedTxns)
    abortedTxnsBuf.isEmpty && abortedTxns.isEmpty || abortedTxnsBuf.nonEmpty && abortedTxns.nonEmpty
  }

  /**
    * Verify that all provided AbortedTxn markers can be serialized and deserialized.
    */
  private def checkReadAllMarkers(abortedTxns: Seq[AbortedTxn]): Prop = {
    ArchiveTask.serializeAbortedTransactions(abortedTxns).map {
      abortedTxnBuf =>
        val inputStream = new ByteBufferInputStream(abortedTxnBuf)
        val readMarkers = TierAbortedTxnReader.readInto(
          CancellationContext.newContext(),
          inputStream,
          0,
          Long.MaxValue
        ).asScala
        abortedTxns.toSet =? readMarkers.toSet
    }.getOrElse(undecided)
  }

  test("testSerializingAbortedTransactions") {
    check {
      forAll { abortedTxns: Seq[AbortedTxn] => {
        checkReadAllMarkers(abortedTxns) && checkEmptyOrNonEmpty(abortedTxns)
      }
      }
    }
  }

  test("testLocateSpecificTransactionRange") {
    check {
      forAll(for {
        abortedTxns <- Generators.abortedTransactionGen().suchThat(_.nonEmpty)
        pick <- Gen.pick(1, abortedTxns).map(_.head)
        // pick our starting offset somewhere inside of `pick`'s range
        startOffset <- Gen.chooseNum[Long](pick.firstOffset, pick.lastOffset)
        endOffset <- Gen.chooseNum[Long](pick.lastOffset + 1, Long.MaxValue)
      } yield (abortedTxns, pick, startOffset, endOffset)) {
        // We root our search around this item to ensure we exercise the range overlap logic in
        // TierAbortedTxnReader.readInto()
        case (abortedTxns: Seq[AbortedTxn], guaranteedItem: AbortedTxn, startOffset: Long, lastOffset: Long) =>
          // Safe to `get()` because we predicate the generator on the set of aborted txns being non empty
          val buffer = ArchiveTask.serializeAbortedTransactions(abortedTxns).get
          val inputStream = new ByteBufferInputStream(buffer)
          val readAbortedTransactions = TierAbortedTxnReader.readInto(CancellationContext.newContext(), inputStream, startOffset, lastOffset).asScala
          all {
            // Check that the item we based our search around is present
            readAbortedTransactions.contains(guaranteedItem)
            // Check that there is no aborted transaction included which starts after the fetch last offset.
            !readAbortedTransactions.exists(_.firstOffset > lastOffset)
            // Check that there is no aborted transaction included which ends before the fetch start offset
            !readAbortedTransactions.exists(_.lastOffset < startOffset)
          }
      }
    }
  }

  private def genNonSequentialAbortedTxns(start: Int, end: Int): Gen[AbortedTxn] = {
    for {
      firstOffset <- Gen.chooseNum(start, end - 1)
      lastOffset <- Gen.chooseNum(firstOffset + 1, end)
      producerId <- Gen.chooseNum(0, 5)
    } yield new AbortedTxn(producerId, firstOffset, lastOffset, 0)
  }

  private def abortedTxnInRange(start: Long, end: Long): AbortedTxn => Boolean = (abortedTxn: AbortedTxn) => {
    start <= abortedTxn.lastOffset && abortedTxn.firstOffset <= end
  }

  test("testNonSequentialAbortedTransactions") {
    check {
      forAll(for {
        rangeStart <- Gen.chooseNum(0, 1000)
        rangeEnd <- Gen.chooseNum(1001, 2000)
        abortedTxn <- Gen.containerOf[Seq, AbortedTxn](genNonSequentialAbortedTxns(rangeStart, rangeEnd))
        fetchStart <- Gen.chooseNum(rangeStart, rangeEnd - 1)
        fetchEnd <- Gen.chooseNum(fetchStart  + 1, rangeEnd)

      } yield (fetchStart, fetchEnd, abortedTxn)) {
        case (fetchStart: Int, fetchEnd: Int, abortedTxns: Seq[AbortedTxn]) =>
          if (abortedTxns.isEmpty) {
            Prop.undecided
          } else {
            val buffer = ArchiveTask.serializeAbortedTransactions(abortedTxns).get
            val inputStream = new ByteBufferInputStream(buffer)
            val readAbortedTxns = TierAbortedTxnReader.readInto(CancellationContext.newContext(), inputStream, fetchStart, fetchEnd)
            val expectedAbortedTxns = abortedTxns.filter(abortedTxnInRange(fetchStart, fetchEnd))
            expectedAbortedTxns.toSet =? readAbortedTxns.asScala.toSet
          }
      }
    }
  }
}
