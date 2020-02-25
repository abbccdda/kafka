/*
Copyright 2018 Confluent Inc.
*/

package kafka.tier.fetcher

import java.io.EOFException
import java.nio.ByteBuffer
import java.util
import java.util.Optional
import java.util.function.Consumer

import kafka.tier.fetcher.TierSegmentReader.NextOffsetAndBatchMetadata
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.ByteBufferInputStream
import org.apache.kafka.test.IntegrationTest
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatestplus.junit.JUnitRunner
import org.scalatestplus.scalacheck.Checkers

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

/**
 * Defines the parameters needed to construct a record batch
 */
case class RecordBatchDefiniton(numRecords: Int, magic: Byte, compressionType: CompressionType, timestamps: List[Long], key: String, value: String) {
  def generateBatch(baseOffset: Long): MemoryRecords = {
    val records: Array[SimpleRecord] = new Array[SimpleRecord](numRecords)
    for (i <- 0 until numRecords) {
      records(i) = new SimpleRecord(timestamps(i), (key + i).getBytes(), (value + i).getBytes())
    }
    if (records.isEmpty) {
      MemoryRecords.EMPTY
    } else {
      MemoryRecords.withRecords(baseOffset, compressionType, records.toSeq: _*)
    }
  }
}

object RecordBatchDefiniton {
  val gen: Gen[RecordBatchDefiniton] = for {
    numRecords <- Gen.posNum[Int]
    timestamps <- Gen.listOfN(numRecords, Gen.posNum[Long])
    magic <- Gen.oneOf(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)
    key <- Gen.alphaNumStr
    value <- Gen.alphaNumStr
    compressionType <- Gen.oneOf(CompressionType.NONE, CompressionType.SNAPPY, CompressionType.ZSTD, CompressionType.GZIP, CompressionType.LZ4)
  } yield RecordBatchDefiniton(numRecords, magic, compressionType, timestamps, key, value)
}

/**
  * Provides a container for List[RecordBatchDefinition], and methods to generate a corresponding ByteBuffer.
  * Also contains metadata about the virtual "segment".
  */
case class SegmentViewDefinition(recordBatchDefinitions: List[RecordBatchDefiniton]) {
  val memoryRecords: List[MemoryRecords] = constructBatches(recordBatchDefinitions)
  val recordBatches: List[RecordBatch] = SegmentViewDefinition.flattenMemoryRecords(memoryRecords)
  val batchOffsetAndPositionMap: util.TreeMap[Long, Int] = new util.TreeMap()
  private val byteBuffer: ByteBuffer = concatenateBatches(memoryRecords)

  val baseOffset: Long = memoryRecords.head.batches.asScala.collectFirst {
    case v => v.baseOffset
  }.getOrElse(0)
  val lastOffset: Long = memoryRecords.last.batches.asScala.lastOption.map(_.lastOffset).getOrElse(0)

  locally {
    var position = 0
    for (batch <- recordBatches.iterator) {
      batchOffsetAndPositionMap.put(batch.baseOffset, position)
      position += batch.sizeInBytes
    }
  }

  private def concatenateBatches(batches: List[MemoryRecords]): ByteBuffer = {
    val totalSize = batches.foldRight(0)((records: MemoryRecords, acc) => { acc + records.sizeInBytes })
    val buffer = ByteBuffer.allocate(totalSize)
    batches.foreach { records =>
      buffer.put(records.buffer())
      records.buffer().rewind()
    }
    buffer.flip()
    buffer
  }

  private def constructBatches(batchDefinitons: List[RecordBatchDefiniton]): List[MemoryRecords] = {
    batchDefinitons.foldRight(List[(Int, MemoryRecords)]()) { (definition, acc) =>
      val prevEndOffset = acc.lastOption.map { case (endOffset, _) => endOffset }.getOrElse(-1)
      val currentBaseOffset = prevEndOffset + 1
      val memRecords = definition.generateBatch(currentBaseOffset)
      acc :+ (currentBaseOffset + definition.numRecords - 1, memRecords)
    }.map { case (_, memoryRecords) => memoryRecords }
  }

  def bytesAsInputStream(startPosition: Int): ByteBufferInputStream = {
    val duplicate = byteBuffer.duplicate()
    duplicate.position(duplicate.position() + startPosition)
    new ByteBufferInputStream(duplicate)
  }

  def bytesAvailable(): Int = {
    byteBuffer.limit() - byteBuffer.position()
  }

  def recordBatchForOffset(offset: Long): Option[RecordBatch] = {
    recordBatches.find { rb =>
      rb.baseOffset() <= offset && rb.lastOffset() >= offset
    }
  }

  def position: Int = byteBuffer.position

  def totalSize: Int = byteBuffer.limit

  /**
    * Check that all RecordBatches in `testList` match the record batches contained within this SegmentViewDefinition.
    * Checks that baseOffset, lastOffset, and checksum are the same.
    */
  def checkRecordBatchesMatch(testList: List[RecordBatch]): Boolean = {
    !recordBatches.zip(testList).exists { case (expected, result) =>
      expected.baseOffset() != result.baseOffset() ||
        expected.lastOffset() != result.lastOffset() ||
        expected.checksum() != result.checksum()
    }
  }

  /**
    * Check that the size (both in bytes and in count) of `testList` matches the record batches contained within
    * this SegmentViewDefinition.
    */
  def checkRecordBatchCountAndSize(testList: List[RecordBatch]): Boolean = {
    recordBatches.size == testList.size &&
    recordBatches.map(_.sizeInBytes()).sum == testList.map(_.sizeInBytes()).sum
  }

  def possibleStartPositionsForOffset(offset: Long): List[Int] = {
    batchOffsetAndPositionMap.headMap(offset, true).asScala.values.toList
  }
}

object SegmentViewDefinition {
  val gen: Gen[SegmentViewDefinition] = {
    Gen.nonEmptyContainerOf[List, RecordBatchDefiniton](RecordBatchDefiniton.gen)
      .map((batches: List[RecordBatchDefiniton]) => SegmentViewDefinition(batches))
  }

  def flattenMemoryRecords(memoryRecordsList: List[MemoryRecords]): List[RecordBatch] = {
    var batches: List[RecordBatch] = List()
    for (memoryRecords <- memoryRecordsList) {
      for (batch <- memoryRecords.batches().asScala) {
        batches :+= batch
      }
    }
    batches
  }

  def byteBuffer: ByteBuffer = {
    this.byteBuffer.duplicate()
  }
}

/**
  * Wrapper class for data needed when calling `TierSegmentReader.loadRecords()`.
  */

case class LoadRecordsRequestDefinition(segmentViewDefinition: SegmentViewDefinition,
                                        targetOffset: Long,
                                        startPosition: Int,
                                        maxBytes: Int) {
  private def firstBatchContainsTargetOffset(loadedRecordBatches: List[RecordBatch]): Boolean = {
    // 1. If both the expected and loaded batches are empty, return true.
    if (loadedRecordBatches.isEmpty && segmentViewDefinition.recordBatches.isEmpty) {
      true
    } else {
      val firstLoadedBatch = loadedRecordBatches.head
      // 2. We always expect the first batch to contain the target offset, regardless of maxBytes.
      if (firstLoadedBatch.baseOffset() <= this.targetOffset && firstLoadedBatch.lastOffset() >= this.targetOffset) {
        true
      } else {
        false
      }
    }
  }

  private def maxBytesRespected(loadedRecordBatches: List[RecordBatch]): Boolean = {
    val firstExpectedRecordBatch = segmentViewDefinition.recordBatchForOffset(targetOffset).get
    if (this.maxBytes <= firstExpectedRecordBatch.sizeInBytes()) {
      // 1. If maxBytes is <= the first batch size, ensure there are no more batches besides the first batch.
      loadedRecordBatches.size == 1
    } else {
      // 2. maxBytes is greater than the size of the first expected record batch, so ensure we don't violate maxBytes.
      val totalSizeOfLoadedBatches = loadedRecordBatches.foldRight(0)((rb, acc) => { acc + rb.sizeInBytes() })
      totalSizeOfLoadedBatches <= maxBytes
    }
  }

  private def validateNextBatchMetadata(batches: List[RecordBatch],
                                        metadata: NextOffsetAndBatchMetadata): Boolean = {
    if (metadata == null)
      return true

    val nextOffset = metadata.nextOffset
    val nextBytePositionOpt = Option(metadata.nextBatchMetadata).map(_.bytePosition)
    val nextBatchSizeOpt = Option(metadata.nextBatchMetadata).flatMap(_.recordBatchSize.asScala)

    val expectedNextOffset = batches.last.nextOffset
    val expectedNextBytePosition = segmentViewDefinition.batchOffsetAndPositionMap.get(expectedNextOffset)
    val expectedNextBatchSize = segmentViewDefinition.recordBatchForOffset(expectedNextOffset).map(_.sizeInBytes)

    nextOffset == expectedNextOffset &&
      nextBytePositionOpt.map(_ == expectedNextBytePosition).getOrElse(true) &&
      nextBatchSizeOpt.map(_ == expectedNextBatchSize.get).getOrElse(true)
  }

  /**
    * Checks the following conditions:
    * 1. Always return at least 1 record batch regardless of what maxBytes is set to, as long as the segment has a
    *    valid record batch for the target offset.
    * 2. If maxBytes is <= the size of the first record batch containing our offset, we only return that first record
    *    batch
    * 3. If maxBytes is > the size of the first record batch containing our offset, we return record batches up to
    *    maxBytes without returning partial record batches.
    */
  def checkRecordBatches(loadedRecordBatches: List[RecordBatch], nextOffsetAndBatchMetadata: NextOffsetAndBatchMetadata): Boolean = {
    firstBatchContainsTargetOffset(loadedRecordBatches) &&
      maxBytesRespected(loadedRecordBatches) &&
      validateNextBatchMetadata(loadedRecordBatches, nextOffsetAndBatchMetadata)
  }
}

object LoadRecordsRequestDefinition {
  val gen: Gen[LoadRecordsRequestDefinition] = {
    for {
      segmentViewDef <- SegmentViewDefinition.gen
      targetOffset <- Gen.chooseNum(segmentViewDef.baseOffset, segmentViewDef.lastOffset)
      startPosition <- Gen.oneOf(segmentViewDef.possibleStartPositionsForOffset(targetOffset))
      maxBytes <- Gen.chooseNum(0, segmentViewDef.bytesAvailable())
    } yield LoadRecordsRequestDefinition(segmentViewDef, targetOffset, startPosition, maxBytes)
  }
}

@Category(Array(classOf[IntegrationTest]))
@RunWith(classOf[JUnitRunner])
class TierSegmentReaderPropertyTest extends FunSuite with Checkers {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000, minSize = 10, sizeRange = 100, workers = 4)

  private def readRecordBatches(inputStream: ByteBufferInputStream, segmentSize: Int): List[RecordBatch] = {
    var continue = true
    var batches: List[RecordBatch] = List()
    val reader = new TierSegmentReader("")

    while (continue) {
      try {
        batches :+= reader.readBatch(inputStream, segmentSize)
      } catch {
        case _: EOFException => continue = false
      }
    }
    batches
  }

  test("readBatchPropertyTest") {
    check {
      forAll(SegmentViewDefinition.gen) { segmentViewDefinition: SegmentViewDefinition =>
        val resultRecords = readRecordBatches(segmentViewDefinition.bytesAsInputStream(0), segmentViewDefinition.totalSize)
        segmentViewDefinition.checkRecordBatchCountAndSize(resultRecords) &&
          resultRecords.forall(r => r.isValid) &&
          segmentViewDefinition.checkRecordBatchesMatch(resultRecords)
      }.viewSeed("readBatchPropertyTest")
    }
  }

  test("offsetForTimestampTest") {
    check {
      forAll(SegmentViewDefinition.gen, Gen.posNum[Long]) { (segmentViewDefinition: SegmentViewDefinition, targetTimestamp: Long) =>
        val inputStream = segmentViewDefinition.bytesAsInputStream(0)
        val cancellationContext = CancellationContext.newContext()
        val offsetTimestampList = new java.util.ArrayList[(Long, Long)]
        val reader = new TierSegmentReader("")

        segmentViewDefinition.memoryRecords.foreach(
          _.batches().forEach(new Consumer[MutableRecordBatch] {
            override def accept(recordBatch: MutableRecordBatch): Unit = {
              recordBatch.forEach(
                new Consumer[Record] {
                  override def accept(record: Record): Unit = {
                    offsetTimestampList.add((record.timestamp(), record.offset()))
                  }
                })
            }
          }))

        val expectedOffsetForTimestamp = offsetTimestampList.asScala.collectFirst {
          case (ts, offset) if ts >= targetTimestamp => offset.asInstanceOf[java.lang.Long]
        }
        expectedOffsetForTimestamp match {
          case Some(expected) =>
            Optional.of(expected) ==
              reader.offsetForTimestamp(cancellationContext, inputStream, targetTimestamp, segmentViewDefinition.totalSize)

          case None =>
            // should not exist in file, so we should hit an EOF exception
            try {
              reader.offsetForTimestamp(cancellationContext, inputStream, targetTimestamp, segmentViewDefinition.totalSize)
              false
            } catch {
              case _: EOFException => true
              case _: Throwable => false
            }
        }
      }.viewSeed("offsetForTimestampTest")
    }
  }

  test("loadTargetOffsetPropertyTest") {
    check {
      val reader = new TierSegmentReader("")

      forAll(SegmentViewDefinition.gen) { segmentViewDefinition =>
        val inputStream = segmentViewDefinition.bytesAsInputStream(0)
        val bytesAvailable = segmentViewDefinition.bytesAvailable()
        val cancellationContext = CancellationContext.newContext()
        val resultMemoryRecords = reader.readRecords(cancellationContext, Optional.empty(), inputStream, bytesAvailable, 0, segmentViewDefinition.position, segmentViewDefinition.totalSize).records
        val resultRecords = SegmentViewDefinition.flattenMemoryRecords(List(resultMemoryRecords))

        segmentViewDefinition.checkRecordBatchCountAndSize(resultRecords) &&
          resultRecords.forall(_.isValid) &&
          segmentViewDefinition.checkRecordBatchesMatch(resultRecords)
      }.viewSeed("loadTargetOffsetPropertyTest")
    }
  }

  test("loadTargetOffsetMaxBytesPropertyTest") {
    val reader = new TierSegmentReader("")

    check {
      forAll(LoadRecordsRequestDefinition.gen) { loadRecordsRequestDef =>
        val segmentViewDefinition = loadRecordsRequestDef.segmentViewDefinition
        val startPosition = loadRecordsRequestDef.startPosition
        val inputStream = segmentViewDefinition.bytesAsInputStream(startPosition)
        val cancellationContext = CancellationContext.newContext()
        val targetOffset = loadRecordsRequestDef.targetOffset
        val maxBytes = loadRecordsRequestDef.maxBytes
        val result = reader.readRecords(cancellationContext, Optional.empty(), inputStream, maxBytes, targetOffset, startPosition, segmentViewDefinition.totalSize)
        val resultMemoryRecords = result.records
        val resultRecordBatches = SegmentViewDefinition.flattenMemoryRecords(List(resultMemoryRecords))
        loadRecordsRequestDef.checkRecordBatches(resultRecordBatches, result.nextOffsetAndBatchMetadata)
      }.viewSeed("loadTargetOffsetMaxBytesPropertyTest")
    }
  }
}
