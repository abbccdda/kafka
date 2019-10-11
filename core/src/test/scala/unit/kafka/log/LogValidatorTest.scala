/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import java.nio.ByteBuffer
import java.util.Collections
import java.util.concurrent.TimeUnit

import com.yammer.metrics.Metrics
import kafka.api.{ApiVersion, KAFKA_2_0_IV1, KAFKA_2_3_IV1}
import kafka.common.{LongRef, RecordValidationException}
import kafka.message._
import kafka.server.BrokerTopicStats
import kafka.utils.TestUtils.meterCount
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{InvalidTimestampException, UnsupportedCompressionTypeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.interceptor.RecordInterceptor
import org.apache.kafka.test.TestUtils
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions
import org.scalatest.Assertions.{assertThrows, intercept}

import scala.collection.JavaConverters._

class LogValidatorTest {

  val time = Time.SYSTEM
  val topicPartition = new TopicPartition("topic", 0)
  val brokerTopicStats = new BrokerTopicStats
  val interceptorStats = new InterceptorStats
  val metricsKeySet = Metrics.defaultRegistry.allMetrics.keySet.asScala

  @Test
  def testOnlyOneBatch(): Unit = {
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V0, CompressionType.GZIP, CompressionType.GZIP)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V1, CompressionType.GZIP, CompressionType.GZIP)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V2, CompressionType.GZIP, CompressionType.GZIP)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V0, CompressionType.GZIP, CompressionType.NONE)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V1, CompressionType.GZIP, CompressionType.NONE)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V2, CompressionType.GZIP, CompressionType.NONE)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, CompressionType.NONE)
    checkOnlyOneBatch(RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, CompressionType.GZIP)
  }

  @Test
  def testAllowMultiBatch(): Unit = {
    checkAllowMultiBatch(RecordBatch.MAGIC_VALUE_V0, CompressionType.NONE, CompressionType.NONE)
    checkAllowMultiBatch(RecordBatch.MAGIC_VALUE_V1, CompressionType.NONE, CompressionType.NONE)
    checkAllowMultiBatch(RecordBatch.MAGIC_VALUE_V0, CompressionType.NONE, CompressionType.GZIP)
    checkAllowMultiBatch(RecordBatch.MAGIC_VALUE_V1, CompressionType.NONE, CompressionType.GZIP)
  }

  @Test
  def testMisMatchMagic(): Unit = {
    checkMismatchMagic(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, CompressionType.GZIP)
    checkMismatchMagic(RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V0, CompressionType.GZIP)
  }

  private def checkOnlyOneBatch(magic: Byte, sourceCompressionType: CompressionType, targetCompressionType: CompressionType): Unit = {
    assertThrows[InvalidRecordException] {
      validateMessages(createTwoBatchedRecords(magic, 0L, sourceCompressionType), magic, sourceCompressionType, targetCompressionType)
    }
  }

  private def checkAllowMultiBatch(magic: Byte, sourceCompressionType: CompressionType, targetCompressionType: CompressionType): Unit = {
    validateMessages(createTwoBatchedRecords(magic, 0L, sourceCompressionType), magic, sourceCompressionType, targetCompressionType)
  }

  private def checkMismatchMagic(batchMagic: Byte, recordMagic: Byte, compressionType: CompressionType): Unit = {
    assertThrows[RecordValidationException] {
      validateMessages(recordsWithInvalidInnerMagic(batchMagic, recordMagic, compressionType), batchMagic, compressionType, compressionType)
      Assertions.fail("InvalidRecordException should have been thrown")
    }
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicStats.InvalidMagicNumberRecordsPerSec}")), 1)
    assertTrue(meterCount(s"${BrokerTopicStats.InvalidMagicNumberRecordsPerSec}") > 0)
  }

  private def validateMessages(records: MemoryRecords, magic: Byte, sourceCompressionType: CompressionType, targetCompressionType: CompressionType): Unit = {
    LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      new LongRef(0L),
      time,
      now = 0L,
      CompressionCodec.getCompressionCodec(sourceCompressionType.name),
      CompressionCodec.getCompressionCodec(targetCompressionType.name),
      compactedTopic = false,
      magic,
      TimestampType.CREATE_TIME,
      1000L,
      Collections.emptyList[RecordInterceptor].asScala,
      interceptorStats,
      RecordBatch.NO_PRODUCER_EPOCH,
      isFromClient = true,
      KAFKA_2_3_IV1,
      brokerTopicStats
    )
  }

  @Test
  def testLogAppendTimeNonCompressedV1(): Unit = {
    checkLogAppendTimeNonCompressed(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkLogAppendTimeNonCompressed(magic: Byte): Unit = {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = magic, timestamp = 1234L, codec = CompressionType.NONE)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time= time,
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = magic,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val validatedRecords = validatedResults.validatedRecords
    assertEquals("message set size should not change", records.records.asScala.size, validatedRecords.records.asScala.size)
    validatedRecords.batches.asScala.foreach(batch => validateLogAppendTime(now, 1234L, batch))
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be 0", 0, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)

    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 0, records,
      compressed = false)
  }

  def testLogAppendTimeNonCompressedV2(): Unit = {
    checkLogAppendTimeNonCompressed(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testLogAppendTimeWithRecompressionV1(): Unit = {
    checkLogAppendTimeWithRecompression(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkLogAppendTimeWithRecompression(targetMagic: Byte): Unit = {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = targetMagic,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val validatedRecords = validatedResults.validatedRecords

    assertEquals("message set size should not change", records.records.asScala.size, validatedRecords.records.asScala.size)
    validatedRecords.batches.asScala.foreach(batch => validateLogAppendTime(now, -1, batch))
    assertTrue("MessageSet should still valid", validatedRecords.batches.iterator.next().isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${records.records.asScala.size - 1}",
      records.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatedResults.messageSizeMaybeChanged)

    val stats = validatedResults.recordConversionStats
    verifyRecordConversionStats(stats, numConvertedRecords = 3, records, compressed = true)
  }

  @Test
  def testLogAppendTimeWithRecompressionV2(): Unit = {
    checkLogAppendTimeWithRecompression(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testLogAppendTimeWithoutRecompressionV1(): Unit = {
    checkLogAppendTimeWithoutRecompression(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkLogAppendTimeWithoutRecompression(magic: Byte): Unit = {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = magic, timestamp = 1234L, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = magic,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val validatedRecords = validatedResults.validatedRecords

    assertEquals("message set size should not change", records.records.asScala.size,
      validatedRecords.records.asScala.size)
    validatedRecords.batches.asScala.foreach(batch => validateLogAppendTime(now, 1234L, batch))
    assertTrue("MessageSet should still valid", validatedRecords.batches.iterator.next().isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${records.records.asScala.size - 1}",
      records.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)

    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 0, records,
      compressed = true)
  }

  @Test
  def testInvalidOffsetRangeAndRecordCount(): Unit = {
    // The batch to be written contains 3 records, so the correct lastOffsetDelta is 2
    validateRecordBatchWithCountOverrides(lastOffsetDelta = 2, count = 3)

    // Count and offset range are inconsistent or invalid
    assertInvalidBatchCountOverrides(lastOffsetDelta = 0, count = 3)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 15, count = 3)
    assertInvalidBatchCountOverrides(lastOffsetDelta = -3, count = 3)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 2, count = -3)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 2, count = 6)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 2, count = 0)
    assertInvalidBatchCountOverrides(lastOffsetDelta = -3, count = -2)

    // Count and offset range are consistent, but do not match the actual number of records
    assertInvalidBatchCountOverrides(lastOffsetDelta = 5, count = 6)
    assertInvalidBatchCountOverrides(lastOffsetDelta = 1, count = 2)
  }

  private def assertInvalidBatchCountOverrides(lastOffsetDelta: Int, count: Int): Unit = {
    intercept[InvalidRecordException] {
      validateRecordBatchWithCountOverrides(lastOffsetDelta, count)
    }
  }

  private def validateRecordBatchWithCountOverrides(lastOffsetDelta: Int, count: Int): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = 1234L, codec = CompressionType.NONE)
    records.buffer.putInt(DefaultRecordBatch.RECORDS_COUNT_OFFSET, count)
    records.buffer.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = time.milliseconds(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
  }

  @Test
  def testLogAppendTimeWithoutRecompressionV2(): Unit = {
    checkLogAppendTimeWithoutRecompression(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testNonCompressedV1(): Unit = {
    checkNonCompressed(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkNonCompressed(magic: Byte): Unit = {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)

    val (producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch) =
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        (1324L, 10.toShort, 984, true, 40)
      else
        (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
          RecordBatch.NO_PARTITION_LEADER_EPOCH)

    val records = MemoryRecords.withRecords(magic, 0L, CompressionType.GZIP, TimestampType.CREATE_TIME, producerId,
      producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional,
      new SimpleRecord(timestampSeq(0), "hello".getBytes),
      new SimpleRecord(timestampSeq(1), "there".getBytes),
      new SimpleRecord(timestampSeq(2), "beautiful".getBytes))

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = magic,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats,
      partitionLeaderEpoch = partitionLeaderEpoch,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val validatedRecords = validatingResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      maybeCheckBaseTimestamp(timestampSeq(0), batch)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      assertEquals(producerEpoch, batch.producerEpoch)
      assertEquals(producerId, batch.producerId)
      assertEquals(baseSequence, batch.baseSequence)
      assertEquals(isTransactional, batch.isTransactional)
      assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch)
      for (record <- batch.asScala) {
        assertTrue(record.isValid)
        assertEquals(timestampSeq(i), record.timestamp)
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)

    verifyRecordConversionStats(validatingResults.recordConversionStats, numConvertedRecords = 0, records,
      compressed = false)
  }

  @Test
  def testNonCompressedV2(): Unit = {
    checkNonCompressed(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testRecompressionV1(): Unit = {
    checkRecompression(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkRecompression(magic: Byte): Unit = {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)

    val (producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch) =
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        (1324L, 10.toShort, 984, true, 40)
      else
        (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
          RecordBatch.NO_PARTITION_LEADER_EPOCH)

    val records = MemoryRecords.withRecords(magic, 0L, CompressionType.GZIP, TimestampType.CREATE_TIME, producerId,
      producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional,
      new SimpleRecord(timestampSeq(0), "hello".getBytes),
      new SimpleRecord(timestampSeq(1), "there".getBytes),
      new SimpleRecord(timestampSeq(2), "beautiful".getBytes))

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = GZIPCompressionCodec,
      compactedTopic = false,
      magic = magic,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats,
      partitionLeaderEpoch = partitionLeaderEpoch,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val validatedRecords = validatingResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      maybeCheckBaseTimestamp(timestampSeq(0), batch)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      assertEquals(producerEpoch, batch.producerEpoch)
      assertEquals(producerId, batch.producerId)
      assertEquals(baseSequence, batch.baseSequence)
      assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch)
      for (record <- batch.asScala) {
        assertTrue(record.isValid)
        assertEquals(timestampSeq(i), record.timestamp)
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals("Offset of max timestamp should be 2", 2, validatingResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size should have been changed", validatingResults.messageSizeMaybeChanged)

    verifyRecordConversionStats(validatingResults.recordConversionStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testRecompressionV2(): Unit = {
    checkRecompression(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testCreateTimeUpConversionV0ToV1(): Unit = {
    checkCreateTimeUpConversionFromV0(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkCreateTimeUpConversionFromV0(toMagic: Byte): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      magic = toMagic,
      compactedTopic = false,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val validatedRecords = validatedResults.validatedRecords

    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      maybeCheckBaseTimestamp(RecordBatch.NO_TIMESTAMP, batch)
      assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp)
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
      assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch)
      assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId)
      assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence)
    }
    assertEquals(s"Max timestamp should be ${RecordBatch.NO_TIMESTAMP}", RecordBatch.NO_TIMESTAMP, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedRecords.records.asScala.size - 1}",
      validatedRecords.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size should have been changed", validatedResults.messageSizeMaybeChanged)

    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testCreateTimeUpConversionV0ToV2(): Unit = {
    checkCreateTimeUpConversionFromV0(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test
  def testCreateTimeUpConversionV1ToV2(): Unit = {
    val timestamp = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, codec = CompressionType.GZIP, timestamp = timestamp)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = timestamp,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      magic = RecordBatch.MAGIC_VALUE_V2,
      compactedTopic = false,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val validatedRecords = validatedResults.validatedRecords

    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      maybeCheckBaseTimestamp(timestamp, batch)
      assertEquals(timestamp, batch.maxTimestamp)
      assertEquals(TimestampType.CREATE_TIME, batch.timestampType)
      assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch)
      assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId)
      assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence)
    }
    assertEquals(timestamp, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedRecords.records.asScala.size - 1}",
      validatedRecords.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertTrue("Message size should have been changed", validatedResults.messageSizeMaybeChanged)

    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testCompressedV1(): Unit = {
    checkCompressed(RecordBatch.MAGIC_VALUE_V1)
  }

  private def checkCompressed(magic: Byte): Unit = {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)

    val (producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch) =
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        (1324L, 10.toShort, 984, true, 40)
      else
        (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false,
          RecordBatch.NO_PARTITION_LEADER_EPOCH)

    val records = MemoryRecords.withRecords(magic, 0L, CompressionType.GZIP, TimestampType.CREATE_TIME, producerId,
      producerEpoch, baseSequence, partitionLeaderEpoch, isTransactional,
      new SimpleRecord(timestampSeq(0), "hello".getBytes),
      new SimpleRecord(timestampSeq(1), "there".getBytes),
      new SimpleRecord(timestampSeq(2), "beautiful".getBytes))

    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      magic = magic,
      compactedTopic = false,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = partitionLeaderEpoch,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val validatedRecords = validatedResults.validatedRecords

    var i = 0
    for (batch <- validatedRecords.batches.asScala) {
      assertTrue(batch.isValid)
      assertEquals(batch.timestampType, TimestampType.CREATE_TIME)
      maybeCheckBaseTimestamp(timestampSeq(0), batch)
      assertEquals(batch.maxTimestamp, batch.asScala.map(_.timestamp).max)
      assertEquals(producerEpoch, batch.producerEpoch)
      assertEquals(producerId, batch.producerId)
      assertEquals(baseSequence, batch.baseSequence)
      assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch)
      for (record <- batch.asScala) {
        assertTrue(record.isValid)
        assertEquals(timestampSeq(i), record.timestamp)
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedRecords.records.asScala.size - 1}",
      validatedRecords.records.asScala.size - 1, validatedResults.shallowOffsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)

    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 0, records,
      compressed = true)
  }

  @Test
  def testCompressedV2(): Unit = {
    checkCompressed(RecordBatch.MAGIC_VALUE_V2)
  }

  @Test(expected = classOf[RecordValidationException])
  def testInvalidCreateTimeNonCompressedV1(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = CompressionType.NONE)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
  }

  @Test(expected = classOf[RecordValidationException])
  def testInvalidCreateTimeNonCompressedV2(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now - 1001L,
      codec = CompressionType.NONE)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
  }

  @Test(expected = classOf[RecordValidationException])
  def testInvalidCreateTimeCompressedV1(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = CompressionType.GZIP)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      magic = RecordBatch.MAGIC_VALUE_V1,
      compactedTopic = false,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
  }

  @Test(expected = classOf[RecordValidationException])
  def testInvalidCreateTimeCompressedV2(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now - 1001L,
      codec = CompressionType.GZIP)
    LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      magic = RecordBatch.MAGIC_VALUE_V1,
      compactedTopic = false,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
  }

  @Test
  def testAbsoluteOffsetAssignmentNonCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      magic = RecordBatch.MAGIC_VALUE_V0,
      compactedTopic = false,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testAbsoluteOffsetAssignmentCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V0,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressedV1(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressedV2(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(records, 0)
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressedV1(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, timestamp = now, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressedV2(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV0ToV1NonCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    val offset = 1234567
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    checkOffsets(validatedResults.validatedRecords, offset)
    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 3, records,
      compressed = false)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV0ToV2NonCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    val offset = 1234567
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    checkOffsets(validatedResults.validatedRecords, offset)
    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 3, records,
      compressed = false)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV0ToV1Compressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    checkOffsets(validatedResults.validatedRecords, offset)
    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV0ToV2Compressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    checkOffsets(validatedResults.validatedRecords, offset)
    verifyRecordConversionStats(validatedResults.recordConversionStats, numConvertedRecords = 3, records,
      compressed = true)
  }

  @Test(expected = classOf[InvalidRecordException])
  def testControlRecordsNotAllowedFromClients(): Unit = {
    val offset = 1234567
    val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    val records = MemoryRecords.withEndTransactionMarker(23423L, 5, endTxnMarker)
    LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.CURRENT_MAGIC_VALUE,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
  }

  @Test
  def testControlRecordsNotCompressed(): Unit = {
    val offset = 1234567
    val endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, 0)
    val records = MemoryRecords.withEndTransactionMarker(23423L, 5, endTxnMarker)
    val result = LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = SnappyCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.CURRENT_MAGIC_VALUE,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = false,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
    val batches = TestUtils.toList(result.validatedRecords.batches)
    assertEquals(1, batches.size)
    val batch = batches.get(0)
    assertFalse(batch.isCompressed)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV1ToV0NonCompressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V0,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV1ToV0Compressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V1, now, CompressionType.GZIP)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V0,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV1ToV2NonCompressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    val offset = 1234567
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterUpConversionV1ToV2Compressed(): Unit = {
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V1, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV2ToV1NonCompressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV2ToV1Compressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.GZIP)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test(expected = classOf[UnsupportedForMessageFormatException])
  def testDownConversionOfTransactionalRecordsNotPermitted(): Unit = {
    val offset = 1234567
    val producerId = 1344L
    val producerEpoch = 16.toShort
    val sequence = 0
    val records = MemoryRecords.withTransactionalRecords(CompressionType.NONE, producerId, producerEpoch, sequence,
      new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes), new SimpleRecord("beautiful".getBytes))
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test(expected = classOf[UnsupportedForMessageFormatException])
  def testDownConversionOfIdempotentRecordsNotPermitted(): Unit = {
    val offset = 1234567
    val producerId = 1344L
    val producerEpoch = 16.toShort
    val sequence = 0
    val records = MemoryRecords.withIdempotentRecords(CompressionType.NONE, producerId, producerEpoch, sequence,
      new SimpleRecord("hello".getBytes), new SimpleRecord("there".getBytes), new SimpleRecord("beautiful".getBytes))
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V1,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV2ToV0NonCompressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, codec = CompressionType.NONE)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V0,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testOffsetAssignmentAfterDownConversionV2ToV0Compressed(): Unit = {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.GZIP)
    checkOffsets(records, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V0,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats).validatedRecords, offset)
  }

  @Test
  def testNonIncreasingOffsetRecordBatchHasMetricsLogged(): Unit = {
    val records = createNonIncreasingOffsetRecords(RecordBatch.MAGIC_VALUE_V2)
    records.batches().asScala.head.setLastOffset(2)
    assertThrows[InvalidRecordException] {
      LogValidator.validateMessagesAndAssignOffsets(records,
        topicPartition,
        offsetCounter = new LongRef(0L),
        time = time,
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        compactedTopic = false,
        magic = RecordBatch.MAGIC_VALUE_V0,
        timestampType = TimestampType.CREATE_TIME,
        timestampDiffMaxMs = 5000L,
        interceptors = Collections.emptyList().asScala,
        interceptorStats,
        partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
        isFromClient = true,
        interBrokerProtocolVersion = ApiVersion.latestVersion,
        brokerTopicStats = brokerTopicStats)
    }
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec}")), 1)
    assertTrue(meterCount(s"${BrokerTopicStats.InvalidOffsetOrSequenceRecordsPerSec}") > 0)
  }

  @Test(expected = classOf[InvalidRecordException])
  def testCompressedBatchWithoutRecordsNotAllowed(): Unit = {
    testBatchWithoutRecordsNotAllowed(DefaultCompressionCodec, DefaultCompressionCodec)
  }

  @Test(expected = classOf[UnsupportedCompressionTypeException])
  def testZStdCompressedWithUnavailableIBPVersion(): Unit = {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = 1234L, codec = CompressionType.NONE)
    LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(0),
      time= time,
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = ZStdCompressionCodec,
      compactedTopic = false,
      magic = RecordBatch.MAGIC_VALUE_V2,
      timestampType = TimestampType.LOG_APPEND_TIME,
      timestampDiffMaxMs = 1000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = KAFKA_2_0_IV1,
      brokerTopicStats = brokerTopicStats)
  }

  @Test(expected = classOf[InvalidRecordException])
  def testUncompressedBatchWithoutRecordsNotAllowed(): Unit = {
    testBatchWithoutRecordsNotAllowed(NoCompressionCodec, NoCompressionCodec)
  }

  @Test(expected = classOf[InvalidRecordException])
  def testRecompressedBatchWithoutRecordsNotAllowed(): Unit = {
    testBatchWithoutRecordsNotAllowed(NoCompressionCodec, DefaultCompressionCodec)
  }

  @Test
  def testInvalidTimestampExceptionHasBatchIndex(): Unit = {
    val now = System.currentTimeMillis()
    val records = createRecords(magicValue = RecordBatch.MAGIC_VALUE_V2, timestamp = now - 1001L,
      codec = CompressionType.GZIP)
    val e = intercept[RecordValidationException] {
      LogValidator.validateMessagesAndAssignOffsets(
        records,
        topicPartition,
        offsetCounter = new LongRef(0),
        time = time,
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        magic = RecordBatch.MAGIC_VALUE_V1,
        compactedTopic = false,
        timestampType = TimestampType.CREATE_TIME,
        timestampDiffMaxMs = 1000L,
        partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
        isFromClient = true,
        interBrokerProtocolVersion = ApiVersion.latestVersion,
        brokerTopicStats = brokerTopicStats,
        interceptors = Collections.emptyList().asScala,
        interceptorStats = interceptorStats)
    }

    assertTrue(e.invalidException.isInstanceOf[InvalidTimestampException])
    assertTrue(e.recordErrors.nonEmpty)
    assertEquals(e.recordErrors.size, 1)
    assertEquals(e.recordErrors.head.batchIndex, 0)
    assertNull(e.recordErrors.head.message)
  }

  @Test
  def testInvalidRecordExceptionHasBatchIndex(): Unit = {
    val e = intercept[RecordValidationException] {
      validateMessages(recordsWithInvalidInnerMagic(
        RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, CompressionType.GZIP),
        RecordBatch.MAGIC_VALUE_V0, CompressionType.GZIP, CompressionType.GZIP)
    }

    assertTrue(e.invalidException.isInstanceOf[InvalidRecordException])
    assertTrue(e.recordErrors.nonEmpty)
    assertEquals(e.recordErrors.size, 1)
    assertEquals(e.recordErrors.head.batchIndex, 0)
    assertNull(e.recordErrors.head.message)
  }

  private def testBatchWithoutRecordsNotAllowed(sourceCodec: CompressionCodec, targetCodec: CompressionCodec): Unit = {
    val offset = 1234567
    val (producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch) =
      (1324L, 10.toShort, 984, true, 40)
    val buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD)
    DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.CURRENT_MAGIC_VALUE, producerId, producerEpoch,
      baseSequence, 0L, 5L, partitionLeaderEpoch, TimestampType.CREATE_TIME, System.currentTimeMillis(),
      isTransactional, false)
    buffer.flip()
    val records = MemoryRecords.readableRecords(buffer)
    LogValidator.validateMessagesAndAssignOffsets(records,
      topicPartition,
      offsetCounter = new LongRef(offset),
      time = time,
      now = System.currentTimeMillis(),
      sourceCodec = sourceCodec,
      targetCodec = targetCodec,
      compactedTopic = false,
      magic = RecordBatch.CURRENT_MAGIC_VALUE,
      timestampType = TimestampType.CREATE_TIME,
      timestampDiffMaxMs = 5000L,
      interceptors = Collections.emptyList().asScala,
      interceptorStats = interceptorStats,
      partitionLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH,
      isFromClient = true,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      brokerTopicStats = brokerTopicStats)
  }

  private def createRecords(magicValue: Byte,
                            timestamp: Long = RecordBatch.NO_TIMESTAMP,
                            codec: CompressionType): MemoryRecords = {
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
    builder.appendWithOffset(0, timestamp, null, "hello".getBytes)
    builder.appendWithOffset(1, timestamp, null, "there".getBytes)
    builder.appendWithOffset(2, timestamp, null, "beautiful".getBytes)
    builder.build()
  }

  private def createNonIncreasingOffsetRecords(magicValue: Byte,
                                               timestamp: Long = RecordBatch.NO_TIMESTAMP,
                                               codec: CompressionType = CompressionType.NONE): MemoryRecords = {
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
    builder.appendWithOffset(0, timestamp, null, "hello".getBytes)
    builder.appendWithOffset(2, timestamp, null, "there".getBytes)
    builder.appendWithOffset(3, timestamp, null, "beautiful".getBytes)
    builder.build()
  }

  private def createTwoBatchedRecords(magicValue: Byte,
                                      timestamp: Long,
                                      codec: CompressionType): MemoryRecords = {
    val buf = ByteBuffer.allocate(2048)
    var builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
    builder.append(10L, "1".getBytes(), "a".getBytes())
    builder.close()
    builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 1L)
    builder.append(11L, "2".getBytes(), "b".getBytes())
    builder.append(12L, "3".getBytes(), "c".getBytes())
    builder.close()

    buf.flip()
    MemoryRecords.readableRecords(buf.slice())
  }

  /* check that offsets are assigned consecutively from the given base offset */
  def checkOffsets(records: MemoryRecords, baseOffset: Long): Unit = {
    assertTrue("Message set should not be empty", records.records.asScala.nonEmpty)
    var offset = baseOffset
    for (entry <- records.records.asScala) {
      assertEquals("Unexpected offset in message set iterator", offset, entry.offset)
      offset += 1
    }
  }

  private def recordsWithInvalidInnerMagic(batchMagicValue: Byte,
                                           recordMagicValue: Byte,
                                           codec: CompressionType): MemoryRecords = {
    val records = (0 until 20).map(id =>
      LegacyRecord.create(recordMagicValue,
        RecordBatch.NO_TIMESTAMP,
        id.toString.getBytes,
        id.toString.getBytes))

    val buffer = ByteBuffer.allocate(math.min(math.max(records.map(_.sizeInBytes()).sum / 2, 1024), 1 << 16))
    val builder = MemoryRecords.builder(buffer, batchMagicValue, codec,
      TimestampType.CREATE_TIME, 0L)

    var offset = 1234567
    records.foreach { record =>
      builder.appendUncheckedWithOffset(offset, record)
      offset += 1
    }

    builder.build()
  }

  def maybeCheckBaseTimestamp(expected: Long, batch: RecordBatch): Unit = {
    batch match {
      case b: DefaultRecordBatch =>
        assertEquals(s"Unexpected base timestamp of batch $batch", expected, b.firstTimestamp)
      case _ => // no-op
    }
  }

  /**
    * expectedLogAppendTime is only checked if batch.magic is V2 or higher
    */
  def validateLogAppendTime(expectedLogAppendTime: Long, expectedBaseTimestamp: Long, batch: RecordBatch): Unit = {
    assertTrue(batch.isValid)
    assertTrue(batch.timestampType == TimestampType.LOG_APPEND_TIME)
    assertEquals(s"Unexpected max timestamp of batch $batch", expectedLogAppendTime, batch.maxTimestamp)
    maybeCheckBaseTimestamp(expectedBaseTimestamp, batch)
    for (record <- batch.asScala) {
      assertTrue(record.isValid)
      assertEquals(s"Unexpected timestamp of record $record", expectedLogAppendTime, record.timestamp)
    }
  }

  def verifyRecordConversionStats(stats: RecordConversionStats, numConvertedRecords: Int, records: MemoryRecords,
                                  compressed: Boolean): Unit = {
    assertNotNull("Records processing info is null", stats)
    assertEquals(numConvertedRecords, stats.numRecordsConverted)
    if (numConvertedRecords > 0) {
      assertTrue(s"Conversion time not recorded $stats", stats.conversionTimeNanos >= 0)
      assertTrue(s"Conversion time not valid $stats", stats.conversionTimeNanos <= TimeUnit.MINUTES.toNanos(1))
    }
    val originalSize = records.sizeInBytes
    val tempBytes = stats.temporaryMemoryBytes
    if (numConvertedRecords > 0 && compressed)
      assertTrue(s"Temp bytes too small, orig=$originalSize actual=$tempBytes", tempBytes > originalSize)
    else if (numConvertedRecords > 0 || compressed)
      assertTrue("Temp bytes not updated", tempBytes > 0)
    else
      assertEquals(0, tempBytes)
  }
}
