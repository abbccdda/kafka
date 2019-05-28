/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.nio.ByteBuffer

import kafka.log.AbstractLog
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.{BufferSupplier, MemoryRecords, Record, RecordBatch, SimpleRecord}
import org.junit.Assert.assertTrue

import scala.collection.JavaConverters._

object TierUtils {
  def ensureTierable(log: AbstractLog, tierEndOffset: Long, topicPartition: TopicPartition, leaderEpoch: Int = 0): Unit = {
    val activeSegment = log.activeSegment

    // ensure active segment is not one of the tierable segments
    if (activeSegment.baseOffset <= tierEndOffset && activeSegment.readNextOffset > tierEndOffset)
      log.roll(None)

    // if the end of the log equals tierEndOffset, we must append another message so we are able to flush past the
    // tierEndOffset
    if (tierEndOffset == log.logEndOffset)
      log.appendAsFollower(createRecords(5, topicPartition, log.logEndOffset, leaderEpoch))

    // ensure tierable portion of the log is flushed
    log.flush()

    // ensure tierable portion of the log is below the highwatermark
    if (log.getHighWatermark.getOrElse(0L) <= tierEndOffset)
      log.onHighWatermarkIncremented(tierEndOffset + 1)

    // ensure tierable portion of the log is below the firstUnstableOffset
    assertTrue(log.firstUnstableOffset.map(_.messageOffset).getOrElse(Long.MaxValue) > tierEndOffset)
  }

  def createRecords(n: Int, partition: TopicPartition, baseOffset: Long, leaderEpoch: Int): MemoryRecords = {
    val recList = Range(0, n).map(_ =>
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
    val records = TestUtils.records(records = recList, baseOffset = baseOffset)
    val filtered = ByteBuffer.allocate(100 * n)
    records.batches().asScala.foreach(_.setPartitionLeaderEpoch(leaderEpoch))
    records.filterTo(partition, new RecordFilter {
      override protected def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetention =
        RecordFilter.BatchRetention.DELETE_EMPTY
      override protected def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean =
        true
    }, filtered, Int.MaxValue, BufferSupplier.NO_CACHING)
    filtered.flip()
    MemoryRecords.readableRecords(filtered)
  }

  def awaitTierTopicPartition(broker: KafkaServer, partition: Integer): Unit = {
    TestUtils.waitUntilTrue(() => {
      broker.replicaManager.nonOfflinePartition(new TopicPartition(Topic.TIER_TOPIC_NAME, partition)).isDefined
    }, "Timed out waiting for replicas to join ISR")
  }

}
