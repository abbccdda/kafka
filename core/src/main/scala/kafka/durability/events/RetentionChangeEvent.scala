/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.events

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.events.DurabilityEventType.DurabilityEventType
import kafka.durability.events.serdes.{BaseInfo, RetentionChange}
import org.apache.kafka.common.TopicPartition

class RetentionChangeEvent(topicPartition: TopicPartition, timeMs: Long, size: Long, version: Int, epoch: Int,
                           highWaterMark: Long, logStartOffset: Long) extends AbstractDurabilityEvent(topicPartition) {

  private def serialize(): ByteBuffer = {
    val builder = new FlatBufferBuilder(KEY_INITIAL_LENGTH).forceDefaults(true)
    RetentionChange.startRetentionChange(builder)
    RetentionChange.addTimeMs(builder, timeMs)
    RetentionChange.addSize(builder, size)
    val infoOffset = BaseInfo.createBaseInfo(builder, version, epoch, highWaterMark, logStartOffset)
    RetentionChange.addInfo(builder, infoOffset)
    val entryId = RetentionChange.endRetentionChange(builder)
    builder.finish(entryId)
    RetentionChange.getRootAsRetentionChange(builder.dataBuffer()).getByteBuffer.duplicate()
  }

  override lazy val payloadBuffer: ByteBuffer = serialize()

  override def eventType: DurabilityEventType = DurabilityEventType.RetentionChangeType

  override def toString: String = {
    s"[TopicPartition: $topicPartition, retentionMs: $timeMs, retentionBytes: $size, version: $version, epoch: $epoch, highWaterMark: $highWaterMark, logStartOffset: $logStartOffset]"
  }
}

object RetentionChangeEvent {
  def apply(id: TopicPartition, timeMs: Long, size: Long, version: Int, epoch: Int, highWaterMark: Long, logStartOffset: Long): RetentionChangeEvent =
    new RetentionChangeEvent(id, timeMs, size, version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, timeMs: Long, size: Long, epoch: Int, highWaterMark: Long, logStartOffset: Long): RetentionChangeEvent =
    new RetentionChangeEvent(id, timeMs, size, CurrentVersion.version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, data: RetentionChange): RetentionChangeEvent = {
    new RetentionChangeEvent(id, data.timeMs(), data.size(), data.info.version, data.info.epoch, data.info.highWaterMark,
      data.info.logStartOffset)
  }
}
