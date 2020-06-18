/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.events

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.events.DurabilityEventType.DurabilityEventType
import kafka.durability.events.serdes.{BaseInfo, OffsetChange}
import org.apache.kafka.common.TopicPartition

class OffsetChangeEvent(topicPartition: TopicPartition, version : Int, val epoch: Int, val highWaterMark: Long, val logStartOffset: Long)
  extends AbstractDurabilityEvent(topicPartition) {

  private def serialize(): ByteBuffer = {
    val builder = new FlatBufferBuilder(KEY_INITIAL_LENGTH).forceDefaults(true)
    OffsetChange.startOffsetChange(builder)
    val infoOffset = BaseInfo.createBaseInfo(builder, version, epoch, highWaterMark, logStartOffset)
    OffsetChange.addInfo(builder, infoOffset)
    val entryId = OffsetChange.endOffsetChange(builder)
    builder.finish(entryId)
    OffsetChange.getRootAsOffsetChange(builder.dataBuffer()).getByteBuffer.duplicate()
  }

  override lazy val payloadBuffer: ByteBuffer = serialize()

  override def eventType: DurabilityEventType = DurabilityEventType.OffsetChangeType

  override def toString: String = {
    s"[TopicPartition: $topicPartition, version: $version, epoch: $epoch, highWaterMark: $highWaterMark, logStartOffset: $logStartOffset]"
  }
}

object OffsetChangeEvent {
  def apply(id: TopicPartition, version: Int, epoch: Int, highWaterMark: Long, logStartOffset: Long): OffsetChangeEvent =
    new OffsetChangeEvent(id, version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, epoch: Int, highWaterMark: Long, logStartOffset: Long): OffsetChangeEvent =
    new OffsetChangeEvent(id, CurrentVersion.version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, data: OffsetChange): OffsetChangeEvent =
    new OffsetChangeEvent(id, data.info.version, data.info.epoch, data.info.highWaterMark, data.info.logStartOffset)
}
