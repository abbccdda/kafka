/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.events

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.events.DurabilityEventType.DurabilityEventType
import kafka.durability.events.serdes.{BaseInfo, EpochChange}
import org.apache.kafka.common.TopicPartition

class EpochChangeEvent(topicPartition: TopicPartition, firstOffset: Long, version: Int, epoch: Int,
                       highWaterMark: Long, logStartOffset: Long) extends AbstractDurabilityEvent(topicPartition) {

  private def serialize(): ByteBuffer = {
     val builder = new FlatBufferBuilder(KEY_INITIAL_LENGTH).forceDefaults(true)

     EpochChange.startEpochChange(builder)
     EpochChange.addFirstOffset(builder, firstOffset)
     EpochChange.addInfo(builder, BaseInfo.createBaseInfo(builder, version, epoch, highWaterMark, logStartOffset))
     val entryId = EpochChange.endEpochChange(builder)

     builder.finish(entryId)
     EpochChange.getRootAsEpochChange(builder.dataBuffer()).getByteBuffer.duplicate()
  }

  override lazy val payloadBuffer: ByteBuffer = serialize()

  override def eventType: DurabilityEventType = DurabilityEventType.EpochChangeType

  override def toString: String = {
    s"[TopicPartition: $topicPartition, firstOffset: $firstOffset, version: $version, epoch: $epoch, highWaterMark: $highWaterMark, logStartOffset: $logStartOffset]"
  }
}

object EpochChangeEvent {
  def apply(id: TopicPartition, firstOffset: Long, version: Int, epoch: Int, highWaterMark: Long, logStartOffset: Long): EpochChangeEvent =
    new EpochChangeEvent(id, firstOffset, version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, firstOffset: Long, epoch: Int, highWaterMark: Long, logStartOffset: Long): EpochChangeEvent =
    new EpochChangeEvent(id, firstOffset, CurrentVersion.version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, data: EpochChange): EpochChangeEvent = {
    new EpochChangeEvent(id, data.firstOffset, data.info.version, data.info.epoch, data.info.highWaterMark,
      data.info.logStartOffset)
  }
}
