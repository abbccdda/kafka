/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.events

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.events.DurabilityEventType.DurabilityEventType
import kafka.durability.events.serdes.{BaseInfo, DeleteRecords}
import org.apache.kafka.common.TopicPartition

class DeleteRecordsEvent(topicPartition: TopicPartition, trimOffset: Long, version: Int, epoch: Int,
                         highWaterMark: Long, logStartOffset: Long) extends AbstractDurabilityEvent(topicPartition) {

  private def serialize(): ByteBuffer = {
    val builder = new FlatBufferBuilder(KEY_INITIAL_LENGTH).forceDefaults(true)
    DeleteRecords.startDeleteRecords(builder)
    DeleteRecords.addTrimmedOffset(builder, trimOffset)
    val infoOffset = BaseInfo.createBaseInfo(builder, version, epoch, highWaterMark, logStartOffset)
    DeleteRecords.addInfo(builder, infoOffset)
    val entryId = DeleteRecords.endDeleteRecords(builder)
    builder.finish(entryId)
    DeleteRecords.getRootAsDeleteRecords(builder.dataBuffer()).getByteBuffer.duplicate()
  }

  override lazy val payloadBuffer: ByteBuffer = serialize()

  override def eventType: DurabilityEventType = DurabilityEventType.DeleteRecordsType

  override def toString: String = {
    s"[TopicPartition: $topicPartition, trimOffset: $trimOffset, version: $version, epoch: $epoch, highWaterMark: $highWaterMark, logStartOffset: $logStartOffset]"
  }
}

object DeleteRecordsEvent {
  def apply(id: TopicPartition, trimmedOffset: Long, version: Int, epoch: Int, highWaterMark: Long, logStartOffset: Long): DeleteRecordsEvent =
    new DeleteRecordsEvent(id, trimmedOffset, version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, trimmedOffset: Long, epoch: Int, highWaterMark: Long, logStartOffset: Long): DeleteRecordsEvent =
    new DeleteRecordsEvent(id, trimmedOffset, CurrentVersion.version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, data: DeleteRecords): DeleteRecordsEvent = {
    new DeleteRecordsEvent(id, data.trimmedOffset(), data.info.version, data.info.epoch, data.info.highWaterMark,
      data.info.logStartOffset)
  }
}

