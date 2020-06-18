/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.events

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.events.DurabilityEventType.DurabilityEventType
import kafka.durability.events.serdes.{BaseInfo, ISRExpand}
import org.apache.kafka.common.TopicPartition

class ISRExpandEvent(topicPartition: TopicPartition, expandBrokerId: Int, version: Int, epoch: Int,
                     highWaterMark: Long, logStartOffset: Long) extends AbstractDurabilityEvent(topicPartition) {

  private def serialize(): ByteBuffer = {
    val builder = new FlatBufferBuilder(KEY_INITIAL_LENGTH).forceDefaults(true)
    ISRExpand.startISRExpand(builder)
    ISRExpand.addExpandBrokerId(builder, expandBrokerId)
    val infoOffset = BaseInfo.createBaseInfo(builder, version, epoch, highWaterMark, logStartOffset)
    ISRExpand.addInfo(builder, infoOffset)
    val entryId = ISRExpand.endISRExpand(builder)
    builder.finish(entryId)
    ISRExpand.getRootAsISRExpand(builder.dataBuffer()).getByteBuffer.duplicate()
  }

  override lazy val payloadBuffer: ByteBuffer = serialize()

  override val eventType: DurabilityEventType = DurabilityEventType.IsrExpandType

  override def toString: String = {
    s"[TopicPartition: $topicPartition, broker: $expandBrokerId, version: $version, epoch: $epoch, highWaterMark: $highWaterMark, logStartOffset: $logStartOffset]"
  }
}

object ISRExpandEvent {
  def apply(id: TopicPartition, expandBrokerId: Int, version: Int, epoch: Int, highWaterMark: Long, logStartOffset: Long): ISRExpandEvent =
    new ISRExpandEvent(id, expandBrokerId, version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, expandBrokerId: Int, epoch: Int, highWaterMark: Long, logStartOffset: Long): ISRExpandEvent =
    new ISRExpandEvent(id, expandBrokerId, CurrentVersion.version, epoch, highWaterMark, logStartOffset)

  def apply(id: TopicPartition, data: ISRExpand): ISRExpandEvent = {
    new ISRExpandEvent(id, data.expandBrokerId(), data.info.version, data.info.epoch, data.info.highWaterMark,
      data.info.logStartOffset)
  }
}
