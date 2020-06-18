/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.events

import java.nio.ByteBuffer

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.events.DurabilityEventType.DurabilityEventType
import kafka.durability.events.serdes.EventKey
import org.apache.kafka.common.TopicPartition

abstract class AbstractDurabilityEvent(val topicPartition: TopicPartition) extends DeserializerTrait {
  def serializeKey: Array[Byte] = {
    val builder = new FlatBufferBuilder(KEY_INITIAL_LENGTH).forceDefaults(true)
    val topicNameOffset = builder.createString(topicPartition.topic());
    EventKey.startEventKey(builder)
    EventKey.addTopicName(builder, topicNameOffset)
    EventKey.addPartition(builder, topicPartition.partition())
    val end = EventKey.endEventKey(builder)
    builder.finish(end)
    val buffer = builder.dataBuffer
    val bytes = new Array[Byte](buffer.remaining)
    buffer.get(bytes)
    bytes
  }

  def serializeValue: Array[Byte] = {
    val payload = payloadBuffer
    val buf = ByteBuffer.allocate(payload.remaining() + EVENT_TYPE_LENGTH)
    buf.put(eventType.id.toByte)
    buf.put(payload)
    buf.array();
  }

  def payloadBuffer: ByteBuffer
  def eventType: DurabilityEventType

  override def equals(other: Any): Boolean = other match {
    case that: AbstractDurabilityEvent =>
        topicPartition == that.topicPartition &&
        eventType == that.eventType &&
        payloadBuffer.array().sameElements(that.payloadBuffer.array())
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(topicPartition, eventType, payloadBuffer)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object DurabilityEventType extends Enumeration {
  type DurabilityEventType = Value
  val OffsetChangeType = Value(0x01.toByte)
  val EpochChangeType = Value(0x02.toByte)
  val IsrExpandType = Value(0x03.toByte)
  val DeleteRecordsType =  Value(0x04.toByte)
  val RetentionChangeType = Value(0x05.toByte)
}

case object CurrentVersion {
  def version: Int = 1
}
