/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.events

import java.nio.ByteBuffer

import kafka.durability.events.DurabilityEventType.DurabilityEventType
import kafka.durability.events.serdes.{DeleteRecords, EpochChange, EventKey, ISRExpand, OffsetChange, RetentionChange}
import kafka.durability.exceptions.DurabilityMetadataDeserializationException
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

trait DeserializerTrait extends Logging {
  val KEY_INITIAL_LENGTH = 500
  val EVENT_TYPE_LENGTH = 1

  def deserializeEventType(value: Byte): DurabilityEventType = DurabilityEventType(value)

  private def deserializeKey(value: ByteBuffer): TopicPartition = {
    val key = EventKey.getRootAsEventKey(value)
    new TopicPartition(key.topicName(), key.partition())
  }

  @throws[DurabilityMetadataDeserializationException]
  def deserialize(key: Array[Byte], value: Array[Byte]): AbstractDurabilityEvent = {
    try {
      deserialize(ByteBuffer.wrap(key), ByteBuffer.wrap(value))
    } catch {
      case ex: Exception => error(s"Exception in de-serializing durability event ")
        throw new DurabilityMetadataDeserializationException(ex)
    }
  }

  private def deserialize(key: ByteBuffer, value: ByteBuffer): AbstractDurabilityEvent = {
    val id = deserializeKey(key)
    val eventType = deserializeEventType(value.get)
    eventType match {
      case DurabilityEventType.OffsetChangeType => {
        val payload: OffsetChange= OffsetChange.getRootAsOffsetChange(value)
        OffsetChangeEvent(id, payload)
      }
      case DurabilityEventType.EpochChangeType => {
        val payload = EpochChange.getRootAsEpochChange(value)
        EpochChangeEvent(id, payload)
      }
      case DurabilityEventType.IsrExpandType => {
        val payload = ISRExpand.getRootAsISRExpand(value)
        ISRExpandEvent(id, payload)
      }
      case DurabilityEventType.DeleteRecordsType => {
        val payload = DeleteRecords.getRootAsDeleteRecords(value)
        DeleteRecordsEvent(id, payload)
      }
      case DurabilityEventType.RetentionChangeType => {
        val payload = RetentionChange.getRootAsRetentionChange(value)
        RetentionChangeEvent(id, payload)
      }
      case _ => error(s"Unknown AbstractDurabilityEvent with type $eventType")
        throw new IllegalArgumentException(s"Unknown AbstractDurabilityEvent with type $eventType")
    }
  }
}

object Deserializer extends DeserializerTrait
