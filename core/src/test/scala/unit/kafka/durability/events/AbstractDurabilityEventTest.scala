/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.events

import kafka.durability.exceptions.DurabilityMetadataDeserializationException
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.Assertions.assertThrows

class AbstractDurabilityEventTest {
  val tpid = new TopicPartition("test", 0)
  val epoch = 0
  val version = 1

  @Test
  def OffsetChangeEventSerializationDeserializationTest() : Unit = {
    val obj = OffsetChangeEvent(tpid, version, epoch, 100, 0)
    val keyBB = obj.serializeKey
    val bb = obj.serializeValue
    val deserialize = Deserializer.deserialize(keyBB, bb)
    assertEquals("Failed to deserialize OffsetChangeEvent",  obj, deserialize)
  }

  @Test
  def EpochChangeSerializationDeserializationTest(): Unit = {
    val obj = EpochChangeEvent(tpid, 50, version, epoch, 100, 0)
    val keyBB = obj.serializeKey
    val bb = obj.serializeValue
    val deserialize = Deserializer.deserialize(keyBB, bb)
    assertEquals("Failed to deserialize EpochChangeEvent", obj, deserialize)
  }

  @Test
  def DeleteRecordsSerializationDeserializationTest(): Unit = {
    val obj = DeleteRecordsEvent(tpid, 100, version, epoch, 100, 0)
    val keyBB = obj.serializeKey
    val bb = obj.serializeValue
    val deserialize = Deserializer.deserialize(keyBB, bb)
    assertEquals("Failed to deserialize DeleteRecordsEvent", obj, deserialize)
  }

  @Test
  def RetentionChangeSerializationDeserializationTest(): Unit = {
    val obj = RetentionChangeEvent(tpid, 50, 1000, version, epoch, 100, 0)
    val keyBB = obj.serializeKey
    val bb = obj.serializeValue
    val deserialize = Deserializer.deserialize(keyBB, bb)
    assertEquals("Failed to deserialize RetentionChangeEvent", obj, deserialize)
  }

  @Test
  def ISRExpandSerializationDeserializationTest(): Unit = {
    val obj = ISRExpandEvent(tpid, 5, version, epoch, 100, 0)
    val keyBB = obj.serializeKey
    val bb = obj.serializeValue
    val deserialize = Deserializer.deserialize(keyBB, bb)
    assertEquals("Failed to deserialize ISRExpandEvent", obj, deserialize)
  }

  @Test
  def ExceptionInDeserializationTest() : Unit = {
    val garbage = Array[Byte](0x01, 0x02, 0x03, 0x04)
    assertThrows[DurabilityMetadataDeserializationException](Deserializer.deserialize(garbage, garbage))
  }
}
