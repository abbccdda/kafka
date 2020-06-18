/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.db

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.db.serdes.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.junit.Assert.assertEquals

import scala.collection.mutable

class PartitionStateTest {
  val tp = new TopicPartition("test", 1)
  val epochChain = mutable.HashMap[Int, Long](0 -> 0, 1 -> 50, 2 -> 80)
  val ps = PartitionState(tp.partition(), 1, 100, 2, 100, 3600, 1024, epochChain)

  /**
   * Test serialization method of PartitionState by serializing the object and
   * using the serialized byteBuffer instantiate back to same object via deserialization.
   */
  @Test
  def PartitionStateTest(): Unit = {
    val orig = ps
    assertEquals(ps.highWaterMark, orig.highWaterMark)
    val builder = new FlatBufferBuilder(1024)
    builder.finish(orig.serialize(builder))
    val dup = PartitionState(PartitionInfo.getRootAsPartitionInfo(builder.dataBuffer()))
    assertEquals("PartitionState not consistent after recovery", dup, orig)
  }
}
