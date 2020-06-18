/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.db

import com.google.flatbuffers.FlatBufferBuilder
import kafka.durability.db.serdes.TopicInfo
import org.junit.Test
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.assertEquals

import scala.collection.mutable

class TopicStateTest() {
  val tp = new TopicPartition("test", 1)
  val epochChain = mutable.HashMap[Int, Long](0 -> 0, 1 -> 50, 2 -> 80)
  val ps = PartitionState(tp.partition(), 1, 7861, 2, 100, 3600, 1024, epochChain)

  @Test
  def TopicStateSerializationTest(): Unit = {
    val orig = TopicState(tp.topic(), mutable.HashMap[Int, PartitionState](1->ps))
    assertEquals(tp.topic, orig.topic)
    val builder = new FlatBufferBuilder(1024)
    builder.finish(orig.serialize(builder))
    val dup = TopicState(TopicInfo.getRootAsTopicInfo(builder.dataBuffer()))
    assertEquals("TopicState not consistent after recovery", dup, orig)
  }
}
