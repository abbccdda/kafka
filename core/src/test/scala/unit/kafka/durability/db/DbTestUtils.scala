/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.db

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

object DbTestUtils {
  def getDbInstance(topic:String, partition: Int): DurabilityDB = {
    val tp = new TopicPartition("test", 1)
    val epochChain = mutable.HashMap[Int, Long](0 -> 0, 1 -> 50, 2 -> 80)
    val tmpDir = TestUtils.tempDir()
    val logDir = TestUtils.randomPartitionLogDir(tmpDir)
    val ps = PartitionState(tp.partition(), 1, 100, 2, 100, 3600, 1024, epochChain)
    val db = DurabilityDB(logDir)
    db.addTopic("test", TopicState("test", mutable.HashMap[Int, PartitionState]()))
    db.addPartition(tp, ps)
    db
  }
}
