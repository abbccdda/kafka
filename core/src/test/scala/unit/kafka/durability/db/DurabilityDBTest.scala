/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.durability.db

import kafka.durability.exceptions.DurabilityDBNotReadyException
import org.junit.{After, Test}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.Assert.{assertEquals, assertTrue}
import org.scalatest.Assertions.assertThrows

import scala.collection.mutable

class DurabilityDBTest {
  val tp = new TopicPartition("test", 1)
  val epochChain = mutable.HashMap[Int, Long](0 -> 0, 1 -> 50, 2 -> 80)
  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val ps = PartitionState(tp.partition(), 1, 100, 2, 100, 3600, 1024, epochChain)

  @After
  def tearDown(): Unit = {
    Utils.delete(tmpDir)
  }

  /**
   * Test db initializes correctly.
   */
  @Test
  def DurabilityDBInitTest(): Unit = {
    val db = DurabilityDB(logDir)
    assertEquals(db.status, DbStatus.Online)
    assertTrue("Database failed to initialize", db.header.equals(DbHeader(1, 1, Array.fill(50)(0L))))
    assertEquals("Database failed to initialize", db.topicStates.size, 0)
  }

  /**
   * Basic serialization and deserialization for entire db with single partition state.
   */
  @Test
  def DurabilityDBSerializationDeserializationTest(): Unit = {
    val db = DurabilityDB(logDir)
    db.addTopic("test", TopicState("test", mutable.HashMap[Int, PartitionState]()))
    db.addPartition(tp, ps)
    db.close()
    val newDB = DurabilityDB(logDir)
    println(newDB.topicStates)
    assertTrue("Check-pointing and recovery of db not consistent", newDB.header.equals(db.header))
    assertEquals("PartitionState not preserved after recovery", newDB.fetchPartitionState(tp).get, ps)
  }

  /**
   * Test if we checkpoint empty db state, deserialization and de-serialization works.
   */
  @Test
  def DurabilityDBInitCheckPointing(): Unit = {
    val db = DurabilityDB(logDir)
    assertEquals(db.status, DbStatus.Online)
    db.close()
    val newDb = DurabilityDB(logDir)
    assertTrue("Inconsistent db after check pointing in empty state", db.header.equals(newDb.header))
    assertEquals("Inconsistent db after check pointing in empty state", newDb.topicStates, db.topicStates)
  }

  /**
   * Test if we recreate the db file, we recover to empty db.
   */
  @Test
  def DurabilityDBEmptyDBFile(): Unit = {
    var db = DurabilityDB(logDir)
    db.dbFile.createNewFile()
    db = DurabilityDB(logDir)
    assertTrue("Database not in empty state", db.header.equals(DbHeader(1, 1, Array.fill(50)(0L))))
  }

  /**
   * Add 1000s of partition state, make sure they are serialized and de-serialized by doing
   * db.close and re-instantiation. Make sure that on re-calling of addPartition, we get
   * old partition state value. Also tests multiple update on partition commit offsets.
   */
  @Test
  def DurabilityDBMultiplePartitionUpdate(): Unit = {
    var db = DurabilityDB(logDir)
    db.addTopic(tp.topic(), TopicState(tp.topic(), mutable.HashMap[Int, PartitionState]()))
    for (ii <- 0 to 4000) {
      val id = new TopicPartition(tp.topic(), ii)
      val ps = PartitionState(id.partition(), ii, 100, 2, 100,3600, 1024, epochChain)
      db.updateDurabilityTopicPartitionOffset(ii%db.DURABILITY_EVENTS_TOPIC_PARTITION_COUNT, ii%db.DURABILITY_EVENTS_TOPIC_PARTITION_COUNT)
      db.addPartition(id, ps)
    }
    db.close()
    db = DurabilityDB(logDir)
    val committed = db.getDurabilityTopicPartitionOffsets()

    for (ii <- 0 to 4000) {
      val id = new TopicPartition(tp.topic(), ii)
      assertEquals("Failed to get last valid state for partition " + ii,
        db.fetchPartitionState(id).get.startOffset, ii)
      assertEquals("Failed to fetch latest committed offset for partition " + (ii % db.DURABILITY_EVENTS_TOPIC_PARTITION_COUNT),
        committed(ii % db.DURABILITY_EVENTS_TOPIC_PARTITION_COUNT), ii % db.DURABILITY_EVENTS_TOPIC_PARTITION_COUNT)
    }
  }

  /**
   * Test for raising exception when API call made while db not in online status.
   */
  @Test
  def APIAccessInInitStatusTest(): Unit = {
    val db = DurabilityDB(logDir)
    db.status = DbStatus.Init
    assertThrows[DurabilityDBNotReadyException](
      db.addTopic(tp.topic(), TopicState(tp.topic(), mutable.HashMap[Int, PartitionState]()))
    )
  }
}
