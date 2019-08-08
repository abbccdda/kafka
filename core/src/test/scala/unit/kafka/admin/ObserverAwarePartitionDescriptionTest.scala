/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.admin

import org.apache.kafka.clients.admin.{Config, ConfigEntry}
import org.apache.kafka.common.config.ConfluentTopicConfig
import org.apache.kafka.common.{Node, TopicPartitionInfo}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class ObserverAwarePartitionDescriptionTest {
  private val nodes = Map(
    0 -> new Node(0, "localhost", 9092, "r1"),
    1 -> new Node(1, "localhost", 9093, "r1"),
    2 -> new Node(2, "localhost", 9094, "r2"),
    3 -> new Node(3, "localhost", 9095, "r2")
  )

  private def partitionInfo(leader: Option[Int], isr: Set[Int]): TopicPartitionInfo = {
    new TopicPartitionInfo(0,
      leader.map(nodes).orNull,
      nodes.values.toList.asJava,
      nodes.filterKeys(isr.contains).values.toList.asJava
    )
  }

  @Test
  def testNoObserversIfPlacementConstraintNotConfigured(): Unit = {
    // If there is no placement constraint configured, there will be no live observers displayed
    val topic = "foo"
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr)
    val config = Some(new Config(List.empty.asJava))
    val desc = TopicCommand.PartitionDescription(topic, info, config, markedForDeletion = false)
    assertEquals(None, desc.liveObservers(Set(0, 1, 2, 3)))
  }

  @Test
  def testNoObserversIfPlacementConstraintIsNull(): Unit = {
    // The placement contstraint may be null
    val topic = "foo"
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr)
    val config = Some(new Config(List(
      new ConfigEntry(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, null)
    ).asJava))
    val desc = TopicCommand.PartitionDescription(topic, info, config, markedForDeletion = false)
    assertEquals(None, desc.liveObservers(Set(0, 1, 2, 3)))
  }

  @Test
  def testObserversWithOnlyReplicaConstraintConfigured(): Unit = {
    // Replicas not matching the replica constraint are treated as observers
    val placementConstraint = """{"version":1,"replicas":[{"constraints":{"rack":"r1"}}]}"""
    val topic = "foo"
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr)
    val config = Some(new Config(List(
      new ConfigEntry(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementConstraint)
    ).asJava))
    val desc = TopicCommand.PartitionDescription(topic, info, config, markedForDeletion = false)
    assertEquals(Some(Seq.empty), desc.liveObservers(Set(0, 1, 2, 3)))
  }

  @Test
  def testObserversCanOnlyMatchObserverConstraint(): Unit = {
    // Any replica which doesn't match the observer constraint is treated as a sync replica.
    val placementConstraint = """{"version":1,"replicas":[{"constraints":{"rack":"r1"}}],""" +
      """"observers":[{"constraints":{"rack":"r3"}}]}"""
    val topic = "foo"
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr)
    val config = Some(new Config(List(
      new ConfigEntry(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementConstraint)
    ).asJava))
    val desc = TopicCommand.PartitionDescription(topic, info, config, markedForDeletion = false)
    assertEquals(Some(Seq.empty), desc.liveObservers(Set(0, 1, 2, 3)))
  }

  @Test
  def testIsrReplicasAreNotObservers(): Unit = {
    // Replicas in the ISR are never counted among the observers even if they do not
    // match the replica constraint
    val placementConstraint = """{"version":1,"replicas":[{"constraints":{"rack":"r1"}}],""" +
      """"observers":[{"constraints":{"rack":"r2"}}]}"""
    val topic = "foo"
    val isr = Set(2, 3)
    val info = partitionInfo(Some(0), isr)
    val config = Some(new Config(List(
      new ConfigEntry(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementConstraint)
    ).asJava))
    val desc = TopicCommand.PartitionDescription(topic, info, config, markedForDeletion = false)
    assertEquals(Some(Seq.empty), desc.liveObservers(Set(0, 1, 2, 3)))
  }

  @Test
  def testOfflineReplicasAreNotObservers(): Unit = {
    // Offline replicas are not counted among live observers
    val placementConstraint = """{"version":1,"replicas":[{"constraints":{"rack":"r1"}}],""" +
      """"observers":[{"constraints":{"rack":"r2"}}]}"""
    val topic = "foo"
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr)
    val config = Some(new Config(List(
      new ConfigEntry(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementConstraint)
    ).asJava))
    val desc = TopicCommand.PartitionDescription(topic, info, config, markedForDeletion = false)
    assertEquals(Some(Seq(2)), desc.liveObservers(Set(0, 1, 2)))
  }

}
