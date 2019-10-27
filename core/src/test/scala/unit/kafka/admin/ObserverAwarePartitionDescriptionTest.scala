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
  private val topic = "foo"

  private val nodes = Map(
    0 -> new Node(0, "localhost", 9092, "r1"),
    1 -> new Node(1, "localhost", 9093, "r1"),
    2 -> new Node(2, "localhost", 9094, "r2"),
    3 -> new Node(3, "localhost", 9095, "r2")
  )

  private def partitionInfo(leader: Option[Int],
                            isr: Set[Int],
                            observers: List[Int]): TopicPartitionInfo = {
    new TopicPartitionInfo(0,
      leader.map(nodes).orNull,
      nodes.values.toList.asJava,
      nodes.filterKeys(observers.contains).values.toList.asJava,
      nodes.filterKeys(isr.contains).values.toList.asJava
    )
  }

  private def configWithPlacement(placement: String): Config = {
    new Config(List(
      new ConfigEntry(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placement)
    ).asJava)
  }

  @Test
  def testNoObserversIfPlacementConstraintNotConfigured(): Unit = {
    // If there is no placement constraint configured, there will be no live observers displayed
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr, List.empty)
    val liveBrokerIds = Set(0, 1, 2, 3)
    val config = Some(new Config(List.empty.asJava))
    val desc = TopicCommand.PartitionDescription(topic, info, config, markedForDeletion = false, liveBrokerIds)
    assertEquals(Seq.empty, desc.observerIds)
    assertTrue(desc.hasUnderReplicatedPartitions)
  }

  @Test
  def testNoObserversIfPlacementConstraintIsNull(): Unit = {
    // The placement constraint may be null
    val config = configWithPlacement(null)
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr, List.empty)
    val liveBrokerIds = Set(0, 1, 2, 3)
    val desc = TopicCommand.PartitionDescription(topic, info, Some(config), markedForDeletion = false, liveBrokerIds)
    assertEquals(Seq.empty, desc.observerIds)
    assertTrue(desc.hasUnderReplicatedPartitions)
  }

  @Test
  def testObserversWithOnlyReplicaConstraintConfigured(): Unit = {
    // Replicas not matching the replica constraint are treated as observers
    val config = configWithPlacement("""{"version":1,"replicas":[{"count": 1, "constraints":{"rack":"r1"}}]}""")
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr, List.empty)
    val liveBrokerIds = Set(0, 1, 2, 3)
    val desc = TopicCommand.PartitionDescription(topic, info, Some(config), markedForDeletion = false, liveBrokerIds)
    assertEquals(Seq.empty, desc.observerIds)
    assertTrue(desc.hasUnderReplicatedPartitions)
  }

  @Test
  def testObserversMatchingObserverConstraint(): Unit = {
    val config = configWithPlacement("""{"version":1,"replicas":[{"count": 1, "constraints":{"rack":"r1"}}],""" +
      """"observers":[{"count": 1, "constraints":{"rack":"r2"}}]}""")
    val isr = Set(0, 1)
    val observers = List(2, 3)
    val info = partitionInfo(Some(0), isr, observers)
    val liveBrokerIds = Set(0, 1, 2, 3)
    val desc = TopicCommand.PartitionDescription(topic, info, Some(config), markedForDeletion = false, liveBrokerIds)
    assertEquals(observers, desc.observerIds)
    assertFalse(desc.hasUnderReplicatedPartitions)
  }

  @Test
  def testObserversCanOnlyMatchObserverConstraint(): Unit = {
    // Any replica which doesn't match the observer constraint is treated as a sync replica.
    val config = configWithPlacement("""{"version":1,"replicas":[{"count": 1, "constraints":{"rack":"r1"}}],""" +
      """"observers":[{"count": 1, "constraints":{"rack":"r3"}}]}""")
    val isr = Set(0, 1)
    val info = partitionInfo(Some(0), isr, List.empty)
    val liveBrokerIds = Set(0, 1, 2, 3)
    val desc = TopicCommand.PartitionDescription(topic, info, Some(config), markedForDeletion = false, liveBrokerIds)
    assertTrue(desc.observerIds.isEmpty)
    assertTrue(desc.hasUnderReplicatedPartitions)
  }

  @Test
  def testIsrReplicasAreNotObservers(): Unit = {
    // Replicas in the ISR are never counted among the observers even if they do not
    // match the replica constraint
    val config = configWithPlacement("""{"version":1,"replicas":[{"count": 1, "constraints":{"rack":"r1"}}],""" +
      """"observers":[{"count": 1, "constraints":{"rack":"r2"}}]}""")
    val isr = Set(0, 1, 2)
    val observers = List(3)
    val info = partitionInfo(Some(0), isr, observers)
    val liveBrokerIds = Set(0, 1, 2, 3)
    val desc = TopicCommand.PartitionDescription(topic, info, Some(config), markedForDeletion = false, liveBrokerIds)
    assertEquals(observers, desc.observerIds)
    assertFalse(desc.hasUnderReplicatedPartitions)
  }

  @Test
  def testOfflineReplicasAreNotObservers(): Unit = {
    // Offline replicas are not counted among live observers
    val config = configWithPlacement("""{"version":1,"replicas":[{"count": 1, "constraints":{"rack":"r1"}}],""" +
      """"observers":[{"count": 1, "constraints":{"rack":"r2"}}]}""")
    val isr = Set(0, 1)
    val observers = List(2)
    val info = partitionInfo(Some(0), isr, observers)
    val liveBrokerIds = Set(0, 1, 2)
    val desc = TopicCommand.PartitionDescription(topic, info, Some(config), markedForDeletion = false, liveBrokerIds)
    assertEquals(observers, desc.observerIds)

    // Offline replicas always count toward URP determination
    assertTrue(desc.hasUnderReplicatedPartitions)
  }

  @Test
  def testIsUnderReplicatedWhenReplicaConstraintMatchingBrokerNotInIsr(): Unit = {
    // A partition is considered an URP if eligible in sync replicas is less than current ISR
    val config = configWithPlacement("""{"version":1,"replicas":[{"count": 1, "constraints":{"rack":"r1"}}],""" +
      """"observers":[{"count": 1, "constraints":{"rack":"r2"}}]}""")
    val isr = Set(0)
    val observers = List(2, 3)
    val info = partitionInfo(Some(0), isr, observers)
    val liveBrokerIds = Set(0, 1, 2, 3)
    val desc = TopicCommand.PartitionDescription(topic, info, Some(config), markedForDeletion = false, liveBrokerIds)
    assertEquals(observers, desc.observerIds)
    assertTrue(desc.hasUnderReplicatedPartitions)
  }

  @Test
  def testNoLiveObserversIfCurrentLeaderMatchesObserverConstraint(): Unit = {
    val config = configWithPlacement("""{"version":1,"replicas":[{"count": 1, "constraints":{"rack":"r1"}}],""" +
      """"observers":[{"count": 1, "constraints":{"rack":"r2"}}]}""")
    val isr = Set(0)
    val info = partitionInfo(Some(3), isr, List.empty)
    val liveBrokerIds = Set(0, 1, 2, 3)
    val desc = TopicCommand.PartitionDescription(topic, info, Some(config), markedForDeletion = false, liveBrokerIds)
    assertTrue(desc.observerIds.isEmpty)
    assertTrue(desc.hasUnderReplicatedPartitions)
  }

}
