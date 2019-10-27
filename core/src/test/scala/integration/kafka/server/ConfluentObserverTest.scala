/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.server

import java.util.Optional
import java.util.Properties
import java.util.Arrays

import kafka.log.LogConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewPartitionReassignment, NewTopic, AdminClient => JAdminClient}
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.network.ListenerName
import org.junit.After
import org.junit.Before
import org.junit.Test

import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

final class ConfluentObserverTest extends ZooKeeperTestHarness {
  import ConfluentObserverTest._

  var servers = Seq.empty[KafkaServer]
  val broker1 = 0
  val broker2 = 1
  val broker3 = 2
  val broker4 = 3
  val broker5 = 4

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val rack = Map(
      broker1 -> "a",
      broker2 -> "a",
      broker3 -> "b",
      broker4 -> "b",
      broker5 -> "c"
    )
    val brokerConfigs = TestUtils.createBrokerConfigs(5, zkConnect, false)
    servers = brokerConfigs.map { config =>
      config.setProperty(KafkaConfig.RackProp, rack(config.getProperty(KafkaConfig.BrokerIdProp).toInt))
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
      config.setProperty(KafkaConfig.ControlledShutdownEnableProp, "true")
      config.setProperty(KafkaConfig.ControlledShutdownMaxRetriesProp, "1")
      config.setProperty(KafkaConfig.ControlledShutdownRetryBackoffMsProp, "1000")
      config.setProperty(KafkaConfig.ObserverFeatureProp, "true")
      TestUtils.createServer(KafkaConfig.fromProps(config))
    }
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testObserversShouldNotJoinIsr(): Unit = {
    // When leader is in the "replicas" constraints...
    // 1. test that brokers in the "observers" constraints shouldn't be in the ISR.
    // 2. test that brokers in the "replicas" constraints should be in the ISR.
    // 3. test that brokers not in the "observers" constraints should be in the ISR.
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(2, "a"),
        observerConstraint = Some(BasicConstraint(2, "b"))
      )).asJava)
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)
      TestUtils.waitForLeaderToBecomeOneOf(client, topicPartition, Set(broker1, broker2))
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3, broker4))
    }
  }

  @Test
  def testObserverElection(): Unit = {
    // Case: All of the replicas are in the ISR when the leader matches "observer" constraints
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(2, "a"),
        observerConstraint = Some(BasicConstraint(2, "b"))
      )).asJava)
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3, broker4))

      // Shutdown all of the sync replicas and force one of the observers to be elected
      val rollingServers = Seq(broker1, broker2)
      rollingServers.foreach { broker =>
        servers(broker).shutdown()
      }

      electLeader(client, topicPartition, ElectionType.UNCLEAN)
      TestUtils.waitForLeaderToBecomeOneOf(client, topicPartition, Set(broker3, broker4))

      rollingServers.foreach { broker =>
        servers(broker).startup()
      }

      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker3, broker4))

      // Preferred leader election should move leadership back to the sync replicas and
      // the observers should be removed from the ISR
      electLeader(client, topicPartition, ElectionType.PREFERRED)

      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3, broker4))
    }
  }

  @Test
  def testReassignObserverMatchingConstraint(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(
        Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(1, "b")))).asJava
      )
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)

      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))

      // Reassign the observer to a different broker matching the existing observer constraint
      client.alterPartitionReassignments(
        Map(topicPartition -> Optional.of(reassignmentEntry(Seq(broker1, broker2, broker4), Seq(broker4)))).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker1, broker2, broker4))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker4))
    }
  }

  @Test
  def testReassignWithChangedObserverConstraint(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(
        Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(1, "b")))).asJava
      )
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)

      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))

      {
        // Change the observer constraint from rack "b" to rack "c"
        val observerCConfig = new Properties()
        observerCConfig.setProperty(
          LogConfig.TopicPlacementConstraintsProp,
          basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(1, "c")))
        )
        TestUtils.alterTopicConfigs(client, topic, observerCConfig)
      }

      client.alterPartitionReassignments(
        Map(topicPartition -> Optional.of(reassignmentEntry(Seq(broker1, broker2, broker5), Seq(broker5)))).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker1, broker2, broker5))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker5))
    }
  }

  @Test
  def testReassignSyncReplicaToObserver(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2, broker3)

      // Create the topic with no initial constraint specified
      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers, new Properties())
      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker1))
      // All replicas part in the ISR because leader is an Observer
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker3))

      {
        // Add an observer constraint
        val observerBConfig = new Properties()
        observerBConfig.setProperty(
          LogConfig.TopicPlacementConstraintsProp, basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(1, "b")))
        )
        TestUtils.alterTopicConfigs(client, topic, observerBConfig)
      }

      client.alterPartitionReassignments(
        Map(topicPartition -> Optional.of(reassignmentEntry(Seq(broker1, broker2, broker3), Seq(broker3)))).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker1, broker2, broker3))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))
    }
  }

  @Test
  def testReassignWithSwappedSyncAndObserverConstraints(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(
        Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(2, "b")))).asJava
      )
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)

      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3, broker4))

      {
        // Add a constraint which swaps the sync replicas and observers
        val observerBConfig = new Properties()
        observerBConfig.setProperty(
          LogConfig.TopicPlacementConstraintsProp, basicTopicPlacement(BasicConstraint(2, "b"), Some(BasicConstraint(2, "a")))
        )
        TestUtils.alterTopicConfigs(client, topic, observerBConfig)
      }

      client.alterPartitionReassignments(
        Map(topicPartition -> Optional.of(reassignmentEntry(Seq(broker3, broker4, broker1, broker2), Seq(broker1, broker2)))).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker3, broker4, broker1, broker2))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker3, broker4))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker1, broker2))
    }
  }

  @Test
  def testReassignObserverToSyncReplica(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(
        Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(1, "b")))).asJava
      )
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)

      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))

      TestUtils.alterTopicConfigs(client, topic, new Properties())

      client.alterPartitionReassignments(
        Map(topicPartition -> Optional.of(reassignmentEntry(Seq(broker1, broker2, broker3), Seq.empty))).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker1, broker2, broker3))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker3))
    }
  }

  @Test
  def testReassignWithRemovedObserver(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(
        Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(1, "b")))).asJava
      )
      client.createTopics(Seq(newTopic).asJava).all().get()

      val topicPartition = new TopicPartition(topic, partition)

      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))

      TestUtils.alterTopicConfigs(client, topic, new Properties())

      client.alterPartitionReassignments(
        Map(topicPartition -> Optional.of(reassignmentEntry(Seq(broker1, broker2), Seq.empty))).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker1, broker2))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
    }
  }

  @Test
  def testReassignWithAddedObserver(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2)

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers, new Properties())
      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker1))
      // All replicas part in the ISR because leader is an Observer
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))

      {
        val observerBConfig = new Properties()
        observerBConfig.setProperty(
          LogConfig.TopicPlacementConstraintsProp,
          basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(1, "b")))
        )
        TestUtils.alterTopicConfigs(client, topic, observerBConfig)
      }

      client.alterPartitionReassignments(
        Map(topicPartition -> Optional.of(reassignmentEntry(Seq(broker1, broker2, broker3), Seq(broker3)))).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker1, broker2, broker3))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))
    }
  }

  private def electLeader(client: Admin, tp: TopicPartition, electionType: ElectionType): Unit = {
    val electionResult = client.electLeaders(electionType, Set(tp).asJava)
    electionResult.partitions.get.get(tp).asScala.foreach { exception =>
      throw exception
    }
  }

}

object ConfluentObserverTest {
  case class BasicConstraint(count: Int, rack: String)

  def createConfig(servers: Seq[KafkaServer]): Map[String, Object] = {
    Map(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers(servers),
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "20000"
    )
  }

  def reassignmentEntry(replicas: Seq[Int], observers: Seq[Int]): NewPartitionReassignment = {
    new NewPartitionReassignment(
      replicas.map(_.asInstanceOf[Integer]).asJava,
      observers.map(_.asInstanceOf[Integer]).asJava
    )
  }

  def waitForAllReassignmentsToComplete(client: JAdminClient): Unit = {
    TestUtils.waitUntilTrue(() => client.listPartitionReassignments().reassignments().get().isEmpty,
      s"There still are ongoing reassignments", pause = 100L)
  }

  def describeTopicPartition(client: JAdminClient, topicPartition: TopicPartition): Option[TopicPartitionInfo] = {
    Option(
      client
        .describeTopics(Arrays.asList(topicPartition.topic))
        .all
        .get
        .get(topicPartition.topic)
        .partitions
        .get(topicPartition.partition)
    )
  }

  def bootstrapServers(servers: Seq[KafkaServer]): String = {
    servers.map { server =>
      val port = server.socketServer.boundPort(ListenerName.normalised("PLAINTEXT"))
      s"localhost:$port"
    }.headOption.mkString(",")
  }

  def basicTopicPlacement(
    replicaConstraint: BasicConstraint,
    observerConstraint: Option[BasicConstraint]
  ): String = {
    val observers = observerConstraint.fold("") { constraint =>
      s""","observers":[{"count": ${constraint.count}, "constraints":{"rack":"${constraint.rack}"}}]"""
    }

    s"""{"version":1,"replicas":[{"count": ${replicaConstraint.count}, "constraints":{"rack":"${replicaConstraint.rack}"}}]$observers}"""
  }

}
