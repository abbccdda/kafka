/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.server

import java.util.{Arrays, Optional, Properties}
import java.util.concurrent.ExecutionException

import kafka.log.LogConfig
import kafka.utils.TestUtils
import kafka.zk.ReassignPartitionsZNode
import kafka.zk.ZkVersion
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigsResult, Config, ConfigEntry, NewPartitionReassignment, NewTopic, AdminClient => JAdminClient}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{ElectionType, TopicPartition, TopicPartitionInfo}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.intercept

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.Map
import scala.collection.Seq
import scala.compat.java8.OptionConverters._

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
    val brokerConfigs = TestUtils.createBrokerConfigs(5, zkConnect, enableControlledShutdown = true)
    servers = brokerConfigs.map { config =>
      config.setProperty(KafkaConfig.RackProp, rack(config.getProperty(KafkaConfig.BrokerIdProp).toInt))
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
      config.setProperty(KafkaConfig.ControlledShutdownMaxRetriesProp, "1")
      config.setProperty(KafkaConfig.ControlledShutdownRetryBackoffMsProp, "1000")
      config.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp, "1000")
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
        Map(topicPartition -> reassignmentEntry(Seq(broker1, broker2, broker4), Seq(broker4))).asJava
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
        TestUtils.incrementalAlterTopicConfigs(client, topic, observerCConfig)
      }

      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker1, broker2, broker5), Seq(broker5))).asJava
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
        TestUtils.incrementalAlterTopicConfigs(client, topic, observerBConfig)
      }

      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker1, broker2, broker3), Seq(broker3))).asJava
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
        TestUtils.incrementalAlterTopicConfigs(client, topic, observerBConfig)
      }

      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker3, broker4, broker1, broker2), Seq(broker1, broker2))).asJava
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

      deleteTopicPlacementConstraints(client, topic)

      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker1, broker2, broker3), Seq.empty)).asJava
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

      deleteTopicPlacementConstraints(client, topic)

      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker1, broker2), Seq.empty)).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Metadata info is eventually consistent wait for update
      TestUtils.waitForReplicasAssigned(client, topicPartition, Seq(broker1, broker2))
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
    }
  }

  private def deleteTopicPlacementConstraints(client: JAdminClient, topic: String): Unit = {
    val deleteTopicPlacementConstraintsProps = new Properties()
    deleteTopicPlacementConstraintsProps.setProperty(LogConfig.TopicPlacementConstraintsProp, "")
    TestUtils.incrementalAlterTopicConfigs(client, topic, deleteTopicPlacementConstraintsProps, OpType.DELETE)
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
        TestUtils.incrementalAlterTopicConfigs(client, topic, observerBConfig)
      }

      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker1, broker2, broker3), Seq(broker3))).asJava
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
  def testReassignWithInvalidSyncReplicas(): Unit = {
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

      val exception = intercept[ExecutionException] {
        client.alterPartitionReassignments(
          Map(topicPartition -> reassignmentEntry(Seq(broker1, broker5, broker3, broker4), Seq(broker3, broker4))).asJava
        ).all().get()
      }
      assertEquals(classOf[InvalidReplicaAssignmentException], exception.getCause.getClass)
    }
  }

  @Test
  def testReassignRearrangeReplicaAndObserver(): Unit = {
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

      // Get current assignment
      val description = client.describeTopics(Set(topic).asJava).all.get.asScala
      val replicas = description
        .values
        .flatMap(_.partitions.asScala.flatMap(_.replicas.asScala))
        .map(_.id)
        .toSeq

      // Swap first two elements and last two elements
      val newReplicaOrder = Seq(replicas(1), replicas(0), replicas(3), replicas(2))
      val newObserverOrder = Seq(replicas(3), replicas(2))

      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(newReplicaOrder, newObserverOrder)).asJava
      ).all().get()

      waitForAllReassignmentsToComplete(client)

      // Check that assignment order changed
      TestUtils.waitForReplicasAssigned(client, topicPartition, newReplicaOrder)
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker2, broker1))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker4, broker3))
    }
  }

  @Test
  def testReassignWithInvalidObserverReplicas(): Unit = {
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

      val exception = intercept[ExecutionException] {
        client.alterPartitionReassignments(
          Map(topicPartition -> reassignmentEntry(Seq(broker1, broker2, broker3, broker5), Seq(broker3, broker5))).asJava
        ).all().get()
      }
      assertEquals(classOf[InvalidReplicaAssignmentException], exception.getCause.getClass)
    }
  }

  @Test
  def testReassignWithNewOfflineBrokers(): Unit = {
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

      val offlineBrokerId = 5
      val future = client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker1, broker2, broker3, offlineBrokerId), Seq(broker3, offlineBrokerId))).asJava
      ).all()

      JTestUtils.assertFutureThrows(future, classOf[InvalidReplicaAssignmentException])
    }
  }

  @Test
  def testFlipReassignWithOfflineObserver(): Unit = {
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
        val observerBConfig = new Properties()
        observerBConfig.setProperty(
          LogConfig.TopicPlacementConstraintsProp,
          basicTopicPlacement(BasicConstraint(2, "b"), Some(BasicConstraint(2, "a")))
        )
        TestUtils.incrementalAlterTopicConfigs(client, topic, observerBConfig)
      }

      // Shutdown one of the sync replica that is going to be converted to obsever replica
      servers(broker1).shutdown()

      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker3, broker4, broker1, broker2), Seq(broker1, broker2))).asJava
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
  def testFlipReassignWithOfflineSync(): Unit = {
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
        val observerBConfig = new Properties()
        observerBConfig.setProperty(
          LogConfig.TopicPlacementConstraintsProp,
          basicTopicPlacement(BasicConstraint(2, "b"), Some(BasicConstraint(2, "a")))
        )
        TestUtils.incrementalAlterTopicConfigs(client, topic, observerBConfig)
      }

      // Shutdown one of the sync replica that is going to be converted to obsever replica
      servers(broker3).shutdown()

      val exception = intercept[ExecutionException] {
        client.alterPartitionReassignments(
          Map(topicPartition -> reassignmentEntry(Seq(broker3, broker4, broker1, broker2), Seq(broker1, broker2))).asJava
        ).all().get()
      }
      assertEquals(classOf[InvalidReplicaAssignmentException], exception.getCause.getClass)
    }
  }

  @Test
  def testZkReassignWithInvalidAssignment(): Unit = {
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

      val zkReassignments = Map(
        topicPartition -> Seq(broker1, broker2, broker3, broker4)
      )

      zkClient.setOrCreatePartitionReassignment(zkReassignments, ZkVersion.MatchAnyVersion)
      waitForZkReassignmentToComplete()

      // Changes should not apply since it violates the topic placement constraint
      // All sync replicas are in the ISR
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2))
      // All observer replicas are not in the ISR
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3, broker4))
    }
  }

  @Test
  def testInvalidPlacementConstraintInConfiguration(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      client.createTopics(Seq(newTopic).asJava).all().get()

      val configUpdate = new Properties()
      configUpdate.setProperty(LogConfig.TopicPlacementConstraintsProp, "invalid json")

      val alterConfigFuture = alterTopicConfigs(client, topic, configUpdate).all()
      JTestUtils.assertFutureError(alterConfigFuture, classOf[InvalidRequestException])

      val incrementalAlterConfigFuture = TestUtils.incrementalAlterTopicConfigs(client, topic, configUpdate).all()
      JTestUtils.assertFutureError(incrementalAlterConfigFuture, classOf[InvalidRequestException])
    }
  }

  private def electLeader(client: Admin, tp: TopicPartition, electionType: ElectionType): Unit = {
    val electionResult = client.electLeaders(electionType, Set(tp).asJava)
    electionResult.partitions.get.get(tp).asScala.foreach { exception =>
      throw exception
    }
  }

  private def waitForZkReassignmentToComplete(pause: Long = 100L): Unit = {
    TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
      s"Znode ${ReassignPartitionsZNode.path} wasn't deleted", pause = pause)
  }
}

object ConfluentObserverTest {
  case class BasicConstraint(count: Int, rack: String)

  @nowarn("cat=deprecation")
  private[server] def alterTopicConfigs(adminClient: Admin, topic: String, topicConfigs: Properties): AlterConfigsResult = {
    val configEntries = topicConfigs.asScala.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava
    val newConfig = new Config(configEntries)
    val configs = Map(new ConfigResource(ConfigResource.Type.TOPIC, topic) -> newConfig).asJava
    adminClient.alterConfigs(configs)
  }

  def createConfig(servers: Seq[KafkaServer]): Map[String, Object] = {
    Map(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers(servers),
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "20000"
    )
  }

  def reassignmentEntry(replicas: Seq[Int], observers: Seq[Int]): Optional[NewPartitionReassignment] = {
    Optional.of(
      NewPartitionReassignment.ofReplicasAndObservers(
        replicas.map(r => r: Integer).asJava,
        observers.map(o => o: Integer).asJava
      )
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
