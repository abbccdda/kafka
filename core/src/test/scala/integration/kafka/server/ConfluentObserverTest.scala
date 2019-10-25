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
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewPartitionReassignment
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.{AdminClient => JAdminClient}
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.network.ListenerName
import org.junit.After
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import scala.collection.JavaConverters._

final class ConfluentObserverTest extends ZooKeeperTestHarness {
  import ConfluentObserverTest._

  var servers = Seq.empty[KafkaServer]
  val broker1 = 0
  val broker2 = 1
  val broker3 = 2
  val broker4 = 3

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val rack = Map(
      broker1 -> "a",
      broker2 -> "a",
      broker3 -> "b",
      broker4 -> "c"
    )
    val brokerConfigs = TestUtils.createBrokerConfigs(4, zkConnect, false)
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
  def testLeaderInReplicaConstraints(): Unit = {
    // When leader is in the "replicas" constraints...
    // 1. test that brokers in the "observers" constraints shouldn't be in the ISR.
    // 2. test that brokers in the "replicas" constraints should be in the ISR.
    // 3. test that brokers not in the "observers" constraints should be in the ISR.
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2, broker3, broker4)

      val topicConfig = new Properties()
      topicConfig.setProperty(
        LogConfig.TopicPlacementConstraintsProp,
        basicTopicPlacement(BasicConstraint(3, "a"), Some(BasicConstraint(1, "b")))
      )

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers, topicConfig)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker1))
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker4))
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker3))
    }
  }

  @Test
  def testLeaderNotInReplicaConstraints(): Unit = {
    // When leader is in the "replicas" constraints...
    // 1. test that brokers in the "observers" constraints shouldn't be in the ISR.
    // 2. test that brokers in the "replicas" constraints should be in the ISR.
    // 3. test that brokers not in the "observers" constraints should be in the ISR.
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2, broker3, broker4)

      val topicConfig = new Properties()
      topicConfig.setProperty(
        LogConfig.TopicPlacementConstraintsProp,
        basicTopicPlacement(BasicConstraint(3, "b"), Some(BasicConstraint(1, "c")))
      )

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers, topicConfig)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker1))
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker3))
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker4))
    }
  }

  @Test
  def testLeaderInObserversConstraints(): Unit = {
    // Case: All of the replicas are in the ISR when the leader matches "observer" constraints
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2, broker3, broker4)

      val topicConfig = new Properties()
      topicConfig.setProperty(
        LogConfig.TopicPlacementConstraintsProp,
        basicTopicPlacement(BasicConstraint(3, "b"), Some(BasicConstraint(1, "c")))
      )

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers, topicConfig)

      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker1))
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker3))
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker4))

      val rollingServers = Seq(broker1, broker2, broker3)
      rollingServers.foreach { broker =>
        servers(broker).shutdown()
      }

      val electResult = client.electLeaders(ElectionType.UNCLEAN, Set(topicPartition).asJava)
      assertFalse(electResult.partitions.get.get(topicPartition).isPresent)
      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker4))

      rollingServers.foreach { broker =>
        servers(broker).startup()
      }

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker4))
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker3, broker4))
    }
  }

  @Test
  def testPreferredBrokerInIsr(): Unit = {
    // Case: preferred replica/broker should always be in the ISR.
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2, broker3, broker4)

      val topicConfig = new Properties()
      topicConfig.setProperty(
        LogConfig.TopicPlacementConstraintsProp,
        basicTopicPlacement(BasicConstraint(3, "b"), Some(BasicConstraint(1, "a")))
      )

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers, topicConfig)
      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker1))
      // All replicas part in the ISR because leader is an Observer
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker3, broker4))

      val rollingServers = Seq(broker1, broker2, broker3)
      rollingServers.foreach { broker =>
        servers(broker).shutdown()
      }

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker4))

      rollingServers.foreach { broker =>
        servers(broker).startup()
      }

      TestUtils.waitForBrokersInIsr(
        client,
        topicPartition,
        Set(
          broker1, // Preferred replica
          broker3, // Replica constraint
          broker4  // Partition leader
        )
      )
      TestUtils.waitForBrokersOutOfIsr(client, Set(topicPartition), Set(broker2)) // Observer
    }
  }

  @Test
  def testChangeObserverAssigned(): Unit = {
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
        val observerCConfig = new Properties()
        observerCConfig.setProperty(
          LogConfig.TopicPlacementConstraintsProp,
          basicTopicPlacement(BasicConstraint(2, "a"), Some(BasicConstraint(1, "c")))
        )
        TestUtils.alterTopicConfigs(client, topic, observerCConfig)
      }

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
  def testChangeSyncToObserver(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val assignment = Seq(broker1, broker2, broker3)

      TestUtils.createTopic(zkClient, topic, Map(partition -> assignment), servers, new Properties())
      val topicPartition = new TopicPartition(topic, partition)

      TestUtils.waitForLeaderToBecome(client, topicPartition, Option(broker1))
      // All replicas part in the ISR because leader is an Observer
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(broker1, broker2, broker3))

      {
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
  def testChangeObserverToSync(): Unit = {
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
  def testRemoveObserver(): Unit = {
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
  def testAddObserver(): Unit = {
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
    replicaRack: BasicConstraint,
    observerRack: Option[BasicConstraint]
  ): String = {
    val observers = observerRack.fold("") { constraint =>
      s""","observers":[{"count": ${constraint.count}, "constraints":{"rack":"${constraint.rack}"}}]"""
    }

    s"""{"version":1,"replicas":[{"count": ${replicaRack.count}, "constraints":{"rack":"${replicaRack.rack}"}}]$observers}"""
  }
}
