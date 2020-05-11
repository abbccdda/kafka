/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.server

import java.util.{Optional, Properties}

import kafka.log.LogConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidReplicaAssignmentException}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.{After, Before, Test}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

/**
 * This class tests the usage of observers/placement constraints on a reduced IBP.
 */
class ObserverCompatibilityTest extends ZooKeeperTestHarness {
  import ConfluentObserverTest._

  var servers = Seq.empty[KafkaServer]
  val broker1 = 0
  val broker2 = 1

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val rack = Map(
      broker1 -> "a",
      broker2 -> "b"
    )
    val brokerConfigs = TestUtils.createBrokerConfigs(2, zkConnect, false)
    servers = brokerConfigs.map { config =>
      config.setProperty(KafkaConfig.RackProp, rack(config.getProperty(KafkaConfig.BrokerIdProp).toInt))
      config.setProperty(KafkaConfig.InterBrokerProtocolVersionProp, "2.3")
      TestUtils.createServer(KafkaConfig.fromProps(config))
    }
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testCanUseReplicaPlacementConstraintWithoutObservers(): Unit = {
    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(1, "a"),
        observerConstraint = None
      )).asJava)
      client.createTopics(Seq(newTopic).asJava).all().get()
    }
  }

  @Test
  def testCanReassignToEmptyObservers(): Unit = {
    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val topicPartition = new TopicPartition(topic, 0)
      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.of(2.toShort: java.lang.Short))
      client.createTopics(Seq(newTopic).asJava).all().get()

      // Get current assignment
      val description = client.describeTopics(Set(topic).asJava).all.get.asScala
      val replicas = description
        .values
        .flatMap(_.partitions.asScala.flatMap(_.replicas.asScala))
        .map(_.id)
        .toSeq

      val swappedBrokers = Seq(replicas(1), replicas(0))
      client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(swappedBrokers, Seq())).asJava
      ).all().get()

      TestUtils.waitForReplicasAssigned(client, topicPartition, swappedBrokers)
    }
  }

  @Test
  def testCannotUseReplicaPlacementConstraintWithObservers(): Unit = {
    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(1, "a"),
        observerConstraint = Some(BasicConstraint(1, "b"))
      )).asJava)

      val future = client.createTopics(Seq(newTopic).asJava).all()
      JTestUtils.assertFutureError(future, classOf[InvalidReplicaAssignmentException])
    }
  }

  @Test
  def testCannotReassignToObservers(): Unit = {
    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val topicPartition = new TopicPartition(topic, 0)

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      client.createTopics(Seq(newTopic).asJava).all().get()
      val future = client.alterPartitionReassignments(
        Map(topicPartition -> reassignmentEntry(Seq(broker1), Seq(broker2))).asJava
      ).all()

      JTestUtils.assertFutureError(future, classOf[InvalidReplicaAssignmentException])
    }
  }

  @Test
  def testCannotEnableObserversThroughAlterConfigAPIs(): Unit = {
    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      client.createTopics(Seq(newTopic).asJava).all().get()

      val configUpdate = new Properties()
      configUpdate.setProperty(LogConfig.TopicPlacementConstraintsProp,
        basicTopicPlacement(BasicConstraint(1, "a"), Some(BasicConstraint(1, "b"))))

      val alterConfigFuture = ConfluentObserverTest.alterTopicConfigs(client, topic, configUpdate).all()
      JTestUtils.assertFutureError(alterConfigFuture, classOf[InvalidConfigurationException])

      val incrementalAlterConfigFuture = TestUtils.incrementalAlterTopicConfigs(client, topic, configUpdate).all()
      JTestUtils.assertFutureError(incrementalAlterConfigFuture, classOf[InvalidConfigurationException])
    }
  }

}
