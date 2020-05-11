/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import java.util.Properties

import kafka.admin.RackAwareTest
import kafka.admin.TopicCommand.AdminClientTopicService
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.ConfluentBrokerPlacementConstraintTest.bootstrapServers
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AdminClient => JAdminClient}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.internals.Topic
import org.junit.Assert._
import org.junit.rules.TestName
import org.junit.{After, Before, Rule, Test}

import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.util.Random

abstract class ConfluentBrokerInternalTopicsPlacementTest extends KafkaServerTestHarness with Logging with RackAwareTest {

  protected val placementJsonInternalTopics: String =
    """
      |{
      | "version": 1,
      | "replicas": [{
      |   "count": 2,
      |   "constraints": {
      |     "rack": "rack1"
      |   }
      | },
      | {
      |   "count": 2,
      |   "constraints": {
      |     "rack": "rack2"
      |   }
      | }],
      | "observers": []
      |}
      |""".stripMargin.split("\\s+").mkString
  protected val placementJsonBrokerDefault: String =
    """
      |{
      | "version": 1,
      | "replicas": [{
      |   "count": 1,
      |   "constraints": {
      |     "rack": "a"
      |     }
      |   }],
      | "observers": [{
      |   "count": 1,
      |   "constraints":{
      |     "rack": "b"
      |   }
      | }]
      |}
      |""".stripMargin

  protected val numPartitions = 1
  protected val defaultReplicationFactor = 1.toShort

  protected var topicService: AdminClientTopicService = _
  protected var adminClient: Admin = _
  protected var testTopicName: String = _

  private val _testName = new TestName

  @Rule def testName = _testName

  @Before
  def setup(): Unit = {
    // create adminClient
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    adminClient = Admin.create(props)
    topicService = AdminClientTopicService(adminClient)
    testTopicName = s"${testName.getMethodName}-${Random.alphanumeric.take(10).mkString}"
  }

  @After
  def close(): Unit = {
    // adminClient is closed by topicService
    if (topicService != null)
      topicService.close()
  }

  def waitForTopicCreated(topicName: String, timeout: Int = 10000): Unit = {
    TestUtils.waitUntilMetadataIsPropagated(servers, topicName, partition = 0, timeout)
  }

  def createConfig(servers: Seq[KafkaServer]): Map[String, Object] = {
    Map(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers(servers),
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "20000"
    )
  }

  def getPlacementConstraint(client: JAdminClient, topicName: String): String = {
    val topic = new ConfigResource(ConfigResource.Type.TOPIC, topicName)
    val describeResult = client.describeConfigs(Seq(topic).asJava).all.get
    val topicConfig = describeResult.get(topic)

    topicConfig.get(LogConfig.TopicPlacementConstraintsProp).value()
  }
}

class OffsetsTopicPlacementTest extends ConfluentBrokerInternalTopicsPlacementTest {
  override def generateConfigs: Seq[KafkaConfig] = {
    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, enableControlledShutdown = true)
    brokerConfigs.map { config =>
      config.put(KafkaConfig.TopicPlacementConstraintsProp, placementJsonBrokerDefault)
      config.put(KafkaConfig.OffsetsTopicPlacementConstraintsProp, placementJsonInternalTopics)

      KafkaConfig.fromProps(config)
    }
  }

  @Test
  def testOffsetsTopicPlacementOverridesBrokerDefaults(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    waitForTopicCreated(Topic.GROUP_METADATA_TOPIC_NAME)

    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val placementConstraint = getPlacementConstraint(client, Topic.GROUP_METADATA_TOPIC_NAME)

      // The placement config for the offsets topic should override the default config for other internal topics.
      assertEquals(placementConstraint, placementJsonInternalTopics)
    }
  }
}

class OffsetssTopicNoPlacementTest extends ConfluentBrokerInternalTopicsPlacementTest {
  override def generateConfigs: Seq[KafkaConfig] = {
    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, enableControlledShutdown = true)
    brokerConfigs.map { config =>
      config.put(KafkaConfig.TopicPlacementConstraintsProp, placementJsonBrokerDefault)
      config.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "3")  // any value here should imply no placement config

      KafkaConfig.fromProps(config)
    }
  }

  @Test
  def testOffsetsTopicCanHaveNoPlacement(): Unit = {
    TestUtils.createOffsetsTopic(zkClient, servers)
    waitForTopicCreated(Topic.GROUP_METADATA_TOPIC_NAME)

    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val placementConstraint = getPlacementConstraint(client, Topic.GROUP_METADATA_TOPIC_NAME)

      // Even though we have default broker-level topic-placement config, we
      // should ignore it for the offsets topic because we configured it to
      // have a replication factor.
      assertEquals(placementConstraint, "")
    }
  }
}

class TransactionsTopicPlacementTest extends ConfluentBrokerInternalTopicsPlacementTest {
  override def generateConfigs: Seq[KafkaConfig] = {
    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, enableControlledShutdown = true)
    brokerConfigs.map { config =>
      config.put(KafkaConfig.TopicPlacementConstraintsProp, placementJsonBrokerDefault)
      config.put(KafkaConfig.TransactionsTopicPlacementConstraintsProp, placementJsonInternalTopics)

      KafkaConfig.fromProps(config)
    }
  }

  @Test
  def testTransactionsTopicPlacementOverridesBrokerDefaults(): Unit = {
    val server = servers.head
    TestUtils.createTopic(zkClient, Topic.TRANSACTION_STATE_TOPIC_NAME,
      server.config.getInt(KafkaConfig.TransactionsTopicPartitionsProp),
      server.config.getShort(KafkaConfig.TransactionsTopicReplicationFactorProp).toInt,
      servers,
      server.transactionCoordinator.transactionTopicConfigs)

    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val placementConstraint = getPlacementConstraint(client, Topic.TRANSACTION_STATE_TOPIC_NAME)

      // The placement config for the transactions topic should override the default config for other internal topics.
      assertEquals(placementConstraint, placementJsonInternalTopics)
    }
  }
}

class TransactionsTopicNoPlacementTest extends ConfluentBrokerInternalTopicsPlacementTest {
  override def generateConfigs: Seq[KafkaConfig] = {
    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, enableControlledShutdown = true)
    brokerConfigs.map { config =>
      config.put(KafkaConfig.TopicPlacementConstraintsProp, placementJsonBrokerDefault)
      config.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "3")  // any value here should imply no placement config

      KafkaConfig.fromProps(config)
    }
  }

  @Test
  def testTransactionsTopicCanHaveNoPlacement(): Unit = {
    val server = servers.head
    TestUtils.createTopic(zkClient, Topic.TRANSACTION_STATE_TOPIC_NAME,
      server.config.getInt(KafkaConfig.TransactionsTopicPartitionsProp),
      server.config.getShort(KafkaConfig.TransactionsTopicReplicationFactorProp).toInt,
      servers,
      server.transactionCoordinator.transactionTopicConfigs)

    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val placementConstraint = getPlacementConstraint(client, Topic.TRANSACTION_STATE_TOPIC_NAME)

      // Even though we have default broker-level topic-placement config, we
      // should ignore it for the transactions topic because we configured it to
      // have a replication factor.
      assertEquals(placementConstraint, "")
    }
  }
}
