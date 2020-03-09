/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import java.util.Optional
import kafka.log.LogConfig
import kafka.log.{Defaults => LogConfigDefaults}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.{AdminClient => JAdminClient}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.network.ListenerName
import org.junit.After
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import scala.collection.JavaConverters._

final class ConfluentBrokerPlacementConstraintTest extends ZooKeeperTestHarness {
  import ConfluentBrokerPlacementConstraintTest._

  var servers = Seq.empty[KafkaServer]
  val broker1 = 0
  val broker2 = 1

  val brokerConstraintValue = """{"version":1,"replicas":[{"count":1,"constraints":{"rack":"a"}}],"observers":[{"count":1,"constraints":{"rack":"b"}}]}"""

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val rack = Map(
      broker1 -> "a",
      broker2 -> "b",
    )
    val brokerConfigs = TestUtils.createBrokerConfigs(2, zkConnect, enableControlledShutdown = true)
    servers = brokerConfigs.map { config =>
      config.setProperty(KafkaConfig.RackProp, rack(config.getProperty(KafkaConfig.BrokerIdProp).toInt))
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
      config.setProperty(KafkaConfig.ControlledShutdownMaxRetriesProp, "1")
      config.setProperty(KafkaConfig.ControlledShutdownRetryBackoffMsProp, "1000")
      config.setProperty(KafkaConfig.ReplicaLagTimeMaxMsProp, "1000")
      config.setProperty(KafkaConfig.TopicPlacementConstraintsProp, brokerConstraintValue)
      TestUtils.createServer(KafkaConfig.fromProps(config))
    }
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testCreateTopicWithBrokerValues(): Unit = {
    // Creating a topic without a constraint uses the broker's value.
    // Describing the config returns the broker's value at creation time
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])

      client.createTopics(Seq(newTopic).asJava).all.get()

      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)

      val config = client
        .describeConfigs(Seq(resource).asJava)
        .values
        .get(resource)
        .get()

      val constraint = config.get(LogConfig.TopicPlacementConstraintsProp)

      assertEquals(brokerConstraintValue, constraint.value)
    }
  }

  @Test
  def testCreateTopicOverrideDefault(): Unit = {
    // Creating a topic with a placement constraint overrides the broker's placement constraint.
    // Describing config returns the value used in the create topic request.
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val overrideConstraintValue = """{"version":1,"replicas":[{"count":1,"constraints":{"rack":"b"}}],"observers":[{"count":1,"constraints":{"rack":"a"}}]}"""

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(
        Map(LogConfig.TopicPlacementConstraintsProp -> overrideConstraintValue).asJava
      )

      client.createTopics(Seq(newTopic).asJava).all.get()

      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)

      val config = client
        .describeConfigs(Seq(resource).asJava)
        .values
        .get(resource)
        .get()

      val constraint = config.get(LogConfig.TopicPlacementConstraintsProp)

      assertNotEquals(brokerConstraintValue, constraint.value)
      assertEquals(overrideConstraintValue , constraint.value)
    }
  }

  @Test
  def testDeletingTopicConfigDoesNotRevertToDefault(): Unit = {
    // Unsetting the constraint doesn't cause describe to return the placement constraint set in the brokers.
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])

      client.createTopics(Seq(newTopic).asJava).all.get()

      val resource = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val operation = new AlterConfigOp(
        new ConfigEntry(LogConfig.TopicPlacementConstraintsProp, ""),
        AlterConfigOp.OpType.DELETE
      )

      client.incrementalAlterConfigs(Map(resource -> Seq(operation).asJavaCollection).asJava).all.get()

      val config = client
        .describeConfigs(Seq(resource).asJava)
        .values
        .get(resource)
        .get()

      val constraint = config.get(LogConfig.TopicPlacementConstraintsProp)

      assertEquals(LogConfigDefaults.TopicPlacementConstraints, constraint.value)
    }
  }

  @Test
  def testValidateConstraintDefault(): Unit = {
    // Calling create topic with validate only returns the broker's placement constraint.
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      val option = new CreateTopicsOptions().validateOnly(true)

      val config = client.createTopics(Seq(newTopic).asJava, option).config(topic).get()

      val constraint = config.get(LogConfig.TopicPlacementConstraintsProp)

      assertEquals(brokerConstraintValue, constraint.value)
    }
  }
}

object ConfluentBrokerPlacementConstraintTest {
  def createConfig(servers: Seq[KafkaServer]): Map[String, Object] = {
    Map(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers(servers),
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "20000"
    )
  }

  def bootstrapServers(servers: Seq[KafkaServer]): String = {
    servers.map { server =>
      val port = server.socketServer.boundPort(ListenerName.normalised("PLAINTEXT"))
      s"localhost:$port"
    }.headOption.mkString(",")
  }
}
