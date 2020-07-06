package kafka.admin

import java.util.{Optional}

import kafka.admin.ReassignPartitionsCommand._
import kafka.admin.ReassignPartitionsIntegrationTest._
import kafka.common.AdminCommandFailedException
import kafka.log.LogConfig
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{Admin, NewTopic, AdminClient => JAdminClient}
import org.apache.kafka.common.{TopicPartition}
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertThrows, assertEquals}
import org.junit.function.ThrowingRunnable

import scala.collection.Map
import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ReassignPartitionsWithObserversIntegrationTest extends ZooKeeperTestHarness {
  import kafka.server.ConfluentObserverTest._

  var servers = Seq.empty[KafkaServer]
  val broker1 = 0
  val broker2 = 1
  val broker3 = 2
  val broker4 = 3
  val broker5 = 4
  val broker6 = 5

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val rack = Map(
      broker1 -> "a",
      broker2 -> "a",
      broker3 -> "a",
      broker4 -> "b",
      broker5 -> "b",
      broker6 -> "b"
    )
    val brokerConfigs = TestUtils.createBrokerConfigs(6, zkConnect, enableControlledShutdown = true)
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

  /**
   * Test running a quick reassignment with observers
   */
  @Test
  def testReassignment(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val topicPartition = new TopicPartition(topic, partition)

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(2, "a"),
        observerConstraint = Some(BasicConstraint(2, "b"))
      )).asJava)
      client.createTopics(Seq(newTopic).asJava).all().get()

      val (assignmentJson, targetAssignment) = generateReassignmentJson(client, topic, partition)

      // Execute the assignment and make sure that Brokers were assigned properly
      runExecuteAssignment(client, false, assignmentJson, -1L, -1L)
      TestUtils.waitForReplicasAssigned(client, topicPartition, targetAssignment.replicas)
      TestUtils.waitForObserversAssigned(client, topicPartition, targetAssignment.observers)
    }
  }

  /**
   * Test running a quick throttled reassignment with observers
   */
  @Test
  def testThrottledReassignment(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val topicPartition = new TopicPartition(topic, partition)
      
      // Create a topic a generate some messages
      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(2, "a"),
        observerConstraint = Some(BasicConstraint(2, "b"))
      )).asJava)
      client.createTopics(Seq(newTopic).asJava).all().get()
      TestUtils.generateAndProduceMessages(servers, topic, 10)

      val (assignmentJson, targetAssignment) = generateReassignmentJson(client, topic, partition)
      val interBrokerThrottle = 300000L
      val throttledBrokerConfigs = generateThrottleConfigs(interBrokerThrottle, targetAssignment.replicas)

      // Execute the assignment and then verify that Brokers were assigned properly
      runExecuteAssignment(client, false, assignmentJson, interBrokerThrottle, -1L)
      waitForBrokerLevelThrottles(client, throttledBrokerConfigs)

      val finalAssignment = Map(
        topicPartition ->
          PartitionReassignmentState(targetAssignment, targetAssignment, true))

      // Now run verify while preserving throttles
      waitForVerifyAssignment(client, assignmentJson, true,
        VerifyAssignmentResult(finalAssignment))
      waitForBrokerLevelThrottles(client, throttledBrokerConfigs)

      // Run again without preserving throttles
      waitForVerifyAssignment(client, assignmentJson, false,
        VerifyAssignmentResult(finalAssignment))

      // Check that all dynamic throttle configs were removed
      waitForDefaultBrokerLevelThrottles(client, throttledBrokerConfigs.keySet.toSeq)
    }
  }

  /**
   * Test producing and consuming from a topic while observers are being reassigned
   */
  @Test
  def testProduceAndConsumeWithObserverReassignmentInProgress(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic = "observer-topic"
      val partition = 0
      val topicPartition = new TopicPartition(topic, partition)

      val newTopic = new NewTopic(topic, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(2, "a"),
        observerConstraint = Some(BasicConstraint(2, "b"))
      )).asJava)
      client.createTopics(Seq(newTopic).asJava).all().get()

      // Generate new assignments
      val (assignmentJson, targetAssignment) = generateReassignmentJson(client, topic, partition)
      val interBrokerThrottle = 300000L
      val throttledBrokerConfigs = generateThrottleConfigs(interBrokerThrottle, targetAssignment.replicas)

      val brokerList = TestUtils.bootstrapServers(servers,
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))

      // Execute reassignments then produce and consume
      runExecuteAssignment(client, false, assignmentJson, interBrokerThrottle, -1L)
      TestUtils.generateAndProduceMessages(servers, topic, 100)
      val consumer = TestUtils.createConsumer(brokerList)
      try {
        consumer.assign(Seq(topicPartition).asJava)
        TestUtils.pollUntilAtLeastNumRecords(consumer, numRecords = 100)
      } finally {
        consumer.close()
      }
      waitForBrokerLevelThrottles(client, throttledBrokerConfigs)

      val finalAssignment = Map(topicPartition ->
        PartitionReassignmentState(
          targetAssignment,
          targetAssignment, true))
      // Verify assignment and remove broker throttles
      waitForVerifyAssignment(client, assignmentJson, false,
        VerifyAssignmentResult(finalAssignment))

      // Check that all dynamic throttle configs were removed
      waitForDefaultBrokerLevelThrottles(client, throttledBrokerConfigs.keySet.toSeq)
    }
  }

  /**
   * Make sure that generateAssignment fails if there is a topic placement config
   */
  @Test
  def testFailGenerateAssignmentWithConstraints(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic1 = "observer-topic"
      val topic2 = "test-not-constrained"
      // Create a new topic
      val newTopic1 = new NewTopic(topic1, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      val newTopic2 = new NewTopic(topic2, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic1.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(2, "a"),
        observerConstraint = Some(BasicConstraint(2, "b"))
      )).asJava)
      client.createTopics(Seq(newTopic1, newTopic2).asJava).all().get()

      // Check that generateAssignment fails with observers
      assertEquals("Assignments could not be generated because the following topics have placement constraints observer-topic",
        assertThrows("Expected generateAssignment to fail",
          classOf[AdminCommandFailedException], new ThrowingRunnable {
            override def run():Unit = {
              generateAssignment(client,
                s"""{"topics":[{"topic":"$topic1"},{"topic":"$topic2"}]}""", 
                s"$broker1,$broker2,$broker4,$broker5", false)
            }
          }).getMessage
      )
    }
  }

  /**
   * Make sure that generateAssignment fails if there is a topic placement config
   */
  @Test
  def testFailGenerateAssignmentWithConstraintsZk(): Unit = {
    TestUtils.resource(JAdminClient.create(createConfig(servers).asJava)) { client =>
      val topic1 = "observer-topic"
      val topic2 = "test-not-constrained"
      // Create a new topic
      val newTopic1 = new NewTopic(topic1, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      val newTopic2 = new NewTopic(topic2, Optional.of(1: Integer), Optional.empty[java.lang.Short])
      newTopic1.configs(Map(LogConfig.TopicPlacementConstraintsProp -> basicTopicPlacement(
        replicaConstraint = BasicConstraint(2, "a"),
        observerConstraint = Some(BasicConstraint(2, "b"))
      )).asJava)
      client.createTopics(Seq(newTopic1, newTopic2).asJava).all().get()

      // Check that generateAssignment fails with observers
      assertEquals("Assignments could not be generated because the following topics have placement constraints observer-topic",
        assertThrows("Expected generateAssignment to fail",
          classOf[AdminCommandFailedException], new ThrowingRunnable {
            override def run():Unit = {
              generateAssignment(zkClient,
                s"""{"topics":[{"topic":"$topic1"},{"topic":"$topic2"}]}""", 
                s"$broker1,$broker2,$broker4,$broker5", false)
            }
          }).getMessage
      )
    }
  }

  /**
   * Assume that there are 2 racks "a" and "b" each with 3 brokers. 
   * Assume that 2 replicas were assigned to "a" and 2 observers assigned to "b".
   * Generate reassignments to guarantee that the broker not in use in each rack is picked as a reassignment.
   *
   * @param client client to use
   * @param topic topic to reassign
   * @param partition partition of topic to reassign
   * @return reassignment json and target assignment
   */
  def generateReassignmentJson(client: Admin, topic: String, partition: Int) : (String, Assignment) = {
      // Wait until the topic description is sent back from the brokers
      val topicDescription = waitForTopicDescription(client, topic)

      // Get the partition from the topic description
      val partitionFound = topicDescription.partitions.asScala.find(part => part.partition == partition).get

      // Get current assignments and calculate reassignments
      val replicas = partitionFound.replicas.asScala.map(node => node.id).toSeq
      val observers = partitionFound.observers.asScala.toSeq.map(node => node.id).toSeq
      val newObservers = observers.map(i => ((i + 1) % 3) + 3)
      val newReplicas = replicas.slice(0,2).map(i => (i + 1) % 3) ++ newObservers

      // Generate reassignment json and throttle configs
      val targetAssignment = Assignment(newReplicas, newObservers)
      val replicaString = targetAssignment.replicas.mkString(",")
      val observerString = targetAssignment.observers.mkString(",")

      val json = """{"version":1,"partitions":""" +
      s"""[{"topic":"$topic","partition":$partition,"replicas":[$replicaString],"observers":[$observerString],""" +
      """"log_dirs":["any","any","any","any"]}]}"""

      (json, targetAssignment)
  }

  /**
   * Generate throttle configurations for brokers
   * 
   * @param throttle throttle to use
   * @param brokers brokers to include in generated map
   * @return map of brokers and their generated throttle configuration
   */
  def generateThrottleConfigs(throttle: Long, brokers: Seq[Int]) = {
      val throttledConfigMap = Map[String, Long](
        brokerLevelLeaderThrottle -> throttle,
        brokerLevelFollowerThrottle -> throttle,
        brokerLevelLogDirThrottle -> -1L
      )

      brokers.map{ brokerId =>
        (brokerId, throttledConfigMap)
      }.toMap
  }

  /**
   * Wait until describeTopics returns the topic that we care about
   *
   * @param client client to use
   * @param topicToWaitOn this function will busy wait until we recieved the description for this topic
   * @return description of topicToWaitOn
   */
  def waitForTopicDescription(client: Admin, 
                              topicToWaitOn: String) : TopicDescription = {
    TestUtils.awaitValue(() => client
            .describeTopics(Seq(topicToWaitOn).asJava)
            .all()
            .get.asScala
            .get(topicToWaitOn),
            s"Timed out waiting for description of topic $topicToWaitOn"
            )
  }

  /**
   * Wait until broker throttles match target throttles
   *
   * @param client client to use
   * @param targetThrottles target configuration to wait for
   */
  private def waitForBrokerLevelThrottles(client: Admin, targetThrottles: Map[Int, Map[String, Long]]): Unit = {
    var curThrottles: Map[Int, Map[String, Long]] = Map.empty
    TestUtils.waitUntilTrue(() => {
      curThrottles = describeBrokerLevelThrottles(client, targetThrottles.keySet.toSeq)
      targetThrottles.equals(curThrottles)
    }, s"timed out waiting for broker throttle to become ${targetThrottles}.  " +
      s"Latest throttles were ${curThrottles}", pause = 25)
  }

  /**
   * Describe the broker-level throttles in the cluster.
   *
   * @param client client to use
   * @param brokerIds brokers to fetch throttle configurations for
   * @return A map whose keys are broker IDs and whose values are throttle
   *         information.  The nested maps are keyed on throttle name.
   */
  private def describeBrokerLevelThrottles(client: Admin, brokerIds: Seq[Int]): Map[Int, Map[String, Long]] = {
    val brokerConfigs = client.describeConfigs(brokerIds
                                                    .map(id => new ConfigResource(Type.BROKER, id.toString)).asJava)
                                                    .all().get().asScala

    brokerConfigs.map {
      case (resource, config) => (resource.name.toInt, brokerLevelThrottles.map {
        throttleName => (throttleName, Option(config.get(throttleName)).fold(-1L)(_.value.toLong))
      }.toMap)
    }.toMap
  }

  /**
   * Wait until none of the broker configurations in the broker list are dynamic
   *
   * @param client client to use
   * @param brokerIds brokers to wait for defaults for
   */
  private def waitForDefaultBrokerLevelThrottles(client: Admin, brokerIds: Seq[Int]) = {
      TestUtils.waitUntilTrue(() => 
        hasAllDefaultBrokerLevelThrottles(client, brokerIds),
        "Timed out waiting for brokerLevelThrottles to be removed")
  }

  /**
   * Check that none of the broker configurations in the broker list are dynamic
   *
   * @param client client to use
   * @param brokerIds brokers to check for no throttles
   */
  private def hasAllDefaultBrokerLevelThrottles(client: Admin, brokerIds: Seq[Int]) : Boolean = {
    // Get broker configs
    val brokerConfigs = client.describeConfigs(brokerIds
                                                    .map(id => new ConfigResource(Type.BROKER, id.toString)).asJava)
                                                    .all().get().asScala

    // Make sure that none of the Config Sources are Dynamic      
    brokerConfigs.forall {
      case (resource, config) => (brokerLevelThrottles.forall {
        throttleName => Option(config.get(throttleName)).forall(_.source != ConfigSource.DYNAMIC_BROKER_CONFIG)
      })
    }
  }
}
