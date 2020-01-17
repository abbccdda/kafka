/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import java.util
import java.util.Properties

import kafka.common.TopicPlacement
import kafka.controller.ReplicaAssignment
import kafka.log._
import kafka.server.DynamicConfig.Broker._
import kafka.server.KafkaConfig._
import kafka.server.{ConfigType, KafkaConfig, KafkaServer}
import kafka.utils.CoreUtils._
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import kafka.zk.{AdminZkClient, KafkaZkClient, ZooKeeperTestHarness}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.apache.kafka.common.errors.{InvalidConfigurationException, InvalidReplicaAssignmentException, InvalidTopicException, TopicExistsException}
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Test}
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, immutable}
import scala.compat.java8.OptionConverters._

class AdminZkClientTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  var servers: Seq[KafkaServer] = Seq()

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testManualReplicaAssignment(): Unit = {
    val brokers = List(0, 1, 2, 3, 4)
    TestUtils.createBrokersInZk(zkClient, brokers)

    val topicConfig = new Properties()

    // duplicate brokers
    intercept[InvalidReplicaAssignmentException] {
      adminZkClient.createTopicWithAssignment(
        "test",
        topicConfig,
        Map(0 -> ReplicaAssignment(Seq(0,0), Seq.empty))
      )
    }

    // inconsistent replication factor
    intercept[InvalidReplicaAssignmentException] {
      adminZkClient.createTopicWithAssignment(
        "test",
        topicConfig,
        Map(
          0 -> ReplicaAssignment(Seq(0,1), Seq.empty),
          1 -> ReplicaAssignment(Seq(0), Seq.empty)
        )
      )
    }

    // partitions should be 0-based
    intercept[InvalidReplicaAssignmentException] {
      adminZkClient.createTopicWithAssignment(
        "test",
        topicConfig,
        Map(
          1 -> ReplicaAssignment(Seq(1,2), Seq.empty),
          2 -> ReplicaAssignment(Seq(1,2), Seq.empty)
        )
      )
    }

    // partitions should be 0-based and consecutive
    intercept[InvalidReplicaAssignmentException] {
      adminZkClient.createTopicWithAssignment(
        "test",
        topicConfig,
        Map(
          0 -> ReplicaAssignment(Seq(1,2), Seq.empty),
          0 -> ReplicaAssignment(Seq(1,2), Seq.empty),
          3 -> ReplicaAssignment(Seq(1,2), Seq.empty)
        )
      )
    }

    // partitions should be 0-based and consecutive
    intercept[InvalidReplicaAssignmentException] {
      adminZkClient.createTopicWithAssignment(
        "test",
        topicConfig,
        Map(
          -1 -> ReplicaAssignment(Seq(1,2), Seq.empty),
          1 -> ReplicaAssignment(Seq(1,2), Seq.empty),
          2 -> ReplicaAssignment(Seq(1,2), Seq.empty),
          4 -> ReplicaAssignment(Seq(1,2), Seq.empty)
        )
      )
    }

    // good assignment
    val assignment = Map(
      0 -> ReplicaAssignment(Seq(0, 1, 2), Seq.empty),
      1 -> ReplicaAssignment(Seq(1, 2, 3), Seq.empty)
    )
    adminZkClient.createTopicWithAssignment("test", topicConfig, assignment)
    val found = zkClient.getPartitionAssignmentForTopics(Set("test"))
    assertEquals(assignment, found("test"))
  }

  @Test
  def testTopicCreationInZK(): Unit = {
    val expectedReplicaAssignment = Map(
      0  -> ReplicaAssignment(List(0, 1, 2), Seq.empty),
      1  -> ReplicaAssignment(List(1, 2, 3), Seq.empty),
      2  -> ReplicaAssignment(List(2, 3, 4), Seq.empty),
      3  -> ReplicaAssignment(List(3, 4, 0), Seq.empty),
      4  -> ReplicaAssignment(List(4, 0, 1), Seq.empty),
      5  -> ReplicaAssignment(List(0, 2, 3), Seq.empty),
      6  -> ReplicaAssignment(List(1, 3, 4), Seq.empty),
      7  -> ReplicaAssignment(List(2, 4, 0), Seq.empty),
      8  -> ReplicaAssignment(List(3, 0, 1), Seq.empty),
      9  -> ReplicaAssignment(List(4, 1, 2), Seq.empty),
      10 -> ReplicaAssignment(List(1, 2, 3), Seq.empty),
      11 -> ReplicaAssignment(List(1, 3, 4), Seq.empty)
    )
    val leaderForPartitionMap = immutable.Map(
      0 -> 0,
      1 -> 1,
      2 -> 2,
      3 -> 3,
      4 -> 4,
      5 -> 0,
      6 -> 1,
      7 -> 2,
      8 -> 3,
      9 -> 4,
      10 -> 1,
      11 -> 1
    )
    val topic = "test"
    val topicConfig = new Properties()
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    // create the topic
    adminZkClient.createTopicWithAssignment(topic, topicConfig, expectedReplicaAssignment)
    // create leaders for all partitions
    TestUtils.makeLeaderForPartition(zkClient, topic, leaderForPartitionMap, 1)
    val actualReplicaMap = zkClient.getPartitionAssignmentForTopics(Set(topic))(topic)
    assertEquals(expectedReplicaAssignment.size, actualReplicaMap.size)
    for((key, value) <- actualReplicaMap) {
      assertEquals(expectedReplicaAssignment(key), value)
    }

    intercept[TopicExistsException] {
      // shouldn't be able to create a topic that already exists
      adminZkClient.createTopicWithAssignment(topic, topicConfig, expectedReplicaAssignment)
    }
  }

  @Test
  def testTopicCreationWithCollision(): Unit = {
    val topic = "test.topic"
    val collidingTopic = "test_topic"
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    // create the topic
    adminZkClient.createTopic(topic, 3, 1)

    intercept[InvalidTopicException] {
      // shouldn't be able to create a topic that collides
      adminZkClient.createTopic(collidingTopic, 3, 1)
    }
  }

  @Test
  def testMockedConcurrentTopicCreation(): Unit = {
    val topic = "test.topic"

    // simulate the ZK interactions that can happen when a topic is concurrently created by multiple processes
    val zkMock: KafkaZkClient = EasyMock.createNiceMock(classOf[KafkaZkClient])
    EasyMock.expect(zkMock.topicExists(topic)).andReturn(false)
    EasyMock.expect(zkMock.getAllTopicsInCluster).andReturn(Set("some.topic", topic, "some.other.topic"))
    EasyMock.replay(zkMock)
    val adminZkClient = new AdminZkClient(zkMock)

    intercept[TopicExistsException] {
      adminZkClient.validateTopicCreate(topic, Map.empty, new Properties)
    }
  }

  @Test
  def testConcurrentTopicCreation(): Unit = {
    val topic = "test-concurrent-topic-creation"
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    val props = new Properties
    props.setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    def createTopic(): Unit = {
      try adminZkClient.createTopic(topic, 3, 1, props)
      catch { case _: TopicExistsException => () }
      val (_, partitionAssignment) = zkClient.getPartitionAssignmentForTopics(Set(topic)).head
      assertEquals(3, partitionAssignment.size)
      partitionAssignment.foreach { case (partition, partitionReplicaAssignment) =>
        assertEquals(s"Unexpected replication factor for $partition",
          1, partitionReplicaAssignment.replicas.size)
      }
      val savedProps = zkClient.getEntityConfigs(ConfigType.Topic, topic)
      assertEquals(props, savedProps)
    }

    TestUtils.assertConcurrent("Concurrent topic creation failed", Seq(() => createTopic, () => createTopic),
      JTestUtils.DEFAULT_MAX_WAIT_MS.toInt)
  }

  /**
   * This test creates a topic with a few config overrides and checks that the configs are applied to the new topic
   * then changes the config and checks that the new values take effect.
   */
  @Test
  def testTopicConfigChange(): Unit = {
    val partitions = 3
    val topic = "my-topic"
    val server = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))
    servers = Seq(server)

    def makeConfig(messageSize: Int, retentionMs: Long, throttledLeaders: String, throttledFollowers: String) = {
      val props = new Properties()
      props.setProperty(LogConfig.MaxMessageBytesProp, messageSize.toString)
      props.setProperty(LogConfig.RetentionMsProp, retentionMs.toString)
      props.setProperty(LogConfig.LeaderReplicationThrottledReplicasProp, throttledLeaders)
      props.setProperty(LogConfig.FollowerReplicationThrottledReplicasProp, throttledFollowers)
      props
    }

    def checkConfig(messageSize: Int, retentionMs: Long, throttledLeaders: String, throttledFollowers: String, quotaManagerIsThrottled: Boolean): Unit = {
      def checkList(actual: util.List[String], expected: String): Unit = {
        assertNotNull(actual)
        if (expected == "")
          assertTrue(actual.isEmpty)
        else
          assertEquals(expected.split(",").toSeq, actual.asScala)
      }
      TestUtils.retry(10000) {
        for (part <- 0 until partitions) {
          val tp = new TopicPartition(topic, part)
          val log = server.logManager.getLog(tp)
          assertTrue(log.isDefined)
          assertEquals(retentionMs, log.get.config.retentionMs)
          assertEquals(messageSize, log.get.config.maxMessageSize)
          checkList(log.get.config.LeaderReplicationThrottledReplicas, throttledLeaders)
          checkList(log.get.config.FollowerReplicationThrottledReplicas, throttledFollowers)
          assertEquals(quotaManagerIsThrottled, server.quotaManagers.leader.isThrottled(tp))
        }
      }
    }

    // create a topic with a few config overrides and check that they are applied
    val maxMessageSize = 1024
    val retentionMs = 1000 * 1000
    adminZkClient.createTopic(topic, partitions, 1, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))

    //Standard topic configs will be propagated at topic creation time, but the quota manager will not have been updated.
    checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", false)

    //Update dynamically and all properties should be applied
    adminZkClient.changeTopicConfig(topic, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))

    checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", true)

    // now double the config values for the topic and check that it is applied
    val newConfig = makeConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*")
    adminZkClient.changeTopicConfig(topic, makeConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*"))
    checkConfig(2 * maxMessageSize, 2 * retentionMs, "*", "*", quotaManagerIsThrottled = true)

    // Verify that the same config can be read from ZK
    val configInZk = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
    assertEquals(newConfig, configInZk)

    //Now delete the config
    adminZkClient.changeTopicConfig(topic, new Properties)
    checkConfig(Defaults.MaxMessageSize, Defaults.RetentionMs, "", "", quotaManagerIsThrottled = false)

    //Add config back
    adminZkClient.changeTopicConfig(topic, makeConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1"))
    checkConfig(maxMessageSize, retentionMs, "0:0,1:0,2:0", "0:1,1:1,2:1", quotaManagerIsThrottled = true)

    //Now ensure updating to "" removes the throttled replica list also
    adminZkClient.changeTopicConfig(topic, propsWith((LogConfig.FollowerReplicationThrottledReplicasProp, ""), (LogConfig.LeaderReplicationThrottledReplicasProp, "")))
    checkConfig(Defaults.MaxMessageSize, Defaults.RetentionMs, "", "",  quotaManagerIsThrottled = false)
  }

  /**
   * Test that addPartition method succeeds when
   * 1. no topic placement constraint is specified, and
   * 2. no replica assignment is provided
   *
   * In this case the code should do a normal replica assignment for new partition as done
   * during topic creation time.
   */
  @Test
  def testAddPartitionWithNoPlacementConstraintNoAssignment(): Unit = {
    val existingAssignment = Map(0 -> ReplicaAssignment(List(0, 1, 2), Seq.empty))

    val brokers = (0 until 10).map { id =>
      val rack = s"rack-${id / 5 + 1}"
      BrokerMetadata(id, Some(rack))
    }

    val topicName = "test-topic"
    TestUtils.createBrokersInZk(brokers, zkClient)
    val props = new Properties
    adminZkClient.createTopic(topicName, 1, 3, props)

    val partitionAssignment = adminZkClient.addPartitions(
      topicName, existingAssignment, brokers, numPartitions = 3, None)

    assertEquals(3, partitionAssignment.size)

    // Test if replica assignment was done as per placement constraint
    partitionAssignment.values.map(_.replicas).foreach {
      assignedBrokers => {
        // Check that each partition gets 3 replicas
        assertEquals(3, assignedBrokers.toSet.size)
      }
    }

    partitionAssignment.values.map(_.observers).foreach { observers => {
        assertTrue(observers.mkString(","), observers.isEmpty)
      }
    }
  }

  /**
   * Test that addPartition method succeeds when
   * 1. no topic placement constraint is specified, and
   * 2. a replica assignment is provided
   *
   * In this case the provided assignment should be used as is.
   */
  @Test
  def testAddPartitionWithNoPlacementConstraintWithAssignment(): Unit = {
    val existingAssignment = Map(0 -> ReplicaAssignment(List(0, 1, 2), Seq.empty))

    val brokers = (0 until 10).map { id =>
      val rack = s"rack-${id / 5 + 1}"
      BrokerMetadata(id, Some(rack))
    }

    val topicName = "test-topic"
    TestUtils.createBrokersInZk(brokers, zkClient)
    val props = new Properties
    adminZkClient.createTopic(topicName, 1, 3, props)

    val newReplicaAssignment = Map(
      1  -> ReplicaAssignment(List(1, 2, 3), Seq.empty),
      2  -> ReplicaAssignment(List(2, 3, 4), Seq.empty),
      3  -> ReplicaAssignment(List(3, 4, 0), Seq.empty),
      4  -> ReplicaAssignment(List(4, 0, 1), Seq.empty),
      5  -> ReplicaAssignment(List(0, 2, 3), Seq.empty),
      6  -> ReplicaAssignment(List(1, 3, 4), Seq.empty),
      7  -> ReplicaAssignment(List(2, 4, 0), Seq.empty),
      8  -> ReplicaAssignment(List(3, 0, 1), Seq.empty),
      9  -> ReplicaAssignment(List(4, 1, 2), Seq.empty)
    )

    val partitionAssignment = adminZkClient.addPartitions(
      topicName, existingAssignment, brokers, numPartitions = 10, Some(newReplicaAssignment))

    assertEquals(existingAssignment ++ newReplicaAssignment, partitionAssignment)
  }

  /**
   * Test that addPartition method succeeds when
   * 1. a topic placement constraint is specified, and
   * 2. no replica assignment is provided
   *
   * In this case the code should do a normal replica assignment for new partition taking
   * topic placement constraint into account. This is same as what is done during topic
   * creation when a "topic placement constraint" is present.
   */
  @Test
  def testAddPartitionWithPlacementConstraintNoPartitionAssignment(): Unit = {
    val placementJson = """{
                          | "version": 1,
                          |  "replicas": [{
                          |      "count": 2,
                          |      "constraints": {
                          |        "rack": "rack-1"
                          |      }
                          |    }
                          |  ],
                          |  "observers": [{
                          |    "count": 2,
                          |    "constraints": {
                          |      "rack": "rack-2"
                          |    }
                          |  }]
                          |}""".stripMargin
    val topicPlacement = TopicPlacement.parse(placementJson).asScala
    val existingAssignment = Map(0 -> ReplicaAssignment(List(0, 1, 5, 6), List(5, 6)))

    val brokers = (0 until 10).map { id =>
      val rack = s"rack-${id / 5 + 1}"
      BrokerMetadata(id, Some(rack))
    }

    val topicName = "test-topic"
    TestUtils.createBrokersInZk(brokers, zkClient)
    val props = new Properties
    props.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementJson)
    adminZkClient.createTopic(topicName, 1, 4, props)

    val partitionAssignment = adminZkClient.addPartitions(
      topicName, existingAssignment, brokers, numPartitions = 3, None, false, topicPlacement)

    assertEquals(3, partitionAssignment.size)

    // Test if replica and observer assignment was done as per placement constraint
    partitionAssignment.values.map(_.replicas).foreach { assignedBrokers => {
        // This checks that each partition gets assigned to racks in placement constraint
        assertEquals(4, assignedBrokers.toSet.size)
        // First 2 should be on rack 1 (broker id from 0 to 4)
        assignedBrokers.take(2).foreach(brokerId => assertTrue(brokerId >= 0 && brokerId <= 4))
        // Last 2 should be on rack 2 (broker id from 5 to 9)
        assignedBrokers.slice(2, 4).foreach(brokerId => assertTrue(brokerId >= 5 && brokerId <= 9))
      }
    }

    partitionAssignment.values.map(_.observers).foreach { observers => {
        assertTrue(observers.mkString(","),
          observers.forall(observerId => observerId >= 5 && observerId <= 9))
      }
    }
  }

  /**
   * Test that addPartition method succeeds when
   * 1. a topic placement constraint is specified, and
   * 2. a replica assignment is provided
   *
   * In this case the code will ignore topic placement constraint and use the provided
   * list as is. The validation against topic placement constraint is done by the
   * caller of the "addPartition" method and an invalid configuration shouldn't
   * reach this method. The "addPartition" method does perform other validation checks.
   */
  @Test
  def testAddPartitionWithPlacementConstraintWithPartitionAssignment(): Unit = {
    val placementJson = """{
                          | "version": 1,
                          |  "replicas": [{
                          |      "count": 2,
                          |      "constraints": {
                          |        "rack": "rack-1"
                          |      }
                          |    }
                          |  ],
                          |  "observers": [{
                          |    "count": 2,
                          |    "constraints": {
                          |      "rack": "rack-2"
                          |    }
                          |  }]
                          |}""".stripMargin
    val topicPlacement = TopicPlacement.parse(placementJson).asScala
    val existingAssignment = Map(0 -> ReplicaAssignment(List(0, 1, 5, 6), List(5, 6)))
    val newAssignment = Map(1 -> ReplicaAssignment(List(2, 3, 7, 8), List(7, 8)))

    val brokers = (0 until 10).map { id =>
      val rack = s"rack-${id / 5 + 1}"
      BrokerMetadata(id, Some(rack))
    }

    val topicName = "test-topic"
    TestUtils.createBrokersInZk(brokers, zkClient)
    val props = new Properties
    props.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementJson)
    adminZkClient.createTopic(topicName, 1, 4, props)

    val partitionAssignment = adminZkClient.addPartitions(
      topicName, existingAssignment, brokers, numPartitions = 2, Some(newAssignment), false, topicPlacement)

    assertEquals(existingAssignment ++ newAssignment, partitionAssignment)
  }

  /**
   * Test that addPartition method fails when the set of brokers don't match the topic placement
   * constraint. In this test, topic placement needs 6 brokers in rack-1, but only 5 are available.
   */
  @Test(expected = classOf[InvalidConfigurationException])
  def testAddPartitionWithReplicaPlacementConstraintNotSatisfied(): Unit = {
    val placementJson = """{
                          | "version": 1,
                          |  "replicas": [{
                          |      "count": 6,
                          |      "constraints": {
                          |        "rack": "rack-1"
                          |      }
                          |    }
                          |  ],
                          |  "observers": [{
                          |    "count": 2,
                          |    "constraints": {
                          |      "rack": "rack-2"
                          |    }
                          |  }]
                          |}""".stripMargin
    val topicPlacement = TopicPlacement.parse(placementJson).asScala
    val existingAssignment = Map(0 -> ReplicaAssignment(0 to 7, List(6, 7)))

    val brokers = (0 until 10).map { id =>
      val rack = s"rack-${id / 5 + 1}"
      BrokerMetadata(id, Some(rack))
    }

    val topicName = "test-topic"
    TestUtils.createBrokersInZk(brokers, zkClient)
    val props = new Properties
    props.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementJson)
    adminZkClient.createTopic(topicName, 1, 4, props)

    // This should throw as we don't have enough brokers to satisfy replica constraint count
    adminZkClient.addPartitions(
      topicName, existingAssignment, brokers, numPartitions = 3, None, false, topicPlacement)
  }

  /**
   * Test that addPartition method fails when the set of brokers don't match the topic placement
   * constraint. In this test, topic placement needs 6 brokers as observers in rack-2, but only
   * 5 are available.
   */
  @Test(expected = classOf[InvalidConfigurationException])
  def testAddPartitionWithObserverPlacementConstraintNotSatisfied(): Unit = {
    val placementJson = """{
                          | "version": 1,
                          |  "replicas": [{
                          |      "count": 3,
                          |      "constraints": {
                          |        "rack": "rack-1"
                          |      }
                          |    }
                          |  ],
                          |  "observers": [{
                          |    "count": 6,
                          |    "constraints": {
                          |      "rack": "rack-2"
                          |    }
                          |  }]
                          |}""".stripMargin
    val topicPlacement = TopicPlacement.parse(placementJson).asScala
    val existingAssignment = Map(0 -> ReplicaAssignment(2 until 10, 4 until 10))

    val brokers = (0 until 10).map { id =>
      val rack = s"rack-${id / 5 + 1}"
      BrokerMetadata(id, Some(rack))
    }

    val topicName = "test-topic"
    TestUtils.createBrokersInZk(brokers, zkClient)
    val props = new Properties
    props.setProperty(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, placementJson)
    adminZkClient.createTopic(topicName, 1, 4, props)

    // This should throw as we don't have enough brokers to satisfy replica constraint count
    adminZkClient.addPartitions(
      topicName, existingAssignment, brokers, numPartitions = 3, None, false, topicPlacement)
  }

  @Test
  def shouldPropagateDynamicBrokerConfigs(): Unit = {
    val brokerIds = Seq(0, 1, 2)
    servers = createBrokerConfigs(3, zkConnect).map(fromProps).map(createServer(_))

    def checkConfig(limit: Long): Unit = {
      retry(10000) {
        for (server <- servers) {
          assertEquals("Leader Quota Manager was not updated", limit, server.quotaManagers.leader.upperBound)
          assertEquals("Follower Quota Manager was not updated", limit, server.quotaManagers.follower.upperBound)
        }
      }
    }

    val limit: Long = 1000000

    // Set the limit & check it is applied to the log
    adminZkClient.changeBrokerConfig(brokerIds, propsWith(
      (LeaderReplicationThrottledRateProp, limit.toString),
      (FollowerReplicationThrottledRateProp, limit.toString)))
    checkConfig(limit)

    // Now double the config values for the topic and check that it is applied
    val newLimit = 2 * limit
    adminZkClient.changeBrokerConfig(brokerIds,  propsWith(
      (LeaderReplicationThrottledRateProp, newLimit.toString),
      (FollowerReplicationThrottledRateProp, newLimit.toString)))
    checkConfig(newLimit)

    // Verify that the same config can be read from ZK
    for (brokerId <- brokerIds) {
      val configInZk = adminZkClient.fetchEntityConfig(ConfigType.Broker, brokerId.toString)
      assertEquals(newLimit, configInZk.getProperty(LeaderReplicationThrottledRateProp).toInt)
      assertEquals(newLimit, configInZk.getProperty(FollowerReplicationThrottledRateProp).toInt)
    }

    //Now delete the config
    adminZkClient.changeBrokerConfig(brokerIds, new Properties)
    checkConfig(DefaultReplicationThrottledRate)
  }

  /**
   * This test simulates a client config change in ZK whose notification has been purged.
   * Basically, it asserts that notifications are bootstrapped from ZK
   */
  @Test
  def testBootstrapClientIdConfig(): Unit = {
    val clientId = "my-client"
    val props = new Properties()
    props.setProperty("producer_byte_rate", "1000")
    props.setProperty("consumer_byte_rate", "2000")

    // Write config without notification to ZK.
    zkClient.setOrCreateEntityConfigs(ConfigType.Client, clientId, props)

    val configInZk: Map[String, Properties] = adminZkClient.fetchAllEntityConfigs(ConfigType.Client)
    assertEquals("Must have 1 overridden client config", 1, configInZk.size)
    assertEquals(props, configInZk(clientId))

    // Test that the existing clientId overrides are read
    val server = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))
    servers = Seq(server)
    assertEquals(new Quota(1000, true), server.dataPlaneRequestProcessor.quotas.produce.quota("ANONYMOUS", clientId))
    assertEquals(new Quota(2000, true), server.dataPlaneRequestProcessor.quotas.fetch.quota("ANONYMOUS", clientId))
  }

  @Test
  def testGetBrokerMetadatas(): Unit = {
    // broker 4 has no rack information
    val brokerList = 0 to 5
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 5 -> "rack3")
    val brokerMetadatas = toBrokerMetadata(rackInfo, brokersWithoutRack = brokerList.filterNot(rackInfo.keySet))
    TestUtils.createBrokersInZk(brokerMetadatas, zkClient)

    val processedMetadatas1 = adminZkClient.getBrokerMetadatas(RackAwareMode.Disabled)
    assertEquals(brokerList, processedMetadatas1.map(_.id))
    assertEquals(List.fill(brokerList.size)(None), processedMetadatas1.map(_.rack))

    val processedMetadatas2 = adminZkClient.getBrokerMetadatas(RackAwareMode.Safe)
    assertEquals(brokerList, processedMetadatas2.map(_.id))
    assertEquals(List.fill(brokerList.size)(None), processedMetadatas2.map(_.rack))

    intercept[AdminOperationException] {
      adminZkClient.getBrokerMetadatas(RackAwareMode.Enforced)
    }

    val partialList = List(0, 1, 2, 3, 5)
    val processedMetadatas3 = adminZkClient.getBrokerMetadatas(RackAwareMode.Enforced, Some(partialList))
    assertEquals(partialList, processedMetadatas3.map(_.id))
    assertEquals(partialList.map(rackInfo), processedMetadatas3.flatMap(_.rack))

    val numPartitions = 3
    adminZkClient.createTopic("foo", numPartitions, 2, rackAwareMode = RackAwareMode.Safe)
    val assignment = zkClient.getReplicaAssignmentForTopics(Set("foo"))
    assertEquals(numPartitions, assignment.size)
  }
}
