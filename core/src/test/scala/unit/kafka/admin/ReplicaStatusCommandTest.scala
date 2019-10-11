/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.junit.After
import org.junit.Assert._
import org.junit.Before
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.immutable

final class ReplicaStatusCommandTest extends ZooKeeperTestHarness {
  import ReplicaStatusCommandTest._

  var servers = Seq.empty[KafkaServer]

  @Before
  override def setUp(): Unit = {
    super.setUp()

    val brokerConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false)
    servers = brokerConfigs.map { config =>
      config.setProperty("auto.leader.rebalance.enable", "false")
      config.setProperty("controlled.shutdown.enable", "true")
      config.setProperty("controlled.shutdown.max.retries", "1")
      config.setProperty("controlled.shutdown.retry.backoff.ms", "1000")
      TestUtils.createServer(KafkaConfig.fromProps(config))
    }
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)

    super.tearDown()
  }

  private def runCommand(topics: Array[String], numPartitions: Int, numReplicas: Int, args: Array[String]): String = {
    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      topics.foreach { topic =>
        TestUtils.createTopic(zkClient, topic, numPartitions, numReplicas, servers)
        for (i <- 0 until numPartitions)
          TestUtils.waitUntilLeaderIsKnown(servers, new TopicPartition(topic, i))
      }
      TestUtils.grabConsoleOutput(ReplicaStatusCommand.main(Array("--bootstrap-server", bootstrapServers(servers)) ++ args))
    }
  }

  @Test
  def testAllTopicPartitions(): Unit = {
    val topics = Array("test-topic-1", "test-topic-2")
    val numPartitions = 2
    val numReplicas = servers.size

    val output = runCommand(topics, numPartitions, numReplicas, Array())
    for (topic <- topics)
      for (partition <- 0 until numPartitions)
        for (replica <- 0 until numReplicas)
          assertTrue(output.contains(s"Topic-Partition-Replica: ${topic}-${partition}-${replica}"))
    assertTrue(output.contains("Mode: LEADER"))
    assertTrue(output.contains("Mode: FOLLOWER"))
    assertFalse(output.contains("Mode: OBSERVER"))  // Not instantiated
  }

  @Test
  def testSpecifiedTopics(): Unit = {
    val topics = Array("test-topic-1", "test-topic-2", "test-topic-3")

    val output = runCommand(topics, 1, 1, Array("--topics", topics(0) + "," + topics(2)))
    assertTrue(output.contains(topics(0)))
    assertFalse(output.contains(topics(1)))
    assertTrue(output.contains(topics(2)))
  }

  @Test
  def testSpecifiedPartitions(): Unit = {
    val topics = Array("test-topic")

    val output = runCommand(topics, 4, 1, Array("--partitions", "0,2-3"))
    assertTrue(output.contains(topics(0) + "-0-"))
    assertFalse(output.contains(topics(0) + "-1-"))
    assertTrue(output.contains(topics(0) + "-2-"))
    assertTrue(output.contains(topics(0) + "-3-"))
  }

  @Test
  def testSpecifiedModes(): Unit = {
    val topics = Array("test-topic")

    val output = runCommand(topics, 1, 2, Array("--modes", "follower"))
    assertFalse(output.contains("Mode: LEADER"))
    assertTrue(output.contains("Mode: FOLLOWER"))
    assertFalse(output.contains("Mode: OBSERVER"))
  }

  @Test
  def testNotCaughtUp(): Unit = {
    val topic = "test-topic"
    val topicPartition = new TopicPartition(topic, 0)
    val assignments = Map(0 -> Seq(0, 1))

    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      TestUtils.createTopic(zkClient, topic, assignments, servers)

      servers(1).shutdown()
      TestUtils.waitForBrokersOutOfIsr(client, immutable.Set(topicPartition), immutable.Set(1))
      TestUtils.waitForLeaderToBecome(client, topicPartition, Some(0))
      TestUtils.produceMessage(servers, topic, "message")

      val output = TestUtils.grabConsoleOutput(ReplicaStatusCommand.main(Array("--bootstrap-server", bootstrapServers(servers), "--only-not-caught-up")))
      assertFalse(output.contains(s"${topic}-0-0"))
      assertFalse(output.contains("IsCaughtUp: yes"))
      assertTrue(output.contains(s"${topic}-0-1"))
      assertTrue(output.contains("IsCaughtUp: no"))
    }
  }

  @Test
  def testMissingArgs(): Unit = {
    try {
      ReplicaStatusCommand.main(Array("--topics", "test-topic"))
      fail()
    } catch {
      case e: Throwable =>
        assertTrue(e.getMessage.startsWith("Missing required option(s)"))
    }
  }

  private def testInvalidArgs(args: Array[String], expectedErrorPrefixStr: String): Unit = {
    try {
      ReplicaStatusCommand.main(Array("--bootstrap-server", bootstrapServers(servers)) ++ args)
      fail()
    } catch {
      case e: Throwable =>
        assertTrue(e.getMessage.startsWith(expectedErrorPrefixStr))
    }
  }

  @Test
  def testInvalidTopics(): Unit = {
    testInvalidArgs(Array("--topics"), "Option topics requires an argument")
    testInvalidArgs(Array("--topics", ""), "Topic name")
    testInvalidArgs(Array("--topics", "."), "Topic name")
    testInvalidArgs(Array("--topics", "invalid-1:topic"), "Topic name")
  }

  @Test
  def testInvalidPartitionsArg(): Unit = {
    def testInvalidRange(partitionStr: String): Unit =
      testInvalidArgs(Array("--partitions", partitionStr), "Invalid partition range")

    def testInvalidValue(partitionStr: String): Unit =
      testInvalidArgs(Array("--partitions", partitionStr), "Failed to parse partition")

    testInvalidRange("2-1")
    testInvalidRange("1-3-5")
    testInvalidValue("")
    testInvalidValue("abc")
  }
}

object ReplicaStatusCommandTest {
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
