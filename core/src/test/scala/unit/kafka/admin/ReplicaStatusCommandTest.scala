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

import java.util.Scanner
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, CreateTopicsOptions, DescribeTopicsOptions, NewTopic}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable

final class ReplicaStatusCommandTest extends KafkaServerTestHarness {
  import ReplicaStatusCommandTest._

  override def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(3, zkConnect, false).map(KafkaConfig.fromProps)

  private def createTopicAndWait(client: AdminClient, topic: String, numPartitions: Int, numReplicas: Int): Unit = {
    client.createTopics(List(new NewTopic(topic, numPartitions, numReplicas.toShort)).asJava,
      new CreateTopicsOptions().timeoutMs(1000)).all.get()

    TestUtils.waitUntilTrue(() => {
      client.describeTopics(List(topic).asJava, new DescribeTopicsOptions()).all().get().get(topic) match {
        case null => false
        case td => td.partitions.asScala.forall(_.leader != null)
      }
    }, "Failed to create topic")
  }

  private def runCommand(topics: Array[String], numPartitions: Int, numReplicas: Int, args: Array[String]): String = {
    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      topics.foreach(createTopicAndWait(client, _, numPartitions, numReplicas))
      TestUtils.grabConsoleOutput(ReplicaStatusCommand.main(Array("--bootstrap-server", bootstrapServers(servers)) ++ args))
    }
  }

  case class ReplicaStatusEntry(topic: String, partition: Int, replica: Int, mode: String, isCaughtUp: Boolean, isInSync: Boolean,
    lastCaughtUpLagMs: Option[Long], lastFetchLagMs: Option[Long], logStartOffset: Option[Long], logEndOffset: Option[Long]) {
  }

  private def runCommandParseCompactOutput(topics: Array[String], numPartitions: Int, numReplicas: Int, args: Array[String]): List[ReplicaStatusEntry] = {
    val output = runCommand(topics, numPartitions, numReplicas, args :+ "--compact")
    val scanner = new Scanner(output)
    assertTrue(scanner.hasNextLine)
    scanner.findInLine("(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)")
    val topMatch = scanner.`match`

    val expectedHeader =
      Array("Topic", "Partition", "Replica", "Mode", "IsCaughtUp", "IsInSync", "LastCaughtUpLagMs", "LastFetchLagMs", "LogStartOffset", "LogEndOffset")
    assertTrue(topMatch.groupCount == expectedHeader.size)
    for (idx <- 0 until topMatch.groupCount) {
      assertTrue(topMatch.group(idx + 1) == expectedHeader(idx))
    }
    scanner.nextLine

    def toBoolean(value: String): Boolean = value match {
      case "yes" => true
      case "no" => false
    }
    def toLongOption(value: String): Option[Long] = value match {
      case "unknown" => None
      case _ => Some(value.toLong)
    }

    val result = mutable.Buffer[ReplicaStatusEntry]()
    while (scanner.hasNextLine) {
      scanner.findInLine("(\\S+)\t(\\d+)\t(\\d+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)\t(\\w+)")
      val subMatch = scanner.`match`
      assertTrue(subMatch.groupCount == expectedHeader.size)
      result += new ReplicaStatusEntry(subMatch.group(1), subMatch.group(2).toInt, subMatch.group(3).toInt, subMatch.group(4),
        toBoolean(subMatch.group(5)), toBoolean(subMatch.group(6)), toLongOption(subMatch.group(7)), toLongOption(subMatch.group(8)),
        toLongOption(subMatch.group(9)), toLongOption(subMatch.group(10)))
      scanner.nextLine
    }
    result.toList
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
      assertFalse(output.contains("IsInSync: yes"))
      assertTrue(output.contains(s"${topic}-0-1"))
      assertTrue(output.contains("IsCaughtUp: no"))
      assertTrue(output.contains("IsInSync: no"))
    }
  }

  @Test
  def testCompact(): Unit = {
    val entries = runCommandParseCompactOutput(Array("test-topic-1", "test-topic-2"), 2, 2, Array())
    assertTrue(entries.size == 8)
    val tpr = mutable.Set[String]()
    val leaders = mutable.Set[String]()
    entries.foreach { entry =>
      assertTrue(entry.topic == "test-topic-1" || entry.topic == "test-topic-2")
      assertTrue(entry.partition == 0 || entry.partition == 1)
      assertTrue(entry.replica >= 0 && entry.replica < servers.size)
      assertTrue(tpr.add(entry.topic + "-" + entry.partition + "-" + entry.replica))
      assertTrue(entry.mode match {
        case "LEADER" =>
          assertTrue(leaders.add(entry.topic + "-" + entry.partition))
          assertTrue(entry.isCaughtUp)
          assertTrue(entry.isInSync)
          true
        case "FOLLOWER" | "OBSERVER" => true
        case _ => false
      })
      entry.lastCaughtUpLagMs.foreach(value => assertTrue(value >= 0))
      entry.lastFetchLagMs.foreach { value =>
        assertTrue(value >= 0)

        // Only assert that the replica is in sync and caught up if it reports a last fetch
        // time, otherwise the leader may not see it as such despite the test producing no data.
        assertTrue(entry.isCaughtUp)
        assertTrue(entry.isInSync)
      }
      entry.logStartOffset.foreach(value => assertTrue(value >= 0))
      entry.logEndOffset.foreach(value => assertTrue(value >= 0))
      for {
        logStartOffset <- entry.logStartOffset
        logEndOffset <- entry.logEndOffset
      } yield assertTrue(logStartOffset <= logEndOffset)
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
