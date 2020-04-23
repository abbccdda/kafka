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

import java.util.Optional
import java.util.Scanner
import java.util.regex.Pattern
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.utils.Json
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.Seq

final class ReplicaStatusCommandTest extends KafkaServerTestHarness {
  import ReplicaStatusCommandTest._

  private val headers =
    Array("Topic", "Partition", "Replica", "IsLeader", "IsObserver", "IsIsrEligible", "IsInIsr", "IsCaughtUp",
      "LastCaughtUpLagMs", "LastFetchLagMs", "LogStartOffset", "LogEndOffset")

  override def generateConfigs: Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(
      numConfigs = 3,
      zkConnect,
      enableControlledShutdown = false,
      rackInfo = Map(0 -> "a", 1 -> "a", 2 -> "b")).map { config =>
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
      KafkaConfig.fromProps(config)
    }

  private def createTopicAndWait(client: AdminClient, topic: String, numPartitions: Integer): Unit = {
    val newTopic = new NewTopic(topic, Optional.of(numPartitions), Optional.empty[java.lang.Short])
    newTopic.configs(Map(LogConfig.TopicPlacementConstraintsProp ->
      """{
        "version":1,
        "replicas":[{
          "count": 2,
          "constraints":{"rack":"a"}
        }],
        "observers":[{
         "count": 1,
         "constraints":{"rack":"b"}
        }]
      }"""
    ).asJava)
    client.createTopics(List(newTopic).asJava).all.get()

    TestUtils.waitUntilTrue(() => {
      client.describeTopics(List(topic).asJava).all().get().get(topic) match {
        case null => false
        case td => td.partitions.asScala.forall(_.leader != null)
      }
    }, "Failed to create topic")
  }

  /**
    * Runs the ReplicaStatusCommand with the given args, first creating the specified topics with the provided number of
    * partitions. The test cluster created will always consist of `3` brokers `{0, 1, 2}`, where broker `2` is always an
    * observer.
    */
  private def runCommand(topics: Array[String], numPartitions: Int, args: Array[String]): String = {
    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      topics.foreach(createTopicAndWait(client, _, numPartitions))

      TestUtils.grabConsoleOutput(ReplicaStatusCommand.main(Array("--bootstrap-server", bootstrapServers(servers)) ++ args))
    }
  }

  case class ReplicaStatusEntry(topic: String, partition: Int, replica: Int, isLeader: Boolean, isObserver: Boolean,
    isIsrEligible: Boolean, isInIsr: Boolean, isCaughtUp: Boolean, lastCaughtUpLagMs: Option[Long], lastFetchLagMs: Option[Long],
    logStartOffset: Option[Long], logEndOffset: Option[Long]) {
  }

  private def runCommandParseOutput(topics: Array[String], numPartitions: Int, args: Array[String]): List[ReplicaStatusEntry] = {
    val output = runCommand(topics, numPartitions, args)
    val scanner = new Scanner(output)
    assertTrue(scanner.hasNextLine)
    scanner.findInLine("""(\w+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(\w+)\s+(\w+)""")
    val topMatch = scanner.`match`

    assertTrue(topMatch.groupCount == headers.size)
    for (idx <- 0 until headers.size) {
      assertTrue(topMatch.group(idx + 1) == headers(idx))
    }
    scanner.nextLine

    def toBoolean(value: String): Boolean = value match {
      case "true" => true
      case "false" => false
    }
    def toLongOption(value: String): Option[Long] = if (value == "-1")
      None
    else {
      val num = value.toLong
      assertTrue(num >= 0)
      Some(num)
    }

    val result = mutable.Buffer[ReplicaStatusEntry]()
    val pattern = Pattern.compile("""(\S+)\s+(\d+)\s+(\d+)\s+(true|false)\s+(true|false)\s+(true|false)\s+(true|false)\s+(true|false)\s+(-?[0-9]+)\s+(-?[0-9]+)\s+(-?[0-9]+)\s+(-?[0-9]+)""")
    while (scanner.hasNextLine) {
      scanner.findInLine(pattern)
      val subMatch = scanner.`match`
      assertTrue(subMatch.groupCount == headers.size)
      result += new ReplicaStatusEntry(subMatch.group(1), subMatch.group(2).toInt, subMatch.group(3).toInt, toBoolean(subMatch.group(4)),
        toBoolean(subMatch.group(5)), toBoolean(subMatch.group(6)), toBoolean(subMatch.group(7)), toBoolean(subMatch.group(8)),
        toLongOption(subMatch.group(9)), toLongOption(subMatch.group(10)), toLongOption(subMatch.group(11)), toLongOption(subMatch.group(12)))
      scanner.nextLine
    }
    result.toList
  }

  @Test
  def testVerboseTopicPartitions(): Unit = {
    val topics = Array("test-topic-1", "test-topic-2")
    val numPartitions = 2

    val output = runCommand(topics, numPartitions, Array("--verbose"))
    for (topic <- topics)
      for (partition <- 0 until numPartitions)
        for (replica <- 0 until servers.size) {
          assertTrue(output.contains(s"Topic: ${topic}"))
          assertTrue(output.contains(s"Partition: ${partition}"))
          assertTrue(output.contains(s"Replica: ${replica}"))
        }
    assertTrue(output.contains("IsLeader: true"))
    assertTrue(output.contains("IsLeader: false"))
    assertTrue(output.contains("IsObserver: true"))
    assertTrue(output.contains("IsObserver: false"))
    assertTrue(output.contains("IsIsrEligible: true"))
    assertTrue(output.contains("IsIsrEligible: false"))
  }

  @Test
  def testSpecifiedTopics(): Unit = {
    val topics = Array("test-topic-1", "test-topic-2", "test-topic-3")
    val output = runCommand(topics, 1, Array("--topics", topics(0) + "," + topics(2), "--verbose"))
    assertTrue(output.contains(s"Topic: ${topics(0)}"))
    assertFalse(output.contains(s"Topic: ${topics(1)}"))
    assertTrue(output.contains(s"Topic: ${topics(2)}"))
  }

  @Test
  def testSpecifiedPartitions(): Unit = {
    val topics = Array("test-topic")
    val output = runCommand(topics, 4, Array("--partitions", "0,2-3", "--verbose"))
    assertTrue(output.contains("Partition: 0"))
    assertFalse(output.contains("Partition: 1"))
    assertTrue(output.contains("Partition: 2"))
    assertTrue(output.contains("Partition: 3"))
  }

  @Test
  def testLeadersOnly(): Unit = {
    val topic = "test-topic"
    val output = runCommand(Array(topic), 1, Array("--leaders", "--verbose"))
    assertFalse(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 2"))
    assertTrue(output.contains("IsLeader: true"))
    assertFalse(output.contains("IsLeader: false"))
  }

  @Test
  def testLeadersExcluded(): Unit = {
    val topic = "test-topic"
    val output = runCommand(Array(topic), 1, Array("--leaders", "exclude", "--verbose"))
    assertTrue(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica"))
    assertFalse(output.contains("IsLeader: true"))
    assertTrue(output.contains("IsLeader: false"))
  }

  @Test
  def testObserversOnly(): Unit = {
    val topic = "test-topic"
    val output = runCommand(Array(topic), 1, Array("--observers", "--verbose"))
    assertFalse(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 0"))
    assertFalse(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 1"))
    assertTrue(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 2"))
    assertTrue(output.contains("IsObserver: true"))
    assertFalse(output.contains("IsObserver: false"))
  }

  @Test
  def testObserversExcluded(): Unit = {
    val topic = "test-topic"
    val output = runCommand(Array(topic), 1, Array("--observers", "exclude", "--verbose"))
    assertTrue(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 0"))
    assertTrue(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 1"))
    assertFalse(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 2"))
    assertFalse(output.contains("IsObserver: true"))
    assertTrue(output.contains("IsObserver: false"))
  }

  @Test
  def testNotInIsr(): Unit = {
    val topic = "test-topic"
    val topicPartition = new TopicPartition(topic, 0)
    val assignments = Map(0 -> Seq(0, 1))

    TestUtils.resource(AdminClient.create(createConfig(servers).asJava)) { client =>
      TestUtils.createTopic(zkClient, topic, assignments, servers)

      servers(1).shutdown()
      TestUtils.waitForBrokersOutOfIsr(client, immutable.Set(topicPartition), immutable.Set(1))
      TestUtils.waitForLeaderToBecome(client, topicPartition, Some(0))
      TestUtils.produceMessage(servers, topic, "message")

      val output = TestUtils.grabConsoleOutput(ReplicaStatusCommand.main(Array("--bootstrap-server", bootstrapServers(servers), "--leaders", "exclude", "--verbose")))
      assertFalse(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 0"))
      assertFalse(output.contains("IsInIsr: true"))
      assertTrue(output.contains(s"Topic: ${topic}\nPartition: 0\nReplica: 1"))
      assertTrue(output.contains("IsInIsr: false"))
    }
  }

  @Test
  def testOutput(): Unit = {
    val entries = runCommandParseOutput(Array("test-topic-1", "test-topic-2"), 2, Array())
    assertTrue(entries.size == 12)
    val tpr = mutable.Set[String]()
    val leaders = mutable.Set[String]()
    entries.foreach { entry =>
      assertTrue(entry.topic == "test-topic-1" || entry.topic == "test-topic-2")
      assertTrue(entry.partition == 0 || entry.partition == 1)
      assertTrue(entry.replica >= 0 && entry.replica < servers.size)
      assertTrue(tpr.add(entry.topic + "-" + entry.partition + "-" + entry.replica))
      if (entry.isLeader) {
        assertTrue(leaders.add(entry.topic + "-" + entry.partition))
        assertTrue(entry.isInIsr)
        assertTrue(entry.isCaughtUp)
      }
      if (entry.replica == 2) {
        assertTrue(entry.isObserver)
        assertFalse(entry.isIsrEligible)

        // An observer may be temporarily considered in the in sync set upon new topic creation, therefore
        // we can't assert that it's not a part of the ISR, despite not being eligible.
      } else {
        assertFalse(entry.isObserver)
        assertTrue(entry.isIsrEligible)
        assertTrue(entry.isInIsr)
      }
      entry.lastCaughtUpLagMs.foreach(value => assertTrue(value >= 0))
      entry.lastFetchLagMs.foreach { value =>
        assertTrue(value >= 0)

        // Only assert that the replica is caught up if it reports a last fetch time, otherwise the leader
        // may not see it as such despite the test producing no data.
        assertTrue(entry.isCaughtUp)
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
  def testJson(): Unit = {
    val topics = Array("test-topic-1", "test-topic-2")
    val jsonTopics = Json.parseFull(runCommand(topics, 2, Array("--json"))).get.asJsonArray.iterator

    var topicsCount = 0
    while (jsonTopics.hasNext) {
      val jsonTopic = jsonTopics.next.asJsonObject
      topicsCount += 1

      val topic = jsonTopic("Topic").to[String]
      assertTrue(topic == "test-topic-1" || topic == "test-topic-2")

      var partitionsCount = 0
      val jsonPartitions = jsonTopic("Partitions").asJsonArray.iterator
      while (jsonPartitions.hasNext) {
        val jsonPartition = jsonPartitions.next.asJsonObject
        partitionsCount += 1

        val partition = jsonPartition("Partition").to[Int]
        assertTrue(partition == 0 || partition == 1)

        var replicasCount = 0
        val jsonReplicas = jsonPartition("Replicas").asJsonArray.iterator
        while (jsonReplicas.hasNext) {
          val jsonReplica = jsonReplicas.next.asJsonObject
          replicasCount += 1

          val replica = jsonReplica("Replica").to[Int]
          assertTrue(replica >= 0 && replica <= 2)
          for (idx <- 3 until headers.size)
            assertTrue(jsonReplica(headers(idx)) != null)
        }
        assertTrue(replicasCount == 3)
      }
      assertTrue(partitionsCount == 2)
    }
    assertTrue(topicsCount == 2)
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
