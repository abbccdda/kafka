/*
 * Copyright 2020 Confluent Inc.
 */
package kafka.server

import java.util.{Collections, Optional, Properties}

import kafka.api.IntegrationTestHarness
import kafka.log.{AbstractLog, LogConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.NewPartitionReassignment
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.util.Random

class StrayPartitionIntegrationTest extends IntegrationTestHarness {
  private val numBrokers = 3
  private val topic = "topic_1"
  private val topicPartition = new TopicPartition(topic, 0)

  override protected def brokerCount: Int = numBrokers

  locally {
    serverConfig.setProperty(ConfluentConfigs.STRAY_PARTITION_DELETION_ENABLE_CONFIG, "true")
  }

  @Test
  def testStrayPartitionDeletionOnBrokerStartup(): Unit = {
    val validTopic_1 = "valid-1"
    val validTopic_2 = "valid-2"
    createTopic(validTopic_1, 3, 3)
    createTopic(validTopic_2, 5, 3)

    val strayTopic_1 = "stray-1"
    val strayTopic_2 = "stray-2"

    // create stray partitions on each broker
    servers.foreach { server =>
      val logManager = server.logManager
      logManager.getOrCreateLog(new TopicPartition(strayTopic_1, Random.nextInt(10)), () => logManager.initialDefaultConfig)
      logManager.getOrCreateLog(new TopicPartition(strayTopic_2, Random.nextInt(10)), () => logManager.initialDefaultConfig)
    }

    // restart broker and assert that stray partitions are deleted
    servers.foreach { server =>
      def allLogs: Iterable[TopicPartition] = server.logManager.allLogs.map(_.topicPartition)

      killBroker(server.config.brokerId)
      restartDeadBrokers()

      TestUtils.waitUntilTrue(() => allLogs.filter(_.topic == strayTopic_1).size == 0,
        "Timed out waiting for stray topic deletion")
      TestUtils.waitUntilTrue(() => allLogs.filter(_.topic == strayTopic_2).size == 0,
        "Timed out waiting for stray topic deletion")
      assertEquals(3, allLogs.filter(_.topic == validTopic_1).size)
      assertEquals(5, allLogs.filter(_.topic == validTopic_2).size)
    }
  }

  @Test
  def testStrayPartitionDeletionOnControllerFailover(): Unit = {
    val validTopic_1 = "valid-1"
    val validTopic_2 = "valid-2"
    createTopic(validTopic_1, 3, 3)
    createTopic(validTopic_2, 5, 3)

    val strayTopic_1 = "stray-1"
    val strayTopic_2 = "stray-2"

    // create stray partitions on each broker
    servers.foreach { server =>
      val logManager = server.logManager
      logManager.getOrCreateLog(new TopicPartition(strayTopic_1, Random.nextInt(10)), () => logManager.initialDefaultConfig)
      logManager.getOrCreateLog(new TopicPartition(strayTopic_2, Random.nextInt(10)), () => logManager.initialDefaultConfig)
    }

    // failover the controller
    zkClient.deleteController(zkClient.getControllerEpoch.get._1)
    TestUtils.waitUntilTrue(() => zkClient.getControllerId.isDefined, "Timed out waiting for controller election")
    val controllerId = zkClient.getControllerId.get

    TestUtils.waitUntilTrue(() => servers(controllerId).kafkaController.isActive, "Timed out waiting for controller initialization")

    // assert stray partitions are deleted and only valid partitions remain
    servers.foreach { server =>
      val allLogs = server.logManager.allLogs.map(_.topicPartition)
      TestUtils.waitUntilTrue(() => allLogs.filter(_.topic == strayTopic_1).size == 0,
        "Timed out waiting for stray topic deletion")
      TestUtils.waitUntilTrue(() => allLogs.filter(_.topic == strayTopic_2).size == 0,
        "Timed out waiting for stray topic deletion")
      assertEquals(3, allLogs.filter(_.topic == validTopic_1).size)
      assertEquals(5, allLogs.filter(_.topic == validTopic_2).size)
    }
  }

  /**
    * Test that a partition undergoing reassignment is not considered a stray partition.
    */
  @Test
  def testPartitionNotStrayDuringReassignment(): Unit = {
    val topicConfig = new Properties()
    topicConfig.setProperty(LogConfig.SegmentBytesProp, 100.toString)
    TestUtils.createTopic(zkClient, topic, Map(0 -> Seq(0, 1)), servers, topicConfig)

    val producerConfig = new Properties()
    producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Int.MaxValue))
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, "key".getBytes, "message".getBytes)

    // Produce messages. This will create multiple segments, as the segment size is set to 1000 bytes.
    val producer = createProducer()
    for (_ <- 0 until 100)
      producer.send(record).get

    val adminClient = createAdminClient()
    val newLeader = 2
    val newReplicas = Seq(2, 1)
    val newAssignment = new NewPartitionReassignment(newReplicas.map(int2Integer).asJava)

    // Initiate reassignment
    adminClient.alterPartitionReassignments(Collections.singletonMap(topicPartition, Optional.of(newAssignment))).all.get

    TestUtils.waitUntilTrue(() => logOpt(newLeader, topicPartition).isDefined, "Timed out waiting for reassignment to initiate")
    TestUtils.waitUntilTrue(() => logOpt(newLeader, topicPartition).get.numberOfSegments > 1, "Timed out waiting for segments to roll")

    // Manipulate the last modified time of the first segment. We will use this to validate that the partition was
    // not deleted / recreated while reassignment is in progress.
    logOpt(newLeader, topicPartition).get.localLogSegments.head.lastModified = System.currentTimeMillis + 100000
    val lastModified = logOpt(newLeader, topicPartition).get.localLogSegments.dropRight(1).map(_.lastModified)

    killBroker(newLeader)
    restartDeadBrokers()

    for (_ <- 0 until 100)
      producer.send(record).get

    // Wait for reassignment to complete and assert that the last modified time is unchanged.
    TestUtils.waitUntilTrue(() => servers(newLeader).replicaManager.leaderPartitionsIterator.map(_.topicPartition).contains(topicPartition),
      "Timed out waiting for reassignment to complete")
    assertEquals(lastModified, logOpt(newLeader, topicPartition).get.localLogSegments.take(lastModified.size).map(_.lastModified))
  }

  private def logOpt(broker: Int, topicPartition: TopicPartition): Option[AbstractLog] = {
    servers(broker).logManager.getLog(topicPartition)
  }
}
