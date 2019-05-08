/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.util
import java.util.Properties
import java.util.function.Supplier
import java.util.Collections

import kafka.log.LogConfig
import kafka.server.LogDirFailureChannel
import kafka.tier.client.{MockConsumerBuilder, MockProducerBuilder}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.serdes.State
import kafka.tier.state.FileTierPartitionStateFactory
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfluentTopicConfig
import org.junit.Assert._
import org.junit.Test

class TierTopicManagerTest {
  val tierTopicName = "__tier_topic"
  val tierTopicNumPartitions = 1.toShort
  val clusterId = "mycluster"
  val objectStoreConfig = new TierObjectStoreConfig()
  val tempDir = TestUtils.tempDir()
  val logDir = tempDir.getAbsolutePath
  val logDirs = new util.ArrayList(util.Collections.singleton(logDir))
  val tierMetadataManager = new TierMetadataManager(new FileTierPartitionStateFactory(),
    Some(new MockInMemoryTierObjectStore(objectStoreConfig)),
    new LogDirFailureChannel(1),
    true)

  @Test
  def testTierTopicManager(): Unit = {
    try {
      val tierTopicManagerConfig = new TierTopicManagerConfig(
        "",
        "",
        tierTopicNumPartitions,
        1.toShort,
        3,
        clusterId,
        5L,
        30000,
        500,
        logDirs)

      val producerBuilder = new MockProducerBuilder()
      val consumerBuilder = new MockConsumerBuilder(tierTopicManagerConfig,
        producerBuilder.producer())

      val bootstrapSupplier = new Supplier[String] { override def get: String = { "" } }
      val tierTopicManager = new TierTopicManager(
        tierTopicManagerConfig,
        consumerBuilder,
        producerBuilder,
        bootstrapSupplier,
        tierMetadataManager)

      tierTopicManager.becomeReady(bootstrapSupplier.get())

      val archivedPartition1 = new TopicPartition("archivedTopic", 0)
      addReplica(archivedPartition1)
      becomeLeader(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        0,
        AppendResult.ACCEPTED)

      tierTopicManager.addMetadata(
        new TierObjectMetadata(archivedPartition1,
          0,
          0L,
          1000,
          15000L,
          16000L,
          1000,
          true,
          false,
          State.AVAILABLE))
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
      tierTopicManager.committer().flush()
      val tierPartitionState = tierTopicManager.partitionState(archivedPartition1)
      assertEquals(1000L, tierPartitionState.committedEndOffset().get())

      // should be ignored
      tierTopicManager.addMetadata(
        new TierObjectMetadata(archivedPartition1,
          0,
          0L,
          1000,
          1000L,
          16000L,
          1000,
          true,
          true,
          State.AVAILABLE))
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
      tierTopicManager.committer().flush()
      // end offset shouldn't have updated - message with identical ranges with same start offset and epoch
      // should be filtered
      assertEquals(1000L, tierPartitionState.totalSize)

      // should be ignored as it doesn't contain any distinct data
      tierTopicManager.addMetadata(
        new TierObjectMetadata(archivedPartition1,
          0,
          5L,
          994,
          999L,
          16000L,
          1000,
          true,
          false,
          State.AVAILABLE))
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
      tierTopicManager.committer().flush()

      // end offset shouldn't have updated - message with overlapping ranges with same epoch
      // should be filtered
      assertEquals(1000L, tierPartitionState.totalSize)

      // test rejoin on the same epoch
      becomeLeader(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        0,
        AppendResult.ACCEPTED)

      // larger epoch should be accepted
      becomeLeader(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        1,
        AppendResult.ACCEPTED)

      // going back to previous epoch should be fenced
      becomeLeader(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        0,
        AppendResult.FENCED)

      // add a second partition and ensure it catches up.
      val archivedPartition2 = new TopicPartition("archivedTopic", 1)
      addReplica(archivedPartition2)
      becomeLeader(consumerBuilder,
        tierTopicManager,
        archivedPartition2,
        0,
        AppendResult.ACCEPTED)

      tierTopicManager.committer.flush()
      assertEquals(tierTopicManager.committer.positions.get(0),
        consumerBuilder.logEndOffset)

      assertFalse(tierTopicManager.catchingUp())
    } finally {
      Option(new File(logDir).listFiles)
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(_.delete())
    }
  }

  @Test
  def testCatchUpConsumer(): Unit = {
    try {
      val tierTopicManagerConfig = new TierTopicManagerConfig(
        "",
        "",
        tierTopicNumPartitions,
        1.toShort,
        3,
        clusterId,
        5L,
        30000,
        500,
        logDirs)

      val producerBuilder = new MockProducerBuilder()
      val consumerBuilder = new MockConsumerBuilder(tierTopicManagerConfig,
        producerBuilder.producer())

      val bootstrapSupplier = new Supplier[String] {
        override def get: String = { "" }
      }

      val tierTopicManager = new TierTopicManager(
        tierTopicManagerConfig,
        consumerBuilder,
        producerBuilder,
        bootstrapSupplier,
        tierMetadataManager)

      tierTopicManager.becomeReady(bootstrapSupplier.get())
      val archivedPartition1 = new TopicPartition("archivedTopic", 0)
      addReplica(archivedPartition1)
      val archivedPartition2 = new TopicPartition("archivedTopic", 1)
      addReplica(archivedPartition2)

      tierTopicManager.immigratePartitions(Collections.singletonList(archivedPartition1))
      tierTopicManager.immigratePartitions(Collections.singletonList(archivedPartition2))
      tierTopicManager.processMigrations()
      assertTrue(tierTopicManager.catchingUp())
      tierMetadataManager.delete(archivedPartition2)
      tierTopicManager.emigratePartitions(Collections.singletonList(archivedPartition2))
      tierTopicManager.processMigrations()
      assertTrue(tierTopicManager.catchingUp())
      tierMetadataManager.delete(archivedPartition1)
      tierTopicManager.emigratePartitions(Collections.singletonList(archivedPartition1))
      tierTopicManager.processMigrations()
      assertFalse(tierTopicManager.catchingUp())
    } finally {
      Option(new File(logDir).listFiles)
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(_.delete())
    }
  }

  private def addReplica(topicPartition: TopicPartition): Unit = {
    val properties = new Properties()
    properties.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    tierMetadataManager.initState(topicPartition, new File(logDir), new LogConfig(properties))
  }

  private def becomeLeader(consumerBuilder: MockConsumerBuilder,
                           tierTopicManager: TierTopicManager,
                           topicPartition: TopicPartition,
                           epoch: Integer,
                           expected: AppendResult): Unit = {
    tierMetadataManager.becomeLeader(topicPartition, epoch)
    // force immigration
    tierTopicManager.doWork()
    val result = tierTopicManager.becomeArchiver(topicPartition, epoch)
    consumerBuilder.moveRecordsFromProducer()
    while (!tierTopicManager.doWork()) assertEquals(expected, result.get())
    tierTopicManager.committer().flush()
  }
}
