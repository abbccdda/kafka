/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.io.IOException
import java.util
import java.util.function.Supplier
import java.util.Collections
import java.util.{Optional, Properties, UUID}
import java.util.concurrent.atomic.AtomicBoolean

import kafka.log.LogConfig
import kafka.server.LogDirFailureChannel
import kafka.tier.client.{MockConsumerBuilder, MockProducerBuilder}
import kafka.tier.client.ConsumerBuilder
import kafka.tier.client.TierTopicConsumerBuilder
import kafka.tier.domain.{TierSegmentUploadComplete, TierSegmentUploadInitiate, TierTopicInitLeader}
import kafka.tier.state.FileTierPartitionStateFactory
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.ConfluentTopicConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.common.utils.Exit.Procedure
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

class TierTopicManagerTest {
  val tierTopicName = "_confluent-tier-state"
  val clusterId = "mycluster"
  val objectStoreConfig = new TierObjectStoreConfig()
  val tempDir = TestUtils.tempDir()
  val logDir = tempDir.getAbsolutePath
  val logDirs = new util.ArrayList(util.Collections.singleton(logDir))
  val tierMetadataManager = new TierMetadataManager(new FileTierPartitionStateFactory(),
    Optional.of(new MockInMemoryTierObjectStore(objectStoreConfig)),
    new LogDirFailureChannel(1),
    true)
  val producerBuilder = new MockProducerBuilder()
  val bootstrapSupplier = new Supplier[String] {
    override def get: String = { "" }
  }

  private def createTierTopicManager(consumerBuilder: TierTopicConsumerBuilder, tierTopicNumPartitions: Short): TierTopicManager = {
    val tierTopicManagerConfig = new TierTopicManagerConfig("", "", tierTopicNumPartitions, 1.toShort, 3, clusterId, 5L, 30000, 500, logDirs)
    new TierTopicManager(
      tierTopicManagerConfig,
      consumerBuilder,
      producerBuilder,
      bootstrapSupplier,
      tierMetadataManager,
      EasyMock.mock(classOf[LogDirFailureChannel]))
  }

  @Test
  def testTierTopicManager(): Unit = {
    try {
      val numPartitions: Short = 1
      val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
      val tierTopicManager = createTierTopicManager(consumerBuilder, numPartitions)
      tierTopicManager.becomeReady(bootstrapSupplier.get())

      val archivedPartition1 = new TopicIdPartition("archivedTopic", UUID.randomUUID(), 0)
      addReplica(archivedPartition1)
      becomeLeader(consumerBuilder,
        tierTopicManager,
        archivedPartition1,
        0,
        AppendResult.ACCEPTED)

      val objectId = UUID.randomUUID
      uploadWithMetadata(tierTopicManager,
        consumerBuilder,
        archivedPartition1,
        0,
        objectId,
        0L,
        1000L,
        16000L,
        16000L,
        1000,
        false,
        true,
        false)
      tierTopicManager.committer().flush()
      val tierPartitionState = tierTopicManager.partitionState(archivedPartition1)
      assertEquals(1000L, tierPartitionState.committedEndOffset().get())

      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
      tierTopicManager.committer().flush()
      // end offset shouldn't have updated - message with identical ranges with same start offset and epoch
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
      val archivedPartition2 = new TopicIdPartition("archivedTopic", UUID.randomUUID(), 1)
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
  def testTierTopicManagerThreadDies(): Unit = {
    val numPartitions : Short = 1
    val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
    val tierTopicManagerConfig = new TierTopicManagerConfig("", "", numPartitions, 1.toShort, 3, clusterId, 5L, 30000, 500, logDirs)
    val didWork = new AtomicBoolean(false)
    val tierTopicManager = new TierTopicManager(
      tierTopicManagerConfig,
      consumerBuilder,
      producerBuilder,
      bootstrapSupplier,
      tierMetadataManager,
      EasyMock.mock(classOf[LogDirFailureChannel])) {
      override def doWork(): Boolean = {
        didWork.set(true)
        throw new IOException("test correct shutdown.")
      }
    }

    tierTopicManager.becomeReady(bootstrapSupplier.get())
    tierTopicManager.startup()
    TestUtils.waitUntilTrue(() => didWork.get(), "waited for doWork to run.")
    TestUtils.waitUntilTrue(() => !tierTopicManager.isReady, "TierTopicManager should revert to !isReady")
    tierTopicManager.shutdown()
  }

  @Test
  def testCatchUpConsumer(): Unit = {
    try {
      val numPartitions: Short = 1
      val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
      val tierTopicManager = createTierTopicManager(consumerBuilder, numPartitions)
      tierTopicManager.becomeReady(bootstrapSupplier.get())
      val topicId = UUID.randomUUID()
      val archivedPartition1 = new TopicIdPartition("archivedTopic", topicId, 0)
      addReplica(archivedPartition1)
      tierMetadataManager.becomeFollower(archivedPartition1)

      val archivedPartition2 = new TopicIdPartition("archivedTopic", topicId, 1)
      addReplica(archivedPartition2)
      tierMetadataManager.becomeFollower(archivedPartition2)

      tierTopicManager.processMigrations()
      assertTrue(tierTopicManager.catchingUp())
      tierMetadataManager.delete(archivedPartition2.topicPartition())
      tierTopicManager.processMigrations()
      assertTrue(tierTopicManager.catchingUp())
      tierMetadataManager.delete(archivedPartition1.topicPartition())
      tierTopicManager.processMigrations()
      assertFalse(tierTopicManager.catchingUp())
    } finally {
      Option(new File(logDir).listFiles)
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(_.delete())
    }
  }

  @Test
  def testPrimaryConsumerOffsetLoad(): Unit = {
    try {
      val numPartitions: Short = 2
      val partition1 = new TopicPartition(tierTopicName, 0)
      val partition2 = new TopicPartition(tierTopicName, 1)
      val partitions = util.Arrays.asList(partition1, partition2)
      val committedOffset = 300L

      val producerBuilder = new MockProducerBuilder()
      val consumer: KafkaConsumer[Array[Byte], Array[Byte]] = EasyMock.createMock(classOf[KafkaConsumer[Array[Byte], Array[Byte]]])
      val consumerBuilder: ConsumerBuilder = EasyMock.createMock(classOf[ConsumerBuilder])
      EasyMock.expect(consumerBuilder.setupConsumer(EasyMock.anyString, EasyMock.anyString, EasyMock.anyString))
        .andReturn(consumer)
      EasyMock.expect(consumer.assign(partitions)).andVoid()
      // partition without committed offset should seek to beginning
      EasyMock.expect(consumer.seekToBeginning(Collections.singletonList(partition1))).andVoid()
      // partition with committed offset should restore position
      EasyMock.expect(consumer.seek(partition2, committedOffset)).andVoid()
      EasyMock.replay(consumerBuilder)
      EasyMock.replay(consumer)

      val tierTopicManager = createTierTopicManager(consumerBuilder, numPartitions)

      // set position for partition 2 on committer to test committed offset recovery
      tierTopicManager.committer().updatePosition(partition2.partition(), committedOffset)
      tierTopicManager.becomeReady(bootstrapSupplier.get())
      EasyMock.verify(consumerBuilder)
      EasyMock.verify(consumer)

    } finally {
      Option(new File(logDir).listFiles)
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(_.delete())
    }
  }

  def testTrackAppendsBeforeReady(): Unit = {
    val numPartitions: Short = 1
    val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
    val tierTopicManager = createTierTopicManager(consumerBuilder, numPartitions)

    val epoch = 0
    val topicIdPartition_1 = new TopicIdPartition("foo_1", UUID.randomUUID, 0)
    val initLeader_1 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 0)
    addReplica(topicIdPartition_1)
    tierMetadataManager.becomeLeader(topicIdPartition_1, epoch)

    val topicIdPartition_2 = new TopicIdPartition("foo_2", UUID.randomUUID, 0)
    val initLeader_2 = new TierTopicInitLeader(topicIdPartition_2, epoch, UUID.randomUUID, 0)
    addReplica(topicIdPartition_2)
    tierMetadataManager.becomeLeader(topicIdPartition_2, epoch)

    val initLeaderResult_1 = tierTopicManager.addMetadata(initLeader_1)
    val initLeaderResult_2 = tierTopicManager.addMetadata(initLeader_2)

    tierTopicManager.becomeReady(bootstrapSupplier.get)

    TestUtils.waitUntilTrue(() => {
      tierTopicManager.doWork()
      consumerBuilder.moveRecordsFromProducer()
      initLeaderResult_1.isDone && initLeaderResult_2.isDone
    }, "Timeout waiting for futures to complete")
    assertEquals(AppendResult.ACCEPTED, initLeaderResult_1.get)
    assertEquals(AppendResult.ACCEPTED, initLeaderResult_2.get)
  }

  private def addReplica(topicIdPartition: TopicIdPartition): Unit = {
    val properties = new Properties()
    properties.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    val dir = new File(logDir + "/" + topicIdPartition.topicPartition.toString)
    dir.mkdir()
    tierMetadataManager.initState(topicIdPartition.topicPartition(), dir, new LogConfig(properties))
  }

  private def becomeLeader(consumerBuilder: MockConsumerBuilder,
                           tierTopicManager: TierTopicManager,
                           topicIdPartition: TopicIdPartition,
                           epoch: Integer,
                           expected: AppendResult): Unit = {
    tierMetadataManager.becomeLeader(topicIdPartition, epoch)
    // force immigration
    tierTopicManager.doWork()
    val result = tierTopicManager.becomeArchiver(topicIdPartition, epoch)
    consumerBuilder.moveRecordsFromProducer()
    while (!tierTopicManager.doWork()) assertEquals(expected, result.get())
    tierTopicManager.committer().flush()
  }

  private def uploadWithMetadata(tierTopicManager: TierTopicManager,
                                 consumerBuilder: MockConsumerBuilder,
                                 topicIdPartition: TopicIdPartition,
                                 tierEpoch: Int,
                                 objectId: UUID,
                                 startOffset: Long,
                                 endOffset: Long,
                                 maxTimestamp: Long,
                                 lastModifiedTime: Long,
                                 size: Int,
                                 hasAbortedTxnIndex: Boolean,
                                 hasEpochState: Boolean,
                                 hasProducerState: Boolean): Unit = {
    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition, tierEpoch, objectId, startOffset, endOffset,
      maxTimestamp, size, hasEpochState, hasAbortedTxnIndex, hasProducerState)
    val initiateResult = tierTopicManager.addMetadata(uploadInitiate)
    consumerBuilder.moveRecordsFromProducer()
    tierTopicManager.doWork()
    assertEquals(AppendResult.ACCEPTED, initiateResult.get)

    val uploadComplete = new TierSegmentUploadComplete(uploadInitiate)
    val completeResult = tierTopicManager.addMetadata(uploadComplete)
    consumerBuilder.moveRecordsFromProducer()
    tierTopicManager.doWork()
    assertEquals(AppendResult.ACCEPTED, completeResult.get)
  }
}
