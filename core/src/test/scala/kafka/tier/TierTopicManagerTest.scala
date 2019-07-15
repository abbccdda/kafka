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
import kafka.tier.state.TierPartitionStatus
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.config.ConfluentTopicConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.easymock.EasyMock
import org.junit.After
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

  private def createTierTopicManager(tierMetadataManager: TierMetadataManager, consumerBuilder: TierTopicConsumerBuilder, tierTopicNumPartitions: Short): TierTopicManager = {
    val tierTopicManagerConfig = new TierTopicManagerConfig("", "", tierTopicNumPartitions, 1.toShort, 3, clusterId, 5L, 30000, 500, logDirs)
    new TierTopicManager(
      tierTopicManagerConfig,
      consumerBuilder,
      producerBuilder,
      bootstrapSupplier,
      tierMetadataManager,
      EasyMock.mock(classOf[LogDirFailureChannel])) {
      override def startup() {}
    }
  }

  @After
  def tearDown(): Unit = {
    tierMetadataManager.close()
  }

  @Test
  def testTierTopicManager(): Unit = {
    try {
      val numPartitions: Short = 1
      val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
      val tierTopicManager = createTierTopicManager(tierMetadataManager, consumerBuilder, numPartitions)
      tierTopicManager.becomeReady(bootstrapSupplier.get())

      val archivedPartition1 = new TopicIdPartition("archivedTopic", UUID.randomUUID(), 0)
      addReplica(tierMetadataManager, archivedPartition1)
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
      addReplica(tierMetadataManager, archivedPartition2)
      becomeLeader(consumerBuilder,
        tierTopicManager,
        archivedPartition2,
        0,
        AppendResult.ACCEPTED)

      tierTopicManager.committer.flush()
      assertEquals(tierTopicManager.committer.positions.get(0),
        consumerBuilder.logEndOffset)

      assertFalse(tierTopicManager.catchingUp())
      assertEquals(0, tierTopicManager.numResultListeners())
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
  def testCatchUpConsumerReconcile(): Unit = {
    try {
      val numPartitions: Short = 1
      val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
      val tierTopicManager = createTierTopicManager(tierMetadataManager, consumerBuilder, numPartitions)
      tierTopicManager.becomeReady(bootstrapSupplier.get())
      val topicId = UUID.randomUUID()
      val archivedPartition1 = new TopicIdPartition("archivedTopic", topicId, 0)
      addReplica(tierMetadataManager, archivedPartition1)
      tierMetadataManager.becomeFollower(archivedPartition1.topicPartition())
      tierMetadataManager.ensureTopicIdPartition(archivedPartition1)

      val archivedPartition2 = new TopicIdPartition("archivedTopic", topicId, 1)
      addReplica(tierMetadataManager, archivedPartition2)
      tierMetadataManager.becomeFollower(archivedPartition2.topicPartition())
      tierMetadataManager.ensureTopicIdPartition(archivedPartition2)

      tierTopicManager.processMigrations()
      assertTrue(tierTopicManager.catchingUp())
      tierMetadataManager.delete(archivedPartition2.topicPartition())
      tierTopicManager.processMigrations()
      assertTrue(tierTopicManager.catchingUp())
      tierMetadataManager.delete(archivedPartition1.topicPartition())
      tierTopicManager.processMigrations()
      assertFalse("tier topic manager should have stopped catching up due to deleted partitions", tierTopicManager.catchingUp())

      assertEquals(0, tierTopicManager.numResultListeners())
    } finally {
      Option(new File(logDir).listFiles)
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(_.delete())
    }
  }

  @Test
  def testCatchUpConsumerSwitchToOnlineAndPrimary(): Unit = {
    try {
      val numPartitions: Short = 1
      val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
      val tierTopicManager = createTierTopicManager(tierMetadataManager, consumerBuilder, numPartitions)
      tierTopicManager.becomeReady(bootstrapSupplier.get())
      val topicId = UUID.randomUUID()
      val archivedPartition1 = new TopicIdPartition("archivedTopic", topicId, 0)
      addReplica(tierMetadataManager, archivedPartition1)
      tierMetadataManager.becomeFollower(archivedPartition1.topicPartition())
      tierMetadataManager.ensureTopicIdPartition(archivedPartition1)

      val archivedPartition2 = new TopicIdPartition("archivedTopic", topicId, 1)
      addReplica(tierMetadataManager, archivedPartition2)
      tierMetadataManager.becomeFollower(archivedPartition2.topicPartition())
      tierMetadataManager.ensureTopicIdPartition(archivedPartition2)

      tierTopicManager.processMigrations()
      assertTrue(tierTopicManager.catchingUp())
      assertEquals(TierPartitionStatus.CATCHUP, tierMetadataManager.tierPartitionState(archivedPartition1.topicPartition()).get().status())
      assertEquals(TierPartitionStatus.CATCHUP, tierMetadataManager.tierPartitionState(archivedPartition2.topicPartition()).get().status())

      // set kafka consumer to return TimeoutException and verify that cachingUp is true 
      // in case the consumer raises TimeoutException on position() call doWork() will be 
      // retried forever, therefore the test is emulating it by attempting it 3 times.
      for (attempt <- 1 to 3) {
        consumerBuilder.setConsumerException("catchup", new TimeoutException("Test exception"))
        tierTopicManager.doWork()
        assertTrue(tierTopicManager.catchingUp())
      }
      
      // test the normal scenario (without consumer exception as exception is reset after position() call)
      tierTopicManager.doWork()
      assertFalse(tierTopicManager.catchingUp())
      // partitions should have been set ONLINE after catchup
      assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager.tierPartitionState(archivedPartition1.topicPartition()).get().status())
      assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager.tierPartitionState(archivedPartition2.topicPartition()).get().status())
      assertEquals(0, tierTopicManager.numResultListeners())
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

      val tierTopicManager = createTierTopicManager(tierMetadataManager, consumerBuilder, numPartitions)

      // set position for partition 2 on committer to test committed offset recovery
      tierTopicManager.committer().updatePosition(partition2.partition(), committedOffset)
      tierTopicManager.becomeReady(bootstrapSupplier.get())
      EasyMock.verify(consumerBuilder)
      EasyMock.verify(consumer)
      assertEquals(0, tierTopicManager.numResultListeners())

    } finally {
      Option(new File(logDir).listFiles)
        .map(_.toList)
        .getOrElse(Nil)
        .foreach(_.delete())
    }
  }

  @Test
  def testResumeMaterializationOnStart(): Unit = {
    val numPartitions: Short = 1
    val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
    val tierTopicManager = createTierTopicManager(tierMetadataManager, consumerBuilder, numPartitions)

    var epoch = 0
    val topicIdPartition_1 = new TopicIdPartition("foo_1", UUID.randomUUID, 0)
    val initLeader_1 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 0)
    addReplica(tierMetadataManager, topicIdPartition_1)
    tierMetadataManager.becomeLeader(topicIdPartition_1.topicPartition(), epoch)
    tierMetadataManager.ensureTopicIdPartition(topicIdPartition_1)
    val initLeaderResult_1 = tierTopicManager.addMetadata(initLeader_1)
    tierTopicManager.becomeReady(bootstrapSupplier.get)

    TestUtils.waitUntilTrue(() => {
      tierTopicManager.doWork()
      consumerBuilder.moveRecordsFromProducer()
      initLeaderResult_1.isDone
    }, "Timeout waiting for futures to complete")
    assertEquals(AppendResult.ACCEPTED, initLeaderResult_1.get)

    assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager.tierPartitionState(topicIdPartition_1.topicPartition()).get().status())

    tierTopicManager.committer().flush()
    assertEquals(Collections.singletonMap[Integer, Long](0, 1), tierTopicManager.committer().positions())

    // another broker takes over archiving, but we do not materialize yet
    epoch = 1
    val initLeader_2 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 1)
    tierTopicManager.shutdown()
    val initLeaderResult_2 = tierTopicManager.addMetadata(initLeader_2)
    assertFalse("second init leader should not have been materialized", initLeaderResult_2.isDone)

    // open a new TierMetadataManager and TierTopicManager to test recovery of ONLINE partitions
    val tierMetadataManager2 = new TierMetadataManager(new FileTierPartitionStateFactory(),
      Optional.of(new MockInMemoryTierObjectStore(objectStoreConfig)),
      new LogDirFailureChannel(1),
      true)

    // simulate log manager reopening log, and testing that resumption of ONLINE partition
    // proceeds correctly
    try {
      addReplica(tierMetadataManager2, topicIdPartition_1)

      assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager2.tierPartitionState(topicIdPartition_1.topicPartition()).get().status())
      assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager2.tierPartitionState(topicIdPartition_1).get().status())

      val tierTopicManager2 = createTierTopicManager(tierMetadataManager2, consumerBuilder, numPartitions)
      try {
        tierTopicManager2.becomeReady(bootstrapSupplier.get)

        TestUtils.waitUntilTrue(() => {
          tierTopicManager2.doWork()
          consumerBuilder.moveRecordsFromProducer()
          tierMetadataManager2.tierPartitionState(topicIdPartition_1).get().tierEpoch() == 1
        }, "materialization of initLeader_2 message missed after restart")
        assertEquals(Collections.singletonMap[Integer, Long](0, 2), tierTopicManager2.committer().positions())

        // finally become leader again
        epoch = 2
        tierMetadataManager2.becomeLeader(topicIdPartition_1.topicPartition(), epoch)
        tierMetadataManager2.ensureTopicIdPartition(topicIdPartition_1)
        assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager2.tierPartitionState(topicIdPartition_1.topicPartition()).get().status())
        assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager2.tierPartitionState(topicIdPartition_1).get().status())

        val initLeader_3 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 0)
        val initLeaderResult_3 = tierTopicManager2.addMetadata(initLeader_3)
        TestUtils.waitUntilTrue(() => {
          tierTopicManager2.doWork()
          consumerBuilder.moveRecordsFromProducer()
          initLeaderResult_3.isDone
        }, "Timeout waiting for futures to complete")
        assertEquals(AppendResult.ACCEPTED, initLeaderResult_3.get)

        assertEquals(0, tierTopicManager2.numResultListeners())
      } finally {
        tierTopicManager2.shutdown()
      }
    } finally {
      tierMetadataManager2.close()
    }
  }


  @Test
  def testTrackAppendsBeforeReady(): Unit = {
    val numPartitions: Short = 1
    val consumerBuilder = new MockConsumerBuilder(numPartitions, producerBuilder.producer())
    val tierTopicManager = createTierTopicManager(tierMetadataManager, consumerBuilder, numPartitions)

    val epoch = 0
    val topicIdPartition_1 = new TopicIdPartition("foo_1", UUID.randomUUID, 0)
    val initLeader_1 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 0)
    addReplica(tierMetadataManager, topicIdPartition_1)
    tierMetadataManager.becomeLeader(topicIdPartition_1.topicPartition(), epoch)
    tierMetadataManager.ensureTopicIdPartition(topicIdPartition_1)

    val topicIdPartition_2 = new TopicIdPartition("foo_2", UUID.randomUUID, 0)
    val initLeader_2 = new TierTopicInitLeader(topicIdPartition_2, epoch, UUID.randomUUID, 0)
    addReplica(tierMetadataManager, topicIdPartition_2)
    tierMetadataManager.becomeLeader(topicIdPartition_2.topicPartition(), epoch)
    tierMetadataManager.ensureTopicIdPartition(topicIdPartition_2)

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
    assertEquals(0, tierTopicManager.numResultListeners())
  }

  private def addReplica(tierMetadataManager: TierMetadataManager, topicIdPartition: TopicIdPartition): Unit = {
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
    tierMetadataManager.becomeLeader(topicIdPartition.topicPartition(), epoch)
    tierMetadataManager.ensureTopicIdPartition(topicIdPartition)
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
    assertEquals(0, tierTopicManager.numResultListeners())
  }
}
