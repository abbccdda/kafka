/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic

import java.io.{File, IOException}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Supplier
import java.util.{Collections, Optional, Properties, UUID}

import kafka.admin.AdminOperationException
import kafka.log.LogConfig
import kafka.server.LogDirFailureChannel
import kafka.tier.client.{MockConsumerSupplier, MockProducerSupplier}
import kafka.tier.domain.{TierSegmentUploadComplete, TierSegmentUploadInitiate, TierTopicInitLeader}
import kafka.tier.exceptions.TierMetadataRetriableException
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{FileTierPartitionStateFactory, TierPartitionStatus}
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.tier.{TierMetadataManager, TierTestUtils, TopicIdPartition}
import kafka.utils.TestUtils
import kafka.zk.AdminZkClient
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.ConfluentTopicConfig
import org.apache.kafka.common.errors.{TimeoutException, TopicExistsException}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.Assertions.assertThrows

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionException

class TierTopicManagerTest {
  private type ConsumerSupplier = MockConsumerSupplier[Array[Byte], Array[Byte]]
  private type ProducerSupplier = MockProducerSupplier[Array[Byte], Array[Byte]]

  private val clusterId = "mycluster"
  private val objectStoreConfig = new TierObjectStoreConfig("cluster", 1)
  private val tempDir = TestUtils.tempDir()
  private val logDir = tempDir.getAbsolutePath
  private val logDirs = new util.ArrayList(util.Collections.singleton(logDir))
  private val tierMetadataManager = new TierMetadataManager(new FileTierPartitionStateFactory(),
    Optional.of(new MockInMemoryTierObjectStore(objectStoreConfig)),
    new LogDirFailureChannel(1),
    true)

  private val bootstrapSupplier = new Supplier[String] {
    override def get: String = "bootstrap-server"
  }
  private val tierTopicNumPartitions = 7.toShort
  private val tierTopicManagerConfig = new TierTopicManagerConfig(bootstrapSupplier, "", tierTopicNumPartitions, 1.toShort, 3, clusterId, 5L, 30000, 500, logDirs)
  private val tierTopicName = TierTopic.topicName("")
  private val tierTopicPartitions = TierTopicManager.partitions(tierTopicName, tierTopicNumPartitions)

  private val producerSupplier = new ProducerSupplier()
  private val primaryConsumerSupplier = new ConsumerSupplier("primary",
    tierTopicPartitions,
    producerSupplier.producer)
  private val catchupConsumerSupplier = new ConsumerSupplier("catchup",
    tierTopicPartitions,
    producerSupplier.producer)

  private val adminZkClient = mock(classOf[AdminZkClient])
  private val adminClientSupplier = new Supplier[AdminZkClient] {
    override def get(): AdminZkClient = adminZkClient
  }

  private var files = List[File]()

  private def createTierTopicManager(tierMetadataManager: TierMetadataManager,
                                     tierTopicManagerConfig: TierTopicManagerConfig = tierTopicManagerConfig,
                                     becomeReady: Boolean = true): TierTopicManager = {
    val tierTopicManager = new TierTopicManager(tierTopicManagerConfig,
      primaryConsumerSupplier,
      catchupConsumerSupplier,
      producerSupplier,
      adminClientSupplier,
      bootstrapSupplier,
      tierMetadataManager,
      mock(classOf[LogDirFailureChannel])) {
        override def startup(): Unit = {
        }
    }

    if (becomeReady) {
      val ready = tierTopicManager.tryBecomeReady()
      assertTrue(ready)
    }

    tierTopicManager
  }

  @After
  def tearDown(): Unit = {
    tierMetadataManager.close()
    files.foreach(Utils.delete)
  }

  @Test
  def testTierTopicManager(): Unit = {
    val tierTopicManager = createTierTopicManager(tierMetadataManager)

    val archivedPartition1 = new TopicIdPartition("archivedTopic", UUID.randomUUID(), 0)
    addReplica(tierMetadataManager, archivedPartition1)
    becomeLeader(tierTopicManager,
      archivedPartition1,
      0,
      AppendResult.ACCEPTED)

    val objectId = UUID.randomUUID
    uploadWithMetadata(tierTopicManager,
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

    moveRecordsToAllConsumers()
    tierTopicManager.doWork()
    tierTopicManager.committer().flush()
    // end offset shouldn't have updated - message with identical ranges with same start offset and epoch
    // should be filtered
    assertEquals(1000L, tierPartitionState.totalSize)

    // test rejoin on the same epoch
    becomeLeader(tierTopicManager,
      archivedPartition1,
      0,
      AppendResult.ACCEPTED)

    // larger epoch should be accepted
    becomeLeader(tierTopicManager,
      archivedPartition1,
      1,
      AppendResult.ACCEPTED)

    // going back to previous epoch should be fenced
    becomeLeader(tierTopicManager,
      archivedPartition1,
      0,
      AppendResult.FENCED)

    // add a second partition and ensure it catches up.
    val archivedPartition2 = new TopicIdPartition("archivedTopic", UUID.randomUUID(), 1)
    addReplica(tierMetadataManager, archivedPartition2)
    becomeLeader(tierTopicManager,
      archivedPartition2,
      0,
      AppendResult.ACCEPTED)

    // primary consumer must be assigned all tier topic partitions
    val primaryConsumer = primaryConsumerSupplier.consumers.get(0)
    val assignment = primaryConsumer.assignment()
    assertEquals(tierTopicNumPartitions, assignment.size)

    // flush and verify last positions match that of the consumer
    tierTopicManager.committer.flush()

    val endOffsets = primaryConsumer.endOffsets(assignment)
    val committedPositions = tierTopicManager.committer.positions

    endOffsets.asScala.foreach { case (topicPartition, endOffset) =>
        assertEquals(s"endOffsets: $endOffsets committedPositions: $committedPositions",
          endOffset, committedPositions.getOrDefault(topicPartition.partition, 0L))
    }

    assertFalse(tierTopicManager.catchingUp())
    assertEquals(0, tierTopicManager.numResultListeners())
  }

  @Test
  def testRetriedMessages(): Unit = {
    val tierTopicManager = createTierTopicManager(tierMetadataManager)

    val archivedPartition1 = new TopicIdPartition("archivedTopic", UUID.randomUUID(), 0)
    addReplica(tierMetadataManager, archivedPartition1)
    becomeLeader(tierTopicManager,
      archivedPartition1,
      0,
      AppendResult.ACCEPTED)

    val objectId = UUID.randomUUID
    val uploadInitiate = new TierSegmentUploadInitiate(archivedPartition1, 0, objectId, 0, 100, 100, 100, true, false, false)
    val initiateResult = tierTopicManager.addMetadata(uploadInitiate)
    moveRecordsToAllConsumers()
    tierTopicManager.doWork()
    assertEquals(AppendResult.ACCEPTED, initiateResult.get)

    // simulate duplicated/retried UploadInitiate message, which will not be consumed until
    // after an UploadComplete message is sent. This message should not complete the later uploadComplete
    // send when consumed
    resendPreviousProduceRequest()

    val uploadComplete = new TierSegmentUploadComplete(uploadInitiate)
    val completeResult = tierTopicManager.addMetadata(uploadComplete)

    // don't move UploadComplete record over from mocked producer to mock consumer yet.
    // we want to test if upload UploadInitiate completes UploadComplete result
    tierTopicManager.doWork()

    assertFalse("Upload complete result should not have been completed by materialization of UploadInitiate",
      completeResult.isDone)
    assertEquals(1, tierTopicManager.numResultListeners())
  }

  @Test
  def testTierTopicManagerThreadDies(): Unit = {
    val didWork = new AtomicBoolean(false)
    val tierTopicManager = new TierTopicManager(
      tierTopicManagerConfig,
      primaryConsumerSupplier,
      catchupConsumerSupplier,
      producerSupplier,
      adminClientSupplier,
      bootstrapSupplier,
      tierMetadataManager,
      mock(classOf[LogDirFailureChannel])) {
        override def doWork(): Boolean = {
          didWork.set(true)
          throw new IOException("test correct shutdown.")
        }
    }

    assertTrue(tierTopicManager.tryBecomeReady())
    tierTopicManager.startup()
    TestUtils.waitUntilTrue(() => didWork.get(), "waited for doWork to run.")
    TestUtils.waitUntilTrue(() => !tierTopicManager.isReady, "TierTopicManager should revert to !isReady")
    tierTopicManager.shutdown()
  }

  @Test
  def testCatchUpConsumerReconcile(): Unit = {
    val tierTopicManagerConfig = new TierTopicManagerConfig(bootstrapSupplier, "", 1, 1.toShort, 3, clusterId, 5L, 30000, 500, logDirs)
    val tierTopicManager = createTierTopicManager(tierMetadataManager, tierTopicManagerConfig)
    val topicId = UUID.randomUUID
    val randomTopicIdPartitions = TierTestUtils.randomTopicIdPartitions(4, 3)

    // write a bunch of InitLeader messages to tier topic
    randomTopicIdPartitions.foreach { topicIdPartition =>
      val initLeader = new TierTopicInitLeader(topicIdPartition, 0, UUID.randomUUID, 1)
      val producerRecord = new ProducerRecord(tierTopicName, 0, initLeader.serializeKey, initLeader.serializeValue)
      val producer = producerSupplier.producer
      val completed = producer.send(producerRecord)
      producer.flush()
      completed.get
    }
    primaryConsumerSupplier.moveRecordsFromProducer()

    // partition 1
    val archivedPartition1 = new TopicIdPartition("archivedTopic", topicId, 0)
    addReplica(tierMetadataManager, archivedPartition1)
    tierMetadataManager.becomeLeader(archivedPartition1.topicPartition, 0)
    tierMetadataManager.ensureTopicIdPartition(archivedPartition1)
    val future1 = tierTopicManager.becomeArchiver(archivedPartition1, 0)

    // partition 2
    val archivedPartition2 = new TopicIdPartition("archivedTopic", topicId, 1)
    addReplica(tierMetadataManager, archivedPartition2)
    tierMetadataManager.becomeLeader(archivedPartition2.topicPartition, 0)
    tierMetadataManager.ensureTopicIdPartition(archivedPartition2)
    val future2 = tierTopicManager.becomeArchiver(archivedPartition2, 0)

    // process immigration for both partitions; will go into catchup mode
    tierTopicManager.doWork()
    assertTrue(tierTopicManager.catchingUp)

    // delete partition 2
    tierMetadataManager.delete(archivedPartition2.topicPartition)
    tierTopicManager.doWork()
    assertTrue(tierTopicManager.catchingUp)

    // delete partition 1; will continue to catchup until we can get back in sync with primary consumer
    tierMetadataManager.delete(archivedPartition1.topicPartition)
    tierTopicManager.doWork()
    assertTrue("tier topic manager should have stopped catching up due to deleted partitions", tierTopicManager.catchingUp)

    // allow catchup consumer to get back in sync with primary
    catchupConsumerSupplier.moveRecordsFromProducer()

    TestUtils.waitUntilTrue(() => {
      tierTopicManager.doWork()
      tierTopicManager.catchingUp
    }, "Timed out waiting for catchup to complete")

    assertThrows[TierMetadataRetriableException] {
      try {
        future1.get
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }

    assertThrows[TierMetadataRetriableException] {
      try {
        future2.get
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
    assertEquals(0, tierTopicManager.numResultListeners)
  }

  @Test
  def testCatchUpConsumerSwitchToOnlineAndPrimary(): Unit = {
    val tierTopicManager = createTierTopicManager(tierMetadataManager)
    val topicId = UUID.randomUUID
    val archivedPartition1 = new TopicIdPartition("archivedTopic", topicId, 0)
    addReplica(tierMetadataManager, archivedPartition1)
    tierMetadataManager.becomeFollower(archivedPartition1.topicPartition)
    tierMetadataManager.ensureTopicIdPartition(archivedPartition1)

    val archivedPartition2 = new TopicIdPartition("archivedTopic", topicId, 1)
    addReplica(tierMetadataManager, archivedPartition2)
    tierMetadataManager.becomeFollower(archivedPartition2.topicPartition)
    tierMetadataManager.ensureTopicIdPartition(archivedPartition2)

    tierTopicManager.doWork()
    assertTrue(tierTopicManager.catchingUp)
    assertEquals(TierPartitionStatus.CATCHUP, tierMetadataManager.tierPartitionState(archivedPartition1.topicPartition).get.status)
    assertEquals(TierPartitionStatus.CATCHUP, tierMetadataManager.tierPartitionState(archivedPartition2.topicPartition).get.status)

    // set kafka consumer to return TimeoutException and verify that cachingUp is true
    // in case the consumer raises TimeoutException on position() call doWork() will be
    // retried forever, therefore the test is emulating it by attempting it 3 times.
    for (attempt <- 1 to 3) {
      catchupConsumerSupplier.setConsumerPositionException(new TimeoutException("Test position exception"))
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
  }

  @Test
  def testPrimaryConsumerOffsetLoad(): Unit = {
    val allPartitions = tierTopicPartitions.asScala
    val partition_1 = new TopicPartition(tierTopicName, 1)
    val position_1 = 200L

    val partition_2 = new TopicPartition(tierTopicName, 5)
    val position_2 = 99L

    val primaryConsumer = mock(classOf[Consumer[Array[Byte], Array[Byte]]])
    val primaryConsumerSupplier = new Supplier[Consumer[Array[Byte], Array[Byte]]] {
      override def get(): Consumer[Array[Byte], Array[Byte]] = primaryConsumer
    }

    val catchupConsumerSupplier = new Supplier[Consumer[Array[Byte], Array[Byte]]] {
      override def get(): Consumer[Array[Byte], Array[Byte]] = throw new IllegalStateException("did not expect catchup consumer to be setup")
    }

    val tierTopicManager = new TierTopicManager(tierTopicManagerConfig,
      primaryConsumerSupplier,
      catchupConsumerSupplier,
      producerSupplier,
      adminClientSupplier,
      bootstrapSupplier,
      tierMetadataManager,
      mock(classOf[LogDirFailureChannel])) {
        override def startup(): Unit = {
        }
    }

    // set position for partition 2 on committer to test committed offset recovery
    tierTopicManager.committer().updatePosition(partition_1.partition(), position_1)
    tierTopicManager.committer().updatePosition(partition_2.partition(), position_2)

    clearInvocations(primaryConsumer)

    // become ready
    assertTrue(tierTopicManager.tryBecomeReady())

    val expectedSeekToBeginning = allPartitions - partition_1 - partition_2

    verify(primaryConsumer, times(1)).assign(tierTopicPartitions)

    expectedSeekToBeginning.foreach { partition =>
      verify(primaryConsumer, times(1)).seekToBeginning(Collections.singletonList(partition))
    }

    verify(primaryConsumer, times(1)).seek(partition_1, position_1)
    verify(primaryConsumer, times(1)).seek(partition_2, position_2)
  }

  @Test
  def testResumeMaterializationOnStart(): Unit = {
    val tierTopicManager = createTierTopicManager(tierMetadataManager, becomeReady = false)

    var epoch = 0
    val topicIdPartition_1 = new TopicIdPartition("foo_1", UUID.randomUUID, 0)
    val initLeader_1 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 0)
    addReplica(tierMetadataManager, topicIdPartition_1)

    tierMetadataManager.becomeLeader(topicIdPartition_1.topicPartition(), epoch)
    tierMetadataManager.ensureTopicIdPartition(topicIdPartition_1)

    assertTrue(tierTopicManager.tryBecomeReady())
    completeCatchUp(tierTopicManager)

    val initLeaderResult_1 = tierTopicManager.addMetadata(initLeader_1)
    val tierTopicPartition_1 = tierTopicManager.tierTopic.toTierTopicPartition(topicIdPartition_1)

    TestUtils.waitUntilTrue(() => {
      tierTopicManager.doWork()
      primaryConsumerSupplier.moveRecordsFromProducer()
      initLeaderResult_1.isDone
    }, "Timeout waiting for futures to complete")
    assertEquals(AppendResult.ACCEPTED, initLeaderResult_1.get)

    assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager.tierPartitionState(topicIdPartition_1.topicPartition).get.status)

    tierTopicManager.committer.flush()
    assertEquals(Collections.singletonMap[Integer, Long](tierTopicPartition_1.partition, 1), tierTopicManager.committer.positions)

    // another broker takes over archiving, but we do not materialize yet
    tierTopicManager.shutdown()
    epoch = 1
    val initLeader_2 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 1)
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

      assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager2.tierPartitionState(topicIdPartition_1.topicPartition).get.status)
      assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager2.tierPartitionState(topicIdPartition_1).get.status)

      val tierTopicManager2 = createTierTopicManager(tierMetadataManager2)
      try {
        assertTrue(tierTopicManager2.tryBecomeReady())

        TestUtils.waitUntilTrue(() => {
          tierTopicManager2.doWork()
          primaryConsumerSupplier.moveRecordsFromProducer()
          tierMetadataManager2.tierPartitionState(topicIdPartition_1).get.tierEpoch == 1
        }, "materialization of initLeader_2 message missed after restart")
        assertEquals(Collections.singletonMap[Integer, Long](tierTopicPartition_1.partition, 2), tierTopicManager2.committer.positions)

        // finally become leader again
        epoch = 2
        tierMetadataManager2.becomeLeader(topicIdPartition_1.topicPartition, epoch)
        tierMetadataManager2.ensureTopicIdPartition(topicIdPartition_1)
        assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager2.tierPartitionState(topicIdPartition_1.topicPartition).get.status)
        assertEquals(TierPartitionStatus.ONLINE, tierMetadataManager2.tierPartitionState(topicIdPartition_1).get.status)

        val initLeader_3 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 0)
        val initLeaderResult_3 = tierTopicManager2.addMetadata(initLeader_3)
        TestUtils.waitUntilTrue(() => {
          tierTopicManager2.doWork()
          primaryConsumerSupplier.moveRecordsFromProducer()
          initLeaderResult_3.isDone
        }, "Timeout waiting for futures to complete")
        assertEquals(AppendResult.ACCEPTED, initLeaderResult_3.get)

        assertEquals(0, tierTopicManager2.numResultListeners)
      } finally {
        tierTopicManager2.shutdown()
      }
    } finally {
      tierMetadataManager2.close()
    }
  }

  @Test
  def testTrackAppendsBeforeReady(): Unit = {
    val tierTopicManager = createTierTopicManager(tierMetadataManager)

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

    assertTrue(tierTopicManager.tryBecomeReady())
    completeCatchUp(tierTopicManager)

    val initLeaderResult_1 = tierTopicManager.addMetadata(initLeader_1)
    val initLeaderResult_2 = tierTopicManager.addMetadata(initLeader_2)

    TestUtils.waitUntilTrue(() => {
      tierTopicManager.doWork()
      primaryConsumerSupplier.moveRecordsFromProducer()
      initLeaderResult_1.isDone && initLeaderResult_2.isDone
    }, "Timeout waiting for futures to complete")
    assertEquals(AppendResult.ACCEPTED, initLeaderResult_1.get)
    assertEquals(AppendResult.ACCEPTED, initLeaderResult_2.get)
    assertEquals(0, tierTopicManager.numResultListeners())
  }

  @Test
  def testRetryOnUnknownExceptionDuringTopicCreation(): Unit = {
    val tierTopicManager = createTierTopicManager(tierMetadataManager, becomeReady = false)
    assertFalse(tierTopicManager.isReady)

    // 1. first call to `createTopic` will throw `TimeoutException`
    // 2. second call will throw `AdminOperationException`
    // 3. third call will return without any exception
    doThrow(new TimeoutException("timeout when creating topic"))
        .doThrow(new AdminOperationException("admin operation exception"))
        .doNothing()
        .when(adminZkClient).createTopic(any(), any(), any(), any(), any(), any())

    tierTopicManager.tryBecomeReady()
    assertFalse(tierTopicManager.isReady)
    verify(adminZkClient, times(1)).createTopic(any(), any(), any(), any(), any(), any())

    tierTopicManager.tryBecomeReady()
    assertFalse(tierTopicManager.isReady)
    verify(adminZkClient, times(2)).createTopic(any(), any(), any(), any(), any(), any())

    tierTopicManager.tryBecomeReady()
    assertTrue(tierTopicManager.isReady)
    verify(adminZkClient, times(3)).createTopic(any(), any(), any(), any(), any(), any())
    assertEquals(tierTopicNumPartitions, tierTopicManager.tierTopic.numPartitions.getAsInt)
  }

  @Test
  def testPartitionerSetupWhenTopicExists(): Unit = {
    val existingPartitions = tierTopicNumPartitions - 2

    val tierTopicManager = createTierTopicManager(tierMetadataManager, becomeReady = false)
    assertFalse(tierTopicManager.isReady)

    when(adminZkClient.createTopic(any(), any(), any(), any(), any(), any())).thenThrow(new TopicExistsException("topic exists"))
    when(adminZkClient.numPartitions(Set(tierTopicName))).thenReturn(Map(tierTopicName -> existingPartitions))

    tierTopicManager.tryBecomeReady()
    assertTrue(tierTopicManager.isReady)
    assertEquals(existingPartitions, tierTopicManager.tierTopic.numPartitions.getAsInt)
  }

  private def addReplica(tierMetadataManager: TierMetadataManager, topicIdPartition: TopicIdPartition): Unit = {
    val properties = new Properties()
    properties.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    val dir = new File(logDir + "/" + topicIdPartition.topicPartition.toString)
    dir.mkdir()
    tierMetadataManager.initState(topicIdPartition.topicPartition(), dir, new LogConfig(properties))
    files :+= dir
  }

  private def resendPreviousProduceRequest(): Unit = {
    val mockProducer = producerSupplier.producer()
    val lastSentRecord = mockProducer.history().get(mockProducer.history().size() - 1)
    producerSupplier.producer().send(lastSentRecord)
    moveRecordsToAllConsumers()
  }

  private def becomeLeader(tierTopicManager: TierTopicManager,
                           topicIdPartition: TopicIdPartition,
                           epoch: Integer,
                           expected: AppendResult): Unit = {
    tierMetadataManager.becomeLeader(topicIdPartition.topicPartition(), epoch)
    tierMetadataManager.ensureTopicIdPartition(topicIdPartition)

    // force immigration and complete catchup
    completeCatchUp(tierTopicManager)

    val result = tierTopicManager.becomeArchiver(topicIdPartition, epoch)
    primaryConsumerSupplier.moveRecordsFromProducer()

    while (!tierTopicManager.doWork())
      assertEquals(expected, result.get)

    tierTopicManager.committer.flush()
  }

  private def completeCatchUp(tierTopicManager: TierTopicManager): Unit = {
    do {
      tierTopicManager.doWork()
      catchupConsumerSupplier.moveRecordsFromProducer()
    } while (tierTopicManager.catchingUp)
  }

  private def uploadWithMetadata(tierTopicManager: TierTopicManager,
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
    moveRecordsToAllConsumers()
    tierTopicManager.doWork()
    assertEquals(AppendResult.ACCEPTED, initiateResult.get)

    val uploadComplete = new TierSegmentUploadComplete(uploadInitiate)
    val completeResult = tierTopicManager.addMetadata(uploadComplete)
    moveRecordsToAllConsumers()
    tierTopicManager.doWork()
    assertEquals(AppendResult.ACCEPTED, completeResult.get)
    assertEquals(0, tierTopicManager.numResultListeners())
  }

  private def moveRecordsToAllConsumers(): Unit = {
    primaryConsumerSupplier.moveRecordsFromProducer()
    catchupConsumerSupplier.moveRecordsFromProducer()
  }
}
