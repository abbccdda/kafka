/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic

import java.io.File
import java.util
import java.util.{Collections, UUID}
import java.util.function.Supplier
import java.util.Optional

import kafka.admin.AdminOperationException
import kafka.log.Log
import kafka.server.LogDirFailureChannel
import kafka.tier.client.{MockConsumerSupplier, MockProducerSupplier}
import kafka.tier.domain.{AbstractTierMetadata, TierSegmentUploadComplete, TierSegmentUploadInitiate, TierTopicInitLeader}
import kafka.tier.exceptions.TierMetadataFatalException
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{FileTierPartitionState, OffsetAndEpoch, TierPartitionStatus}
import kafka.tier.topic.TierTopicConsumer.ClientCtx
import kafka.tier.{TierReplicaManager, TierTopicManagerCommitter, TopicIdPartition}
import kafka.utils.TestUtils
import kafka.zk.AdminZkClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.errors.{TimeoutException, TopicExistsException}
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Test}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.Assertions.intercept

import scala.collection.JavaConverters._

class TierTopicManagerTest {
  private type ConsumerSupplier = MockConsumerSupplier[Array[Byte], Array[Byte]]
  private type ProducerSupplier = MockProducerSupplier[Array[Byte], Array[Byte]]

  private val clusterId = "mycluster"
  private val tempDir = TestUtils.tempDir()
  private val logDir = tempDir.getAbsolutePath
  private val logDirs = new util.ArrayList(util.Collections.singleton(logDir))

  private val tierTopicNumPartitions = 7.toShort
  private val tierTopicManagerConfig = new TierTopicManagerConfig(
    () => Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "bootstrap"),
    "",
    tierTopicNumPartitions,
    1.toShort,
    3,
    clusterId,
    5L,
    30000,
    500,
    logDirs)
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
  private var tierPartitionStateFiles: Array[FileTierPartitionState] = Array()

  @After
  def teardown(): Unit = {
    tierPartitionStateFiles.foreach { tierPartitionState =>
      tierPartitionState.close()
      tierPartitionState.delete()
    }

    Utils.delete(new File(logDir))
  }

  @Test
  def testAddMetadataBeforeReady(): Unit = {
    val epoch = 0
    val (tierTopicConsumer, _, tierTopicManager) = setupTierComponents(becomeReady = false)
    assertFalse(tierTopicManager.isReady)

    val topicIdPartition_1 = new TopicIdPartition("foo_1", UUID.randomUUID, 0)
    val initLeader_1 = new TierTopicInitLeader(topicIdPartition_1, epoch, UUID.randomUUID, 0)
    val clientCtx_1 = mock(classOf[ClientCtx])
    when(clientCtx_1.status).thenReturn(TierPartitionStatus.ONLINE)
    when(clientCtx_1.process(ArgumentMatchers.eq(initLeader_1), any())).thenReturn(AppendResult.ACCEPTED)
    tierTopicConsumer.register(topicIdPartition_1, clientCtx_1)

    val topicIdPartition_2 = new TopicIdPartition("foo_2", UUID.randomUUID, 0)
    val initLeader_2 = new TierTopicInitLeader(topicIdPartition_2, epoch, UUID.randomUUID, 0)
    val clientCtx_2 = mock(classOf[ClientCtx])
    when(clientCtx_2.status).thenReturn(TierPartitionStatus.ONLINE)
    when(clientCtx_2.process(ArgumentMatchers.eq(initLeader_2), any())).thenReturn(AppendResult.ACCEPTED)
    tierTopicConsumer.register(topicIdPartition_2, clientCtx_2)

    val future_1 = tierTopicManager.addMetadata(initLeader_1)

    val future_2 = tierTopicManager.addMetadata(initLeader_2)

    assertTrue(tierTopicManager.tryBecomeReady(false))

    TestUtils.waitUntilTrue(() => {
      primaryConsumerSupplier.moveRecordsFromProducer()
      tierTopicConsumer.doWork()
      future_1.isDone && future_2.isDone
    }, "Timed out waiting for futures to complete")

    assertEquals(AppendResult.ACCEPTED, future_1.get)
    assertEquals(AppendResult.ACCEPTED, future_2.get)
  }

  @Test
  def testDuplicateRequestBeforeReady(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID, 0)
    val epoch = 0

    val (tierTopicConsumer, _, tierTopicManager) = setupTierComponents(becomeReady = false)
    addReplica(topicIdPartition, tierTopicConsumer)
    assertFalse(tierTopicManager.isReady)

    val objectId = UUID.randomUUID
    val initLeader = new TierTopicInitLeader(topicIdPartition, epoch, objectId, 0)
    val oldInitLeaderResult = tierTopicManager.addMetadata(initLeader)
    val newInitLeaderResult = tierTopicManager.addMetadata(initLeader)
    val caught = intercept[java.util.concurrent.ExecutionException] {
      oldInitLeaderResult.get
    }
    // Before the TierTopicManager is ready to go, oldInitLeaderResult should get a
    // TierMetadataFatalException because it was replaced by newInitLeaderResult.
    assertTrue(caught.getCause.isInstanceOf[TierMetadataFatalException])

    // Now, after the TierTopicManager is ready to go, and the consumer has materialized the events,
    // newInitLeaderResult should not get a TierMetadataFatalException. Instead, it should complete
    // with AppendResult.ACCEPTED.
    val ready = tierTopicManager.tryBecomeReady(false)
    assertTrue(ready)

    TestUtils.waitUntilTrue(() => {
      moveRecordsToAllConsumers()
      tierTopicConsumer.doWork()
      newInitLeaderResult.isDone
    }, "Timed out waiting to become archiver")

    assertEquals(AppendResult.ACCEPTED, newInitLeaderResult.get)
  }

  @Test
  def testRetryOnUnknownExceptionDuringTopicCreation(): Unit = {
    val (tierTopicConsumer, _, tierTopicManager) = setupTierComponents(becomeReady = false)
    assertFalse(tierTopicManager.isReady)

    // 1. first call to `createTopic` will throw `TimeoutException`o
    // 2. second call will throw `AdminOperationException`
    // 3. third call will return without any exception
    doThrow(new TimeoutException("timeout when creating topic"))
        .doThrow(new AdminOperationException("admin operation exception"))
        .doNothing()
        .when(adminZkClient).createTopic(any(), any(), any(), any(), any(), any())

    tierTopicManager.tryBecomeReady(false)
    assertFalse(tierTopicManager.isReady)
    verify(adminZkClient, times(1)).createTopic(any(), any(), any(), any(), any(), any())

    tierTopicManager.tryBecomeReady(false)
    assertFalse(tierTopicManager.isReady)
    verify(adminZkClient, times(2)).createTopic(any(), any(), any(), any(), any(), any())

    tierTopicManager.tryBecomeReady(false)
    assertTrue(tierTopicManager.isReady)
    verify(adminZkClient, times(3)).createTopic(any(), any(), any(), any(), any(), any())
    assertEquals(tierTopicNumPartitions, tierTopicConsumer.tierTopic.numPartitions.getAsInt)
  }

  @Test
  def testPartitionerSetupWhenTopicExists(): Unit = {
    val existingPartitions = tierTopicNumPartitions - 2

    val (tierTopicConsumer, _, tierTopicManager) = setupTierComponents(becomeReady = false)
    assertFalse(tierTopicManager.isReady)

    when(adminZkClient.createTopic(any(), any(), any(), any(), any(), any())).thenThrow(new TopicExistsException("topic exists"))
    when(adminZkClient.numPartitions(Set(tierTopicName))).thenReturn(Map(tierTopicName -> existingPartitions))

    tierTopicManager.tryBecomeReady(false)
    assertTrue(tierTopicManager.isReady)
    assertEquals(existingPartitions, tierTopicConsumer.tierTopic.numPartitions.getAsInt)
  }

  @Test
  def testRetriedMessages(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID, 0)
    val leaderEpoch = 0

    val (tierTopicConsumer, _, tierTopicManager) = setupTierComponents(becomeReady = true)
    addReplica(topicIdPartition, tierTopicConsumer)
    becomeArchiver(topicIdPartition, leaderEpoch, tierTopicManager, tierTopicConsumer)

    val objectId = UUID.randomUUID
    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition, 0, objectId, 0, 100, 100, 100, true, false, false)
    val initiateResult = tierTopicManager.addMetadata(uploadInitiate)
    moveRecordsToAllConsumers()
    tierTopicConsumer.doWork()
    assertEquals(AppendResult.ACCEPTED, initiateResult.get)

    // simulate duplicated/retried UploadInitiate message, which will not be consumed until
    // after an UploadComplete message is sent. This message should not complete the later uploadComplete
    // send when consumed
    resendPreviousProduceRequest()

    val uploadComplete = new TierSegmentUploadComplete(uploadInitiate)
    val completeResult = tierTopicManager.addMetadata(uploadComplete)

    // don't move UploadComplete record over from mocked producer to mock consumer yet.
    // we want to test if upload UploadInitiate completes UploadComplete result
    tierTopicConsumer.doWork()

    assertFalse("Upload complete result should not have been completed by materialization of UploadInitiate",
      completeResult.isDone)
    assertEquals(1, tierTopicConsumer.numListeners)
  }

  @Test
  def testSetErrorPartitionsDuringFencing(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID, 0)

    val (tierTopicConsumer, _, tierTopicManager) = setupTierComponents(becomeReady = true)
    addReplica(topicIdPartition, tierTopicConsumer)

    val objectId = UUID.randomUUID
    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition, 0, objectId, 0, 100, 100, 100, true, false, false)

    val initiateResultFuture = tierTopicManager.addMetadata(uploadInitiate)
    TestUtils.waitUntilTrue(() => {
      moveRecordsToAllConsumers()
      tierTopicConsumer.doWork()
      initiateResultFuture.isDone
    }, "Timed out trying to finish TierSegmentUploadInitiate")
    // TierSegmentUploadInitiate was attempted without TierTopicInitLeader, therefore it should
    // fence the partition.
    assertEquals(AppendResult.FAILED, initiateResultFuture.get)
    assertEquals(1, tierTopicConsumer.errorPartitions().size())
    assertEquals(Set(topicIdPartition), tierTopicConsumer.errorPartitions().asScala)
  }

  @Test
  def testProcessMessagesPostStateFencingDuringOnlineState(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID, 0)
    val leaderEpoch = 0

    val (tierTopicConsumer, _, tierTopicManager) = setupTierComponents(becomeReady = true)
    addReplica(topicIdPartition, tierTopicConsumer)
    assertEquals(TierPartitionStatus.INIT, tierPartitionStateFiles(0).status())
    becomeArchiver(topicIdPartition, leaderEpoch, tierTopicManager, tierTopicConsumer)
    assertEquals(TierPartitionStatus.CATCHUP, tierPartitionStateFiles(0).status())
    moveRecordsToAllConsumers()
    tierTopicConsumer.doWork()
    assertEquals(TierPartitionStatus.ONLINE, tierPartitionStateFiles(0).status())

    // TierSegmentUploadComplete is attempted without TierSegmentUploadInitiate, therefore it should
    // fence the partition state.
    val objectId = UUID.randomUUID
    val uploadComplete = new TierSegmentUploadComplete(topicIdPartition, 0, objectId)
    val uploadCompleteFuture = tierTopicManager.addMetadata(uploadComplete)
    moveRecordsToAllConsumers()
    tierTopicConsumer.doWork()
    assertEquals(TierPartitionStatus.ERROR, tierPartitionStateFiles(0).status())
    assertTrue(uploadCompleteFuture.isDone)
    assertEquals(AppendResult.FAILED, uploadCompleteFuture.get)
    assertEquals(1, tierTopicConsumer.errorPartitions().size())
    assertEquals(Set(topicIdPartition), tierTopicConsumer.errorPartitions().asScala)

    // Now, TierSegmentUploadInitiate is attempted. It still gets processed with
    // AppendResult.FAILED.
    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition, 0, objectId, 0, 100, 100, 100, true, false, false)
    val uploadInitiateFuture = tierTopicManager.addMetadata(uploadInitiate)
    moveRecordsToAllConsumers()
    tierTopicConsumer.doWork()
    assertEquals(TierPartitionStatus.ERROR, tierPartitionStateFiles(0).status())
    assertTrue(uploadInitiateFuture.isDone)
    assertEquals(AppendResult.FAILED, uploadInitiateFuture.get)
    assertEquals(1, tierTopicConsumer.errorPartitions().size())
    assertEquals(Set(topicIdPartition), tierTopicConsumer.errorPartitions().asScala)
  }

  @Test
  def testProcessMessagesPostStateFencingDuringCatchup(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID, 0)
    val leaderEpoch = 0

    val (tierTopicConsumer, _, tierTopicManager) = setupTierComponents(becomeReady = true)
    addReplica(topicIdPartition, tierTopicConsumer)
    assertEquals(TierPartitionStatus.INIT, tierPartitionStateFiles(0).status())
    moveRecordsToAllConsumers()
    tierTopicConsumer.doWork()
    assertEquals(TierPartitionStatus.CATCHUP, tierPartitionStateFiles(0).status())

    // TierSegmentUploadInitiate is attempted without TierTopicInitLeader. It still gets processed with
    // AppendResult.FAILED.
    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition, 0, UUID.randomUUID, 0, 100, 100, 100, true, false, false)
    val uploadInitiateFuture = tierTopicManager.addMetadata(uploadInitiate)
    TestUtils.waitUntilTrue(() => {
      moveRecordsToAllConsumers()
      tierTopicConsumer.doWork()
      uploadInitiateFuture.isDone
    }, "Timed out waiting for upload initiate future")
    assertTrue(uploadInitiateFuture.isDone)
    assertEquals(TierPartitionStatus.ERROR, tierPartitionStateFiles(0).status())
    assertEquals(AppendResult.FAILED, uploadInitiateFuture.get)
    assertEquals(1, tierTopicConsumer.errorPartitions().size())
    assertEquals(Set(topicIdPartition), tierTopicConsumer.errorPartitions().asScala)

    // Now TierTopicInitLeader is attempted. It still gets processed with
    // AppendResult.FAILED.
    val becomeArchiverFuture = tierTopicManager.becomeArchiver(topicIdPartition, leaderEpoch)
    moveRecordsToAllConsumers()
    tierTopicConsumer.doWork()
    assertTrue(becomeArchiverFuture.isDone)
    assertEquals(AppendResult.FAILED, becomeArchiverFuture.get)
  }

  private def addReplica(topicIdPartition: TopicIdPartition, tierTopicConsumer: TierTopicConsumer): Unit = {
    val dir = new File(logDir + "/" + Log.logDirName(topicIdPartition.topicPartition))
    dir.mkdir()

    val tierPartitionState = new FileTierPartitionState(dir, new LogDirFailureChannel(5), topicIdPartition.topicPartition, true)
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionStateFiles :+= tierPartitionState

    tierTopicConsumer.register(topicIdPartition, new ClientCtx {
      override def process(metadata: AbstractTierMetadata, offsetAndEpoch: OffsetAndEpoch): AppendResult = tierPartitionState.append(metadata, offsetAndEpoch)
      override def status(): TierPartitionStatus = tierPartitionState.status
      override def beginCatchup(): Unit = tierPartitionState.beginCatchup()
      override def completeCatchup(): Unit = tierPartitionState.onCatchUpComplete()
    })
  }

  private def setupTierComponents(tierTopicManagerConfig: TierTopicManagerConfig = tierTopicManagerConfig,
                                  becomeReady: Boolean): (TierTopicConsumer, TierReplicaManager, TierTopicManager) = {
    val tierTopicConsumer = new TierTopicConsumer(tierTopicManagerConfig,
      primaryConsumerSupplier,
      catchupConsumerSupplier,
      new TierTopicManagerCommitter(tierTopicManagerConfig, new LogDirFailureChannel(1)),
      Optional.empty())

    val tierReplicaManager = new TierReplicaManager()
    val tierTopicManager = new TierTopicManager(tierTopicManagerConfig,
      tierTopicConsumer,
      producerSupplier,
      adminClientSupplier)

    if (becomeReady) {
      val ready = tierTopicManager.tryBecomeReady(false)
      assertTrue(ready)
    }

    (tierTopicConsumer, tierReplicaManager, tierTopicManager)
  }

  private def moveRecordsToAllConsumers(): Unit = {
    primaryConsumerSupplier.moveRecordsFromProducer()
    catchupConsumerSupplier.moveRecordsFromProducer()
  }

  private def resendPreviousProduceRequest(): Unit = {
    val mockProducer = producerSupplier.producer()
    val lastSentRecord = mockProducer.history().get(mockProducer.history().size() - 1)
    producerSupplier.producer().send(lastSentRecord)
    moveRecordsToAllConsumers()
  }

  private def becomeArchiver(topicIdPartition: TopicIdPartition,
                             leaderEpoch: Int,
                             tierTopicManager: TierTopicManager,
                             tierTopicConsumer: TierTopicConsumer): Unit = {
    val becomeArchiverFuture = tierTopicManager.becomeArchiver(topicIdPartition, leaderEpoch)

    TestUtils.waitUntilTrue(() => {
      moveRecordsToAllConsumers()
      tierTopicConsumer.doWork()
      becomeArchiverFuture.isDone
    }, "Timed out waiting to become archiver")

    assertEquals(AppendResult.ACCEPTED, becomeArchiverFuture.get)
  }
}
