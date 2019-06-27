/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.File
import java.util.{UUID, function}
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ExecutorService, Executors}

import kafka.log.{AbstractLog, LogTest}
import kafka.server.{BrokerTopicStats, KafkaConfig, ReplicaManager}
import kafka.tier.{TierTopicAppender, TopicIdPartition}
import kafka.tier.domain.{AbstractTierMetadata, TierTopicInitLeader}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.{FileTierPartitionState, TierPartitionState}
import kafka.tier.store.TierObjectStore.FileType
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.utils.Utils
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.mockito.Mockito._

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

// Verify interactions between the Log and Archiver.
class ArchiveTaskIntegrationTest {
  var executor: ExecutorService = _
  implicit var ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)
  val topicIdPartition = new TopicIdPartition("tp0", UUID.randomUUID(), 0)
  var config: KafkaConfig = null
  val brokerTopicStats = new BrokerTopicStats
  var tmpDir: File = _
  var logDir: File = _
  val mockTime = new MockTime()

  val transitionWaitTime = 30 seconds

  @Before
  def setup(): Unit = {
    tmpDir = TestUtils.tempDir()
    logDir = TestUtils.randomPartitionLogDir(tmpDir)
    executor = Executors.newFixedThreadPool(1)
    val props = TestUtils.createBrokerConfig(0, "127.0.0.1:1", port = -1)
    config = KafkaConfig.fromProps(props)
  }

  @After()
  def teardown(): Unit = {
    executor.shutdownNow()
    brokerTopicStats.close()
    Utils.delete(tmpDir)
    Utils.delete(logDir)
  }

  private def logProvidingReplicaManager(topicPartition: TopicPartition,
                                 abstractLog: AbstractLog): ReplicaManager = {
    when(mock(classOf[ReplicaManager]).getLog(topicPartition)).thenReturn(Some(abstractLog))
      .getMock[ReplicaManager]()
  }

  class MockTierTopicManager extends TierTopicAppender {
    val tierPartitionStates: ConcurrentHashMap[TopicIdPartition, TierPartitionState] = new ConcurrentHashMap[TopicIdPartition, TierPartitionState]()

    private def createTierPartitionState(topicIdPartition: TopicIdPartition): TierPartitionState = {
      val tierPartitionState = new FileTierPartitionState(TestUtils.tempDir(), topicIdPartition.topicPartition(), true)
      tierPartitionState.setTopicIdPartition(topicIdPartition)
      tierPartitionState
    }

    override def becomeArchiver(topicPartition: TopicIdPartition, tierEpoch: Int): CompletableFuture[TierPartitionState.AppendResult] = {
      val tierPartitionState = tierPartitionStates.computeIfAbsent(topicPartition, new function.Function[TopicIdPartition, TierPartitionState] {
        override def apply(t: TopicIdPartition): TierPartitionState = createTierPartitionState(t)
      })
      val becomeLeaderMessage = new TierTopicInitLeader(topicPartition, tierEpoch, UUID.randomUUID(), 0)
      Future.successful(tierPartitionState.append(becomeLeaderMessage)).toJava.toCompletableFuture
    }
    override def addMetadata(entry: AbstractTierMetadata): CompletableFuture[TierPartitionState.AppendResult] = {
      val tierPartitionState = tierPartitionStates.get(topicIdPartition)
      Future.successful(tierPartitionState.append(entry)).toJava.toCompletableFuture
    }
    override def partitionState(topicIdPartition: TopicIdPartition): TierPartitionState = {
      tierPartitionStates.get(topicIdPartition)
    }
    override def isReady: Boolean = true
  }

  @Test
  def testArchiveTaskEmptyLog(): Unit = {
    val tierObjectStore: MockInMemoryTierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig())
    val ctx = CancellationContext.newContext()
    val task = new ArchiveTask(ctx, topicIdPartition, BeforeUpload(0))
    val leaderEpoch = 0

    val tierTopicManger: MockTierTopicManager = new MockTierTopicManager()
    tierTopicManger.becomeArchiver(topicIdPartition, leaderEpoch)

    val logConfig = LogTest.createLogConfig(segmentBytes = 1024 * 1024 * 5)
    val log = LogTest.createLog(logDir, logConfig, brokerTopicStats, mockTime.scheduler, mockTime)
    val mockReplicaManager = logProvidingReplicaManager(topicIdPartition.topicPartition(), log)
    val nextState = Await.result(task.transition(mockTime, tierTopicManger, tierObjectStore,
      mockReplicaManager, None, None), transitionWaitTime)

    assertTrue("expected to be in BeforeUpload", nextState.state.isInstanceOf[BeforeUpload])
    assertFalse("expected task to not be cancelled", nextState.ctx.isCancelled)
    assertEquals("expected zero segments to be uploaded",
      tierObjectStore.getObjectCounts.getOrDefault(FileType.SEGMENT, 0), 0)
  }

  @Test
  def testArchiveTaskUploadsProducerState(): Unit = {
    val tierObjectStore: MockInMemoryTierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig())
    val ctx = CancellationContext.newContext()
    val task = new ArchiveTask(ctx, topicIdPartition, BeforeUpload(0))
    val leaderEpoch = 0

    val tierTopicManger: MockTierTopicManager = new MockTierTopicManager()
    tierTopicManger.becomeArchiver(topicIdPartition, leaderEpoch)

    val logConfig = LogTest.createLogConfig(segmentBytes = 1024)
    val log = LogTest.createLog(logDir, logConfig, brokerTopicStats, mockTime.scheduler, mockTime)
    val mockReplicaManager = logProvidingReplicaManager(topicIdPartition.topicPartition(), log)

    val pid1 = 1L

    var lastOffset = 0L
    for (i <- 0 to 20) {
      val appendInfo = log.appendAsLeader(TestUtils.records(Seq(new SimpleRecord(mockTime
        .milliseconds(), new
          Array[Byte](128))),
        producerId = pid1, producerEpoch = 0, sequence = i),
        leaderEpoch = 0)
      lastOffset = appendInfo.lastOffset
    }
    log.highWatermark = lastOffset
    assertEquals("expected 5 local log segments", 5, log.localLogSegments.size)
    assertEquals("expected 4 tierable segments", 4, log.tierableLogSegments.size)
    val baseOffsets = log.localLogSegments.map(_.baseOffset).toList
    val snapshotFiles = baseOffsets.flatMap(log.producerStateManager.snapshotFileForOffset(_))
    assertEquals("expected to be able to use segment base offset to get snapshot files for" +
      " 4 segments", 4, snapshotFiles.size)

    val maybeUpload = Await.result(
      task.transition(mockTime, tierTopicManger, tierObjectStore, mockReplicaManager, None, None), transitionWaitTime)
    assertEquals("expected successful transition to Upload", classOf[Upload], maybeUpload.state.getClass)
    val maybeAfterUpload = Await.result(
      task.transition(mockTime, tierTopicManger, tierObjectStore, mockReplicaManager, None, None), transitionWaitTime)
    assertEquals("expected successful transition to afterUpload", classOf[AfterUpload], maybeAfterUpload.state.getClass)

    val objectCounts = tierObjectStore.getObjectCounts
    assertEquals("expected 1 segment file", 1, objectCounts.get(FileType.SEGMENT))
    assertEquals("expected 1 producer state", 1, objectCounts.get(FileType.PRODUCER_STATE))

    val maybeBeforeUpload = Await.result(maybeAfterUpload.transition(mockTime, tierTopicManger, tierObjectStore, mockReplicaManager, None, None), transitionWaitTime)
    assertTrue("expected successful transition to beforeUpload", maybeBeforeUpload.state.isInstanceOf[BeforeUpload])

    val tierPartitionState = tierTopicManger.partitionState(topicIdPartition)
    assertEquals("expected 1 segment to be materialized", tierPartitionState.numSegments(), 1)
    val metadata = tierPartitionState.metadata(0L).get()
    assertTrue("expected hasProducerState metadata flag to be set", metadata.hasProducerState)
  }
}
