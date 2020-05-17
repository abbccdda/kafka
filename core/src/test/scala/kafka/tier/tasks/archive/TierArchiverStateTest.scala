/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.archive

import java.io.File
import java.nio.file.Paths
import java.util
import java.util.concurrent.{CompletableFuture, Executors, ScheduledExecutorService, TimeUnit}
import java.util.{Collections, Properties, UUID}
import java.util.Optional

import com.yammer.metrics.core.Meter
import kafka.cluster.Partition
import kafka.log.{AbstractLog, LogManager, LogSegment, LogTest, _}
import kafka.server.{BrokerTopicStats, KafkaConfig, LogDirFailureChannel, ReplicaManager}
import kafka.tier.domain.{AbstractTierMetadata, TierTopicInitLeader}
import kafka.tier.exceptions.TierArchiverFencedException
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{FileTierPartitionState, TierPartitionState, TierPartitionStateFactory}
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.tier.tasks.CompletableFutureUtil
import kafka.tier.topic.{TierTopicConsumer, TierTopicManager}
import kafka.tier.{TierReplicaManager, TierTestUtils, TopicIdPartition}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.exceptions.TierArchiverRestoreFencedException
import kafka.tier.state.OffsetAndEpoch
import kafka.tier.state.TierPartitionStatus
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.utils.CloseableIterator
import org.apache.kafka.common.utils.Time
import org.junit.Assert.assertTrue
import org.junit.{After, Before, Test}
import org.mockito.Mockito.{mock, when}
import org.mockito.ArgumentMatchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class TierArchiverStateTest {
  var ctx: CancellationContext = _
  val mockTime = new MockTime()
  val tierTopicName = "_confluent-tier-state"
  val tierTopicNumPartitions: Short = 1
  val logDirs = new util.ArrayList(Collections.singleton(System.getProperty("java.io.tmpdir")))
  val objectStoreConfig = new TierObjectStoreConfig("cluster", 1)
  val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
  val tierTopicConsumer = mock(classOf[TierTopicConsumer])
  val tierPartitionStateFactory = mock(classOf[TierPartitionStateFactory])
  var tierPartitionStates = Array[TierPartitionState]()
  val tierLogComponents = TierLogComponents(Some(tierTopicConsumer), Some(tierObjectStore), tierPartitionStateFactory)
  val tierReplicaManager = new TierReplicaManager()
  val blockingTaskExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  val time = Time.SYSTEM
  val maxWaitTime = 30 seconds
  var byteRate: Meter = _

  @Before
  def setUp(): Unit = {
    ctx = CancellationContext.newContext()
    kafka.metrics.KafkaMetricsGroup.removeMetric("TierArchiver.UploadRate")
    byteRate = kafka.metrics.KafkaMetricsGroup.newMeter("TierArchiver.UploadRate", "bytes", TimeUnit.SECONDS)
    TierTestUtils.initTierTopicOffset()
  }

  @After
  def tearDown(): Unit = {
    tierPartitionStates.foreach(_.delete())
    ctx.cancel()
  }

  @Test
  def testAwaitingLeaderResult(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicIdPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))

    val properties = new Properties()
    properties.put(KafkaConfig.TierEnableProp, "true")

    val logDirFailureChannel = new LogDirFailureChannel(10)
    val tierPartitionState = createTierPartitionState(new File(logDirs.get(0)), topicIdPartition, logDirFailureChannel, tieringEnabled = true)
    tierReplicaManager.becomeLeader(tierPartitionState, 1)

    val log = mock(classOf[AbstractLog])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(log.tierPartitionState).thenReturn(tierPartitionState)

    val task = ArchiveTask(ctx, topicIdPartition, 0, ArchiverMetrics(None, None))
    val nextStage = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val result = Await.result(nextStage, maxWaitTime)
    assertTrue("Should advance to BeforeUpload", result.state.isInstanceOf[BeforeUpload])
  }

  /**
   * Test that an archive task in Failed state, followed by a retry, followed by a restore at the correct epoch transitions to BeforeUpload state
   */
  @Test
  def testFailedInitLeaderRetryThenRestoreTriggeringBeforeLeader(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicIdPartition, 1))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FAILED))
    val log = mock(classOf[AbstractLog])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(log.tierPartitionState).thenReturn(tierPartitionState)
    when(tierPartitionState.committedEndOffset()).thenReturn(-1L: java.lang.Long)
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierPartitionState.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(Long.MaxValue, Optional.empty()))
    when(tierPartitionState.status).thenReturn(TierPartitionStatus.ERROR).thenReturn(TierPartitionStatus.ONLINE)

    val task = ArchiveTask(ctx, topicIdPartition, 1, ArchiverMetrics(None, None))
    val nextStage = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val failedResult = Await.result(nextStage, maxWaitTime)
    assertTrue("Should advance to FailedState due to AppendResult.FAILED", failedResult.state.isInstanceOf[FailedState])

    val failedResultNextStage = failedResult.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val failedResultAgain = Await.result(failedResultNextStage, maxWaitTime)
    assertTrue("Should advance to FailedState due to AppendResult.FAILED as status is still ERROR", failedResultAgain.state.isInstanceOf[FailedState])

    val resultReturnToBeforeLeader = Await.result(failedResultAgain.transition(time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should advance back to BeforeLeader as correct epoch was never established in initial BeforeLeader", resultReturnToBeforeLeader.state.isInstanceOf[BeforeLeader])

    when(tierTopicManager.becomeArchiver(topicIdPartition, 1))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))

    val transitionToBeforeLeader = Await.result(failedResultAgain.transition(time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should correctly transition into BeforeUpload", transitionToBeforeLeader.state.isInstanceOf[BeforeUpload])
  }

  /**
   * Test that an archive task in Failed state with a restore at a higher epoch results in task cancellation
   */
  @Test
  def testFailedLeaderResultFailedRestoreNewerEpoch(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicIdPartition, 1))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FAILED))
    val log = mock(classOf[AbstractLog])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(log.tierPartitionState).thenReturn(tierPartitionState)
    when(tierPartitionState.committedEndOffset()).thenReturn(-1L: java.lang.Long)
    // tier epoch = 2 after recovery which should result in task cancellation
    when(tierPartitionState.tierEpoch).thenReturn(2)
    when(tierPartitionState.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(Long.MaxValue, Optional.empty()))
    when(tierPartitionState.status).thenReturn(TierPartitionStatus.CATCHUP)

    val task = ArchiveTask(ctx, topicIdPartition, 1, ArchiverMetrics(None, None))
    val nextStage = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val failedResult = Await.result(nextStage, maxWaitTime)
    assertTrue("Should advance to FailedState due to AppendResult.FAILED", failedResult.state.isInstanceOf[FailedState])
    val resultCancelled = Await.result(failedResult.transition(time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("We are the stale leader and the task should be cancelled", resultCancelled.ctx.isCancelled)
    assertTrue("We are the stale leader and the task should remain in FailedState", resultCancelled.state.isInstanceOf[FailedState])
  }

  /**
   * Test that an archive task in Failed state with a restore at a correct epoch results in transition to BeforeUpload
   */
  @Test
  def testFailedLeaderResultFailedRestoreEqualEpoch(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicIdPartition, 1))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FAILED))
    val log = mock(classOf[AbstractLog])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(log.tierPartitionState).thenReturn(tierPartitionState)
    when(tierPartitionState.committedEndOffset()).thenReturn(-1L: java.lang.Long)
    when(tierPartitionState.tierEpoch).thenReturn(1)
    when(tierPartitionState.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(Long.MaxValue, Optional.empty()))
    when(tierPartitionState.status).thenReturn(TierPartitionStatus.ONLINE)

    val task = ArchiveTask(ctx, topicIdPartition, 1, ArchiverMetrics(None, None))
    val nextStage = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val failedResult = Await.result(nextStage, maxWaitTime)
    assertTrue("Should advance to FailedState due to AppendResult.FAILED", failedResult.state.isInstanceOf[FailedState])
    val failedResult2 = Await.result(failedResult.transition(time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should advance back to BeforeUpload as epoch is correct", failedResult2.state.isInstanceOf[BeforeUpload])
  }

  /**
   * Test that an archive task in Failed state with a restore at a correct epoch results in transition to BeforeUpload
   */
  @Test
  def testFailedLeaderResultFailedRestoreLowerEpoch(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicIdPartition, 1))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FAILED))
    val log = mock(classOf[AbstractLog])
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(log.tierPartitionState).thenReturn(tierPartitionState)
    when(tierPartitionState.committedEndOffset()).thenReturn(-1L: java.lang.Long)
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierPartitionState.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(Long.MaxValue, Optional.empty()))
    when(tierPartitionState.status).thenReturn(TierPartitionStatus.ONLINE)

    val task = ArchiveTask(ctx, topicIdPartition, 1, ArchiverMetrics(None, None))
    val nextStage = task.transition(time, tierTopicManager, tierObjectStore, replicaManager)
    val failedResult = Await.result(nextStage, maxWaitTime)
    assertTrue("Should advance to FailedState due to AppendResult.FAILED", failedResult.state.isInstanceOf[FailedState])
    val failedResult2 = Await.result(failedResult.transition(time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should advance back to BeforeUpload as epoch is correct", failedResult2.state.isInstanceOf[BeforeLeader])
  }

  @Test(expected = classOf[TierArchiverFencedException])
  def testAwaitingLeaderResultFenced(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicIdPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FENCED))

    Await.result(ArchiveTask.establishLeadership(BeforeLeader(0), topicIdPartition, tierTopicManager), maxWaitTime)
  }

  /**
   * Test that an upload fenced by a restore message correctly results in a TierArchiverRestoreFenced exception being thrown
   */
  @Test(expected = classOf[TierArchiverRestoreFencedException])
  def testRestoreFencedException(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)
    val topicPartition = topicIdPartition.topicPartition
    val tierTopicManager = mock(classOf[TierTopicManager])
    val logDirFailureChannel = new LogDirFailureChannel(10)

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(-1L: java.lang.Long)
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierPartitionState.endOffset).thenReturn(0)
    when(tierPartitionState.segments(any(), any())).thenReturn(CloseableIterator.wrap(new util.ArrayList[TierObjectMetadata]().iterator()))
    when(tierPartitionState.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(Long.MaxValue, Optional.empty()))
    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.RESTORE_FENCED))

    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicIdPartition.topicPartition.toString).toFile
    when(tierPartitionStateFactory.initState(logDir, topicPartition, logConfig, logDirFailureChannel)).thenReturn(tierPartitionState)

    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
      0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs,
      Some(tierLogComponents), logDirFailureChannel)
    val partition = mock(classOf[Partition])
    when(partition.log).thenReturn(Some(log))
    when(partition.getIsUncleanLeader).thenReturn(false)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))
    when(replicaManager.getPartitionOrError(topicPartition, expectLeader = true)).thenReturn(Right(partition))
    log.appendAsFollower(TierTestUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.updateHighWatermark(log.logEndOffset)

    // should correctly throw TierArchiverRestoreFencedException
    Await.result(ArchiveTask.maybeInitiateUpload(BeforeUpload(0), topicIdPartition, time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
  }

  @Test(expected = classOf[TierArchiverFencedException])
  def testBeforeUploadFenced(): Unit = {
    val partition = mock(classOf[Partition])
    val log = mock(classOf[AbstractLog])

    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)
    val tierTopicManager = mock(classOf[TierTopicManager])

    when(partition.getIsUncleanLeader).thenReturn(false)
    when(partition.log).thenReturn(Some(log))

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(replicaManager.getPartitionOrError(topicIdPartition.topicPartition(), expectLeader = true)).thenReturn(Right(partition))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(-1L: java.lang.Long)
    when(tierPartitionState.tierEpoch).thenReturn(1)
    when(log.tierPartitionState).thenReturn(tierPartitionState)

    Await.result(ArchiveTask.maybeInitiateUpload(BeforeUpload(0), topicIdPartition, time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
  }

  @Test
  def testBeforeUploadRetryWhenNoSegment(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val partition = mock(classOf[Partition])

    val log = mock(classOf[AbstractLog])
    when(log.tierableLogSegments).thenReturn(List.empty[LogSegment])
    when(log.activeSegment).thenReturn(null)

    when(partition.getIsUncleanLeader).thenReturn(false)
    when(partition.log).thenReturn(Some(log))

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(replicaManager.getPartitionOrError(topicIdPartition.topicPartition(), expectLeader = true)).thenReturn(Right(partition))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(-1L: java.lang.Long)
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierPartitionState.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(Long.MaxValue, Optional.empty()))
    when(log.tierPartitionState).thenReturn(tierPartitionState)

    val result = Await.result(ArchiveTask.maybeInitiateUpload(BeforeUpload(0), topicIdPartition, time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should advance to BeforeUpload", result.isInstanceOf[BeforeUpload])
  }

  @Test
  def testBeforeUploadAdvancesToNextState(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)
    val topicPartition = topicIdPartition.topicPartition
    val tierTopicManager = mock(classOf[TierTopicManager])
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val partition = mock(classOf[Partition])

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(-1L: java.lang.Long)
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierPartitionState.endOffset).thenReturn(0)
    when(tierPartitionState.segments(any(), any())).thenReturn(CloseableIterator.wrap(new util.ArrayList[TierObjectMetadata]().iterator()))
    when(tierPartitionState.lastLocalMaterializedSrcOffsetAndEpoch).thenReturn(new OffsetAndEpoch(Long.MaxValue, Optional.empty()))
    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicIdPartition.topicPartition.toString).toFile
    when(tierPartitionStateFactory.initState(logDir, topicPartition, logConfig, logDirFailureChannel)).thenReturn(tierPartitionState)

    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
      0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs,
      Some(tierLogComponents), logDirFailureChannel)

    when(partition.log).thenReturn(Some(log))
    when(partition.getIsUncleanLeader).thenReturn(false)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))
    when(replicaManager.getPartitionOrError(topicPartition, expectLeader = true)).thenReturn(Right(partition))

    log.appendAsFollower(TierTestUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.updateHighWatermark(log.logEndOffset)

    val result = Await.result(ArchiveTask.maybeInitiateUpload(BeforeUpload(0), topicIdPartition, time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should advance to AfterUpload", result.isInstanceOf[Upload])
  }

  @Test
  def testBeforeUploadOverlappingSegment(): Unit = {
    val tierTopicManager = mock(classOf[TierTopicManager])
    val logConfig = LogTest.createLogConfig(segmentBytes =  1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = TestUtils.randomPartitionLogDir(TestUtils.tempDir())
    val topicPartition = Log.parseTopicPartitionName(logDir)
    val topicIdPartition = new TopicIdPartition(topicPartition.topic, UUID.randomUUID, topicPartition.partition)
    val logDirFailureChannel = new LogDirFailureChannel(10)
    val partition = mock(classOf[Partition])

    val tierPartitionState = createTierPartitionState(logDir, topicIdPartition, logDirFailureChannel, tieringEnabled = true)
    tierPartitionState.beginCatchup()
    tierPartitionState.onCatchUpComplete()

    when(tierPartitionStateFactory.mayEnableTiering(topicPartition, logConfig)).thenReturn(true)
    when(tierPartitionStateFactory.initState(logDir, topicIdPartition.topicPartition, logConfig, logDirFailureChannel)).thenReturn(tierPartitionState)

    val log = Log(logDir, logConfig, 0L, 0L, mockTime.scheduler, new BrokerTopicStats, mockTime, 60 * 60 * 1000,
      LogManager.ProducerIdExpirationCheckIntervalMs, logDirFailureChannel, Some(tierLogComponents))

    log.appendAsFollower(TierTestUtils.createRecords(50, topicIdPartition.topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(50, topicIdPartition.topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(50, topicIdPartition.topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(50, topicIdPartition.topicPartition, log.logEndOffset, 0))

    when(partition.getIsUncleanLeader).thenReturn(false)
    when(partition.log).thenReturn(Some(log))

    // overlaps with one of our segments
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition, 0, UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    TierTestUtils.uploadWithMetadata(tierPartitionState,
      topicIdPartition,
      0,
      UUID.randomUUID,
      0L,
      60L,
      15000L,
      15000L,
      1000)

    val newTierEpoch = 1
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition, newTierEpoch, UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch())
    log.updateHighWatermark(log.logEndOffset)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(replicaManager.getPartitionOrError(topicPartition, expectLeader = true)).thenReturn(Right(partition))
    when(tierTopicManager.addMetadata(any())).thenAnswer(new Answer[CompletableFuture[AppendResult]] {
      override def answer(invocation: InvocationOnMock): CompletableFuture[AppendResult] = {
        val metadata = invocation.getArgument(0).asInstanceOf[AbstractTierMetadata]
        CompletableFuture.completedFuture(tierPartitionState.append(metadata, TierTestUtils.nextTierTopicOffsetAndEpoch()))
      }
    })

    val result = Await.result(ArchiveTask.maybeInitiateUpload(BeforeUpload(newTierEpoch), topicIdPartition, time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should advance to Upload", result.isInstanceOf[Upload])
  }

  private def createTierPartitionState(dir: File,
                                       topicIdPartition: TopicIdPartition,
                                       logDirFailureChannel: LogDirFailureChannel,
                                       tieringEnabled: Boolean): TierPartitionState = {
    val tierPartitionState = new FileTierPartitionState(dir, logDirFailureChannel, topicIdPartition.topicPartition, tieringEnabled)
    tierPartitionState.setTopicId(topicIdPartition.topicId)
    tierPartitionStates :+= tierPartitionState
    tierPartitionState
  }
}
