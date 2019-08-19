/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.File
import java.nio.file.Paths
import java.util
import java.util.concurrent.{CompletableFuture, Executors, ScheduledExecutorService, TimeUnit}
import java.util.{Collections, Optional, Properties, UUID}

import com.yammer.metrics.core.Meter
import kafka.log.{AbstractLog, LogManager, LogSegment, LogTest, _}
import kafka.server.{BrokerTopicStats, KafkaConfig, LogDirFailureChannel, ReplicaManager}
import kafka.tier.domain.{AbstractTierMetadata, TierTopicInitLeader}
import kafka.tier.exceptions.TierArchiverFencedException
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{FileTierPartitionState, FileTierPartitionStateFactory, TierPartitionState}
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.tier.topic.TierTopicManager
import kafka.tier.{TierMetadataManager, TierTestUtils, TopicIdPartition}
import kafka.utils.{MockTime, TestUtils}
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
  val tierMetadataManager = new TierMetadataManager(new FileTierPartitionStateFactory(),
    Optional.of(new MockInMemoryTierObjectStore(objectStoreConfig)),
    new LogDirFailureChannel(1),
    true)
  val blockingTaskExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  val time = Time.SYSTEM

  val maxWaitTime = 30 seconds

  var byteRate: Meter = _

  @Before
  def setUp(): Unit = {
    ctx = CancellationContext.newContext()
    kafka.metrics.KafkaMetricsGroup.removeMetric("TierArchiver.UploadRate")
    byteRate = kafka.metrics.KafkaMetricsGroup.newMeter("TierArchiver.UploadRate", "bytes", TimeUnit.SECONDS)
  }

  @After
  def tearDown(): Unit = {
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

    tierMetadataManager.initState(topicIdPartition.topicPartition, new File(logDirs.get(0)), new LogConfig(properties))
    tierMetadataManager.becomeLeader(topicIdPartition.topicPartition(), 1)
    tierMetadataManager.ensureTopicIdPartition(topicIdPartition)

    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val replicaManager = mock(classOf[ReplicaManager])
    val task = ArchiveTask(ctx, topicIdPartition, 0)
    val nextStage = task.transition(time, tierTopicManager, tierObjectStore, replicaManager, Some(byteRate))
    val result = Await.result(nextStage, maxWaitTime)
    assertTrue("Should advance to BeforeUpload", result.state.isInstanceOf[BeforeUpload])
  }

  @Test(expected = classOf[TierArchiverFencedException])
  def testAwaitingLeaderResultFenced(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicIdPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FENCED))

    Await.result(ArchiveTask.establishLeadership(BeforeLeader(0), topicIdPartition, tierTopicManager), maxWaitTime)
  }

  @Test(expected = classOf[TierArchiverFencedException])
  def testBeforeUploadFenced(): Unit = {
    val log = mock(classOf[AbstractLog])

    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)
    val tierTopicManager = mock(classOf[TierTopicManager])

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(1)
    when(tierTopicManager.partitionState(topicIdPartition)).thenReturn(tierPartitionState)

    Await.result(ArchiveTask.maybeInitiateUpload(BeforeUpload(0), topicIdPartition, time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
  }

  @Test
  def testBeforeUploadRetryWhenNoSegment(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)

    val log = mock(classOf[AbstractLog])
    when(log.tierableLogSegments).thenReturn(List.empty[LogSegment])
    when(log.activeSegment).thenReturn(null)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierTopicManager.partitionState(topicIdPartition)).thenReturn(tierPartitionState)

    val result = Await.result(ArchiveTask.maybeInitiateUpload(BeforeUpload(0), topicIdPartition, time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should advance to BeforeUpload", result.isInstanceOf[BeforeUpload])
  }

  @Test
  def testBeforeUploadAdvancesToNextState(): Unit = {
    val topicIdPartition = new TopicIdPartition("foo", UUID.fromString("9808a113-1876-42fb-9396-6bc9baa0526b"), 0)
    val topicPartition = topicIdPartition.topicPartition
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)

    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicIdPartition.topicPartition.toString).toFile
    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
      0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierTopicManager.partitionState(topicIdPartition)).thenReturn(tierPartitionState)
    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

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
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val logConfig = LogTest.createLogConfig(segmentBytes =  1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = TestUtils.randomPartitionLogDir(TestUtils.tempDir())
    val topicPartition = Log.parseTopicPartitionName(logDir)
    val topicIdPartition = new TopicIdPartition(topicPartition.topic, UUID.randomUUID, topicPartition.partition)

    val tierPartitionState = new FileTierPartitionState(logDir, topicIdPartition.topicPartition, true)
    tierPartitionState.setTopicIdPartition(topicIdPartition)
    tierPartitionState.beginCatchup()
    tierPartitionState.onCatchUpComplete()
    when(tierTopicManager.partitionState(topicIdPartition)).thenReturn(tierPartitionState)

    val tierMetadataManager = mock(classOf[TierMetadataManager])
    when(tierMetadataManager.initState(topicIdPartition.topicPartition, logDir, logConfig)).thenReturn(tierPartitionState)

    val log = Log(logDir, logConfig, 0L, 0L, mockTime.scheduler, new BrokerTopicStats, mockTime, 60 * 60 * 1000,
      LogManager.ProducerIdExpirationCheckIntervalMs, new LogDirFailureChannel(10), Some(tierMetadataManager))

    log.appendAsFollower(TierTestUtils.createRecords(50, topicIdPartition.topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(50, topicIdPartition.topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(50, topicIdPartition.topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierTestUtils.createRecords(50, topicIdPartition.topicPartition, log.logEndOffset, 0))

    // overlaps with one of our segments
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition, 0, UUID.randomUUID(), 0))
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
    tierPartitionState.append(new TierTopicInitLeader(topicIdPartition, newTierEpoch, UUID.randomUUID(), 0))
    log.updateHighWatermark(log.logEndOffset)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicIdPartition.topicPartition)).thenReturn(Some(log))
    when(tierTopicManager.addMetadata(any())).thenAnswer(new Answer[CompletableFuture[AppendResult]] {
      override def answer(invocation: InvocationOnMock): CompletableFuture[AppendResult] = {
        val metadata = invocation.getArgument(0).asInstanceOf[AbstractTierMetadata]
        CompletableFuture.completedFuture(tierPartitionState.append(metadata))
      }
    })

    val result = Await.result(ArchiveTask.maybeInitiateUpload(BeforeUpload(newTierEpoch), topicIdPartition, time, tierTopicManager, tierObjectStore, replicaManager), maxWaitTime)
    assertTrue("Should advance to Upload", result.isInstanceOf[Upload])
  }
}
