/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.File
import java.nio.file.Paths
import java.util
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.{Collections, Optional, Properties, UUID}

import com.yammer.metrics.core.Meter
import kafka.log.{AbstractLog, LogManager, LogSegment, LogTest, _}
import kafka.server.{BrokerTopicStats, KafkaConfig, LogDirFailureChannel, ReplicaManager}
import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.exceptions.TierArchiverFencedException
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{MemoryTierPartitionState, MemoryTierPartitionStateFactory, TierPartitionState}
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.tier.{TierMetadataManager, TierTopicManager, TierUtils}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.junit.Assert.assertTrue
import org.junit.{After, Before, Test}
import org.mockito.Mockito.{mock, when}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class TierArchiverStateTest {
  var ctx: CancellationContext = _
  val mockTime = new MockTime()
  val tierTopicName = "__tier_topic"
  val tierTopicNumPartitions: Short = 1
  val logDirs = new util.ArrayList(Collections.singleton(System.getProperty("java.io.tmpdir")))
  val objectStoreConfig = new TierObjectStoreConfig()
  val tierMetadataManager = new TierMetadataManager(new MemoryTierPartitionStateFactory(),
    Some(new MockInMemoryTierObjectStore(objectStoreConfig)),
    new LogDirFailureChannel(1),
    true)
  val blockingTaskExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  val time = Time.SYSTEM

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
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))

    val properties = new Properties()
    properties.put(KafkaConfig.TierEnableProp, "true")

    tierTopicManager.becomeReady("fakebootstrap")

    tierMetadataManager.initState(topicPartition, new File(logDirs.get(0)), new LogConfig(properties))
    tierMetadataManager.becomeLeader(topicPartition, 1)

    while (tierTopicManager.doWork()) {}

    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val replicaManager = mock(classOf[ReplicaManager])
    val task = ArchiveTask(ctx, topicPartition, 0)
    val nextStage = task.transition(time, tierTopicManager, tierObjectStore, replicaManager, Some(byteRate))
    val result = Await.result(nextStage, 1 second)
    assertTrue("Should advance to BeforeUpload", result.state.isInstanceOf[BeforeUpload])
  }

  @Test(expected = classOf[TierArchiverFencedException])
  def testAwaitingLeaderResultFenced(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FENCED))

    Await.result(ArchiveTask.establishLeadership(BeforeLeader(0), topicPartition, tierTopicManager), 1 second)
  }

  @Test
  def testAwaitingUpload(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val metadata = new TierObjectMetadata(
      new TopicPartition("foo", 0),
      0,
      0L,
      1,
      1L,
      0,
      1,
      true,
      true,
      1.asInstanceOf[Byte]
    )
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(None)
    when(log.localLogSegments(0L, 0L)).thenReturn(List.empty[LogSegment])
    when(log.activeSegment).thenReturn(null)
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(Optional.of(new java.lang.Long(1L)))

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.addMetadata(metadata))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))
    val result = Await.result(ArchiveTask.finalizeUpload(AfterUpload(0, metadata, 0), topicPartition, time, tierTopicManager), 1 second)
    assertTrue("Should advance to BeforeUpload", result.isInstanceOf[BeforeUpload])
  }


  @Test(expected = classOf[TierArchiverFencedException])
  def testBeforeUploadFenced(): Unit = {
    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(None)

    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(1)
    when(tierTopicManager.partitionState(topicPartition)).thenReturn(tierPartitionState)

    Await.result(ArchiveTask.tierSegment(BeforeUpload(0), topicPartition, time, tierTopicManager, tierObjectStore, replicaManager, None), 1 second)
  }

  @Test
  def testBeforeUploadRetryWhenNoSegment(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)

    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(None)
    when(log.tierableLogSegments).thenReturn(List.empty[LogSegment])
    when(log.activeSegment).thenReturn(null)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierTopicManager.partitionState(topicPartition)).thenReturn(tierPartitionState)

    val result = Await.result(ArchiveTask.tierSegment(BeforeUpload(0), topicPartition, time, tierTopicManager, tierObjectStore, replicaManager, None), 1 second)
    assertTrue("Should advance to BeforeUpload", result.isInstanceOf[BeforeUpload])
  }


  @Test
  def testBeforeUploadAdvancesToNextState(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)

    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicPartition.toString).toFile
    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
      0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.committedEndOffset()).thenReturn(Optional.empty(): Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(0)
    when(tierTopicManager.partitionState(topicPartition)).thenReturn(tierPartitionState)

    log.appendAsFollower(TierUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.onHighWatermarkIncremented(log.logEndOffset)

    val result = Await.result(ArchiveTask.tierSegment(BeforeUpload(0), topicPartition, time, tierTopicManager, tierObjectStore, replicaManager, None), 1 second)
    assertTrue("Should advance to AfterUpload", result.isInstanceOf[AfterUpload])
  }


  @Test
  def testBeforeUploadOverlappingSegment(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val logConfig = LogTest.createLogConfig(segmentBytes = 1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicPartition.toString).toFile

    val tierPartitionState = new MemoryTierPartitionState(logDir, topicPartition, true)
    tierPartitionState.beginCatchup()
    tierPartitionState.onCatchUpComplete()
    when(tierTopicManager.partitionState(topicPartition)).thenReturn(tierPartitionState)

    val tierMetadataManager = mock(classOf[TierMetadataManager])
    when(tierMetadataManager.initState(topicPartition, logDir, logConfig)).thenReturn(tierPartitionState)

    val log = Log(logDir, logConfig, 0L, 0L, mockTime.scheduler, new BrokerTopicStats, mockTime, 60 * 60 * 1000,
      LogManager.ProducerIdExpirationCheckIntervalMs, new LogDirFailureChannel(10), Some(tierMetadataManager))

    log.appendAsFollower(TierUtils.createRecords(50, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierUtils.createRecords(50, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierUtils.createRecords(50, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierUtils.createRecords(50, topicPartition, log.logEndOffset, 0))

    // overlaps with one of our segments
    tierPartitionState.append(new TierTopicInitLeader(topicPartition, 0, UUID.randomUUID(), 0))
    tierPartitionState.append(new TierObjectMetadata(topicPartition, 0, 0L, 60, 50L, 1551311973419L, 1000, false, false, kafka.tier.serdes.State.AVAILABLE))

    val newTierEpoch = 1
    tierPartitionState.append(new TierTopicInitLeader(topicPartition, newTierEpoch, UUID.randomUUID(), 0))
    log.onHighWatermarkIncremented(log.logEndOffset)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    val result = Await.result(ArchiveTask.tierSegment(BeforeUpload(newTierEpoch), topicPartition, time, tierTopicManager, tierObjectStore, replicaManager, None), 1 second)
    assertTrue("Should advance to AfterUpload", result.isInstanceOf[AfterUpload])
  }
}
