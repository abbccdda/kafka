/*
 * Copyright 2019 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.{Collections, Optional, Properties, UUID}

import kafka.log.{AbstractLog, LogManager, LogSegment, LogTest, _}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel, ReplicaManager}
import kafka.server.KafkaConfig
import kafka.tier.archiver.TierArchiverState.{AfterUpload, BeforeLeader, BeforeUpload, Priority}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.exceptions.TierArchiverFencedException
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{MemoryTierPartitionStateFactory, TierPartitionState}
import kafka.tier.store.TierObjectStore.TierObjectStoreFileType
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStore, TierObjectStoreConfig}
import kafka.tier.{TierMetadataManager, TierTopicManager, TierUtils}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.mockito.Mockito.{mock, when}
import kafka.tier.archiver.JavaFunctionConversions._
import kafka.tier.domain.TierTopicInitLeader
import kafka.tier.state.MemoryTierPartitionState

import scala.collection.JavaConverters._

class TierArchiverStateTest {
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

  @Test
  def testAwaitingLeaderResult(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))

    val properties = new Properties()
    properties.put(KafkaConfig.TierEnableProp, "true")

    tierTopicManager.becomeReady()

    tierMetadataManager.initState(topicPartition, new File(logDirs.get(0)), new LogConfig(properties))
    tierMetadataManager.becomeLeader(topicPartition, 1)

    while(tierTopicManager.doWork()) {}

    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)

    val logConfig = LogTest.createLogConfig(segmentBytes = 2048 * 5, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicPartition.toString).toFile
    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
      0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs)
    val replicaManager = mock(classOf[ReplicaManager])

    val nextStage = BeforeLeader(replicaManager, tierTopicManager, tierObjectStore, topicPartition, 0, blockingTaskExecutor, TierArchiverConfig())
      .nextState()
      .handle { (state: TierArchiverState, ex: Throwable) =>
        assertTrue("Should advance to BeforeUpload", state.isInstanceOf[BeforeUpload])
        state
      }

    nextStage.get(100, TimeUnit.MILLISECONDS)
  }

  @Test
  def testAwaitingLeaderResultFenced(): Unit = {
    val replicaManager = mock(classOf[ReplicaManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val topicPartition = new TopicPartition("foo", 0)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.becomeArchiver(topicPartition, 0))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.FENCED))

    BeforeLeader(replicaManager, tierTopicManager, tierObjectStore, topicPartition, 0, blockingTaskExecutor, TierArchiverConfig())
      .nextState()
      .handle { (state: TierArchiverState, ex: Throwable) =>
        assertTrue("Should be fenced", ex.getCause.isInstanceOf[TierArchiverFencedException])
        assertEquals(state, null)
        null
      }.get(100, TimeUnit.MILLISECONDS)
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
    when(tierPartitionState.endOffset()).thenReturn(Optional.of(new java.lang.Long(1L)))

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.addMetadata(metadata))
      .thenReturn(CompletableFutureUtil.completed(AppendResult.ACCEPTED))

    val nextStage = AfterUpload(metadata, mock(classOf[LogSegment]), replicaManager, tierTopicManager, tierObjectStore, topicPartition, tierPartitionState, 0, blockingTaskExecutor, TierArchiverConfig()).nextState().handle { (result: TierArchiverState, ex: Throwable) =>
      assertTrue("Should advance to BeforeUpload", result.isInstanceOf[BeforeUpload])
      result
    }

    nextStage.get(100, TimeUnit.MILLISECONDS)
  }

  @Test
  def testBeforeUploadFenced(): Unit = {
    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(None)

    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.endOffset()).thenReturn(Optional.empty() : Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(1)

    val nextStage = BeforeUpload(replicaManager, tierTopicManager, tierObjectStore, topicPartition, tierPartitionState, 0, blockingTaskExecutor, TierArchiverConfig())
      .nextState()
      .handle { (state: TierArchiverState, ex: Throwable) =>
        assertTrue("Should be fenced", ex.isInstanceOf[TierArchiverFencedException])
        state
      }

    nextStage.get(100, TimeUnit.MILLISECONDS)
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
    when(tierPartitionState.endOffset()).thenReturn(Optional.empty() : Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(0)

    val nextStage = BeforeUpload(replicaManager, tierTopicManager, tierObjectStore, topicPartition, tierPartitionState, 0, blockingTaskExecutor, TierArchiverConfig()).nextState().handle { (result: TierArchiverState, ex: Throwable) =>
      assertTrue("Should advance to BeforeUpload", result.isInstanceOf[BeforeUpload])
      result
    }

    nextStage.get(2000, TimeUnit.MILLISECONDS)
  }

  @Test
  def testBeforeUploadAdvancesToNextState(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)

    val logConfig = LogTest.createLogConfig(segmentBytes =  150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicPartition.toString).toFile
    val log = LogTest.createLog(logDir, logConfig, new BrokerTopicStats, mockTime.scheduler, mockTime,
      0L, 0L, 60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs)

    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.endOffset()).thenReturn(Optional.empty() : Optional[java.lang.Long])
    when(tierPartitionState.tierEpoch).thenReturn(0)

    log.appendAsFollower(TierUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.appendAsFollower(TierUtils.createRecords(5, topicPartition, log.logEndOffset, 0))
    log.onHighWatermarkIncremented(log.logEndOffset)

    val nextStage = BeforeUpload(replicaManager, tierTopicManager, tierObjectStore, topicPartition, tierPartitionState,
      0, blockingTaskExecutor, TierArchiverConfig()).nextState().handle { (result: TierArchiverState, ex: Throwable) =>
      assertTrue("Should advance to AfterUpload", result.isInstanceOf[AfterUpload])
      result
    }

    nextStage.get(2000, TimeUnit.MILLISECONDS)
  }

  @Test
  def testBeforeUploadOverlappingSegment(): Unit = {
    val topicPartition = new TopicPartition("foo", 0)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val logConfig = LogTest.createLogConfig(segmentBytes =  1000, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val logDir = Paths.get(TestUtils.tempDir().getPath, topicPartition.toString).toFile

    val tierPartitionState = new MemoryTierPartitionState(logDir, topicPartition, true)
    tierPartitionState.beginCatchup()
    tierPartitionState.onCatchUpComplete()

    val tierMetadataManager = mock(classOf[TierMetadataManager])
    when(tierMetadataManager.initState(topicPartition, logDir, logConfig)).thenReturn(tierPartitionState)

    val log = Log(logDir, logConfig, 0L, 0L,  mockTime.scheduler, new BrokerTopicStats, mockTime, 60 * 60 * 1000,
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

    val nextStage = BeforeUpload(replicaManager, tierTopicManager, tierObjectStore, topicPartition, tierPartitionState,
      newTierEpoch, blockingTaskExecutor, TierArchiverConfig()).nextState().handle { (result: TierArchiverState, ex: Throwable) =>
      assertTrue("Should advance to AfterUpload", result.isInstanceOf[AfterUpload])
      result
    }

    nextStage.get(2000, TimeUnit.MILLISECONDS)
  }


  @Test
  def testArchiverStateRelativePriority(): Unit = {
    val tierTopicManager = mock(classOf[TierTopicManager])
    val tierObjectStore = new MockInMemoryTierObjectStore(objectStoreConfig)
    val topicPartition = new TopicPartition("foo", 0)
    val tierEpoch = 1
    val config = TierArchiverConfig()

    val beforeLeader = BeforeLeader(mockLogAtHighwatermark(100L, topicPartition), tierTopicManager, tierObjectStore, topicPartition, tierEpoch, blockingTaskExecutor, config)

    val beforeUploadA = BeforeUpload(mockLogAtHighwatermark(100L, topicPartition), tierTopicManager, tierObjectStore, topicPartition, mockTierPartitionStateAtEndOffset(0L), tierEpoch, blockingTaskExecutor, config)

    val beforeUploadB = BeforeUpload(mockLogAtHighwatermark(100L, topicPartition), tierTopicManager, tierObjectStore, topicPartition, mockTierPartitionStateAtEndOffset(0L), tierEpoch, blockingTaskExecutor, config)

    val beforeUploadC = BeforeUpload(mockLogAtHighwatermark(100L, topicPartition), tierTopicManager, tierObjectStore, topicPartition, mockTierPartitionStateAtEndOffset(900L), tierEpoch, blockingTaskExecutor, config)

    val afterUpload = AfterUpload(mock(classOf[TierObjectMetadata]), mock(classOf[LogSegment]), mockLogAtHighwatermark(100L, topicPartition), tierTopicManager, tierObjectStore, topicPartition, mockTierPartitionStateAtEndOffset(0L), tierEpoch, blockingTaskExecutor, config)

    assertEquals("BeforeLeader states have greater priority than AfterUpload states",
      Priority.Higher, beforeLeader.relativePriority(afterUpload))

    assertEquals("AfterUpload states have greater priority than BeforeUpload states",
      Priority.Higher, afterUpload.relativePriority(beforeUploadA))

    assertEquals("BeforeUpload states with equal lag have same priority",
      Priority.Same, beforeUploadA.relativePriority(beforeUploadB))

    assertEquals("BeforeUpload states with greater lag have higher priority",
      Priority.Higher, beforeUploadA.relativePriority(beforeUploadC))
  }

  private def mockLogAtHighwatermark(hwm: Long, topicPartition: TopicPartition): ReplicaManager = {
    val log = mock(classOf[AbstractLog])
    when(log.getHighWatermark).thenReturn(Some(hwm))
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))
    replicaManager
  }

  private def mockTierPartitionStateAtEndOffset(offset: Long): TierPartitionState = {
    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.endOffset()).thenReturn(Optional.of(offset) : Optional[java.lang.Long])
    tierPartitionState
  }

  private def isEqual(localFile: File,
                      tierObjectStore: TierObjectStore,
                      metadata: TierObjectMetadata,
                      fileType: TierObjectStoreFileType): Boolean = {
    val localStream = new FileInputStream(localFile.getPath)
    val tieredObject = tierObjectStore.getObject(metadata, fileType, 0, Integer.MAX_VALUE)

    val localData = ByteBuffer.allocate(localFile.length.toInt)
    val tieredData = ByteBuffer.allocate(tieredObject.getObjectSize.toInt)

    try {
      Utils.readFully(localStream, localData)
      Utils.readFully(tieredObject.getInputStream, tieredData)
    } finally {
      localStream.close()
      tieredObject.close()
    }
    localData.flip()
    tieredData.flip()

    localData.equals(tieredData)
  }
}
