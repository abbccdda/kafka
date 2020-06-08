/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.cluster

import java.io.File
import java.nio.ByteBuffer
import java.util.{Optional, Properties, UUID}
import java.util.concurrent.{CompletableFuture, CountDownLatch, Executors, Semaphore, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.yammer.metrics.core.Metric
import kafka.api.{ApiVersion, LeaderAndIsr}
import kafka.common.UnexpectedAppendOffsetException
import kafka.log.{Defaults => _, _}
import kafka.metrics.KafkaYammerMetrics
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.server.link._
import kafka.tier.{TierReplicaManager, TierTestUtils, TopicIdPartition}
import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.fetcher.TierStateFetcher
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{FileTierPartitionState, TierPartitionStateFactory}
import kafka.utils._
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ApiException, InvalidRequestException, OffsetNotAvailableException, ReplicaNotAvailableException}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.{FileTimestampAndOffset, TimestampAndOffset}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{EpochEndOffset, ListOffsetRequest}
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.mockito.Mockito.{atLeastOnce, doAnswer, doNothing, mock, spy, times, verify, when}
import org.scalatest.Assertions.assertThrows
import org.mockito.ArgumentMatchers
import org.mockito.invocation.InvocationOnMock
import org.scalatest.Assertions.intercept

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer

class PartitionTest {
  import PartitionTest._

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val tieredTopicPartition = new TopicPartition("tiered-test-topic", 0)
  val time = new MockTime()
  var tmpDir: File = _
  var logDir1: File = _
  var logDir2: File = _
  var logDir3: File = _
  var logManager: LogManager = _
  var tierEnabledLogManager: LogManager = _
  var logConfig: LogConfig = _
  var tieredLogConfig: LogConfig = _
  val stateStore: PartitionStateStore = mock(classOf[PartitionStateStore])
  val tieredPartitionStateStore: PartitionStateStore = mock(classOf[PartitionStateStore])
  val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
  val metadataCache: MetadataCache = mock(classOf[MetadataCache])
  val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])
  val tierReplicaManager = mock(classOf[TierReplicaManager])
  val tierLogComponents = TierLogComponents.EMPTY
  var partition: Partition = _
  var tieredPartition: Partition = _
  val executor = Executors.newCachedThreadPool()
  val clusterLinkId = UUID.randomUUID()

  @Before
  def setup(): Unit = {
    TestUtils.clearYammerMetrics()

    val logProps = createLogProperties(Map.empty)
    logConfig = LogConfig(logProps)
    val tieredLogProps = createLogProperties(Map(LogConfig.TierEnableProp -> "true"))
    tieredLogConfig = LogConfig(tieredLogProps)

    tmpDir = TestUtils.tempDir()
    logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logDir3 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, CleanerConfig(enableCleaner = false), time,
      tierLogComponents = TierLogComponents.EMPTY)
    logManager.startup()
    tierEnabledLogManager = TestUtils.createLogManager(
      logDirs = Seq(logDir3), defaultConfig = tieredLogConfig, CleanerConfig(enableCleaner = false), time,
      tierLogComponents = TierLogComponents(None, None, new TierPartitionStateFactory(true)))
    tierEnabledLogManager.startup()

    partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      stateStore,
      delayedOperations,
      metadataCache,
      logManager,
      Some(tierReplicaManager),
      None)

    tieredPartition = new Partition(
      tieredTopicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      tieredPartitionStateStore,
      delayedOperations,
      metadataCache,
      tierEnabledLogManager,
      Some(tierReplicaManager),
      None,
      Some(executor))

    when(stateStore.fetchTopicConfig()).thenReturn(createLogProperties(Map.empty))
    when(tieredPartitionStateStore.fetchTopicConfig()).thenReturn(tieredLogProps)
    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition)))
      .thenReturn(None)
    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(tieredTopicPartition)))
      .thenReturn(None)
  }

  private def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }

  @After
  def tearDown(): Unit = {
    executor.shutdownNow()
    logManager.shutdown()
    Utils.delete(tmpDir)
    TestUtils.clearYammerMetrics()
  }

  @Test
  def testMakeLeaderUpdatesEpochCache(): Unit = {
    val leaderEpoch = 8

    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    log.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)
    ), leaderEpoch = 0)
    log.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 5,
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)
    ), leaderEpoch = 5)
    assertEquals(4, log.logEndOffset)

    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true, log = log)
    assertEquals(Some(4), partition.leaderLogIfLocal.map(_.logEndOffset))

    val epochEndOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpoch = Optional.of[Integer](leaderEpoch),
      leaderEpoch = leaderEpoch, fetchOnlyFromLeader = true)
    assertEquals(4, epochEndOffset.endOffset)
    assertEquals(leaderEpoch, epochEndOffset.leaderEpoch)
  }

  @Test
  def testMakeLeaderDoesNotUpdateEpochCacheForOldFormats(): Unit = {
    val leaderEpoch = 8

    val logConfig = LogConfig(createLogProperties(Map(
      LogConfig.MessageFormatVersionProp -> kafka.api.KAFKA_0_10_2_IV0.shortVersion)))
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes)),
      magicValue = RecordVersion.V1.value
    ), leaderEpoch = 0)
    log.appendAsLeader(TestUtils.records(List(
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes)),
      magicValue = RecordVersion.V1.value
    ), leaderEpoch = 5)
    assertEquals(4, log.logEndOffset)

    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true, log = log)
    assertEquals(Some(4), partition.leaderLogIfLocal.map(_.logEndOffset))
    assertEquals(None, log.latestEpoch)

    val epochEndOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpoch = Optional.of[Integer](leaderEpoch),
      leaderEpoch = leaderEpoch, fetchOnlyFromLeader = true)
    assertEquals(EpochEndOffset.UNDEFINED_EPOCH_OFFSET, epochEndOffset.endOffset)
    assertEquals(EpochEndOffset.UNDEFINED_EPOCH, epochEndOffset.leaderEpoch)
  }

  @Test
  // Verify that partition.removeFutureLocalReplica() and partition.maybeReplaceCurrentWithFutureReplica() can run concurrently
  def testMaybeReplaceCurrentWithFutureReplica(): Unit = {
    val latch = new CountDownLatch(1)

    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    partition.maybeCreateFutureReplica(logDir2.getAbsolutePath, offsetCheckpoints)

    val thread1 = new Thread {
      override def run(): Unit = {
        latch.await()
        partition.removeFutureLocalReplica()
      }
    }

    val thread2 = new Thread {
      override def run(): Unit = {
        latch.await()
        partition.maybeReplaceCurrentWithFutureReplica()
      }
    }

    thread1.start()
    thread2.start()

    latch.countDown()
    thread1.join()
    thread2.join()
    assertEquals(None, partition.futureLog)
  }

  // Verify that partition.makeFollower() and partition.appendRecordsToFollowerOrFutureReplica() can run concurrently
  @Test
  def testMakeFollowerWithWithFollowerAppendRecords(): Unit = {
    val appendSemaphore = new Semaphore(0)

    partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      stateStore,
      delayedOperations,
      metadataCache,
      logManager,
      Some(tierReplicaManager),
      None) {

      override def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints): AbstractLog = {
        val log = super.createLog(isNew, isFutureReplica, offsetCheckpoints)
        new LogUtils.SlowAppendAsFollowerLog(log.asInstanceOf[MergedLog], tierLogComponents, appendSemaphore)
      }
    }

    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints)

    val appendThread = new Thread {
      override def run(): Unit = {
        val records = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
          new SimpleRecord("k2".getBytes, "v2".getBytes)),
          baseOffset = 0)
        partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)
      }
    }
    appendThread.start()
    TestUtils.waitUntilTrue(() => appendSemaphore.hasQueuedThreads, "follower log append is not called.")

    val partitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(0)
      .setLeader(2)
      .setLeaderEpoch(1)
      .setIsr(List[Integer](0, 1, 2, brokerId).asJava)
      .setZkVersion(1)
      .setReplicas(List[Integer](0, 1, 2, brokerId).asJava)
      .setIsNew(false)
    assertTrue(partition.makeFollower(partitionState, offsetCheckpoints))

    appendSemaphore.release()
    appendThread.join()

    assertEquals(2L, partition.localLogOrException.logEndOffset)
    assertEquals(2L, partition.leaderReplicaIdOpt.get)
  }

  @Test
  // Verify that replacement works when the replicas have the same log end offset but different base offsets in the
  // active segment
  def testMaybeReplaceCurrentWithFutureReplicaDifferentBaseOffsets(): Unit = {
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir1.getAbsolutePath)
    partition.createLogIfNotExists(isNew = true, isFutureReplica = false, offsetCheckpoints)
    logManager.maybeUpdatePreferredLogDir(topicPartition, logDir2.getAbsolutePath)
    partition.maybeCreateFutureReplica(logDir2.getAbsolutePath, offsetCheckpoints)

    // Write records with duplicate keys to current replica and roll at offset 6
    val currentLog = partition.log.get
    currentLog.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k1".getBytes, "v2".getBytes),
      new SimpleRecord("k1".getBytes, "v3".getBytes),
      new SimpleRecord("k2".getBytes, "v4".getBytes),
      new SimpleRecord("k2".getBytes, "v5".getBytes),
      new SimpleRecord("k2".getBytes, "v6".getBytes)
    ), leaderEpoch = 0)
    currentLog.roll()
    currentLog.appendAsLeader(MemoryRecords.withRecords(0L, CompressionType.NONE, 0,
      new SimpleRecord("k3".getBytes, "v7".getBytes),
      new SimpleRecord("k4".getBytes, "v8".getBytes)
    ), leaderEpoch = 0)

    // Write to the future replica as if the log had been compacted, and do not roll the segment

    val buffer = ByteBuffer.allocate(1024)
    val builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, 0)
    builder.appendWithOffset(2L, new SimpleRecord("k1".getBytes, "v3".getBytes))
    builder.appendWithOffset(5L, new SimpleRecord("k2".getBytes, "v6".getBytes))
    builder.appendWithOffset(6L, new SimpleRecord("k3".getBytes, "v7".getBytes))
    builder.appendWithOffset(7L, new SimpleRecord("k4".getBytes, "v8".getBytes))

    val futureLog = partition.futureLocalLogOrException
    futureLog.appendAsFollower(builder.build())

    assertTrue(partition.maybeReplaceCurrentWithFutureReplica())
  }

  @Test
  def testFetchOffsetSnapshotEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertSnapshotError(expectedError: Errors, currentLeaderEpoch: Optional[Integer]): Unit = {
      try {
        partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = true)
        assertEquals(Errors.NONE, expectedError)
      } catch {
        case error: ApiException => assertEquals(expectedError, Errors.forException(error))
      }
    }

    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
    assertSnapshotError(Errors.NONE, Optional.of(leaderEpoch))
    assertSnapshotError(Errors.NONE, Optional.empty())
  }

  @Test
  def testFetchOffsetSnapshotEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertSnapshotError(expectedError: Errors,
                            currentLeaderEpoch: Optional[Integer],
                            fetchOnlyLeader: Boolean): Unit = {
      try {
        partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = fetchOnlyLeader)
        assertEquals(Errors.NONE, expectedError)
      } catch {
        case error: ApiException => assertEquals(expectedError, Errors.forException(error))
      }
    }

    assertSnapshotError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertSnapshotError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertSnapshotError(Errors.NOT_LEADER_FOR_PARTITION, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertSnapshotError(Errors.NOT_LEADER_FOR_PARTITION, Optional.empty(), fetchOnlyLeader = true)
    assertSnapshotError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertSnapshotError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testOffsetForLeaderEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertLastOffsetForLeaderError(error: Errors, currentLeaderEpochOpt: Optional[Integer]): Unit = {
      val endOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpochOpt, 0,
        fetchOnlyFromLeader = true)
      assertEquals(error, endOffset.error)
    }

    assertLastOffsetForLeaderError(Errors.NONE, Optional.empty())
    assertLastOffsetForLeaderError(Errors.NONE, Optional.of(leaderEpoch))
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testOffsetForLeaderEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertLastOffsetForLeaderError(error: Errors,
                                       currentLeaderEpochOpt: Optional[Integer],
                                       fetchOnlyLeader: Boolean): Unit = {
      val endOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpochOpt, 0,
        fetchOnlyFromLeader = fetchOnlyLeader)
      assertEquals(error, endOffset.error)
    }

    assertLastOffsetForLeaderError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertLastOffsetForLeaderError(Errors.NOT_LEADER_FOR_PARTITION, Optional.empty(), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.NOT_LEADER_FOR_PARTITION, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertLastOffsetForLeaderError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testLastOffsetForLeaderEpochForMirrorLeader(): Unit = {
    verifyOffsetForLeaderEpochForLinkedLeader(Some(clusterLinkId), Some(TopicLinkMirror))
  }

  @Test
  def testLastOffsetForLeaderEpochForFailedMirrorLeader(): Unit = {
    verifyOffsetForLeaderEpochForNonLinkedLeader(Some(clusterLinkId), Some(TopicLinkFailedMirror))
  }

  @Test
  def testLastOffsetForLeaderEpochForStoppedMirrorLeader(): Unit = {
    verifyOffsetForLeaderEpochForNonLinkedLeader(Some(clusterLinkId), Some(TopicLinkStoppedMirror))
  }

  @Test
  def testLastOffsetForLeaderEpochForNonLinkedLeader(): Unit = {
    verifyOffsetForLeaderEpochForNonLinkedLeader(None, None)
  }

  @Test
  def testLastOffsetForLeaderEpochForMirrorFollower(): Unit = {
    verifyOffsetForLeaderEpochForFollower(Some(clusterLinkId), Some(TopicLinkMirror))
  }

  @Test
  def testLastOffsetForLeaderEpochForFailedMirrorFollower(): Unit = {
    verifyOffsetForLeaderEpochForFollower(Some(clusterLinkId), Some(TopicLinkFailedMirror))
  }

  @Test
  def testLastOffsetForLeaderEpochForStoppedMirrorFollower(): Unit = {
    verifyOffsetForLeaderEpochForFollower(Some(clusterLinkId), Some(TopicLinkStoppedMirror))
  }

  @Test
  def testLastOffsetForLeaderEpochForNonLinkedFollower(): Unit = {
    verifyOffsetForLeaderEpochForFollower(None, None)
  }

  private def assertLastOffsetForLeaderEpoch(currentLeaderEpochOpt: Optional[Integer],
                                             requestedEpoch: Int,
                                             expectedValue: EpochEndOffset,
                                             fetchOnlyFromLeader: Boolean = true): Unit = {
    val endOffset = partition.lastOffsetForLeaderEpoch(currentLeaderEpochOpt, requestedEpoch,
      fetchOnlyFromLeader)
    assertEquals(expectedValue, endOffset)
  }

  private def verifyOffsetForLeaderEpochForLinkedLeader(clusterLinkId: Option[UUID],
                                                        clusterLinkState: Option[TopicLinkState]): Unit = {
    val leaderEpoch = 5
    partition = setupPartitionWithMocks(leaderEpoch, isLeader = true,
      clusterLinkId = clusterLinkId, clusterLinkState = clusterLinkState)

    assertTrue(partition.getLinkedLeaderOffsetsPending)
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), 0, new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, -1, -1))
    partition.linkedLeaderOffsetsPending(false)

    assertLastOffsetForLeaderEpoch(Optional.empty(), 0, new EpochEndOffset(Errors.NONE, 0, 0))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), 0, new EpochEndOffset(Errors.NONE, 0, 0))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch - 1), 0, new EpochEndOffset(Errors.FENCED_LEADER_EPOCH, -1, -1))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch + 1), 0, new EpochEndOffset(Errors.UNKNOWN_LEADER_EPOCH, -1, -1))

    // Don't return offsets for linked partitions when source offsets are pending
    partition.linkedLeaderOffsetsPending(true)
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), 0, new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, -1, -1))
    partition.linkedLeaderOffsetsPending(false)
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), 0, new EpochEndOffset(Errors.NONE, 0, 0))

    // Return log offset if requested epoch is higher than any in the cache but within current leader epoch
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), leaderEpoch - 1, new EpochEndOffset(Errors.NONE, 0, 0))
    assertLastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch - 1, new EpochEndOffset(Errors.NONE, 0, 0))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), leaderEpoch, new EpochEndOffset(Errors.NONE, 0, 0))
    assertLastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, new EpochEndOffset(Errors.NONE, 0, 0))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), leaderEpoch + 1, new EpochEndOffset(Errors.NONE, -1, -1))
    assertLastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch + 1, new EpochEndOffset(Errors.NONE, -1, -1))
  }

  private def verifyOffsetForLeaderEpochForNonLinkedLeader(clusterLinkId: Option[UUID],
                                                           clusterLinkState: Option[TopicLinkState]): Unit = {
    val leaderEpoch = 5
    partition = setupPartitionWithMocks(leaderEpoch, isLeader = true,
      clusterLinkId = clusterLinkId, clusterLinkState = clusterLinkState)

    assertFalse(partition.getLinkedLeaderOffsetsPending)
    assertLastOffsetForLeaderEpoch(Optional.empty(), 0, new EpochEndOffset(Errors.NONE, 0, 0))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), 0, new EpochEndOffset(Errors.NONE, 0, 0))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch - 1), 0, new EpochEndOffset(Errors.FENCED_LEADER_EPOCH, -1, -1))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch + 1), 0, new EpochEndOffset(Errors.UNKNOWN_LEADER_EPOCH, -1, -1))

    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), leaderEpoch - 1, new EpochEndOffset(Errors.NONE, leaderEpoch - 1, 0))
    assertLastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch - 1, new EpochEndOffset(Errors.NONE, leaderEpoch - 1, 0))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), leaderEpoch, new EpochEndOffset(Errors.NONE, leaderEpoch, 0))
    assertLastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch, new EpochEndOffset(Errors.NONE, leaderEpoch, 0))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), leaderEpoch + 1, new EpochEndOffset(Errors.NONE, -1, -1))
    assertLastOffsetForLeaderEpoch(Optional.empty(), leaderEpoch + 1, new EpochEndOffset(Errors.NONE, -1, -1))
  }

  private def verifyOffsetForLeaderEpochForFollower(clusterLinkId: Option[UUID],
                                                    clusterLinkState: Option[TopicLinkState]): Unit = {
    val leaderEpoch = 5
    partition = setupPartitionWithMocks(leaderEpoch, isLeader = false,
      clusterLinkId = clusterLinkId, clusterLinkState = clusterLinkState)

    assertFalse(partition.getLinkedLeaderOffsetsPending)
    assertLastOffsetForLeaderEpoch(Optional.empty(), 0, new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, -1, -1))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), 0, new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, -1, -1))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch - 1), 0, new EpochEndOffset(Errors.FENCED_LEADER_EPOCH, -1, -1))
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch + 1), 0, new EpochEndOffset(Errors.UNKNOWN_LEADER_EPOCH, -1, -1))

    assertLastOffsetForLeaderEpoch(Optional.empty(), 0,
      new EpochEndOffset(Errors.NONE, -1, -1), fetchOnlyFromLeader = false)
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch), 0,
      new EpochEndOffset(Errors.NONE, -1, -1), fetchOnlyFromLeader = false)
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch - 1), 0,
      new EpochEndOffset(Errors.FENCED_LEADER_EPOCH, -1, -1), fetchOnlyFromLeader = false)
    assertLastOffsetForLeaderEpoch(Optional.of(leaderEpoch + 1), 0,
      new EpochEndOffset(Errors.UNKNOWN_LEADER_EPOCH, -1, -1), fetchOnlyFromLeader = false)

  }

  @Test
  def testReadRecordEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertReadRecordsError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer]): Unit = {
      try {
        partition.readRecords(0L, currentLeaderEpochOpt,
          maxBytes = 1024,
          fetchIsolation = FetchLogEnd,
          fetchOnlyFromLeader = true,
          minOneMessage = false,
          permitPreferredTierRead = false)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertReadRecordsError(Errors.NONE, Optional.empty())
    assertReadRecordsError(Errors.NONE, Optional.of(leaderEpoch))
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testReadRecordEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertReadRecordsError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer],
                               fetchOnlyFromLeader: Boolean): Unit = {
      try {
        partition.readRecords(0L, currentLeaderEpochOpt,
          maxBytes = 1024,
          fetchIsolation = FetchLogEnd,
          fetchOnlyFromLeader = fetchOnlyFromLeader,
          minOneMessage = false,
          permitPreferredTierRead = false)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertReadRecordsError(Errors.NONE, Optional.empty(), fetchOnlyFromLeader = false)
    assertReadRecordsError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyFromLeader = false)
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyFromLeader = false)
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyFromLeader = false)

    assertReadRecordsError(Errors.NOT_LEADER_FOR_PARTITION, Optional.empty(), fetchOnlyFromLeader = true)
    assertReadRecordsError(Errors.NOT_LEADER_FOR_PARTITION, Optional.of(leaderEpoch), fetchOnlyFromLeader = true)
    assertReadRecordsError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyFromLeader = true)
    assertReadRecordsError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyFromLeader = true)
  }

  @Test
  def testFetchOffsetForTimestampEpochValidationForLeader(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    def assertFetchOffsetError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer]): Unit = {
      try {
        partition.fetchOffsetForTimestamp(0L,
          isolationLevel = None,
          currentLeaderEpoch = currentLeaderEpochOpt,
          fetchOnlyFromLeader = true)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertFetchOffsetError(Errors.NONE, Optional.empty())
    assertFetchOffsetError(Errors.NONE, Optional.of(leaderEpoch))
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1))
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1))
  }

  @Test
  def testFetchOffsetForTimestampEpochValidationForFollower(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = false)

    def assertFetchOffsetError(error: Errors,
                               currentLeaderEpochOpt: Optional[Integer],
                               fetchOnlyLeader: Boolean): Unit = {
      try {
        partition.fetchOffsetForTimestamp(0L,
          isolationLevel = None,
          currentLeaderEpoch = currentLeaderEpochOpt,
          fetchOnlyFromLeader = fetchOnlyLeader)
        if (error != Errors.NONE)
          fail(s"Expected readRecords to fail with error $error")
      } catch {
        case e: Exception =>
          assertEquals(error, Errors.forException(e))
      }
    }

    assertFetchOffsetError(Errors.NONE, Optional.empty(), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.NONE, Optional.of(leaderEpoch), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = false)
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = false)

    assertFetchOffsetError(Errors.NOT_LEADER_FOR_PARTITION, Optional.empty(), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.NOT_LEADER_FOR_PARTITION, Optional.of(leaderEpoch), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.FENCED_LEADER_EPOCH, Optional.of(leaderEpoch - 1), fetchOnlyLeader = true)
    assertFetchOffsetError(Errors.UNKNOWN_LEADER_EPOCH, Optional.of(leaderEpoch + 1), fetchOnlyLeader = true)
  }

  @Test
  def testFetchLatestOffsetIncludesLeaderEpoch(): Unit = {
    val leaderEpoch = 5
    val partition = setupPartitionWithMocks(leaderEpoch, isLeader = true)

    val timestampAndOffsetOpt = partition.fetchOffsetForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP,
      isolationLevel = None,
      currentLeaderEpoch = Optional.empty(),
      fetchOnlyFromLeader = true)

    assertTrue(timestampAndOffsetOpt.isDefined)

    val timestampAndOffset = timestampAndOffsetOpt.get
    assertEquals(Optional.of(leaderEpoch), timestampAndOffset.leaderEpoch)
  }

  /**
    * This test checks that after a new leader election, we don't answer any ListOffsetsRequest until
    * the HW of the new leader has caught up to its startLogOffset for this epoch. From a client
    * perspective this helps guarantee monotonic offsets
    *
    * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-207%3A+Offsets+returned+by+ListOffsetsResponse+should+be+monotonically+increasing+even+during+a+partition+leader+change">KIP-207</a>
    */
  @Test
  def testMonotonicOffsetsAfterLeaderChange(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = List(leader, follower1, follower2)
    val isr = List[Integer](leader, follower2).asJava
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(
      new SimpleRecord(10, "k1".getBytes, "v1".getBytes),
      new SimpleRecord(11,"k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
      new SimpleRecord(20,"k4".getBytes, "v2".getBytes),
      new SimpleRecord(21,"k5".getBytes, "v3".getBytes)))

    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(true)

    assertTrue("Expected first makeLeader() to return 'leader changed'",
      partition.makeLeader(leaderState, offsetCheckpoints))
    assertEquals("Current leader epoch", leaderEpoch, partition.getLeaderEpoch)
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicaIds)

    // after makeLeader(() call, partition should know about all the replicas
    // append records with initial leader epoch
    partition.appendRecordsToLeader(batch1, origin = AppendOrigin.Client, requiredAcks = 0)
    partition.appendRecordsToLeader(batch2, origin = AppendOrigin.Client, requiredAcks = 0)
    assertEquals("Expected leader's HW not move", partition.localLogOrException.logStartOffset,
      partition.localLogOrException.highWatermark)

    // let the follower in ISR move leader's HW to move further but below LEO
    def updateFollowerFetchState(followerId: Int, fetchOffsetMetadata: LogOffsetMetadata): Unit = {
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = fetchOffsetMetadata,
        followerStartOffset = 0L,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = partition.localLogOrException.logEndOffset)
    }

    def fetchOffsetsForTimestamp(timestamp: Long, isolation: Option[IsolationLevel]): Either[ApiException, Option[TimestampAndOffset]] = {
      try {
        Right(partition.fetchOffsetForTimestamp(
          timestamp = timestamp,
          isolationLevel = isolation,
          currentLeaderEpoch = Optional.of(partition.getLeaderEpoch),
          fetchOnlyFromLeader = true
        ))
      } catch {
        case e: ApiException => Left(e)
      }
    }

    mockAliveBrokers(metadataCache, replicas)

    when(stateStore.expandIsr(controllerEpoch, new LeaderAndIsr(leader, leaderEpoch,
      List(leader, follower2, follower1), 1, false, None)))
      .thenReturn(Some(2))

    updateFollowerFetchState(follower1, LogOffsetMetadata(0))
    updateFollowerFetchState(follower1, LogOffsetMetadata(2))

    updateFollowerFetchState(follower2, LogOffsetMetadata(0))
    updateFollowerFetchState(follower2, LogOffsetMetadata(2))

    // At this point, the leader has gotten 5 writes, but followers have only fetched two
    assertEquals(2, partition.localLogOrException.highWatermark)

    // Get the LEO
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, None) match {
      case Right(Some(offsetAndTimestamp)) => assertOffset(5L, offsetAndTimestamp)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e) => fail("Should not have seen an error")
    }

    // Get the HW
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertOffset(2L, offsetAndTimestamp)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e) => fail("Should not have seen an error")
    }

    // Get a offset beyond the HW by timestamp, get a None
    assertEquals(Right(None), fetchOffsetsForTimestamp(30, Some(IsolationLevel.READ_UNCOMMITTED)))

    // Make into a follower
    val followerState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(follower2)
      .setLeaderEpoch(leaderEpoch + 1)
      .setIsr(isr)
      .setZkVersion(4)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(false)

    assertTrue(partition.makeFollower(followerState, offsetCheckpoints))

    // Back to leader, this resets the startLogOffset for this epoch (to 2), we're now in the fault condition
    val newLeaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch + 2)
      .setIsr(isr)
      .setZkVersion(5)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(false)

    assertTrue(partition.makeLeader(newLeaderState, offsetCheckpoints))

    // Try to get offsets as a client
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => fail("Should have failed with OffsetNotAvailable")
      case Right(None) => fail("Should have seen an error")
      case Left(e: OffsetNotAvailableException) => // ok
      case Left(e: ApiException) => fail(s"Expected OffsetNotAvailableException, got $e")
    }

    // If request is not from a client, we skip the check
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, None) match {
      case Right(Some(offsetAndTimestamp)) => assertOffset(5L, offsetAndTimestamp)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // If we request the earliest timestamp, we skip the check
    fetchOffsetsForTimestamp(ListOffsetRequest.EARLIEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertOffset(0L, offsetAndTimestamp)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // If we request an offset by timestamp earlier than the HW, we are ok
    fetchOffsetsForTimestamp(11, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) =>
        assertOffset(1L, offsetAndTimestamp)
        assertEquals(11, offsetAndTimestamp.timestamp)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // Request an offset by timestamp beyond the HW, get an error now since we're in a bad state
    fetchOffsetsForTimestamp(100, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => fail("Should have failed")
      case Right(None) => fail("Should have failed")
      case Left(e: OffsetNotAvailableException) => // ok
      case Left(e: ApiException) => fail(s"Should have seen OffsetNotAvailableException, saw $e")
    }

    when(stateStore.expandIsr(controllerEpoch, new LeaderAndIsr(leader, leaderEpoch + 2,
      List(leader, follower2, follower1), 5, false, None)))
      .thenReturn(Some(2))

    // Next fetch from replicas, HW is moved up to 5 (ahead of the LEO)
    updateFollowerFetchState(follower1, LogOffsetMetadata(5))
    updateFollowerFetchState(follower2, LogOffsetMetadata(5))

    // Error goes away
    fetchOffsetsForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP, Some(IsolationLevel.READ_UNCOMMITTED)) match {
      case Right(Some(offsetAndTimestamp)) => assertOffset(5L, offsetAndTimestamp)
      case Right(None) => fail("Should have seen some offsets")
      case Left(e: ApiException) => fail(s"Got ApiException $e")
    }

    // Now we see None instead of an error for out of range timestamp
    assertEquals(Right(None), fetchOffsetsForTimestamp(100, Some(IsolationLevel.READ_UNCOMMITTED)))
  }

  private def setupPartitionWithMocks(leaderEpoch: Int,
                                      isLeader: Boolean,
                                      log: AbstractLog = logManager.getOrCreateLog(topicPartition, () => logConfig),
                                      topicIdOpt: Option[UUID] = None,
                                      clusterLinkId: Option[UUID] = None,
                                      clusterLinkState: Option[TopicLinkState] = None): Partition = {
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)

    val controllerEpoch = 0
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicas

    if (isLeader) {
      val partitionState = new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas)
          .setIsNew(true)
          .setClusterLinkId(clusterLinkId.map(_.toString).orNull)
          .setClusterLinkTopicState(clusterLinkState.map(_.name).orNull)
      topicIdOpt.foreach { topicId => partitionState.setTopicId(topicId) }

      assertTrue("Expected become leader transition to succeed",
        partition.makeLeader(partitionState, offsetCheckpoints))
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
    } else {
      val partitionState = new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId + 1)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(1)
        .setReplicas(replicas)
        .setIsNew(true)
        .setClusterLinkId(clusterLinkId.map(_.toString).orNull)
        .setClusterLinkTopicState(clusterLinkState.map(_.name).orNull)
      topicIdOpt.foreach { topicId => partitionState.setTopicId(topicId) }

      assertTrue("Expected become follower transition to succeed",
        partition.makeFollower(partitionState, offsetCheckpoints))
      assertEquals(leaderEpoch, partition.getLeaderEpoch)
      assertEquals(None, partition.leaderLogIfLocal)
    }

    partition
  }

  @Test
  def testAppendRecordsAsFollowerOverlappingLogStartOffsetAndTieringEnabled(): Unit = {
    // 0. Ensure tiering is enabled in the topic config
    val tieringEnabledProps = createLogProperties(Map(LogConfig.TierEnableProp -> "true"))
    when(stateStore.fetchTopicConfig()).thenReturn(tieringEnabledProps)

    // 1. Create log and enable tiering
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)
    val log = partition.localLogOrException
    log.tierPartitionState.enableTierConfig()
    val tpid = new TopicIdPartition(topicPartition.topic(), UUID.randomUUID(), topicPartition.partition())
    assertTrue(log.tierPartitionState.setTopicId(tpid.topicId()))
    assertTrue(log.tierPartitionState.isTieringEnabled())

    // 2. Mark a segment to have been uploaded with uploadStartOffset = 50 and uploadEndOffset = 58
    assertTrue(!log.tierPartitionState.startOffset().isPresent())
    val leaderEpoch = 1000
    val uploadStartOffset: Long = 50
    val uploadEndOffset = 58
    assertEquals(AppendResult.ACCEPTED,
      log.tierPartitionState.append(new TierTopicInitLeader(tpid, leaderEpoch, UUID.randomUUID(), 0), TierTestUtils.nextTierTopicOffsetAndEpoch()))
    assertEquals(AppendResult.ACCEPTED,
      TierTestUtils.uploadWithMetadata(
        log.tierPartitionState,
        tpid,
        leaderEpoch,
        UUID.randomUUID(),
        uploadStartOffset,
        uploadEndOffset,
        validity = TierTestUtils.currentOffsetAndEpoch()))
    assertTrue(log.tierPartitionState.startOffset().isPresent)
    assertEquals(uploadStartOffset, log.tierPartitionState.startOffset().get())

    // 3. Truncate upto offset 59 (= uploadEndOffset + 1).
    // Ensure this call is not influenced by the first tiered offset that was set above.
    val initialLogStartOffset = uploadEndOffset + 1
    partition.truncateFullyAndStartAt(initialLogStartOffset, isFuture = false)
    assertEquals(s"Log end offset after truncate fully and start at $initialLogStartOffset:",
      initialLogStartOffset, log.logEndOffset)
    assertEquals(s"Log start offset after truncate fully and start at $initialLogStartOffset:",
      initialLogStartOffset, log.logStartOffset)

    // 4. Append a list of record with offsets: 58, 59, 60.
    // This should result in UnexpectedAppendOffsetException getting caught.
    // Subsequently, logStartOffset and logEndOffset should be reset suitably and the records
    // should get appended successfully.
    partition.appendRecordsToFollowerOrFutureReplica(
      createRecords(
        List(
          new SimpleRecord("k1".getBytes, "v1".getBytes),
          new SimpleRecord("k2".getBytes, "v2".getBytes),
          new SimpleRecord("k2".getBytes, "v2".getBytes)
        ), baseOffset = 58
      ), isFuture = false)
    assertEquals(s"Log start offset should change after successful append", 58, log.logStartOffset)
    assertEquals(s"Log end offset should change after successful append", 61, log.logEndOffset)
  }

  @Test
  def testAppendRecordsAsFollowerBelowLogStartOffset(): Unit = {
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)
    val log = partition.localLogOrException

    val initialLogStartOffset = 5L
    partition.truncateFullyAndStartAt(initialLogStartOffset, isFuture = false)
    assertEquals(s"Log end offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, log.logEndOffset)
    assertEquals(s"Log start offset after truncate fully and start at $initialLogStartOffset:",
                 initialLogStartOffset, log.logStartOffset)

    // verify that we cannot append records that do not contain log start offset even if the log is empty
    assertThrows[UnexpectedAppendOffsetException] {
      // append one record with offset = 3
      partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 3L), isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", initialLogStartOffset, log.logEndOffset)

    // verify that we can append records that contain log start offset, even when first
    // offset < log start offset if the log is empty
    val newLogStartOffset = 4L
    val records = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                     new SimpleRecord("k2".getBytes, "v2".getBytes),
                                     new SimpleRecord("k3".getBytes, "v3".getBytes)),
                                baseOffset = newLogStartOffset)
    partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)
    assertEquals(s"Log end offset after append of 3 records with base offset $newLogStartOffset:", 7L, log.logEndOffset)
    assertEquals(s"Log start offset after append of 3 records with base offset $newLogStartOffset:", newLogStartOffset, log.logStartOffset)

    // and we can append more records after that
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 7L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 7:", 8L, log.logEndOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, log.logStartOffset)

    // but we cannot append to offset < log start if the log is not empty
    assertThrows[UnexpectedAppendOffsetException] {
      val records2 = createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                        new SimpleRecord("k2".getBytes, "v2".getBytes)),
                                   baseOffset = 3L)
      partition.appendRecordsToFollowerOrFutureReplica(records2, isFuture = false)
    }
    assertEquals(s"Log end offset should not change after failure to append", 8L, log.logEndOffset)

    // we still can append to next offset
    partition.appendRecordsToFollowerOrFutureReplica(createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 8L), isFuture = false)
    assertEquals(s"Log end offset after append of 1 record at offset 8:", 9L, log.logEndOffset)
    assertEquals(s"Log start offset not expected to change:", newLogStartOffset, log.logStartOffset)
  }

  @Test
  def testListOffsetIsolationLevels(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicas

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)

    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(1)
        .setReplicas(replicas)
        .setIsNew(true), offsetCheckpoints))
    assertEquals(leaderEpoch, partition.getLeaderEpoch)

    val records = createTransactionalRecords(List(
      new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes)),
      baseOffset = 0L)
    partition.appendRecordsToLeader(records, origin = AppendOrigin.Client, requiredAcks = 0)

    def fetchLatestOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      val res = partition.fetchOffsetForTimestamp(ListOffsetRequest.LATEST_TIMESTAMP,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true)
      assertTrue(res.isDefined)
      res.get
    }

    def fetchEarliestOffset(isolationLevel: Option[IsolationLevel]): TimestampAndOffset = {
      val res = partition.fetchOffsetForTimestamp(ListOffsetRequest.EARLIEST_TIMESTAMP,
        isolationLevel = isolationLevel,
        currentLeaderEpoch = Optional.empty(),
        fetchOnlyFromLeader = true)
      assertTrue(res.isDefined)
      res.get
    }

    assertOffset(3L, fetchLatestOffset(isolationLevel = None))
    assertOffset(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)))
    assertOffset(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)))

    partition.log.get.updateHighWatermark(1L)

    assertOffset(3L, fetchLatestOffset(isolationLevel = None))
    assertOffset(1L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)))
    assertOffset(0L, fetchLatestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)))

    assertOffset(0L, fetchEarliestOffset(isolationLevel = None))
    assertOffset(0L, fetchEarliestOffset(isolationLevel = Some(IsolationLevel.READ_UNCOMMITTED)))
    assertOffset(0L, fetchEarliestOffset(isolationLevel = Some(IsolationLevel.READ_COMMITTED)))
  }

  @Test
  def testGetReplica(): Unit = {
    assertEquals(None, partition.log)
    assertThrows[ReplicaNotAvailableException] {
      partition.localLogOrException
    }
  }

  @Test
  def testAppendRecordsToFollowerWithNoReplicaThrowsException(): Unit = {
    assertThrows[ReplicaNotAvailableException] {
      partition.appendRecordsToFollowerOrFutureReplica(
           createRecords(List(new SimpleRecord("k1".getBytes, "v1".getBytes)), baseOffset = 0L), isFuture = false)
    }
  }

  @Test
  def testMakeFollowerWithNoLeaderIdChange(): Unit = {
    // Start off as follower
    var partitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(0)
      .setLeader(1)
      .setLeaderEpoch(1)
      .setIsr(List[Integer](0, 1, 2, brokerId).asJava)
      .setZkVersion(1)
      .setReplicas(List[Integer](0, 1, 2, brokerId).asJava)
      .setIsNew(false)
    partition.makeFollower(partitionState, offsetCheckpoints)

    // Request with same leader and epoch increases by only 1, do become-follower steps
    partitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(0)
      .setLeader(1)
      .setLeaderEpoch(4)
      .setIsr(List[Integer](0, 1, 2, brokerId).asJava)
      .setZkVersion(1)
      .setReplicas(List[Integer](0, 1, 2, brokerId).asJava)
      .setIsNew(false)
    assertTrue(partition.makeFollower(partitionState, offsetCheckpoints))

    // Request with same leader and same epoch, skip become-follower steps
    partitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(0)
      .setLeader(1)
      .setLeaderEpoch(4)
      .setIsr(List[Integer](0, 1, 2, brokerId).asJava)
      .setZkVersion(1)
      .setReplicas(List[Integer](0, 1, 2, brokerId).asJava)
    assertFalse(partition.makeFollower(partitionState, offsetCheckpoints))
  }

  @Test
  def testFollowerDoesNotJoinISRUntilCaughtUpToOffsetWithinCurrentLeaderEpoch(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = List[Integer](leader, follower1, follower2).asJava
    val isr = List[Integer](leader, follower2).asJava
    val leaderEpoch = 8
    val batch1 = TestUtils.records(records = List(new SimpleRecord("k1".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k2".getBytes, "v2".getBytes)))
    val batch2 = TestUtils.records(records = List(new SimpleRecord("k3".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k4".getBytes, "v2".getBytes),
                                                  new SimpleRecord("k5".getBytes, "v3".getBytes)))
    val batch3 = TestUtils.records(records = List(new SimpleRecord("k6".getBytes, "v1".getBytes),
                                                  new SimpleRecord("k7".getBytes, "v2".getBytes)))

    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true)
    assertTrue("Expected first makeLeader() to return 'leader changed'",
               partition.makeLeader(leaderState, offsetCheckpoints))
    assertEquals("Current leader epoch", leaderEpoch, partition.getLeaderEpoch)
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicaIds)

    // after makeLeader(() call, partition should know about all the replicas
    // append records with initial leader epoch
    val lastOffsetOfFirstBatch = partition.appendRecordsToLeader(batch1, origin = AppendOrigin.Client,
      requiredAcks = 0).lastOffset
    partition.appendRecordsToLeader(batch2, origin = AppendOrigin.Client, requiredAcks = 0)
    assertEquals("Expected leader's HW not move", partition.localLogOrException.logStartOffset,
      partition.log.get.highWatermark)

    // let the follower in ISR move leader's HW to move further but below LEO
    def updateFollowerFetchState(followerId: Int, fetchOffsetMetadata: LogOffsetMetadata): Unit = {
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = fetchOffsetMetadata,
        followerStartOffset = 0L,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = partition.localLogOrException.logEndOffset)
    }

    updateFollowerFetchState(follower2, LogOffsetMetadata(0))
    updateFollowerFetchState(follower2, LogOffsetMetadata(lastOffsetOfFirstBatch))
    assertEquals("Expected leader's HW", lastOffsetOfFirstBatch, partition.log.get.highWatermark)

    // current leader becomes follower and then leader again (without any new records appended)
    val followerState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(follower2)
      .setLeaderEpoch(leaderEpoch + 1)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    partition.makeFollower(followerState, offsetCheckpoints)

    val newLeaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch + 2)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    assertTrue("Expected makeLeader() to return 'leader changed' after makeFollower()",
               partition.makeLeader(newLeaderState, offsetCheckpoints))
    val currentLeaderEpochStartOffset = partition.localLogOrException.logEndOffset

    // append records with the latest leader epoch
    partition.appendRecordsToLeader(batch3, origin = AppendOrigin.Client, requiredAcks = 0)

    // fetch from follower not in ISR from log start offset should not add this follower to ISR
    updateFollowerFetchState(follower1, LogOffsetMetadata(0))
    updateFollowerFetchState(follower1, LogOffsetMetadata(lastOffsetOfFirstBatch))
    assertEquals("ISR", Set[Integer](leader, follower2), partition.inSyncReplicaIds)

    // fetch from the follower not in ISR from start offset of the current leader epoch should
    // add this follower to ISR
    when(stateStore.expandIsr(controllerEpoch, new LeaderAndIsr(leader, leaderEpoch + 2,
      List(leader, follower2, follower1), 1, false, None))).thenReturn(Some(2))
    updateFollowerFetchState(follower1, LogOffsetMetadata(currentLeaderEpochStartOffset))
    assertEquals("ISR", Set[Integer](leader, follower1, follower2), partition.inSyncReplicaIds)
  }

  /**
   * Verify that delayed fetch operations which are completed when records are appended don't result in deadlocks.
   * Delayed fetch operations acquire Partition leaderIsrUpdate read lock for one or more partitions. So they
   * need to be completed after releasing the lock acquired to append records. Otherwise, waiting writers
   * (e.g. to check if ISR needs to be shrinked) can trigger deadlock in request handler threads waiting for
   * read lock of one Partition while holding on to read lock of another Partition.
   */
  @Test
  def testDelayedFetchAfterAppendRecords(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicaIds = List[Integer](brokerId, brokerId + 1).asJava
    val isr = replicaIds
    val logConfig = LogConfig(new Properties)

    val topicPartitions = (0 until 5).map { i => new TopicPartition("test-topic", i) }
    val logs = topicPartitions.map { tp => logManager.getOrCreateLog(tp, () => logConfig) }
    val partitions = ListBuffer.empty[Partition]

    logs.foreach { log =>
      val tp = log.topicPartition
      val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
      val partition = new Partition(tp,
        replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
        interBrokerProtocolVersion = ApiVersion.latestVersion,
        localBrokerId = brokerId,
        time,
        stateStore,
        delayedOperations,
        metadataCache,
        logManager,
        Some(mock(classOf[TierReplicaManager])),
        None)

      when(delayedOperations.checkAndCompleteFetch())
        .thenAnswer((invocation: InvocationOnMock) => {
          // Acquire leaderIsrUpdate read lock of a different partition when completing delayed fetch
          val anotherPartition = (tp.partition + 1) % topicPartitions.size
          val partition = partitions(anotherPartition)
          partition.fetchOffsetSnapshot(Optional.of(leaderEpoch), fetchOnlyFromLeader = true)
        })

      partition.setLog(log, isFutureLog = false)
      val leaderState = new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(1)
        .setReplicas(replicaIds)
        .setIsNew(true)
      partition.makeLeader(leaderState, offsetCheckpoints)
      partitions += partition
    }

    def createRecords(baseOffset: Long): MemoryRecords = {
      val records = List(
        new SimpleRecord("k1".getBytes, "v1".getBytes),
        new SimpleRecord("k2".getBytes, "v2".getBytes))
      val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
      val builder = MemoryRecords.builder(
        buf, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME,
        baseOffset, time.milliseconds, 0)
      records.foreach(builder.append)
      builder.build()
    }

    val done = new AtomicBoolean()
    val executor = Executors.newFixedThreadPool(topicPartitions.size + 1)
    try {
      // Invoke some operation that acquires leaderIsrUpdate write lock on one thread
      executor.submit((() => {
        while (!done.get) {
          partitions.foreach(_.maybeShrinkIsr())
        }
      }): Runnable)
      // Append records to partitions, one partition-per-thread
      val futures = partitions.map { partition =>
        executor.submit((() => {
          (1 to 10000).foreach { _ =>
            partition.appendRecordsToLeader(createRecords(baseOffset = 0),
              origin = AppendOrigin.Client,
              requiredAcks = 0)
          }
        }): Runnable)
      }
      futures.foreach(_.get(15, TimeUnit.SECONDS))
      done.set(true)
    } catch {
      case e: TimeoutException =>
        val allThreads = TestUtils.allThreadStackTraces()
        fail(s"Test timed out with exception $e, thread stack traces: $allThreads")
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  private def assertOffset(expected: Long, timestampAndOffset: TimestampAndOffset): Unit = {
    assertTrue(timestampAndOffset.isInstanceOf[FileTimestampAndOffset])
    assertEquals(expected, timestampAndOffset.asInstanceOf[FileTimestampAndOffset].offset)
  }

  def createRecords(records: Iterable[SimpleRecord], baseOffset: Long, partitionLeaderEpoch: Int = 0, timestampType: TimestampType = TimestampType.LOG_APPEND_TIME): MemoryRecords = {
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(
      buf, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, timestampType,
      baseOffset, time.milliseconds, partitionLeaderEpoch)
    records.foreach(builder.append)
    builder.build()
  }

  def createTransactionalRecords(records: Iterable[SimpleRecord],
                                 baseOffset: Long): MemoryRecords = {
    val producerId = 1L
    val producerEpoch = 0.toShort
    val baseSequence = 0
    val isTransactional = true
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(buf, CompressionType.NONE, baseOffset, producerId,
      producerEpoch, baseSequence, isTransactional)
    records.foreach(builder.append)
    builder.build()
  }

  /**
    * Test for AtMinIsr partition state. We set the partition replica set size as 3, but only set one replica as an ISR.
    * As the default minIsr configuration is 1, then the partition should be at min ISR (isAtMinIsr = true).
    */
  @Test
  def testAtMinIsr(): Unit = {
    val controllerEpoch = 3
    val leader = brokerId
    val follower1 = brokerId + 1
    val follower2 = brokerId + 2
    val replicas = List[Integer](leader, follower1, follower2).asJava
    val isr = List[Integer](leader).asJava
    val leaderEpoch = 8

    assertFalse(partition.isAtMinIsr)
    // Make isr set to only have leader to trigger AtMinIsr (default min isr config is 1)
    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true)
    partition.makeLeader(leaderState, offsetCheckpoints)
    assertTrue(partition.isAtMinIsr)
  }

  @Test
  def testUpdateFollowerFetchState(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    seedLogData(log, numRecords = 6, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List[Integer](brokerId, remoteBrokerId).asJava
    val isr = replicas

    doNothing().when(delayedOperations).checkAndCompleteFetch()

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)

    val initializeTimeMs = time.milliseconds()
    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas)
          .setIsNew(true),
        offsetCheckpoints))

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    time.sleep(partition.replicaLagTimeMaxMs / 2)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(3),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(3L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    time.sleep(partition.replicaLagTimeMaxMs / 2)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(6L),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(time.milliseconds(), remoteReplica.lastCaughtUpTimeMs)
    assertEquals(6L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

  }

  @Test
  def testIsrExpansion(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()
    mockAliveBrokers(metadataCache, replicas)

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Int.box).asJava)
          .setIsNew(true),
        offsetCheckpoints)
    )
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(3),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(3L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // The next update should bring the follower back into the ISR
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId, remoteBrokerId),
      zkVersion = 1,
      isUnclean = false,
      None)
    when(stateStore.expandIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(Some(2))

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)

    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)
  }

  @Test
  def testIsrNotExpandedIfUpdateFails(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()
    mockAliveBrokers(metadataCache, replicas)

    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Integer.valueOf).asJava)
          .setIsNew(true),
        offsetCheckpoints))
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // Mock the expected ISR update failure
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId, remoteBrokerId),
      zkVersion = 1,
      isUnclean = false,
      None)
    when(stateStore.expandIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(None)

    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10L)

    // Follower state is updated, but the ISR has not expanded
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)
  }

  @Test
  def testMaybeShrinkIsr(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId, remoteBrokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()
    mockAliveBrokers(metadataCache, replicas)

    val initializeTimeMs = time.milliseconds()
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Int.box).asJava)
          .setIsNew(true),
        offsetCheckpoints)
    )
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // On initialization, the replica is considered caught up and should not be removed
    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)

    // If enough time passes without a fetch update, the ISR should shrink
    time.sleep(partition.replicaLagTimeMaxMs + 1)
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId),
      zkVersion = 1,
      isUnclean = false,
      None)
    when(stateStore.shrinkIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(Some(2))

    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId), partition.inSyncReplicaIds)
    assertEquals(10L, partition.localLogOrException.highWatermark)
  }

  @Test
  def testShouldNotShrinkIsrIfPreviousFetchIsCaughtUp(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId, remoteBrokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()
    mockAliveBrokers(metadataCache, replicas)

    val initializeTimeMs = time.milliseconds()
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Int.box).asJava)
          .setIsNew(true),
        offsetCheckpoints)
    )
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // There is a short delay before the first fetch. The follower is not yet caught up to the log end.
    time.sleep(5000)
    val firstFetchTimeMs = time.milliseconds()
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(5),
      followerStartOffset = 0L,
      followerFetchTimeMs = firstFetchTimeMs,
      leaderEndOffset = 10L)
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(5L, partition.localLogOrException.highWatermark)
    assertEquals(5L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Some new data is appended, but the follower catches up to the old end offset.
    // The total elapsed time from initialization is larger than the max allowed replica lag.
    time.sleep(5001)
    seedLogData(log, numRecords = 5, leaderEpoch = leaderEpoch)
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 15L)
    assertEquals(firstFetchTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(10L, partition.localLogOrException.highWatermark)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // The ISR should not be shrunk because the follower has caught up with the leader at the
    // time of the first fetch.
    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
  }

  @Test
  def testShouldNotShrinkIsrIfFollowerCaughtUpToLogEnd(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId, remoteBrokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()
    mockAliveBrokers(metadataCache, replicas)

    val initializeTimeMs = time.milliseconds()
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Int.box).asJava)
          .setIsNew(true),
        offsetCheckpoints)
    )
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // The follower catches up to the log end immediately.
    partition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 10L)
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(10L, partition.localLogOrException.highWatermark)
    assertEquals(10L, remoteReplica.logEndOffset)
    assertEquals(0L, remoteReplica.logStartOffset)

    // Sleep longer than the max allowed follower lag
    time.sleep(partition.replicaLagTimeMaxMs + 1)

    // The ISR should not be shrunk because the follower is caught up to the leader's log end
    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
  }

  @Test
  def testIsrNotShrunkIfUpdateFails(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId, remoteBrokerId).asJava

    doNothing().when(delayedOperations).checkAndCompleteFetch()
    mockAliveBrokers(metadataCache, replicas)

    val initializeTimeMs = time.milliseconds()
    partition.createLogIfNotExists(isNew = false, isFutureReplica = false, offsetCheckpoints)
    assertTrue("Expected become leader transition to succeed",
      partition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Integer.valueOf).asJava)
          .setIsNew(true),
        offsetCheckpoints))
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)

    val remoteReplica = partition.getReplica(remoteBrokerId).get
    assertEquals(initializeTimeMs, remoteReplica.lastCaughtUpTimeMs)
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    time.sleep(partition.replicaLagTimeMaxMs + 1)

    // Mock the expected ISR update failure
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId),
      zkVersion = 1,
      isUnclean = false,
      None)
    when(stateStore.shrinkIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(None)

    partition.maybeShrinkIsr()
    assertEquals(Set(brokerId, remoteBrokerId), partition.inSyncReplicaIds)
    assertEquals(0L, partition.localLogOrException.highWatermark)
  }

  @Test
  def testUseCheckpointToInitializeHighWatermark(): Unit = {
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    seedLogData(log, numRecords = 6, leaderEpoch = 5)

    when(offsetCheckpoints.fetch(logDir1.getAbsolutePath, topicPartition))
      .thenReturn(Some(4L))

    val controllerEpoch = 3
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(6)
      .setIsr(replicas)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    partition.makeLeader(leaderState, offsetCheckpoints)
    assertEquals(4, partition.localLogOrException.highWatermark)
  }

  @Test
  def testAddAndRemoveMetrics(): Unit = {
    val metricsToCheck = List(
      "UnderReplicated",
      "UnderMinIsr",
      "InSyncReplicasCount",
      "CaughtUpReplicasCount",
      "ReplicasCount",
      "LastStableOffsetLag",
      "AtMinIsr",
      "IsNotCaughtUp",
      "ObserverReplicasCount"
    )

    def getMetric(metric: String): Option[Metric] = {
      KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.filter { case (metricName, _) =>
        metricName.getName == metric && metricName.getType == "Partition"
      }.headOption.map(_._2)
    }

    assertTrue(metricsToCheck.forall(getMetric(_).isDefined))

    Partition.removeMetrics(topicPartition)
    Partition.removeMetrics(tieredTopicPartition)

    assertEquals(Set(), KafkaYammerMetrics.defaultRegistry().allMetrics().asScala.keySet.filter(_.getType == "Partition"))
  }

  @Test
  def testUnderReplicatedPartitionsCorrectSemantics(): Unit = {
    val controllerEpoch = 3
    val replicas = List[Integer](brokerId, brokerId + 1, brokerId + 2).asJava
    val isr = List[Integer](brokerId, brokerId + 1).asJava

    var leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(6)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(false)
    partition.makeLeader(leaderState, offsetCheckpoints)
    assertTrue(partition.isUnderReplicated)

    leaderState = leaderState.setIsr(replicas)
    partition.makeLeader(leaderState, offsetCheckpoints)
    assertFalse(partition.isUnderReplicated)
  }

  @Test
  def testUpdateAssignmentAndIsr(): Unit = {
    val topicPartition = new TopicPartition("test", 1)
    val partition = new Partition(
      topicPartition, 1000, ApiVersion.latestVersion, 0,
      Time.SYSTEM, mock(classOf[PartitionStateStore]), mock(classOf[DelayedOperations]),
      mock(classOf[MetadataCache]), mock(classOf[LogManager]), None, None)

    val replicas = Seq(0, 1, 2, 3)
    val isr = Set(0, 1, 2, 3)
    val adding = Seq(4, 5)
    val removing = Seq(1, 2)

    // Test with ongoing reassignment
    partition.updateAssignmentAndIsr(replicas, isr, adding, removing, Set.empty, None)

    assertTrue("The assignmentState is not OngoingReassignmentState",
      partition.assignmentState.isInstanceOf[OngoingReassignmentState])
    assertEquals(replicas, partition.assignmentState.replicas)
    assertEquals(isr, partition.inSyncReplicaIds)
    assertEquals(adding, partition.assignmentState.asInstanceOf[OngoingReassignmentState].addingReplicas)
    assertEquals(removing, partition.assignmentState.asInstanceOf[OngoingReassignmentState].removingReplicas)
    assertEquals(Seq(1, 2, 3), partition.remoteReplicas.map(_.brokerId))

    // Test with simple assignment
    val replicas2 = Seq(0, 3, 4, 5)
    val isr2 = Set(0, 3, 4, 5)
    partition.updateAssignmentAndIsr(replicas2, isr2, Seq.empty, Seq.empty, Set.empty, None)

    assertTrue("The assignmentState is not SimpleAssignmentState",
      partition.assignmentState.isInstanceOf[SimpleAssignmentState])
    assertEquals(replicas2, partition.assignmentState.replicas)
    assertEquals(isr2, partition.inSyncReplicaIds)
    assertEquals(Seq(3, 4, 5), partition.remoteReplicas.map(_.brokerId))
  }

  /**
   * Test when log is getting initialized, its config remains untouched after initialization is done.
   */
  @Test
  def testLogConfigNotDirty(): Unit = {
    val spyLogManager = spy(logManager)
    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      stateStore,
      delayedOperations,
      metadataCache,
      spyLogManager,
      tierReplicaManagerOpt = None,
      tierStateFetcherOpt = None)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()) // This doesn't get evaluated, but needed to satisfy compilation

    // We should get config from ZK only once
    verify(stateStore).fetchTopicConfig()
  }

  /**
   * Test when log is getting initialized, its config remains gets reloaded if Topic config gets changed
   * before initialization is done.
   */
  @Test
  def testLogConfigDirtyAsTopicUpdated(): Unit = {
    val spyLogManager = spy(logManager)
    doAnswer((invocation: InvocationOnMock) => {
      logManager.initializingLog(topicPartition)
      logManager.topicConfigUpdated(topicPartition.topic())
    }).when(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))

    val partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      stateStore,
      delayedOperations,
      metadataCache,
      spyLogManager,
      tierReplicaManagerOpt = None,
      tierStateFetcherOpt = None)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()) // This doesn't get evaluated, but needed to satisfy compilation

    // We should get config from ZK twice, once before log is created, and second time once
    // we find log config is dirty and refresh it.
    verify(stateStore, times(2)).fetchTopicConfig()
  }

  /**
   * Test when log is getting initialized, its config remains gets reloaded if Broker config gets changed
   * before initialization is done.
   */
  @Test
  def testLogConfigDirtyAsBrokerUpdated(): Unit = {
    val spyLogManager = spy(logManager)
    doAnswer((invocation: InvocationOnMock) => {
      logManager.initializingLog(topicPartition)
      logManager.brokerConfigUpdated()
    }).when(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))

    val partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      stateStore,
      delayedOperations,
      metadataCache,
      spyLogManager,
      tierReplicaManagerOpt = None,
      tierStateFetcherOpt = None)

    partition.createLog(isNew = true, isFutureReplica = false, offsetCheckpoints)

    // Validate that initializingLog and finishedInitializingLog was called
    verify(spyLogManager).initializingLog(ArgumentMatchers.eq(topicPartition))
    verify(spyLogManager).finishedInitializingLog(ArgumentMatchers.eq(topicPartition),
      ArgumentMatchers.any(),
      ArgumentMatchers.any()) // This doesn't get evaluated, but needed to satisfy compilation

    // We should get config from ZK twice, once before log is created, and second time once
    // we find log config is dirty and refresh it.
    verify(stateStore, times(2)).fetchTopicConfig()
  }

  @Test
  def testMakeLeaderWithTopicId(): Unit = {
    var leaderEpoch = 7
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    val tierPartitionState = log.tierPartitionState
    val topicId = UUID.randomUUID

    val partition = setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = true, log = log, topicIdOpt = Some(topicId))
    val replicas = partition.assignmentState.replicas.map(Int.box).asJava

    // assert topic id was propagated
    assertTrue(tierPartitionState.topicIdPartition.isPresent)
    verify(tierReplicaManager, times(1)).becomeLeader(tierPartitionState, leaderEpoch)

    // make leader with a new epoch
    leaderEpoch = 8
    partition.makeLeader(
      new LeaderAndIsrPartitionState()
        .setControllerEpoch(0)
        .setLeader(1)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(replicas)
        .setReplicas(replicas)
        .setZkVersion(1)
        .setIsNew(true),
      offsetCheckpoints)
    assertTrue(tierPartitionState.topicIdPartition.isPresent)
    verify(tierReplicaManager, times(1)).becomeLeader(tierPartitionState, leaderEpoch)
  }

  @Test
  def testMakeFollowerWithTopicId(): Unit = {
    val leaderEpoch = 7
    val log = logManager.getOrCreateLog(topicPartition, () => logConfig)
    val tierPartitionState = log.tierPartitionState
    val topicId = UUID.randomUUID

    setupPartitionWithMocks(leaderEpoch = leaderEpoch, isLeader = false, log = log, topicIdOpt = Some(topicId))

    // assert topic id was propagated
    assertTrue(tierPartitionState.topicIdPartition.isPresent)
    verify(tierReplicaManager, times(1)).becomeFollower(tierPartitionState)
  }

  @Test
  def testShouldRemoveObserversFromIsr(): Unit = {
    val zkVersion = 23934
    val controllerEpoch = 137
    val leaderEpoch = 245
    val syncReplicaId1 = brokerId
    val syncReplicaId2 = brokerId + 1
    val observerId1 = brokerId + 2
    val observerId2 = brokerId + 3

    val leaderAndIsrUpdate = new LeaderAndIsrPartitionState()
      .setLeader(syncReplicaId1)
      .setLeaderEpoch(leaderEpoch)
      .setReplicas(List[Integer](syncReplicaId1, syncReplicaId2, observerId1, observerId2).asJava)
      .setControllerEpoch(controllerEpoch)
      .setObservers(List[Integer](observerId1, observerId2).asJava)
      .setTopicName(topicPartition.topic)
      .setIsr(List[Integer](syncReplicaId1, syncReplicaId2, observerId1).asJava)
      .setZkVersion(zkVersion)
      .setPartitionIndex(0)

    assertTrue(partition.makeLeader(partitionState = leaderAndIsrUpdate,
      highWatermarkCheckpoints = offsetCheckpoints))

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(syncReplicaId1, syncReplicaId2, observerId1), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertFalse(partition.isUnderReplicated)

    when(stateStore.shrinkIsr(controllerEpoch, LeaderAndIsr(brokerId, leaderEpoch,
      List(syncReplicaId1, syncReplicaId2), zkVersion, isUnclean = false, None)))
      .thenReturn(Some(zkVersion + 1))

    partition.maybeShrinkIsr()

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(syncReplicaId1, syncReplicaId2), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertFalse(partition.isUnderReplicated)
  }

  @Test
  def testShouldNotRemoveObserverFromIsrIfThatCausesUnderMinIsr(): Unit = {
    val logConfig = LogConfig(createLogProperties(Map(LogConfig.MinInSyncReplicasProp -> "2")))
    logManager.getOrCreateLog(topicPartition, () => logConfig)

    val zkVersion = 23934
    val controllerEpoch = 137
    val leaderEpoch = 245
    val syncReplicaId1 = brokerId
    val syncReplicaId2 = brokerId + 1
    val observerId1 = brokerId + 2
    val observerId2 = brokerId + 3

    val leaderAndIsrUpdate = new LeaderAndIsrPartitionState()
      .setLeader(syncReplicaId1)
      .setLeaderEpoch(leaderEpoch)
      .setReplicas(List[Integer](syncReplicaId1, syncReplicaId2, observerId1, observerId2).asJava)
      .setControllerEpoch(controllerEpoch)
      .setObservers(List[Integer](observerId1, observerId2).asJava)
      .setTopicName(topicPartition.topic)
      .setIsr(List[Integer](syncReplicaId1, observerId1).asJava)
      .setZkVersion(zkVersion)
      .setPartitionIndex(0)

    assertTrue(partition.makeLeader(partitionState = leaderAndIsrUpdate,
      highWatermarkCheckpoints = offsetCheckpoints))

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(syncReplicaId1, observerId1), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertFalse(partition.isUnderReplicated)

    partition.maybeShrinkIsr()

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(syncReplicaId1, observerId1), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertFalse(partition.isUnderReplicated)
  }

  @Test
  def testShouldAddAllReplicasToIsrWhenLeaderIsAnObserver(): Unit = {
    val zkVersion = 23934
    val controllerEpoch = 137
    val leaderEpoch = 245
    val observerId1 = brokerId
    val observerId2 = brokerId + 1
    val syncReplicaId1 = brokerId + 2
    val syncReplicaId2 = brokerId + 3

    val leaderAndIsrUpdate = new LeaderAndIsrPartitionState()
      .setLeader(observerId1)
      .setLeaderEpoch(leaderEpoch)
      .setReplicas(List[Integer](syncReplicaId1, syncReplicaId2, observerId1, observerId2).asJava)
      .setControllerEpoch(controllerEpoch)
      .setObservers(List[Integer](observerId1, observerId2).asJava)
      .setTopicName(topicPartition.topic)
      .setIsr(List[Integer](observerId1).asJava)
      .setZkVersion(zkVersion)
      .setPartitionIndex(0)

    assertTrue(partition.makeLeader(partitionState = leaderAndIsrUpdate,
      highWatermarkCheckpoints = offsetCheckpoints))

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(observerId1), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertTrue(partition.isUnderReplicated)

    var expectedIsr = Seq(observerId1)
    var expectedZkVersion = zkVersion

    for (replicaId <- Seq(observerId2, syncReplicaId1, syncReplicaId2)) {
      expectedIsr ++= Seq(replicaId)
      expectedZkVersion += 1

      when(stateStore.expandIsr(controllerEpoch,
        LeaderAndIsr(brokerId, leaderEpoch, expectedIsr.toList, expectedZkVersion - 1, isUnclean = false, None)))
        .thenReturn(Some(expectedZkVersion))

      partition.updateFollowerFetchState(replicaId,
        LogOffsetMetadata(0L),
        followerStartOffset = 0L,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = 0L)

      assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
      assertEquals(expectedIsr.toSet, partition.inSyncReplicaIds)
      assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
      assertEquals(partition.inSyncReplicaIds.size < 2, partition.isUnderReplicated)
      assertEquals(partition.inSyncReplicaIds.size < 4, partition.isNotCaughtUp)
    }

    assertEquals(false, partition.isUnderReplicated)
    assertEquals(false, partition.isNotCaughtUp)
  }

  @Test
  def testShouldNotAddObserversToIsrWhenLeaderIsNotAnObserver(): Unit = {
    val zkVersion = 23934
    val controllerEpoch = 137
    val leaderEpoch = 245
    val syncReplicaId1 = brokerId
    val syncReplicaId2 = brokerId + 1
    val observerId1 = brokerId + 2
    val observerId2 = brokerId + 3

    val leaderAndIsrUpdate = new LeaderAndIsrPartitionState()
      .setLeader(syncReplicaId1)
      .setLeaderEpoch(leaderEpoch)
      .setReplicas(List[Integer](syncReplicaId1, syncReplicaId2, observerId1, observerId2).asJava)
      .setControllerEpoch(controllerEpoch)
      .setObservers(List[Integer](observerId1, observerId2).asJava)
      .setTopicName(topicPartition.topic)
      .setIsr(List[Integer](syncReplicaId1, syncReplicaId2).asJava)
      .setZkVersion(zkVersion)
      .setPartitionIndex(0)

    assertTrue(partition.makeLeader(partitionState = leaderAndIsrUpdate,
      highWatermarkCheckpoints = offsetCheckpoints))

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(syncReplicaId1, syncReplicaId2), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertFalse(partition.isUnderReplicated)

    partition.updateFollowerFetchState(observerId2,
      LogOffsetMetadata(0L),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 0L)

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(syncReplicaId1, syncReplicaId2), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertFalse(partition.isUnderReplicated)
  }

  @Test
  def testShouldAddSyncReplicaToIsrWhenLeaderIsNotAnObserver(): Unit = {
    val zkVersion = 23934
    val controllerEpoch = 137
    val leaderEpoch = 245
    val syncReplicaId1 = brokerId
    val syncReplicaId2 = brokerId + 1
    val observerId1 = brokerId + 2
    val observerId2 = brokerId + 3

    val leaderAndIsrUpdate = new LeaderAndIsrPartitionState()
      .setLeader(syncReplicaId1)
      .setLeaderEpoch(leaderEpoch)
      .setReplicas(List[Integer](syncReplicaId1, syncReplicaId2, observerId1, observerId2).asJava)
      .setControllerEpoch(controllerEpoch)
      .setObservers(List[Integer](observerId1, observerId2).asJava)
      .setTopicName(topicPartition.topic)
      .setIsr(List[Integer](syncReplicaId1).asJava)
      .setZkVersion(zkVersion)
      .setPartitionIndex(0)

    assertTrue(partition.makeLeader(partitionState = leaderAndIsrUpdate,
      highWatermarkCheckpoints = offsetCheckpoints))

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(syncReplicaId1), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertTrue(partition.isUnderReplicated)

    when(stateStore.expandIsr(controllerEpoch,
      LeaderAndIsr(brokerId, leaderEpoch, List(syncReplicaId1, syncReplicaId2), zkVersion, isUnclean = false, None)))
      .thenReturn(Some(zkVersion + 1))

    partition.updateFollowerFetchState(syncReplicaId2,
      LogOffsetMetadata(0L),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 0L)

    assertEquals(Set(observerId1, observerId2), partition.assignmentState.observers)
    assertEquals(Set(syncReplicaId1, syncReplicaId2), partition.inSyncReplicaIds)
    assertEquals(Seq(syncReplicaId1, syncReplicaId2, observerId1, observerId2), partition.assignmentState.replicas)
    assertFalse(partition.isUnderReplicated)
  }

  @Test
  def testMakeLeaderUpdatesUncleanLeaderState(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = List[Integer](brokerId).asJava
    val topicId = UUID.randomUUID

    // elect as unclean leader
    val uncleanLeaderAndIsrPartitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true)
      .setTopicId(topicId)
      .setConfluentIsUncleanLeader(true)
    tieredPartition.makeLeader(uncleanLeaderAndIsrPartitionState, offsetCheckpoints)
    assertEquals(true, tieredPartition.getIsUncleanLeader)

    // elect as clean leader
    val cleanLeaderAndIsrPartitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas)
      .setIsNew(true)
      .setConfluentIsUncleanLeader(false)
    tieredPartition.makeLeader(cleanLeaderAndIsrPartitionState, offsetCheckpoints)
    assertEquals(false, tieredPartition.getIsUncleanLeader)
  }

  @Test
  def testClearUncleanLeaderState(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = List(brokerId)
    val zkVersion = 1
    val topicId = UUID.randomUUID

    // elect as unclean leader
    val uncleanLeaderAndIsrPartitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr.map(int2Integer).asJava)
      .setZkVersion(zkVersion)
      .setReplicas(replicas)
      .setIsNew(true)
      .setTopicId(topicId)
      .setConfluentIsUncleanLeader(true)
    tieredPartition.makeLeader(uncleanLeaderAndIsrPartitionState, offsetCheckpoints)
    assertEquals(true, tieredPartition.getIsUncleanLeader)

    val newLeaderAndIsr = new LeaderAndIsr(brokerId, leaderEpoch, isr, zkVersion, isUnclean = false, None)
    when(tieredPartitionStateStore.clearUncleanLeaderState(controllerEpoch, newLeaderAndIsr)).thenReturn(Some(zkVersion + 1))

    // clear unclean leader state
    tieredPartition.maybeClearUncleanLeaderState(leaderEpoch)
    assertEquals(false, tieredPartition.getIsUncleanLeader)
    assertEquals(leaderEpoch, tieredPartition.getLeaderEpoch)
    assertEquals(isr.toSet, tieredPartition.inSyncReplicaIds)
    assertEquals(zkVersion + 1, tieredPartition.getZkVersion)

    verify(tieredPartitionStateStore, times(1)).clearUncleanLeaderState(controllerEpoch, newLeaderAndIsr)
  }

  @Test
  def testClearUncleanLeaderStateWithOldEpochFails(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = List(brokerId)
    val zkVersion = 1
    val topicId = UUID.randomUUID

    // elect as unclean leader
    val uncleanLeaderAndIsrPartitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr.map(int2Integer).asJava)
      .setZkVersion(zkVersion)
      .setReplicas(replicas)
      .setIsNew(true)
      .setTopicId(topicId)
      .setConfluentIsUncleanLeader(true)
    tieredPartition.makeLeader(uncleanLeaderAndIsrPartitionState, offsetCheckpoints)
    assertEquals(true, tieredPartition.getIsUncleanLeader)

    // attempt to clear unclean leader state at older epoch is a NOOP
    tieredPartition.maybeClearUncleanLeaderState(leaderEpoch - 1)
    assertEquals(true, tieredPartition.getIsUncleanLeader)
    assertEquals(leaderEpoch, tieredPartition.getLeaderEpoch)
    assertEquals(isr.toSet, tieredPartition.inSyncReplicaIds)
    assertEquals(zkVersion, tieredPartition.getZkVersion)

    verify(tieredPartitionStateStore, times(0)).clearUncleanLeaderState(ArgumentMatchers.any(), ArgumentMatchers.any())
  }

  @Test
  def testClearUncleanLeaderStateZkWriteRetry(): Unit = {
    val controllerEpoch = 0
    val leaderEpoch = 5
    val replicas = List[Integer](brokerId, brokerId + 1).asJava
    val isr = List(brokerId)
    val zkVersion = 1
    val topicId = UUID.randomUUID

    // elect as unclean leader
    val uncleanLeaderAndIsrPartitionState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(isr.map(int2Integer).asJava)
      .setZkVersion(zkVersion)
      .setReplicas(replicas)
      .setIsNew(true)
      .setTopicId(topicId)
      .setConfluentIsUncleanLeader(true)
    tieredPartition.makeLeader(uncleanLeaderAndIsrPartitionState, offsetCheckpoints)
    assertEquals(true, tieredPartition.getIsUncleanLeader)

    // simulate failed ZK writes
    val newLeaderAndIsr = new LeaderAndIsr(brokerId, leaderEpoch, isr, zkVersion, isUnclean = false, None)
    val numInvocations = new AtomicInteger(0)
    when(tieredPartitionStateStore.clearUncleanLeaderState(controllerEpoch, newLeaderAndIsr))
      .thenAnswer(_ => {
        numInvocations.incrementAndGet()
        None
      })

    val clearStateThread = new Thread(() => tieredPartition.maybeClearUncleanLeaderState(leaderEpoch))
    clearStateThread.start()

    // partition state should remain as-is while ZK writes fail
    TestUtils.waitUntilTrue(() => numInvocations.get > 1, "Timed out waiting for clearUncleanLeaderState to be called")
    assertEquals(true, tieredPartition.getIsUncleanLeader)
    assertEquals(zkVersion, tieredPartition.getZkVersion)
    assertTrue(clearStateThread.isAlive)
    verify(tieredPartitionStateStore, atLeastOnce()).clearUncleanLeaderState(controllerEpoch, newLeaderAndIsr)

    // simulate successful ZK write and validate that unclean leader state is cleared and zk version is bumped
    when(tieredPartitionStateStore.clearUncleanLeaderState(controllerEpoch, newLeaderAndIsr)).thenReturn(Some(zkVersion + 2))
    clearStateThread.join(200)
    assertEquals(false, tieredPartition.getIsUncleanLeader)
    assertEquals(leaderEpoch, tieredPartition.getLeaderEpoch)
    assertEquals(isr.toSet, tieredPartition.inSyncReplicaIds)
    assertEquals(zkVersion + 2, tieredPartition.getZkVersion)
  }

  @Test
  def testIsrExpandPreservesUncleanLeaderState(): Unit = {
    val log = logManager.getOrCreateLog(tieredTopicPartition, () => tieredLogConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId).asJava
    val topicId = UUID.randomUUID

    doNothing().when(delayedOperations).checkAndCompleteFetch()
    mockAliveBrokers(metadataCache, replicas)

    assertTrue(
      "Expected become leader transition to succeed",
      tieredPartition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Int.box).asJava)
          .setIsNew(true)
          .setTopicId(topicId)
          .setConfluentIsUncleanLeader(true),
        offsetCheckpoints)
    )
    assertEquals(Set(brokerId), tieredPartition.inSyncReplicaIds)
    assertEquals(true, tieredPartition.getIsUncleanLeader)

    val remoteReplica = tieredPartition.getReplica(remoteBrokerId).get
    assertEquals(LogOffsetMetadata.UnknownOffsetMetadata.messageOffset, remoteReplica.logEndOffset)
    assertEquals(Log.UnknownOffset, remoteReplica.logStartOffset)

    // The next update should bring the follower back into the ISR
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId, remoteBrokerId),
      zkVersion = 1,
      isUnclean = true,
      clusterLinkState = None)
    when(tieredPartitionStateStore.expandIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(Some(2))

    tieredPartition.updateFollowerFetchState(remoteBrokerId,
      followerFetchOffsetMetadata = LogOffsetMetadata(10),
      followerStartOffset = 0L,
      followerFetchTimeMs = time.milliseconds(),
      leaderEndOffset = 6L)
    verify(tieredPartitionStateStore, times(1)).expandIsr(controllerEpoch, updatedLeaderAndIsr)
    assertEquals(Set(brokerId, remoteBrokerId), tieredPartition.inSyncReplicaIds)
    assertEquals(true, tieredPartition.getIsUncleanLeader)
  }

  @Test
  def testShrinkIsrPreservesUncleanLeaderState(): Unit = {
    val log = logManager.getOrCreateLog(tieredTopicPartition, () => tieredLogConfig)
    seedLogData(log, numRecords = 10, leaderEpoch = 4)

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId, remoteBrokerId).asJava
    val topicId = UUID.randomUUID

    doNothing().when(delayedOperations).checkAndCompleteFetch()
    mockAliveBrokers(metadataCache, replicas)

    assertTrue(
      "Expected become leader transition to succeed",
      tieredPartition.makeLeader(
        new LeaderAndIsrPartitionState()
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas.map(Int.box).asJava)
          .setIsNew(true)
          .setTopicId(topicId)
          .setConfluentIsUncleanLeader(true),
        offsetCheckpoints)
    )
    assertEquals(Set(brokerId, remoteBrokerId), tieredPartition.inSyncReplicaIds)
    assertEquals(0L, tieredPartition.localLogOrException.highWatermark)
    assertEquals(true, tieredPartition.getIsUncleanLeader)

    // If enough time passes without a fetch update, the ISR should shrink
    time.sleep(tieredPartition.replicaLagTimeMaxMs + 1)
    val updatedLeaderAndIsr = LeaderAndIsr(
      leader = brokerId,
      leaderEpoch = leaderEpoch,
      isr = List(brokerId),
      zkVersion = 1,
      isUnclean = true,
      clusterLinkState = None)
    when(tieredPartitionStateStore.shrinkIsr(controllerEpoch, updatedLeaderAndIsr)).thenReturn(Some(2))

    tieredPartition.maybeShrinkIsr()
    verify(tieredPartitionStateStore, times(1)).shrinkIsr(controllerEpoch, updatedLeaderAndIsr)
    assertEquals(Set(brokerId), tieredPartition.inSyncReplicaIds)
    assertEquals(true, tieredPartition.getIsUncleanLeader)
  }

  @Test
  def testClusterLinkAppendDisallowed(): Unit = {
    val controllerEpoch = 3
    val replicas = Seq(0, 1, 2)
    val origins = Seq(AppendOrigin.Coordinator, AppendOrigin.Client)

    def newRecord(): MemoryRecords =
      createRecords(List(new SimpleRecord("k".getBytes, "v".getBytes)), baseOffset = 0, timestampType = TimestampType.CREATE_TIME)

    // No cluster link set. Verify the partition can accept records.
    val leaderState1 = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(0)
      .setLeaderEpoch(1)
      .setIsr(replicas.map(Int.box).asJava)
      .setZkVersion(1)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(true)
    assertTrue(partition.makeLeader(leaderState1, offsetCheckpoints))
    origins.foreach { origin =>
      partition.appendRecordsToLeader(newRecord(), origin, requiredAcks = 0)
    }

    // Set the cluster link. The partition should not accept new records.
    val leaderState2 = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(0)
      .setLeaderEpoch(2)
      .setIsr(replicas.map(Int.box).asJava)
      .setZkVersion(1)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(false)
      .setClusterLinkId(UUID.randomUUID().toString)
      .setClusterLinkTopicState("Mirror")
    assertFalse(partition.makeLeader(leaderState2, offsetCheckpoints))
    origins.foreach { origin =>
      intercept[InvalidRequestException] {
        partition.appendRecordsToLeader(newRecord(), origin, requiredAcks = 0)
      }
    }

    // Remove the cluster link. The partition should be mutable again.
    val leaderState3 = new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(0)
      .setLeaderEpoch(3)
      .setIsr(replicas.map(Int.box).asJava)
      .setZkVersion(1)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(false)
    assertFalse(partition.makeLeader(leaderState3, offsetCheckpoints))
    origins.foreach { origin =>
      partition.appendRecordsToLeader(newRecord(), origin, requiredAcks = 0)
    }
  }

  def testUncleanLeaderRecoveryExceptionHandling(): Unit = {
    val logManager = mock(classOf[LogManager])
    val log = mock(classOf[MergedLog])
    val tierPartitionState = mock(classOf[FileTierPartitionState])
    val tierStateFetcher = mock(classOf[TierStateFetcher])

    val controllerEpoch = 0
    val leaderEpoch = 5
    val remoteBrokerId = brokerId + 1
    val replicas = List(brokerId, remoteBrokerId)
    val isr = List[Integer](brokerId).asJava
    val topicId = UUID.randomUUID

    // setup logManager mocks
    when(logManager.getOrCreateLog(ArgumentMatchers.eq(tieredTopicPartition), ArgumentMatchers.any(),
      ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(log)

    // setup log mocks
    when(log.parentDir).thenReturn(logDir1.getAbsolutePath)
    when(log.tierPartitionState).thenReturn(tierPartitionState)
    when(log.maybeIncrementHighWatermark(ArgumentMatchers.any())).thenReturn(None)

    // setup tierPartitionState mocks
    when(tierPartitionState.isTieringEnabled).thenReturn(true)
    when(tierPartitionState.materializeUptoEpoch(leaderEpoch))
      .thenThrow(new IllegalStateException("unknown exception during materializeUptoEpoch"))

    // setup tieredPartitionStateStore mocks
    when(tieredPartitionStateStore.clearUncleanLeaderState(ArgumentMatchers.any(), ArgumentMatchers.any()))
      .thenReturn(Some(1))

    val partition = new Partition(
      tieredTopicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      tieredPartitionStateStore,
      delayedOperations,
      metadataCache,
      logManager,
      Some(tierReplicaManager),
      Some(tierStateFetcher),
      Some(executor))

    // Elect an unclean leader for tiered partition. This will initiate recovery for unclean leader which will abort
    // mid-way because of the exception thrown from TierPartitionState#materializeUptoEpoch. The exception handling mechansim
    // will complete this future successfully, without clearing the unclean leader state.
    partition.makeLeader(new LeaderAndIsrPartitionState()
        .setControllerEpoch(controllerEpoch)
        .setLeader(brokerId)
        .setLeaderEpoch(leaderEpoch)
        .setIsr(isr)
        .setZkVersion(1)
        .setReplicas(replicas.map(Int.box).asJava)
        .setIsNew(true)
        .setTopicId(topicId)
        .setConfluentIsUncleanLeader(true),
      offsetCheckpoints)
    TestUtils.waitUntilTrue(() => partition.uncleanLeaderRecoveryFutureOpt.exists(_.isDone),
      "Timed out waiting for future to complete")
    assertFalse(partition.uncleanLeaderRecoveryFutureOpt.get.isCompletedExceptionally)
    assertTrue(partition.getIsUncleanLeader)
    verify(tierPartitionState, times(1)).materializeUptoEpoch(leaderEpoch)
    verify(tierStateFetcher, times(0)).fetchLeaderEpochStateAsync(ArgumentMatchers.any())
    verify(log, times(0)).recoverLocalLogAfterUncleanLeaderElection(ArgumentMatchers.any())

    // Elect an unclean leader at higher epoch. This simulates the positive case where recovery succeeds.
    when(tierPartitionState.materializeUptoEpoch(leaderEpoch + 1))
      .thenReturn(CompletableFuture.completedFuture(Optional.empty[TierObjectMetadata]))
    partition.makeLeader(new LeaderAndIsrPartitionState()
      .setControllerEpoch(controllerEpoch)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch + 1)
      .setIsr(isr)
      .setZkVersion(1)
      .setReplicas(replicas.map(Int.box).asJava)
      .setIsNew(true)
      .setTopicId(topicId)
      .setConfluentIsUncleanLeader(true),
      offsetCheckpoints)
    Thread.sleep(100)
    TestUtils.waitUntilTrue(() => partition.uncleanLeaderRecoveryFutureOpt.isEmpty,
      "Timed out waiting for future to complete")
    assertFalse(partition.getIsUncleanLeader)
    verify(tierPartitionState, times(1)).materializeUptoEpoch(leaderEpoch + 1)
    // since we are returning empty TierObjectMetadata, tier state will not be fetched and code will not go to
    // recoverLocalLogAfterUncleanLeaderElection. This is expected behavior
    verify(tierStateFetcher, times(0)).fetchLeaderEpochStateAsync(ArgumentMatchers.any())
    verify(log, times(0)).recoverLocalLogAfterUncleanLeaderElection(ArgumentMatchers.any())
  }

  private def seedLogData(log: AbstractLog, numRecords: Int, leaderEpoch: Int): Unit = {
    for (i <- 0 until numRecords) {
      val records = MemoryRecords.withRecords(0L, CompressionType.NONE, leaderEpoch,
        new SimpleRecord(s"k$i".getBytes, s"v$i".getBytes))
      log.appendAsLeader(records, leaderEpoch)
    }
  }

}

object PartitionTest {
  def mockAliveBrokers(metadataCache: MetadataCache, ids: Iterable[Int]): Unit = {
    val aliveBrokers = ids.map { brokerId =>
      TestUtils.createBroker(brokerId, s"host$brokerId", brokerId)
    }.toSeq

    when(metadataCache.getAliveBrokers).thenReturn(aliveBrokers)

    ids.foreach { brokerId =>
      when(metadataCache.getAliveBroker(brokerId))
        .thenReturn(Option(TestUtils.createBroker(brokerId, s"host$brokerId", brokerId)))
    }
  }
}
