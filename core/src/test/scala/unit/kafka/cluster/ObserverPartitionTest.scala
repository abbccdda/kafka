/*
 * Copyright 2019 Confluent Inc.
 */
package kafka.cluster

import java.io.File
import java.util.Properties

import kafka.api.ApiVersion
import kafka.log.AbstractLog
import kafka.log.CleanerConfig
import kafka.log.LogConfig
import kafka.log.LogManager
import kafka.server.ConfluentObserverTest
import kafka.server.Defaults
import kafka.server.LogOffsetMetadata
import kafka.server.MetadataCache
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils.MockTime
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.utils.Utils
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.mock
import org.mockito.Mockito.when

import scala.collection.JavaConverters._


final class ObserverPartitionTest {
  import ObserverPartitionTest._

  val time = new MockTime()
  val brokerId = 101
  var logConfig: LogConfig = _
  var logManager: LogManager = _
  var tmpDir: File = _
  var partition: Partition = _
  val stateStore = mock(classOf[PartitionStateStore])
  val delayOperations = mock(classOf[DelayedOperations])
  val metadataCache = mock(classOf[MetadataCache])
  val offsetCheckpoints = mock(classOf[OffsetCheckpoints])
  val topicPartition = new TopicPartition("test-observer", 0)


  @Before
  def setup(): Unit = {
    logConfig = LogConfig(
      createLogProperties(
        Map(
          LogConfig.TopicPlacementConstraintsProp -> ConfluentObserverTest.basicTopicPlacement("replica", Some("observer"))
        )
      )
    )

    tmpDir = TestUtils.tempDir()
    val logDir1 = TestUtils.randomPartitionLogDir(tmpDir)

    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1),
      defaultConfig = logConfig,
      CleanerConfig(enableCleaner = false),
      time)
    logManager.startup()

    partition = new Partition(
      topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      observerFeature = true,
      time,
      stateStore,
      delayOperations,
      metadataCache,
      logManager
    )

    when(stateStore.fetchTopicConfig()).thenReturn(createLogProperties(Map.empty))
    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition))).thenReturn(None)
  }

  @After
  def tearDown(): Unit = {
    logManager.shutdown()
    Utils.delete(tmpDir)
  }

  @Test
  def testHighWatermarkIncreasesWithoutObserverFetch(): Unit = {
    val controllerId = 0
    val controllerEpoch = 0
    val leaderEpoch = 5
    val followerId = brokerId + 1
    val observerId = followerId + 1
    val replicas = List[Integer](brokerId, followerId, observerId).asJava
    val isr = List[Integer](brokerId, followerId).asJava

    when(metadataCache.getAliveBroker(brokerId))
      .thenReturn(Some(Broker(brokerId, Seq.empty, Some("replica"))))
    when(metadataCache.getAliveBroker(followerId))
      .thenReturn(Some(Broker(followerId, Seq.empty, Some("replica"))))
    when(metadataCache.getAliveBroker(observerId))
      .thenReturn(Some(Broker(observerId, Seq.empty, Some("observer"))))

    val log = logManager.getOrCreateLog(topicPartition, logConfig)
    seedLogData(log, numRecords = 6, leaderEpoch = 4)

    partition.createLogIfNotExists(brokerId, isNew = false, isFutureReplica = false, offsetCheckpoints)

    assertTrue(
      "Expected become leader transition to succeed",
      partition.makeLeader(
        controllerId,
        new LeaderAndIsrPartitionState()
          .setTopicName(topicPartition.topic)
          .setPartitionIndex(topicPartition.partition)
          .setControllerEpoch(controllerEpoch)
          .setLeader(brokerId)
          .setLeaderEpoch(leaderEpoch)
          .setIsr(isr)
          .setZkVersion(1)
          .setReplicas(replicas)
          .setIsNew(true),
        0,
        offsetCheckpoints
      )
    )

    // We are using a leaderEndOffset of 3 to trick the leader that the observer is caught up to the leader
    assertTrue(
      "Leader didn't recognize replica",
      partition.updateFollowerFetchState(
        observerId,
        followerFetchOffsetMetadata = LogOffsetMetadata(3),
        followerStartOffset = 0,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = 3
      )
    )

    // We are using a leaderEndOffset of 5 to trick the leader that the follower is caught up to the leader.
    // This is techinically not needed because the leader assumes the follower is caugh up if it is included in
    // the ISR which this the case for the follower.
    assertTrue(
      "Leader didn't recognize replica",
      partition.updateFollowerFetchState(
        followerId,
        followerFetchOffsetMetadata = LogOffsetMetadata(5),
        followerStartOffset = 0,
        followerFetchTimeMs = time.milliseconds(),
        leaderEndOffset = 5
      )
    )

    // Without observer support the high watermark should be 3
    assertEquals(
      "Expect the highwatermark to only include followers",
      partition.localLogOrException.highWatermark,
      5
    )
  }
}

object ObserverPartitionTest {
  def seedLogData(log: AbstractLog, numRecords: Int, leaderEpoch: Int): Unit = {
    for (i <- 0 until numRecords) {
      val records = MemoryRecords.withRecords(0L, CompressionType.NONE, leaderEpoch,
        new SimpleRecord(s"k$i".getBytes, s"v$i".getBytes))
      log.appendAsLeader(records, leaderEpoch)
    }
  }

  def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }
}
