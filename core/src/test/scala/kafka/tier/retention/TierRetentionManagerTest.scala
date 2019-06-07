/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.retention

import java.io.File
import java.util.{Optional, UUID}
import java.util.concurrent.CompletableFuture

import kafka.cluster.Partition
import kafka.log.MergedLogTest.createRecords
import kafka.log.{Log, LogConfig, LogTest, MergedLog, MergedLogTest}
import kafka.server.{BrokerTopicStats, LogDirFailureChannel, ReplicaManager}
import kafka.tier.domain.AbstractTierMetadata
import kafka.tier.state.FileTierPartitionStateFactory
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.tier.{TierMetadataManager, TierTopicManager, TopicIdPartition}
import kafka.utils.{MockTime, TestUtils}
import org.junit.{After, Before, Test}
import org.junit.Assert.assertEquals
import org.mockito.Mockito.{mock, when}
import org.mockito.ArgumentMatchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

class TierRetentionManagerTest {
  val time = new MockTime
  val scheduler = time.scheduler
  val replicaManager = mock(classOf[ReplicaManager])
  val tierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig)
  val tierMetadataManager = new TierMetadataManager(new FileTierPartitionStateFactory(),
    Optional.of(tierObjectStore),
    new LogDirFailureChannel(1),
    true)
  val tierTopicManager = mock(classOf[TierTopicManager])
  val tierRetentionManager = new TierRetentionManager(scheduler, replicaManager, tierMetadataManager,
    tierTopicManager, tierObjectStore, retentionCheckMs = 10, time = time)
  val dir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(dir)
  val topicPartition = Log.parseTopicPartitionName(logDir)
  val topicIdPartition = new TopicIdPartition(topicPartition.topic, UUID.randomUUID, topicPartition.partition)

  @Before
  def setup(): Unit = {
    when(tierTopicManager.addMetadata(any())).thenAnswer(new Answer[CompletableFuture[AppendResult]] {
      override def answer(invocation: InvocationOnMock): CompletableFuture[AppendResult] = {
        val metadata = invocation.getArgument(0).asInstanceOf[AbstractTierMetadata]
        CompletableFuture.completedFuture(tierMetadataManager.tierPartitionState(metadata.topicIdPartition.topicPartition).get.append(metadata))
      }
    })
  }

  @After
  def tearDown(): Unit = {
    logDir.delete()
    dir.delete()
  }

  @Test
  def testSizeRetention(): Unit = {
    val segmentBytes = createRecords().sizeInBytes * 2
    val retentionBytes = segmentBytes * 9
    val partition = mock(classOf[Partition])

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, retentionBytes = retentionBytes, tierEnable = true)
    val log = createLogWithOverlap(numTieredSegments = 15, numLocalSegments = 10, numOverlap = 5, logConfig, logDir)
    val topicIdPartition = tierMetadataManager.tierPartitionState(topicPartition).get.topicIdPartition.get
    tierMetadataManager.becomeLeader(topicIdPartition, 0)

    try {
      assertEquals(20, log.tieredLogSegments.size)

      when(partition.topicPartition).thenReturn(log.topicPartition)
      when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))

      TestUtils.waitUntilTrue(() => {
        tierRetentionManager.makeTransitions()
        time.sleep(logConfig.fileDeleteDelayMs)
        log.tieredLogSegments.isEmpty
      }, "Timed out waiting for tiered segments to be deleted", pause = 0)
    } finally {
      log.close()
    }
  }

  @Test
  def testCollectDeletableSegments(): Unit = {
    val segmentBytes = createRecords().sizeInBytes * 2
    val numTiered = 15
    val numOverlap = 5
    val numLocal = 10
    val numSegmentsToRetain = 16
    val numTieredToRetain = numSegmentsToRetain - (numLocal - 1)  // -1 to exclude the empty active segment

    val retentionBytes = segmentBytes * numSegmentsToRetain
    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, retentionBytes = retentionBytes, tierEnable = true)
    val log = createLogWithOverlap(numTieredSegments = 15, numLocalSegments = 10, numOverlap = 5, logConfig, logDir)

    try {
      val initialTieredSegments = log.tieredLogSegments
      val deletable = tierRetentionManager.collectDeletableSegments(log, log.tieredLogSegments)
      assertEquals(numTiered + numOverlap - numTieredToRetain, deletable.size)
      assertEquals(initialTieredSegments.takeRight(numTieredToRetain).head.baseOffset, log.logStartOffset)
    } finally {
      log.close()
    }
  }

  private def createLogWithOverlap(numTieredSegments: Int, numLocalSegments: Int, numOverlap: Int, config: LogConfig, logDir: File): MergedLog = {
    MergedLogTest.createLogWithOverlap(numTieredSegments, numLocalSegments, numOverlap, tierMetadataManager,
      logDir, config, new BrokerTopicStats, time.scheduler, time, topicIdPartition)
  }
}
