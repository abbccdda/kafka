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
import kafka.tier.{TierMetadataManager, TopicIdPartition}
import kafka.tier.domain.TierTopicInitLeader
import kafka.tier.topic.TierTopicManager
import kafka.utils.{MockTime, TestUtils}
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertTrue}
import org.mockito.Mockito.{mock, when}
import org.mockito.ArgumentMatchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

class TierRetentionManagerTest {
  val time = new MockTime
  val scheduler = time.scheduler
  val replicaManager = mock(classOf[ReplicaManager])
  val tierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig("cluster", 1))
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
    when(tierTopicManager.isReady).thenReturn(true)
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
    tierMetadataManager.becomeLeader(topicIdPartition.topicPartition(), 0)
    tierMetadataManager.ensureTopicIdPartition(topicIdPartition)

    try {
      assertEquals(20, log.tieredLogSegments.size)

      when(partition.topicPartition).thenReturn(log.topicPartition)
      when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))
      when(replicaManager.tierMetadataManager).thenReturn(tierMetadataManager)

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
  def testWaitsForLeadership(): Unit = {
    val segmentBytes = createRecords().sizeInBytes * 2
    val retentionBytes = segmentBytes * 9
    val partition = mock(classOf[Partition])

    val logConfig = LogTest.createLogConfig(segmentBytes = segmentBytes, retentionBytes = retentionBytes, tierEnable = true)
    val log = createLogWithOverlap(numTieredSegments = 15, numLocalSegments = 10, numOverlap = 5, logConfig, logDir)
    val topicIdPartition = tierMetadataManager.tierPartitionState(topicPartition).get.topicIdPartition.get
    try {
      assertEquals(20, log.tieredLogSegments.size)

      when(partition.topicPartition).thenReturn(log.topicPartition)
      when(replicaManager.getLog(topicPartition)).thenReturn(Some(log))
      when(replicaManager.tierMetadataManager).thenReturn(tierMetadataManager)

      // check that the tier retention manager will not delete segments as becomeLeader has not been called
      tierRetentionManager.makeTransitions()
      time.sleep(logConfig.fileDeleteDelayMs)
      assertEquals("expected no segments to be deleted because become leader has not been called",
        20, log.tieredLogSegments.size)
      assertEquals(0, tierMetadataManager.tierPartitionState(topicPartition).get().tierEpoch())

      // becomeLeader for the partition. This will cause this partition to be
      // checked in the TierRetentionManager, however it must first wait for the TierPartitionState
      // to be at the right epoch before making any actions
      tierMetadataManager.becomeLeader(topicIdPartition.topicPartition(), 1)
      tierMetadataManager.ensureTopicIdPartition(topicIdPartition)
      tierRetentionManager.makeTransitions()
      time.sleep(logConfig.fileDeleteDelayMs)
      assertEquals("expected no segments to be deleted because metadata has not been read",
        20, log.tieredLogSegments.size)

      // write out the init marker at the expected epoch, this should trigger retention
      tierMetadataManager.tierPartitionState(topicPartition).get().append(new TierTopicInitLeader(topicIdPartition, 1, UUID.randomUUID(), 10))
      assertEquals(1, tierMetadataManager.tierPartitionState(topicPartition).get().tierEpoch())
      tierRetentionManager.makeTransitions()
      time.sleep(logConfig.fileDeleteDelayMs)
      assertTrue("expected some segments to be deleted now that the leader epoch has been written",
        log.tieredLogSegments.size < 20)

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
