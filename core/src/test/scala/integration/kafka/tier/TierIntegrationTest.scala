/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util
import java.util.function.Supplier
import java.util.{Collections, Optional}

import javax.management.{MBeanServer, ObjectName}
import kafka.log._
import kafka.server.{BrokerTopicStats, LogDirFailureChannel, ReplicaManager}
import kafka.tier.archiver.TierArchiverState.{BeforeUpload, TierArchiverStateComparator}
import kafka.tier.archiver._
import kafka.tier.client.{MockConsumerBuilder, MockProducerBuilder}
import kafka.tier.state.{MemoryTierPartitionStateFactory, TierPartitionStatus}
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStoreConfig}
import kafka.tier.store.TierObjectStore.TierObjectStoreFileType
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record._
import org.junit.Assert._
import org.junit.{After, Test}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._

class TierIntegrationTest {
  private val mockTime = new MockTime()
  val logDirs = new util.ArrayList(Collections.singleton(TestUtils.tempDir().getAbsolutePath))

  val tierTopicManagerConfig = new TierTopicManagerConfig(
    "bootstrap",
    null,
    1,
    1,
    33,
    "cluster99",
    10L,
    500,
    500,
    logDirs
  )
  var tierMetadataManager: TierMetadataManager = _
  var tempDir: File = _
  var tierArchiver: TierArchiver = _
  var replicaManager: ReplicaManager = _
  var tierObjectStore: MockInMemoryTierObjectStore = _
  var logs: Seq[AbstractLog] = _
  var tierTopicManager: TierTopicManager = _
  var consumerBuilder: MockConsumerBuilder = _
  val maxWaitTimeMs = 2000L

  val mBeanServer: MBeanServer = ManagementFactory.getPlatformMBeanServer

  def setup(numLogs: Integer = 2, numArchiverThreads: Integer = 10): Unit = {
    val tierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig())
    val tierMetadataManager = new TierMetadataManager(new MemoryTierPartitionStateFactory(),
      Some(tierObjectStore), new LogDirFailureChannel(1), true)
    val (tierTopicManager, consumerBuilder) = setupTierTopicManager(tierMetadataManager)
    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(numLogs, logConfig, tempDir, tierMetadataManager)
    val replicaManager = mockReplicaManager(logs)
    val tierArchiver = new TierArchiver(TierArchiverConfig(numArchiverThreads), replicaManager, tierMetadataManager, tierTopicManager, tierObjectStore, mockTime)

    this.tierArchiver = tierArchiver
    this.replicaManager = replicaManager
    this.tierObjectStore = tierObjectStore
    this.tierMetadataManager = tierMetadataManager
    this.logs = logs
    this.tierTopicManager = tierTopicManager
    this.consumerBuilder = consumerBuilder
    this.tempDir = tempDir
  }

  @After
  def teardown(): Unit = {
    tierArchiver.shutdown()
    replicaManager.shutdown()
    tierObjectStore.close()
    tierTopicManager.shutdown()
    tierMetadataManager.close()
    logs.foreach(l => l.close())
  }

  @Test
  def testArchiverEmigrate(): Unit = {
    setup()

    waitForImmigration(logs, 1, tierArchiver, tierTopicManager, consumerBuilder)

    // Emigrate one partition
    tierArchiver.handleEmigration(logs.head.topicPartition)

    assertEquals("One topic partition should be queued for emigration.", 1, tierArchiver.immigrationEmigrationQueue.size())

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending emigrations", 2000L)

    assertEquals("No topic partitions should be queued for emigration.", 0, tierArchiver.immigrationEmigrationQueue.size())
    assertEquals("Remaining topic partition should be transitioning to the next status.", 1, tierArchiver.stateTransitionsInProgress.size)

    // Re-immigrate with valid epoch
    tierArchiver.handleImmigration(logs.head.topicPartition, 2)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending immigrations", 2000L)

    assertEquals("All topic partitions should be transitioning to the next status.", logs.size, tierArchiver.stateTransitionsInProgress.size)

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "Topic manager should process an init entry", 2000L)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should advance one partition to the BeforeUploadState", 2000L)
  }

  @Test
  def testArchiverFencing(): Unit = {
    setup()
    // Immigrate all test logs
    logs.foreach { log =>
      tierMetadataManager.becomeLeader(log.topicPartition, 1)
    }

    assertEquals("Topic partitions should be queued for immigration.", logs.size, tierArchiver.immigrationEmigrationQueue.size())

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should process pending immigrations", 2000L)

    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
    }, "Topic manager should process init entries", 2000L)

    TestUtils.waitUntilTrue(() => tierArchiver.processTransitions(), "Archiver should handle fenced exception", 2000L)

    assertEquals("Topic partitions should be queued for immigration.", 0, tierArchiver.immigrationEmigrationQueue.size())
  }

  @Test
  def testArchiverUploadAndMaterialize(): Unit = {
    setup(numLogs = 10)
    val numBatches = 6
    val leaderEpoch = 1

    // Write batches
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, numBatches, 4) }

    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerBuilder)

    logs.foreach { log =>
      assertEquals(s"topic manager should materialize entry for ${log.topicPartition}",
        tierTopicManager.partitionState(log.topicPartition).tierEpoch, leaderEpoch)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize the first segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        tierTopicManager.partitionState(log.topicPartition).numSegments() == 1
      }
    }, "Should materialize segments", tierArchiver, tierTopicManager, consumerBuilder)

    logs.foreach { log =>
      assertEquals("batch 1: segment should be materialized with correct offset relationship",
        0L, tierTopicManager.partitionState(log.topicPartition).metadata(0).get().startOffset)
      assertEquals("batch 1: segment should be materialized with correct end offset",
        3L, tierTopicManager.partitionState(log.topicPartition).endOffset.get())
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize the second segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        tierTopicManager.partitionState(log.topicPartition).numSegments() == 2
      }
    }, "Should materialize segments", tierArchiver, tierTopicManager, consumerBuilder)


    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    logs.foreach { log =>
      assertEquals("batch 2: segment should be materialized with correct offset relationship",
        4L, tierTopicManager.partitionState(log.topicPartition).metadata(6).get().startOffset)
      assertEquals("batch 2: segment should be materialized with correct end offset",
        7L, tierTopicManager.partitionState(log.topicPartition).endOffset.get())
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize the third segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        tierTopicManager.partitionState(log.topicPartition).numSegments() == 3
      }
    }, "Should materialize segments", tierArchiver, tierTopicManager, consumerBuilder)

    logs.foreach { log =>
      assertEquals("batch 3: segment should be materialized with correct offset relationship",
        8L, tierTopicManager.partitionState(log.topicPartition).metadata(10).get().startOffset)
      assertEquals("batch 3: segment should be materialized with correct end offset",
        11L, tierTopicManager.partitionState(log.topicPartition).endOffset.get())
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)
  }

  @Test
  def testArchiverUploadAndMaterializeWhenWriteHappensAfterBecomeLeader(): Unit = {
    setup(numLogs = 10)

    val leaderEpoch = 1

    // Immigrate all test logs
    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerBuilder)

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Write batches
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, 6, 4) }

    // Wait for the first segments to materialize
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        tierTopicManager.partitionState(log.topicPartition).numSegments() > 0
      }
    }, "Should materialize segments", tierArchiver, tierTopicManager, consumerBuilder)

    logs.foreach { log =>
      assertEquals("Segment should be materialized with correct offset relationship",
        0L, tierTopicManager.partitionState(log.topicPartition).metadata(0).get().startOffset)
      assertEquals("Segment should be materialized with correct end offset",
        3L, tierTopicManager.partitionState(log.topicPartition).endOffset.get())
    }
    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)
  }

  @Test
  def testArchiverUploadWithLimitedUploadConcurrency(): Unit = {
    val maxConcurrentUploads = 2
    val nLogs = 3
    setup(nLogs, maxConcurrentUploads)

    val batches = 3
    val recordsPerBatch = 4
    val leaderEpoch = 1

    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerBuilder)

    // The partitions have immigrated, but so far the logs are empty.
    snapshotArchiver(tierArchiver) { s =>
      assertEquals(s"$maxConcurrentUploads state(s) should be in progress (BeforeUpload)",
        maxConcurrentUploads, s.transitioning.size)
      assertEquals(s"${nLogs - maxConcurrentUploads} state(s) should be paused at BeforeUpload, awaiting room on the executor",
        nLogs - maxConcurrentUploads,
        s.pausedStates.collect { case x: BeforeUpload => x }.size)
    }

    // Write two batches to all partitions
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, batches, recordsPerBatch) }

    var materializedFirstCycle = Seq.empty[TopicPartition]

    // Wait for the partitions that happened to be at the front of the priority queue to each upload their first segment and store metadata.
    val lastOffsetOfInitialSegment = Optional.of(recordsPerBatch - 1L : java.lang.Long)
    archiveAndMaterializeUntilTrue(() => {
      materializedFirstCycle = logs.collect {
        case log if tierTopicManager.partitionState(log.topicPartition).uncommittedEndOffset()
          .equals(lastOffsetOfInitialSegment) => log.topicPartition
      }
      maxConcurrentUploads == materializedFirstCycle.size
    }, s"Do work until the initial segment for each of the first $maxConcurrentUploads states is materialized",
      tierArchiver, tierTopicManager, consumerBuilder)


    // end offsets match, process transitions to stage next upload
    tierArchiver.processTransitions()

    // Just before the end of the above cycle, the highest priority paused state(s) had yet to start uploading any segments.
    // Now, of those, we expect at least one (up to the max states-in-progress ceiling) to be transitioning from BeforeUpload to AfterUpload.
    val expectedInProgressNotMaterialized = Math.min(nLogs - maxConcurrentUploads, maxConcurrentUploads)
    snapshotArchiver(tierArchiver) { s =>
      assertEquals(s"$expectedInProgressNotMaterialized not yet materialized partition(s) should correspond to an in-progress BeforeUpload state",
        expectedInProgressNotMaterialized,
        logs.count { log =>
          s.transitioning.contains(log.topicPartition) &&
            !materializedFirstCycle.contains(log.topicPartition)
        })
    }
    // The previous assertions cover priority fairness in practice.
    // Now ensure the archiver can exhaust all tierable segs.
    val lastOffsetOfFinalTierableSegment = Optional.of(recordsPerBatch * (batches - 1) - 1L : java.lang.Long)
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        lastOffsetOfFinalTierableSegment == tierTopicManager.partitionState(log.topicPartition).uncommittedEndOffset()
      }
    }, s"Eventually, all tierable segments should be materialized.",
      tierArchiver, tierTopicManager, consumerBuilder)
  }

  @Test
  def testArchiverUploadWithAsymmetricalWrites(): Unit = {
    setup(numLogs = 3, numArchiverThreads = 2)

    val leaderEpoch = 1
    val lastTopicPartition = logs.last.topicPartition

    val firstLogs = logs.take(2)

    // Write a batch only to the first two logs
    firstLogs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, 1, 4) }

    // Immigrate all test logs.
    // Given maxConcurrentUploads < logs, this will also transition states for second two logs.
    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerBuilder)

    snapshotArchiver(tierArchiver) { s =>
      assertTrue("First two partitions should have state transitions in progress",
        firstLogs.forall { log =>
          s.transitioning.contains(log.topicPartition) })
      assertFalse(s"$lastTopicPartition should not have a state transition in progress.",
        s.transitioning.contains(lastTopicPartition))
      assertTrue(s"$lastTopicPartition should be in BeforeUpload state",
        s.pausedStates.exists { case x: BeforeUpload if x.topicPartition == lastTopicPartition => true })
    }

    // Write ten batches to last log.
    writeRecordBatches(logs.last, leaderEpoch, 0L, 10, 4)

    // The last log's state should be transitioned in the next cycle.
    TestUtils.waitUntilTrue(
      () => tierArchiver.processTransitions(),
      "Archiver should un-pause the first topic partition's state",
      maxWaitTimeMs)

    snapshotArchiver(tierArchiver) { s =>
      assertTrue(s"$lastTopicPartition should be transitioning state.",
        s.transitioning.contains(lastTopicPartition))
    }
  }

  @Test
  def testArchiverTotalLag(): Unit = {
    val numLogs = 5
    val batches = 6
    val recordsPerBatch = 4
    val leaderEpoch = 1

    setup(numLogs)

    val bean = tierArchiver.metricName("TotalLag", Map.empty).getMBeanName
    def totalLag: Long = mBeanServer
      .getAttribute(new ObjectName(bean), "Value")
      .asInstanceOf[Long]

    def awaitMaterializeBatchAndAssertLag(archivedBatches: Int): Unit = {
      archiveAndMaterializeUntilTrue(() => {
        logs.forall { log =>
          tierTopicManager.partitionState(log.topicPartition).numSegments() == archivedBatches
        }
      }, s"Should materialize segments for batch $archivedBatches", tierArchiver, tierTopicManager, consumerBuilder)

      // one more tick for the transition from AfterUpload to BeforeUpload
      tierArchiver.processTransitions()

      // -1 because the final segment will not be archived
      assertEquals(numLogs * (batches - archivedBatches - 1) * recordsPerBatch, totalLag)
    }

    // when tracking no partitions, lag should be zero
    assertEquals(0, totalLag)

    // immigrate all test logs
    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerBuilder)

    assertEquals(0, totalLag)

    // write batches
    logs.foreach { log =>
      writeRecordBatches(log, leaderEpoch, 0L, batches, recordsPerBatch)
    }

    // assert initial lag based on the sum of expected log end offsets
    // minus the last segment
    assertEquals(numLogs * (batches - 1) * recordsPerBatch, totalLag)

    // assert lag after each batch is materialized by the archiver
    (1 until batches).foreach(awaitMaterializeBatchAndAssertLag)
  }

  /**
    *  For a sequence of logs, do the following:
    *  1. Ensure the archiver is the leader for each log TopicPartition.
    *  2. Ensure that the TierPartitionState becomes ONLINE for all topic partitions.
    *  3. Issue an archiver state transition to move from BeforeLeader => BeforeUpload.
    *  4. Ensure there are no outstanding BeforeLeader states.
    */
  private def waitForImmigration(logs: Seq[AbstractLog],
                                 leaderEpoch: Int,
                                 tierArchiver: TierArchiver,
                                 tierTopicManager: TierTopicManager,
                                 consumerBuilder: MockConsumerBuilder): Unit = {
    // Immigrate all test logs
    logs.foreach { log =>
      tierMetadataManager.becomeLeader(log.topicPartition, leaderEpoch)
    }

    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        Option(tierTopicManager.partitionState(log.topicPartition)).exists { tps =>
          tps.status() == TierPartitionStatus.ONLINE
        }
      }
    }, "Expect leadership to materialize", tierArchiver, tierTopicManager, consumerBuilder)

    // resolve BeforeLeader transitions in progress
    tierArchiver.processTransitions()

    snapshotArchiver(tierArchiver) { s =>
      s.pausedStates.foreach {
        case _: TierArchiverState.BeforeLeader =>
          fail("Expect zero BeforeLeader.")
        case _ =>
      }
    }
  }

  private def archiveAndMaterializeUntilTrue(pred: () => Boolean,
                                             msg: String,
                                             tierArchiver: TierArchiver,
                                             tierTopicManager: TierTopicManager,
                                             consumerBuilder: MockConsumerBuilder): Unit = {
    TestUtils.waitUntilTrue(() => {
      tierArchiver.processTransitions()
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()

      pred()
    }, msg, maxWaitTimeMs)
  }

  private case class Snapshot(immigrating: Seq[(Int, TopicPartition)],
                              emigrating: Seq[TopicPartition],
                              transitioning: Seq[TopicPartition],
                              pausedStates: Seq[TierArchiverState])

  private def snapshotArchiver[T](archiver: TierArchiver)(f: Snapshot => T): T = {
    val migrations = archiver.immigrationEmigrationQueue.toArray(Array.empty[ImmigratingOrEmigratingTopicPartitions])
    val immigrating = migrations.collect { case x: ImmigratingTopicPartition => (x.leaderEpoch.intValue(), x.topicPartition) }
    val emigrating = migrations.collect { case x: EmigratingTopicPartition => x.topicPartition }
    val transitioning = archiver.stateTransitionsInProgress.keys.toList
    val pausedStates = archiver.pausedStates.toArray(Array.empty[TierArchiverState])
    // PriorityQueue toArray/iteration order is not guaranteed

    pausedStates.sortWith((a, b) => TierArchiverStateComparator.compare(a, b) < 0)
    f(Snapshot(immigrating, emigrating, transitioning, pausedStates))
  }

  private def validatePartitionStateContainedInObjectStore(tierTopicManager: TierTopicManager,
                                                           tierObjectStore: MockInMemoryTierObjectStore,
                                                           logs: Iterable[AbstractLog]): Unit = {
    logs.foreach { log =>
      val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
      val tierSegmentOffsets = tierPartitionState.segmentOffsets
      tierSegmentOffsets.asScala.foreach { offset =>
        val tierObjectMetadata = tierPartitionState.metadata(offset).get
        assertNotNull(tierObjectStore.getObject(tierObjectMetadata, TierObjectStoreFileType.SEGMENT, 0, 1000).getObjectSize)
      }
    }
  }


  private def setupTierTopicManager(tierMetadataManager: TierMetadataManager): (TierTopicManager, MockConsumerBuilder) = {
    val producerBuilder = new MockProducerBuilder()
    val consumerBuilder = new MockConsumerBuilder(tierTopicManagerConfig, producerBuilder.producer())
    val bootstrapSupplier = new Supplier[String] { override def get: String = { "" } }
    val tierTopicManager = new TierTopicManager(
      tierTopicManagerConfig,
      consumerBuilder,
      producerBuilder,
      bootstrapSupplier,
      tierMetadataManager)
    tierTopicManager.becomeReady(bootstrapSupplier.get())
    (tierTopicManager, consumerBuilder)
  }

  private def createLogs(n: Int, logConfig: LogConfig, tempDir: File, tierMetadataManager: TierMetadataManager): IndexedSeq[AbstractLog] = {
    val logDirFailureChannel = new LogDirFailureChannel(n)
    (0 until n).map { i =>
      val logDir = tempDir.toPath.resolve(s"tierlogtest-$i").toFile
      logDir.mkdir()
      MergedLog(logDir, logConfig, 0L, 0L, mockTime.scheduler, new BrokerTopicStats, mockTime,
        60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs, logDirFailureChannel,
        Some(tierMetadataManager))
    }
  }

  private def mockReplicaManager(logs: Iterable[AbstractLog]): ReplicaManager = {
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.getLog(ArgumentMatchers.any(classOf[TopicPartition])))
      .thenAnswer(new Answer[Option[AbstractLog]] {
        override def answer(invocation: InvocationOnMock): Option[AbstractLog] = {
          val target: TopicPartition = invocation.getArgument(0)
          logs.find { log => log.topicPartition == target }
        }
      })
    replicaManager
  }

  private def writeRecordBatches(log: AbstractLog, leaderEpoch: Int, baseOffset: Long, batches: Int, recordsPerBatch: Int): Unit = {
    (0 until batches).foreach { idx =>
      val records = createRecords(log.topicPartition, leaderEpoch, baseOffset + idx * recordsPerBatch, recordsPerBatch)
      log.appendAsFollower(records)
      log.flush()
      log.onHighWatermarkIncremented(batches * recordsPerBatch)
    }
  }

  private def createRecords(topicPartition: TopicPartition, leaderEpoch: Int, baseOffset: Long, numRecords: Int): MemoryRecords = {
    val recList = (0 until numRecords).map { _ =>
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), "value".getBytes())
    }
    val records = TestUtils.records(records = recList, baseOffset = baseOffset)
    val filtered = ByteBuffer.allocate(100 * numRecords)
    records.batches().asScala.foreach(_.setPartitionLeaderEpoch(leaderEpoch))
    records.filterTo(topicPartition, new RecordFilter {
      override protected def checkBatchRetention(batch: RecordBatch): RecordFilter.BatchRetention = RecordFilter.BatchRetention.DELETE_EMPTY

      override protected def shouldRetainRecord(recordBatch: RecordBatch, record: Record): Boolean = true
    }, filtered, Int.MaxValue, BufferSupplier.NO_CACHING)
    filtered.flip()
    MemoryRecords.readableRecords(filtered)
  }

}
