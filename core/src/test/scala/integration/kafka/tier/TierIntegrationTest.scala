/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util
import java.util.{Collections, UUID}
import java.util.function.Supplier

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import javax.management.MBeanServer
import kafka.cluster.Partition
import kafka.log._
import kafka.server.{BrokerTopicStats, LogDirFailureChannel, ReplicaManager}
import kafka.tier.tasks.archive._
import kafka.tier.client.{MockConsumerSupplier, MockProducerSupplier}
import kafka.tier.state.{TierPartitionState, TierPartitionStateFactory, TierPartitionStatus}
import kafka.tier.store.TierObjectStore.FileType
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStore, TierObjectStoreConfig}
import kafka.tier.tasks.{TierTasks, TierTasksConfig}
import kafka.tier.topic.{TierTopic, TierTopicConsumer, TierTopicManager, TierTopicManagerConfig}
import kafka.utils.{MockTime, TestUtils}
import kafka.zk.{AdminZkClient, ZooKeeperTestHarness}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConverters._

class TierIntegrationTest {
  private val mockTime = new MockTime()
  val logDirs = new util.ArrayList(Collections.singleton(TestUtils.tempDir().getAbsolutePath))

  val bootstrapSupplier = new Supplier[String] {
    override def get: String = {
      "bootstrap-server"
    }
  }
  val tierTopicManagerConfig = new TierTopicManagerConfig(
    bootstrapSupplier,
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
  var tierTopicConsumer: TierTopicConsumer = _
  var tierLogComponents: TierLogComponents = _
  var tempDir: File = _
  var tierTasks: TierTasks = _
  var replicaManager: ReplicaManager = _
  var tierObjectStore: MockInMemoryTierObjectStore = _
  var tierDeletedPartitionsCoordinator = mock(classOf[TierDeletedPartitionsCoordinator])
  var logs: Seq[MergedLog] = _
  var tierTopicManager: TierTopicManager = _
  var consumerSupplier: MockConsumerSupplier[Array[Byte], Array[Byte]] = _
  val maxWaitTimeMs = 20 * 1000
  val tierReplicaManager = new TierReplicaManager()

  val mBeanServer: MBeanServer = ManagementFactory.getPlatformMBeanServer

  def setup(numLogs: Integer = 2, numArchiverThreads: Integer = 10): Unit = {
    this.tierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig("cluster", 1))
    setupTierComponents()

    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(numLogs, logConfig, tempDir, tierLogComponents)
    val replicaManager = mockReplicaManager(logs)
    val tierTasks = new TierTasks(TierTasksConfig(numArchiverThreads, mainLoopBackoffMs = 20, maxRetryBackoffMs = 20),
      replicaManager, tierReplicaManager, tierDeletedPartitionsCoordinator, tierTopicManager, tierObjectStore, mockTime)

    this.tierTasks = tierTasks
    this.replicaManager = replicaManager
    this.logs = logs
    this.tempDir = tempDir
  }

  @Before
  def before(): Unit = {
    ZooKeeperTestHarness.verifyNoUnexpectedThreads("@Before")
  }

  @After
  def teardown(): Unit = {
    tierTasks.shutdown()
    tierTopicManager.shutdown()
    tierTopicConsumer.shutdown()
    replicaManager.shutdown()
    tierObjectStore.close()
    logs.foreach(_.close())
    logDirs.asScala.foreach { path => Utils.delete(new File(path)) }
    ZooKeeperTestHarness.verifyNoUnexpectedThreads("@After")
  }

  @Test
  def testArchiverImmigrate(): Unit = {
    setup()
    tierTasks.start()
    waitForImmigration(logs, 1, tierTasks, tierTopicManager, consumerSupplier)
    // Emigrate one partition
    tierReplicaManager.becomeFollower(tierPartitionState(logs.head.topicPartition))

    TestUtils.waitUntilTrue(() => tierTasks.archiverTaskQueue.withAllTasks(_.size == 1),
      "Archiver should process pending emigrations", 2000L)

    // Re-immigrate with valid epoch
    tierReplicaManager.becomeLeader(tierPartitionState(logs.head.topicPartition), 2)

    TestUtils.waitUntilTrue(() => {
      consumerSupplier.moveRecordsFromProducer()
      tierTopicConsumer.doWork()
      tierTasks.archiverTaskQueue.withAllTasks { tasks =>
        tasks.forall { task =>
          task.state.isInstanceOf[BeforeUpload] || task.state.isInstanceOf[AfterUpload]
        }
      }
    }, "Archiver should process pending immigrations", 2000L)
  }

  @Test
  def testArchiverUploadAndMaterialize(): Unit = {
    setup(numLogs = 10)
    tierTasks.start()

    val numBatches = 6
    val leaderEpoch = 1

    // Write batches
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, numBatches, 4) }

    waitForImmigration(logs, leaderEpoch, tierTasks, tierTopicManager, consumerSupplier)

    logs.foreach { log =>
      assertEquals(s"topic manager should materialize entry for ${log.topicPartition}",
        log.tierPartitionState.tierEpoch, leaderEpoch)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize at least the first segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        val tierPartitionState = log.tierPartitionState
        tierPartitionState.flush()
        tierPartitionState.numSegments >= 1
      }
    }, "Should materialize segments", tierTopicManager, consumerSupplier)

    logs.foreach { log =>
      assertEquals("batch 1: segment should be materialized with correct offset relationship",
        0L, log.tierPartitionState.metadata(0).get().baseOffset)
      assertTrue("batch 1: segment should be materialized with correct end offset",
        log.tierPartitionState.committedEndOffset.get() >= 3L)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize at least the second segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        val tierPartitionState = log.tierPartitionState
        tierPartitionState.flush()
        tierPartitionState.numSegments >= 2 && tierPartitionState.committedEndOffset == tierPartitionState.endOffset
      }
    }, "Should materialize segments", tierTopicManager, consumerSupplier)


    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    logs.foreach { log =>
      val tierPartitionState = log.tierPartitionState
      assertEquals("batch 2: segment should be materialized with correct offset relationship",
        4L, tierPartitionState.metadata(6).get().baseOffset)
      assertTrue("batch 2: segment should be materialized with correct end offset",
        tierPartitionState.committedEndOffset.get() >= 7L)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize the third segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        val tierPartitionState = log.tierPartitionState
        tierPartitionState.flush()
        tierPartitionState.numSegments >= 3 && tierPartitionState.committedEndOffset == tierPartitionState.endOffset
      }
    }, "Should materialize segments", tierTopicManager, consumerSupplier)

    logs.foreach { log =>
      assertEquals("batch 3: segment should be materialized with correct offset relationship",
        8L, log.tierPartitionState.metadata(10).get().baseOffset)
      assertTrue("batch 3: segment should be materialized with correct end offset",
        log.tierPartitionState.committedEndOffset.get() >= 11L)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)
  }

  @Test
  def testArchiverUploadAndMaterializeWhenWriteHappensAfterBecomeLeader(): Unit = {
    setup(numLogs = 10)
    tierTasks.start()

    val leaderEpoch = 1

    // Immigrate all test logs
    waitForImmigration(logs, leaderEpoch, tierTasks, tierTopicManager, consumerSupplier)

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Write batches
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, 6, 4) }

    // Wait for the first segments to materialize
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        val tierPartitionState = log.tierPartitionState
        tierPartitionState.flush()
        tierPartitionState.numSegments > 0 && tierPartitionState.committedEndOffset == tierPartitionState.endOffset
      }
    }, "Should materialize segments", tierTopicManager, consumerSupplier)

    logs.foreach { log =>
      assertEquals("Segment should be materialized with correct offset relationship",
        0L, log.tierPartitionState.metadata(0).get().baseOffset)
      assertTrue("Segment should be materialized with correct end offset",
        log.tierPartitionState.committedEndOffset.get() >= 3)
    }
    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)
  }

  @Test
  def testArchiverUploadWithLimitedUploadConcurrency(): Unit = {
    val maxConcurrentUploads = 2
    val nLogs = 3
    setup(nLogs, maxConcurrentUploads)
    tierTasks.start()

    val batches = 3
    val recordsPerBatch = 4
    val leaderEpoch = 1

    waitForImmigration(logs, leaderEpoch, tierTasks, tierTopicManager, consumerSupplier)

    // Write two batches to all partitions
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, batches, recordsPerBatch) }

    // eventually, we would have tiered all segments
    archiveAndMaterializeUntilTrue(() => {
      logs.forall(_.tierableLogSegments.isEmpty) && logs.forall(_.tieredLogSegments.toIterator.nonEmpty)
    }, s"Expected all logs to eventually become tiered", tierTopicManager, consumerSupplier)
  }

  @Test
  def testArchiverTotalLag(): Unit = {
    val numLogs = 5
    val batches = 6
    val recordsPerBatch = 4
    val leaderEpoch = 1

    setup(numLogs)
    tierTasks.start()

    def totalLag: Long = metricValue("TotalLag")

    def awaitMaterializeBatchAndAssertLag(archivedBatches: Int): Unit = {
      archiveAndMaterializeUntilTrue(() => {
        logs.forall { log =>
          val tierPartitionState = log.tierPartitionState
          tierPartitionState.flush()
          tierPartitionState.numSegments >= archivedBatches && tierPartitionState.committedEndOffset == tierPartitionState.endOffset
        }
      }, s"Should materialize segments for batch $archivedBatches or greater", tierTopicManager, consumerSupplier)

      // one more tick for the transition from AfterUpload to BeforeUpload
      assertEquals(logs.map(_.tierableLogSegments.map(_.size).sum).sum, totalLag)
    }

    // when tracking no partitions, lag should be zero
    assertEquals(0, totalLag)

    // become leader for all partitions
    logs.foreach { log =>
      val topicIdPartition = new TopicIdPartition(log.topicPartition.topic(), UUID.randomUUID, log.topicPartition.partition())
      log.assignTopicId(topicIdPartition.topicId)
      tierReplicaManager.becomeLeader(log.tierPartitionState, leaderEpoch)
    }

    assertEquals(0, totalLag)

    // write batches
    logs.foreach { log =>
      writeRecordBatches(log, leaderEpoch, 0L, batches, recordsPerBatch)
    }

    // assert initial lag based on the sum of expected log end offsets
    // minus the last segment
    assertEquals(logs.map(_.tierableLogSegments.map(_.size).sum).sum, totalLag)

    // wait until all partitions are immigrated
    waitForImmigration(logs, leaderEpoch, tierTasks, tierTopicManager, consumerSupplier, becomeLeader = false)

    // assert lag after each batch is materialized by the archiver
    (1 until batches).foreach(awaitMaterializeBatchAndAssertLag)
  }

  /**
    * For a sequence of logs, do the following:
    *  1. Ensure the archiver is the leader for each log TopicPartition.
    *  2. Ensure that the TierPartitionState becomes ONLINE for all topic partitions.
    *  3. Issue an archiver state transition to move from BeforeLeader => BeforeUpload.
    */
  private def waitForImmigration(logs: Seq[MergedLog],
                                 leaderEpoch: Int,
                                 tierTasks: TierTasks,
                                 tierTopicManager: TierTopicManager,
                                 consumerSupplier: MockConsumerSupplier[Array[Byte], Array[Byte]],
                                 becomeLeader: Boolean = true): Unit = {
    // Immigrate all test logs
    if (becomeLeader) {
      logs.foreach { log =>
        val topicIdPartition = new TopicIdPartition(log.topicPartition.topic(), UUID.randomUUID, log.topicPartition.partition())
        log.assignTopicId(topicIdPartition.topicId)
        tierReplicaManager.becomeLeader(log.tierPartitionState, leaderEpoch)
      }
    }

    archiveAndMaterializeUntilTrue(() => logs.forall(_.tierPartitionState.status == TierPartitionStatus.ONLINE),
      "Expect leadership to materialize", tierTopicManager, consumerSupplier)

    TestUtils.waitUntilTrue(() => {
      consumerSupplier.moveRecordsFromProducer()
      tierTopicConsumer.doWork()
      tierTasks.archiverTaskQueue.withAllTasks { tasks =>
        tasks.size == logs.size && !tasks.forall(_.state.isInstanceOf[BeforeLeader])
      }
    }, s"Expect zero BeforeLeader in ${tierTasks.archiverTaskQueue}")
  }

  private def archiveAndMaterializeUntilTrue(pred: () => Boolean,
                                             msg: String,
                                             tierTopicManager: TierTopicManager,
                                             consumerSupplier: MockConsumerSupplier[Array[Byte], Array[Byte]]): Unit = {
    TestUtils.waitUntilTrue(() => {
      consumerSupplier.moveRecordsFromProducer()
      tierTopicConsumer.doWork()
      pred()
    }, msg, maxWaitTimeMs)
  }

  private def validatePartitionStateContainedInObjectStore(tierTopicManager: TierTopicManager,
                                                           tierObjectStore: MockInMemoryTierObjectStore,
                                                           logs: Iterable[AbstractLog]): Unit = {
    logs.foreach { log =>
      val tierPartitionState = log.tierPartitionState
      val tierSegmentOffsets = tierPartitionState.segmentOffsets
      tierSegmentOffsets.asScala.foreach { offset =>
        val tierObjectMetadata = tierPartitionState.metadata(offset).get
        assertNotNull(tierObjectStore.getObject(new TierObjectStore.ObjectMetadata(tierObjectMetadata), FileType.SEGMENT, 0, 1000).getObjectSize)
      }
    }
  }

  private def setupTierComponents(): Unit = {
    val producerSupplier = new MockProducerSupplier[Array[Byte], Array[Byte]]()
    consumerSupplier = new MockConsumerSupplier[Array[Byte], Array[Byte]]("primary",
      TierTopicManager.partitions(TierTopic.topicName(tierTopicManagerConfig.tierNamespace), tierTopicManagerConfig.configuredNumPartitions),
      producerSupplier.producer)

    val adminZkClientSupplier = new Supplier[AdminZkClient] {
      override def get(): AdminZkClient = mock(classOf[AdminZkClient])
    }

    tierTopicConsumer = new TierTopicConsumer(tierTopicManagerConfig,
      consumerSupplier,
      consumerSupplier,
      new TierTopicManagerCommitter(tierTopicManagerConfig, EasyMock.mock(classOf[LogDirFailureChannel])))

    tierTopicManager = new TierTopicManager(tierTopicManagerConfig,
      tierTopicConsumer,
      producerSupplier,
      adminZkClientSupplier,
      bootstrapSupplier)

    tierLogComponents = TierLogComponents(Some(tierTopicConsumer), Some(tierObjectStore), new TierPartitionStateFactory(true))

    TestUtils.waitUntilTrue(() => {
      tierTopicManager.tryBecomeReady(false)
      tierTopicManager.isReady
    }, "Timed out waiting for TierTopicManager to be ready")
  }

  private def createLogs(n: Int, logConfig: LogConfig, tempDir: File, tierLogComponents: TierLogComponents): IndexedSeq[MergedLog] = {
    val logDirFailureChannel = new LogDirFailureChannel(n)
    (0 until n).map { i =>
      val logDir = tempDir.toPath.resolve(s"tierlogtest-$i").toFile
      logDir.mkdir()
      MergedLog(logDir, logConfig, 0L, 0L, mockTime.scheduler, new BrokerTopicStats, mockTime,
        60 * 60 * 1000, LogManager.ProducerIdExpirationCheckIntervalMs, logDirFailureChannel,
        tierLogComponents)
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

    val partitions = logs.map { log =>
      val partition = mock(classOf[Partition])
      when(partition.log).thenReturn(Some(log))
      partition
    }
    when(replicaManager.leaderPartitionsIterator).thenAnswer(new Answer[Iterator[Partition]] {
      override def answer(invocation: InvocationOnMock): Iterator[Partition] = partitions.toIterator
    })

    replicaManager
  }

  private def writeRecordBatches(log: AbstractLog, leaderEpoch: Int, baseOffset: Long, batches: Int, recordsPerBatch: Int): Unit = {
    (0 until batches).foreach { idx =>
      val records = createRecords(log.topicPartition, leaderEpoch, baseOffset + idx * recordsPerBatch, recordsPerBatch)
      log.appendAsFollower(records)
    }
    log.flush()
    log.updateHighWatermark(batches * recordsPerBatch)
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

  private def metricValue(name: String): Long = {
    Metrics.defaultRegistry.allMetrics.asScala.filterKeys(_.getName == name).values.head.asInstanceOf[Gauge[Long]].value()
  }

  private def tierPartitionState(partition: TopicPartition): TierPartitionState = {
    logs.find(_.topicPartition == partition).map(_.tierPartitionState).get
  }
}
