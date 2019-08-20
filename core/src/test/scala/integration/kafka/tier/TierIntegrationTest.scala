/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util
import java.util.{Collections, Optional, UUID}
import java.util.function.Supplier

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.Gauge
import javax.management.MBeanServer
import kafka.log._
import kafka.server.{BrokerTopicStats, LogDirFailureChannel, ReplicaManager}
import kafka.tier.tasks.archive._
import kafka.tier.client.{MockConsumerSupplier, MockProducerSupplier}
import kafka.tier.state.{FileTierPartitionStateFactory, TierPartitionStatus}
import kafka.tier.store.TierObjectStore.FileType
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStore, TierObjectStoreConfig}
import kafka.tier.tasks.{TierTasks, TierTasksConfig}
import kafka.tier.topic.{TierTopic, TierTopicManager, TierTopicManagerConfig}
import kafka.utils.{MockTime, TestUtils}
import kafka.zk.AdminZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record._
import org.easymock.EasyMock
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
  var tierMetadataManager: TierMetadataManager = _
  var tempDir: File = _
  var tierTasks: TierTasks = _
  var replicaManager: ReplicaManager = _
  var tierObjectStore: MockInMemoryTierObjectStore = _
  var logs: Seq[MergedLog] = _
  var tierTopicManager: TierTopicManager = _
  var consumerSupplier: MockConsumerSupplier[Array[Byte], Array[Byte]] = _
  val maxWaitTimeMs = 20 * 1000

  val mBeanServer: MBeanServer = ManagementFactory.getPlatformMBeanServer

  def setup(numLogs: Integer = 2, numArchiverThreads: Integer = 10): Unit = {
    val tierObjectStore = new MockInMemoryTierObjectStore(new TierObjectStoreConfig("cluster", 1))
    val tierMetadataManager = new TierMetadataManager(new FileTierPartitionStateFactory(),
      Optional.of(tierObjectStore), new LogDirFailureChannel(1), true)
    val (tierTopicManager, consumerBuilder) = setupTierTopicManager(tierMetadataManager)
    val logConfig = LogTest.createLogConfig(segmentBytes = 150, indexIntervalBytes = 1, maxMessageBytes = 64 * 1024, tierEnable = true)
    val tempDir = TestUtils.tempDir()
    val logs = createLogs(numLogs, logConfig, tempDir, tierMetadataManager)
    val replicaManager = mockReplicaManager(logs)
    val tierTasks = new TierTasks(TierTasksConfig(numArchiverThreads, mainLoopBackoffMs = 20, maxRetryBackoffMs = 20),
      replicaManager, tierMetadataManager, tierTopicManager, tierObjectStore, mockTime)

    this.tierTasks = tierTasks
    this.replicaManager = replicaManager
    this.tierObjectStore = tierObjectStore
    this.tierMetadataManager = tierMetadataManager
    this.logs = logs
    this.tierTopicManager = tierTopicManager
    this.consumerSupplier = consumerBuilder
    this.tempDir = tempDir
  }

  @After
  def teardown(): Unit = {
    tierTasks.shutdown()
    replicaManager.shutdown()
    tierObjectStore.close()
    tierTopicManager.shutdown()
    tierMetadataManager.close()
    logs.foreach(l => l.close())
  }

  @Test
  def testArchiverImmigrate(): Unit = {
    setup()
    tierTasks.start()
    waitForImmigration(logs, 1, tierTasks, tierTopicManager, consumerSupplier)
    // Emigrate one partition
    tierMetadataManager.becomeFollower(logs.head.topicPartition)

    TestUtils.waitUntilTrue(() => tierTasks.archiverTaskQueue.withAllTasks(_.size == 1),
      "Archiver should process pending emigrations", 2000L)

    // Re-immigrate with valid epoch
    tierMetadataManager.becomeLeader(logs.head.topicPartition, 2)

    TestUtils.waitUntilTrue(() => {
      consumerSupplier.moveRecordsFromProducer()
      tierTopicManager.doWork()

      tierTasks.archiverTaskQueue.withAllTasks(tasks => {
        tasks.forall(task => {
          task.state.isInstanceOf[BeforeUpload] || task.state.isInstanceOf[AfterUpload]
        })
      })
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
        tierTopicManager.partitionState(log.topicIdPartition.get).tierEpoch,
        leaderEpoch)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize at least the first segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        val tierPartitionState = tierTopicManager.partitionState(log.topicIdPartition.get)
        tierPartitionState.flush()
        tierPartitionState.numSegments >= 1
      }
    }, "Should materialize segments", tierTopicManager, consumerSupplier)

    logs.foreach { log =>
      assertEquals("batch 1: segment should be materialized with correct offset relationship",
        0L, tierTopicManager.partitionState(log.topicIdPartition.get).metadata(0).get().baseOffset)
      assertTrue("batch 1: segment should be materialized with correct end offset",
        tierTopicManager.partitionState(log.topicIdPartition.get).committedEndOffset.get() >= 3L)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize at least the second segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        val tierPartitionState = tierTopicManager.partitionState(log.topicIdPartition.get)
        tierPartitionState.flush()
        tierPartitionState.numSegments >= 2 && tierPartitionState.committedEndOffset == tierPartitionState.endOffset
      }
    }, "Should materialize segments", tierTopicManager, consumerSupplier)


    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    logs.foreach { log =>
      assertEquals("batch 2: segment should be materialized with correct offset relationship",
        4L, tierTopicManager.partitionState(log.topicIdPartition.get).metadata(6).get().baseOffset)
      assertTrue("batch 2: segment should be materialized with correct end offset",
        tierTopicManager.partitionState(log.topicIdPartition.get).committedEndOffset.get() >= 7L)
    }

    validatePartitionStateContainedInObjectStore(tierTopicManager, tierObjectStore, logs)

    // Materialize the third segment for each
    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        val tierPartitionState = tierTopicManager.partitionState(log.topicIdPartition.get)
        tierPartitionState.flush()
        tierPartitionState.numSegments >= 3 && tierPartitionState.committedEndOffset == tierPartitionState.endOffset
      }
    }, "Should materialize segments", tierTopicManager, consumerSupplier)

    logs.foreach { log =>
      assertEquals("batch 3: segment should be materialized with correct offset relationship",
        8L, tierTopicManager.partitionState(log.topicIdPartition.get).metadata(10).get().baseOffset)
      assertTrue("batch 3: segment should be materialized with correct end offset",
        tierTopicManager.partitionState(log.topicIdPartition.get).committedEndOffset.get() >= 11L)
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
        val tierPartitionState = tierTopicManager.partitionState(log.topicIdPartition.get)
        tierPartitionState.flush()
        tierPartitionState.numSegments > 0 && tierPartitionState.committedEndOffset == tierPartitionState.endOffset
      }
    }, "Should materialize segments", tierTopicManager, consumerSupplier)

    logs.foreach { log =>
      assertEquals("Segment should be materialized with correct offset relationship",
        0L, tierTopicManager.partitionState(log.topicIdPartition.get).metadata(0).get().baseOffset)
      assertTrue("Segment should be materialized with correct end offset",
        tierTopicManager.partitionState(log.topicIdPartition.get).committedEndOffset.get() >= 3)
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
          val tierPartitionState = tierTopicManager.partitionState(log.topicIdPartition.get)
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
      tierMetadataManager.becomeLeader(topicIdPartition.topicPartition, leaderEpoch)
      tierMetadataManager.ensureTopicIdPartition(topicIdPartition)
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
        tierMetadataManager.becomeLeader(topicIdPartition.topicPartition(), leaderEpoch)
        tierMetadataManager.ensureTopicIdPartition(topicIdPartition)
      }
    }

    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        Option(tierTopicManager.partitionState(log.topicIdPartition.get)).exists { tps =>
          tps.status() == TierPartitionStatus.ONLINE
        }
      }
    }, "Expect leadership to materialize", tierTopicManager, consumerSupplier)

    TestUtils.waitUntilTrue(() => {
      consumerSupplier.moveRecordsFromProducer()
      tierTopicManager.doWork()
      tierTasks.archiverTaskQueue.withAllTasks(tasks => {
        tasks.size == logs.size && tasks.forall(task => {
          !task.state.isInstanceOf[BeforeLeader]
        })
      })
    }, s"Expect zero BeforeLeader in ${tierTasks.archiverTaskQueue}")
  }

  private def archiveAndMaterializeUntilTrue(pred: () => Boolean,
                                             msg: String,
                                             tierTopicManager: TierTopicManager,
                                             consumerSupplier: MockConsumerSupplier[Array[Byte], Array[Byte]]): Unit = {
    TestUtils.waitUntilTrue(() => {
      consumerSupplier.moveRecordsFromProducer()
      tierTopicManager.doWork()
      pred()
    }, msg, maxWaitTimeMs)
  }

  private def validatePartitionStateContainedInObjectStore(tierTopicManager: TierTopicManager,
                                                           tierObjectStore: MockInMemoryTierObjectStore,
                                                           logs: Iterable[AbstractLog]): Unit = {
    logs.foreach { log =>
      val tierPartitionState = tierMetadataManager.tierPartitionState(log.topicPartition).get
      val tierSegmentOffsets = tierPartitionState.segmentOffsets
      tierSegmentOffsets.asScala.foreach { offset =>
        val tierObjectMetadata = tierPartitionState.metadata(offset).get
        assertNotNull(tierObjectStore.getObject(new TierObjectStore.ObjectMetadata(tierObjectMetadata), FileType.SEGMENT, 0, 1000).getObjectSize)
      }
    }
  }


  private def setupTierTopicManager(tierMetadataManager: TierMetadataManager): (TierTopicManager, MockConsumerSupplier[Array[Byte], Array[Byte]]) = {
    val producerSupplier = new MockProducerSupplier[Array[Byte], Array[Byte]]()
    val consumerSupplier = new MockConsumerSupplier[Array[Byte], Array[Byte]]("primary",
      TierTopicManager.partitions(TierTopic.topicName(tierTopicManagerConfig.tierNamespace), tierTopicManagerConfig.configuredNumPartitions),
      producerSupplier.producer)

    val adminZkClientSupplier = new Supplier[AdminZkClient] {
      override def get(): AdminZkClient = mock(classOf[AdminZkClient])
    }

    val tierTopicManager = new TierTopicManager(
      tierTopicManagerConfig,
      consumerSupplier,
      consumerSupplier,
      producerSupplier,
      adminZkClientSupplier,
      bootstrapSupplier,
      tierMetadataManager,
      EasyMock.mock(classOf[LogDirFailureChannel]))

    TestUtils.waitUntilTrue(() => {
      tierTopicManager.tryBecomeReady()
      tierTopicManager.isReady
    }, "Timed out waiting for TierTopicManager to be ready")

    (tierTopicManager, consumerSupplier)
  }

  private def createLogs(n: Int, logConfig: LogConfig, tempDir: File, tierMetadataManager: TierMetadataManager): IndexedSeq[MergedLog] = {
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
}
