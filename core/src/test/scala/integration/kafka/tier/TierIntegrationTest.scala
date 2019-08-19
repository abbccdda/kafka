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

import javax.management.{MBeanServer, ObjectName}
import kafka.log._
import kafka.server.{BrokerTopicStats, LogDirFailureChannel, ReplicaManager}
import kafka.tier.archiver._
import kafka.tier.client.{MockConsumerSupplier, MockProducerSupplier}
import kafka.tier.state.{FileTierPartitionStateFactory, TierPartitionStatus}
import kafka.tier.store.TierObjectStore.FileType
import kafka.tier.store.{MockInMemoryTierObjectStore, TierObjectStore, TierObjectStoreConfig}
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
  var tierArchiver: TierArchiver = _
  var replicaManager: ReplicaManager = _
  var tierObjectStore: MockInMemoryTierObjectStore = _
  var logs: Seq[MergedLog] = _
  var tierTopicManager: TierTopicManager = _
  var consumerSupplier: MockConsumerSupplier[Array[Byte], Array[Byte]] = _
  val maxWaitTimeMs = 2000L

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
    val tierArchiver = new TierArchiver(TierArchiverConfig(numArchiverThreads), replicaManager, tierMetadataManager, tierTopicManager, tierObjectStore, mockTime)

    this.tierArchiver = tierArchiver
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
    tierArchiver.shutdown()
    replicaManager.shutdown()
    tierObjectStore.close()
    tierTopicManager.shutdown()
    tierMetadataManager.close()
    logs.foreach(l => l.close())
  }

  @Test
  def testArchiverImmigrate(): Unit = {
    setup()
    tierArchiver.start()
    waitForImmigration(logs, 1, tierArchiver, tierTopicManager, consumerSupplier)
    // Emigrate one partition
    tierArchiver.taskQueue.onBecomeFollower(logs.head.topicIdPartition.get)

    TestUtils.waitUntilTrue(() => tierArchiver.taskQueue.withAllTasks(_.size == 1),
      "Archiver should process pending emigrations", 2000L)

    // Re-immigrate with valid epoch
    tierArchiver.taskQueue.onBecomeLeader(logs.head.topicIdPartition.get, 2)

    TestUtils.waitUntilTrue(() => {
      consumerSupplier.moveRecordsFromProducer()
      tierTopicManager.doWork()

      tierArchiver.taskQueue.withAllTasks(tasks => {
        tasks.forall(task => {
          task.state.isInstanceOf[BeforeUpload] || task.state.isInstanceOf[AfterUpload]
        })
      })
    }, "Archiver should process pending immigrations", 2000L)
  }

  @Test
  def testArchiverUploadAndMaterialize(): Unit = {
    setup(numLogs = 10)
    tierArchiver.start()

    val numBatches = 6
    val leaderEpoch = 1

    // Write batches
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, numBatches, 4) }

    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerSupplier)

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
    }, "Should materialize segments", tierArchiver, tierTopicManager, consumerSupplier)

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
    }, "Should materialize segments", tierArchiver, tierTopicManager, consumerSupplier)


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
    }, "Should materialize segments", tierArchiver, tierTopicManager, consumerSupplier)

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
    tierArchiver.start()

    val leaderEpoch = 1

    // Immigrate all test logs
    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerSupplier)

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
    }, "Should materialize segments", tierArchiver, tierTopicManager, consumerSupplier)

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
    tierArchiver.start()

    val batches = 3
    val recordsPerBatch = 4
    val leaderEpoch = 1

    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerSupplier)

    // Write two batches to all partitions
    logs.foreach { log => writeRecordBatches(log, leaderEpoch, 0L, batches, recordsPerBatch) }

    // two partitions should be chosen initially, and both should be materialized fully in parallel
    // and the other partition should not be materialized at al
    archiveAndMaterializeUntilTrue(() => {
      logs.count(_.tierableLogSegments.isEmpty) == nLogs
    }, s"Expected all logs to eventually become tiered",
      tierArchiver, tierTopicManager, consumerSupplier)
  }

  @Test
  def testArchiverTotalLag(): Unit = {
    val numLogs = 5
    val batches = 6
    val recordsPerBatch = 4
    val leaderEpoch = 1

    setup(numLogs)
    tierArchiver.start()

    val bean = tierArchiver.metricName("TotalLag", Map.empty).getMBeanName

    def totalLag: Long = mBeanServer
      .getAttribute(new ObjectName(bean), "Value")
      .asInstanceOf[Long]

    def awaitMaterializeBatchAndAssertLag(archivedBatches: Int): Unit = {
      archiveAndMaterializeUntilTrue(() => {
        logs.forall { log =>
          val tierPartitionState = tierTopicManager.partitionState(log.topicIdPartition.get)
          tierPartitionState.flush()
          tierPartitionState.numSegments >= archivedBatches && tierPartitionState.committedEndOffset == tierPartitionState.endOffset
        }
      }, s"Should materialize segments for batch $archivedBatches or greater", tierArchiver, tierTopicManager, consumerSupplier)

      // one more tick for the transition from AfterUpload to BeforeUpload

      assertEquals(logs.map(_.tierableLogSegments.map(_.size).sum).sum, totalLag)
    }

    // when tracking no partitions, lag should be zero
    assertEquals(0, totalLag)

    // immigrate all test logs
    waitForImmigration(logs, leaderEpoch, tierArchiver, tierTopicManager, consumerSupplier)

    assertEquals(0, totalLag)

    // write batches
    logs.foreach { log =>
      writeRecordBatches(log, leaderEpoch, 0L, batches, recordsPerBatch)
    }

    // assert initial lag based on the sum of expected log end offsets
    // minus the last segment
    assertEquals(logs.map(_.tierableLogSegments.map(_.size).sum).sum, totalLag)

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
                                 tierArchiver: TierArchiver,
                                 tierTopicManager: TierTopicManager,
                                 consumerBuilder: MockConsumerSupplier[Array[Byte], Array[Byte]]): Unit = {
    // Immigrate all test logs
    logs.foreach { log =>
      val topicIdPartition = new TopicIdPartition(log.topicPartition.topic(), UUID.randomUUID, log.topicPartition.partition())
      tierMetadataManager.becomeLeader(topicIdPartition.topicPartition(), leaderEpoch)
      tierMetadataManager.ensureTopicIdPartition(topicIdPartition)
    }

    archiveAndMaterializeUntilTrue(() => {
      logs.forall { log =>
        Option(tierTopicManager.partitionState(log.topicIdPartition.get)).exists { tps =>
          tps.status() == TierPartitionStatus.ONLINE
        }
      }
    }, "Expect leadership to materialize", tierArchiver, tierTopicManager, consumerBuilder)
    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
      tierTopicManager.doWork()
      tierArchiver.taskQueue.withAllTasks(tasks => {
        tasks.size == logs.size && tasks.forall(task => {
          task.state.isInstanceOf[BeforeUpload] || task.state.isInstanceOf[AfterUpload]
        })
      })
    }, "Expect zero BeforeLeader")
  }

  private def archiveAndMaterializeUntilTrue(pred: () => Boolean,
                                             msg: String,
                                             tierArchiver: TierArchiver,
                                             tierTopicManager: TierTopicManager,
                                             consumerBuilder: MockConsumerSupplier[Array[Byte], Array[Byte]]): Unit = {
    TestUtils.waitUntilTrue(() => {
      consumerBuilder.moveRecordsFromProducer()
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
      log.flush()
      log.updateHighWatermark(batches * recordsPerBatch)
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
