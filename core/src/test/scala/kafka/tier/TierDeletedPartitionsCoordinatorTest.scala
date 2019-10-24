/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier

import java.io.File
import java.nio.ByteBuffer
import java.util.UUID

import kafka.log._
import kafka.server.{BrokerTopicStats, ReplicaManager}
import kafka.tier.domain._
import kafka.tier.store.TierObjectStore
import kafka.tier.topic.TierTopicConsumer
import kafka.utils.{MockTime, Scheduler, TestUtils}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.utils.Time
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._

import scala.collection.JavaConverters._
import scala.collection.mutable

class TierDeletedPartitionsCoordinatorTest {
  private val replicaManager = mock(classOf[ReplicaManager])
  private val tierTopicConsumer = mock(classOf[TierTopicConsumer])
  private val time = new MockTime()
  private val scheduler = time.scheduler
  private val tieredObjects = mutable.Map[TopicIdPartition, List[TierObjectStore.ObjectMetadata]]()
  private val deletedPartitionsCoordinator = new TierDeletedPartitionsCoordinator(scheduler, replicaManager, tierTopicConsumer,
    tierDeletedPartitionsIntervalMs = 1, tierNamespace = "foo", time)
  private val dir = TestUtils.tempDir()
  private val logDir = TestUtils.randomPartitionLogDir(dir)
  private val tierTopicPartition = Log.parseTopicPartitionName(logDir)

  @Before
  def setup(): Unit = {
    deletedPartitionsCoordinator.registerListener(new Listener())
  }

  @After
  def teardown(): Unit = {
    dir.delete()
  }

  @Test
  def testInitiateDeletion(): Unit = {
    // tier topic partition id -> topic partition
    val deletedPartitionsMap = Map(0 -> Set(new TopicIdPartition("foo-1", UUID.randomUUID, 0), new TopicIdPartition("foo-2", UUID.randomUUID, 2)),
      3 -> Set(new TopicIdPartition("foo-3", UUID.randomUUID, 0)),
      5 -> Set[TopicIdPartition]())

    deletedPartitionsMap.keySet.foreach { tierTopicPartitionId =>
      deletedPartitionsCoordinator.handleImmigration(tierTopicPartitionId)
    }

    deletedPartitionsMap.foreach { case (tierTopicPartitionId, deletedPartitions) =>
      deletedPartitions.foreach { deletedPartition =>
        deletedPartitionsCoordinator.trackInitiatePartitionDelete(tierTopicPartitionId, deletedPartition)
      }
    }
    val immigratedPartitions = deletedPartitionsCoordinator.immigratedPartitions
    assertEquals(deletedPartitionsMap, immigratedPartitions.map { case (tierTopicPartitionId, immigratedPartition) =>
      (tierTopicPartitionId, immigratedPartition.pendingDeletions)
    })

    // initiate delete for untracked tier topic partition should be a NOOP
    deletedPartitionsCoordinator.trackInitiatePartitionDelete(tierTopicPartitionId = 1, new TopicIdPartition("foo", UUID.randomUUID, 0))
    assertEquals(deletedPartitionsMap, immigratedPartitions.map { case (tierTopicPartitionId, immigratedPartition) =>
      (tierTopicPartitionId, immigratedPartition.pendingDeletions)
    })
  }

  @Test
  def testCompleteDeletion(): Unit = {
    val foo_1 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val foo_2 = new TopicIdPartition("foo-2", UUID.randomUUID, 2)
    val foo_3 = new TopicIdPartition("foo-3", UUID.randomUUID, 5)

    val deletedPartitionsMap = Map(0 -> Set(foo_1, foo_2),
      3 -> Set(foo_3),
      5 -> Set[TopicIdPartition]())

    deletedPartitionsMap.keySet.foreach { tierTopicPartitionId =>
      deletedPartitionsCoordinator.handleImmigration(tierTopicPartitionId)
    }

    deletedPartitionsMap.foreach { case (tierTopicPartitionId, deletedPartitions) =>
      deletedPartitions.foreach { deletedPartition =>
        deletedPartitionsCoordinator.trackInitiatePartitionDelete(tierTopicPartitionId, deletedPartition)
      }
    }

    // mark foo-1 and foo-3 as completed
    deletedPartitionsCoordinator.trackCompletePartitionDelete(0, foo_1)
    deletedPartitionsCoordinator.trackCompletePartitionDelete(3, foo_3)

    // foo-2 should now be the only partition queued for deletion
    val immigratedPartitions = deletedPartitionsCoordinator.immigratedPartitions
    assertEquals(Set(foo_2), immigratedPartitions(0).pendingDeletions)
    assertEquals(Set(), immigratedPartitions(3).pendingDeletions)
    assertEquals(Set(), immigratedPartitions(5).pendingDeletions)
    assertEquals(3, immigratedPartitions.size)

    // emigrate tier topic partition 0
    deletedPartitionsCoordinator.handleEmigration(0)
    assertEquals(Set(3, 5), immigratedPartitions.keySet)
  }

  @Test
  def testBeginMaterialization(): Unit = {
    // tier topic partition id -> topic partition
    val foo_1 = 0 -> new TopicIdPartition("foo-1", UUID.randomUUID, 0)
    val foo_2 = 0 -> new TopicIdPartition("foo-2", UUID.randomUUID, 2)
    val foo_3 = 0 -> new TopicIdPartition("foo-3", UUID.randomUUID, 2)
    val foo_4 = 3 -> new TopicIdPartition("foo-4", UUID.randomUUID, 5)

    deletedPartitionsCoordinator.handleImmigration(0)
    deletedPartitionsCoordinator.handleImmigration(3)

    // initiate deletion for foo_1, foo_2 and foo_4
    deletedPartitionsCoordinator.trackInitiatePartitionDelete(foo_1._1, foo_1._2)
    deletedPartitionsCoordinator.trackInitiatePartitionDelete(foo_2._1, foo_2._2)
    deletedPartitionsCoordinator.trackInitiatePartitionDelete(foo_4._1, foo_4._2)

    // begin materialization
    deletedPartitionsCoordinator.maybeBeginMaterialization()

    // partition deletion should now be in progress for foo_1, foo_2 and foo_4
    val immigratedPartitions = deletedPartitionsCoordinator.immigratedPartitions
    assertTrue(immigratedPartitions(foo_1._1).inProgressDeletions.contains(foo_1._2))
    assertTrue(immigratedPartitions(foo_2._1).inProgressDeletions.contains(foo_2._2))
    assertTrue(immigratedPartitions(foo_4._1).inProgressDeletions.contains(foo_4._2))
    assertEquals(0, immigratedPartitions.values.map(_.pendingDeletions.size).sum)

    verify(tierTopicConsumer, times(1))
      .register(ArgumentMatchers.argThat(new ArgumentMatcher[java.util.Map[TopicIdPartition, TierTopicConsumer.ClientCtx]]() {
        override def matches(argument: java.util.Map[TopicIdPartition, TierTopicConsumer.ClientCtx]): Boolean = {
          argument.keySet.asScala == Set(foo_1._2, foo_2._2, foo_4._2)
        }
      }))

    verifyNoMoreInteractions(tierTopicConsumer)

    // initiate deletion for foo_3
    deletedPartitionsCoordinator.trackInitiatePartitionDelete(foo_3._1, foo_3._2)

    // calling begin materialization should be a NOOP because we already have a materialization in progress for partition 0
    deletedPartitionsCoordinator.maybeBeginMaterialization()
    assertEquals(Set(foo_3._2), immigratedPartitions(foo_3._1).pendingDeletions)

    // complete deletion for foo_1 and foo_2
    deletedPartitionsCoordinator.trackCompletePartitionDelete(foo_1._1, foo_1._2)
    deletedPartitionsCoordinator.trackCompletePartitionDelete(foo_2._1, foo_2._2)

    // begin materialization should now move foo_3 to in progress
    deletedPartitionsCoordinator.maybeBeginMaterialization()
    assertEquals(Set(foo_3._2), immigratedPartitions(foo_3._1).inProgressDeletions.keySet)
  }

  @Test
  def testDeletePartition(): Unit = {
    val tierTopicPartition = 1
    val deletedPartition_1 = new TopicIdPartition("foo", UUID.randomUUID, 0)
    val deletedPartition_2 = new TopicIdPartition("bar", UUID.randomUUID, 0)

    val tieredSegments_1 = for (i <- 0 until 5) yield new TierObjectStore.ObjectMetadata(deletedPartition_1, UUID.randomUUID, 0, i, false)
    tieredObjects += (deletedPartition_1 -> tieredSegments_1.toList)

    // immigrate partition
    deletedPartitionsCoordinator.handleImmigration(tierTopicPartition)

    // initiate deletion
    deletedPartitionsCoordinator.trackInitiatePartitionDelete(tierTopicPartition, deletedPartition_1)
    deletedPartitionsCoordinator.trackInitiatePartitionDelete(tierTopicPartition, deletedPartition_2)
    assertEquals(Set(deletedPartition_1, deletedPartition_2), deletedPartitionsCoordinator.immigratedPartitions(tierTopicPartition).pendingDeletions)

    // begin materialization
    deletedPartitionsCoordinator.maybeBeginMaterialization()
    val inProgressPartitions = deletedPartitionsCoordinator.immigratedPartitions(tierTopicPartition).inProgressDeletions
    assertEquals(2, inProgressPartitions.size)
    assertEquals(Set(deletedPartition_1, deletedPartition_2), inProgressPartitions.keySet)
    assertEquals(List(MaterializingState, MaterializingState), inProgressPartitions.values.map(_.currentState))
    assertTrue(deletedPartitionsCoordinator.immigratedPartitions(tierTopicPartition).pendingDeletions.isEmpty)

    // simulate reading upload initiated messages for partition 1
    val inProgress_1 = inProgressPartitions(deletedPartition_1)
    tieredSegments_1.foreach { segment =>
      inProgress_1.process(new TierSegmentUploadInitiate(segment.topicIdPartition, segment.tierEpoch, segment.objectId,
        segment.baseOffet, segment.baseOffet + 1, 0, 100, false, false, false))
    }

    // simulate reading of deleteInitiated message for partition 1 ==> signals completion of materialization
    inProgress_1.process(new TierPartitionDeleteInitiate(deletedPartition_1, 0, UUID.randomUUID))
    assertEquals(MaterializationComplete, inProgress_1.deletionState)

    // begin deletion for partition 1
    deletedPartitionsCoordinator.maybeBeginDeletion()
    assertEquals(AwaitingDeleteComplete, inProgress_1.deletionState)

    // complete deletion for partition 1
    deletedPartitionsCoordinator.trackCompletePartitionDelete(tierTopicPartition, deletedPartition_1)

    // partition 2 will continue to be tracked as in progress
    assertEquals(Set(deletedPartition_2), inProgressPartitions.keySet)
    assertEquals(List(MaterializingState), inProgressPartitions.values.map(_.currentState))
  }

  @Test
  def testCollectDeletedPartitions(): Unit = {
    val log = createLog(logDir)
    val leaderEpoch = 0

    // immigrate partition
    deletedPartitionsCoordinator.handleImmigration(tierTopicPartition.partition)

    when(replicaManager.getLog(tierTopicPartition)).thenReturn(Some(log))

    val topicIdPartition_1 = new TopicIdPartition("foo", UUID.randomUUID, 0)
    val topicIdPartition_2 = new TopicIdPartition("bar", UUID.randomUUID, 3)

    val records = List(
      initiateSegmentUpload(topicIdPartition_1, leaderEpoch = 0, UUID.randomUUID, baseOffset = 0, endOffset = 100),
      initiatePartitionDeletion(topicIdPartition_1),
      initiateSegmentUpload(topicIdPartition_2, leaderEpoch = 0, UUID.randomUUID, baseOffset = 100, endOffset = 1000),
      initiatePartitionDeletion(topicIdPartition_2),
      completePartitionDeletion(topicIdPartition_1)
    )

    records.foreach { record =>
      log.appendAsLeader(record, leaderEpoch)
    }
    log.updateHighWatermark(log.logEndOffset)

    val buffer = ByteBuffer.allocate(200)
    val (lastReadOffset, _) = deletedPartitionsCoordinator.collectDeletedPartitions(tierTopicPartition, 0, buffer)

    // should have read until the end of the log
    assertEquals(log.logEndOffset, lastReadOffset)

    // validate tracked deleted partitions
    val immigratedPartition = deletedPartitionsCoordinator.immigratedPartitions(tierTopicPartition.partition)
    assertEquals(List(topicIdPartition_2), immigratedPartition.pendingDeletions.toList)
  }

  private def initiateSegmentUpload(topicIdPartition: TopicIdPartition,
                                    leaderEpoch: Int,
                                    objectId: UUID,
                                    baseOffset: Long,
                                    endOffset: Long): MemoryRecords = {
    val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition,
      leaderEpoch,
      objectId,
      baseOffset,
      endOffset,
      10,
      500,
      true,
      true,
      true)
    memoryRecords(uploadInitiate)
  }

  private def initiatePartitionDeletion(topicIdPartition: TopicIdPartition): MemoryRecords = {
    memoryRecords(new TierPartitionDeleteInitiate(topicIdPartition, 0, UUID.randomUUID))
  }

  private def completePartitionDeletion(topicIdPartition: TopicIdPartition): MemoryRecords = {
    memoryRecords(new TierPartitionDeleteComplete(topicIdPartition, UUID.randomUUID))
  }

  private def memoryRecords(metadata: AbstractTierMetadata): MemoryRecords = {
    MemoryRecords.withRecords(0,
      CompressionType.NONE,
      new SimpleRecord(0, metadata.serializeKey, metadata.serializeValue))
  }

  private def createLog(dir: File,
                        config: LogConfig = LogTest.createLogConfig(),
                        brokerTopicStats: BrokerTopicStats = new BrokerTopicStats,
                        logStartOffset: Long = 0L,
                        recoveryPoint: Long = 0L,
                        scheduler: Scheduler = scheduler,
                        time: Time = time,
                        maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
                        producerIdExpirationCheckIntervalMs: Int = LogManager.ProducerIdExpirationCheckIntervalMs): AbstractLog = {
    LogTest.createLog(dir, config, brokerTopicStats, scheduler, time, logStartOffset, recoveryPoint,
      maxProducerIdExpirationMs, producerIdExpirationCheckIntervalMs)
  }

  private class Listener extends DeletedPartitionsChangeListener {
    override def initiatePartitionDeletion(topicIdPartition: TopicIdPartition,
                                           tieredObjects: List[TierObjectStore.ObjectMetadata]): Unit = {
    }

    override def stopPartitionDeletion(topicIdPartition: TopicIdPartition): Unit = {
    }
  }
}
