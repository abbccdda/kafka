package kafka.tier

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{FetchDataInfo, FetchHighWatermark, ReplicaManager}
import kafka.tier.domain.{AbstractTierMetadata, TierPartitionDeleteComplete, TierPartitionDeleteInitiate, TierSegmentDeleteComplete, TierSegmentUploadInitiate}
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.state.{TierPartitionState, TierPartitionStatus}
import kafka.tier.store.TierObjectStore
import kafka.tier.topic.TierTopicConsumer.ClientCtx
import kafka.tier.topic.{TierTopic, TierTopicConsumer}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{FileRecords, MemoryRecords}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.java8.OptionConverters._

object TierDeletedPartitionsCoordinator {
  val MaxInProgressPartitions = 100
}

/**
  * Coordinator for handling partition deletion for tiered storage. Each broker hosts a coordinator instance and drives
  * partition deletions based on the tier topic partitions it is a leader of. For example, if broker A is a leader of
  * tier topic partitions 0, 10 and 19, it will be responsible for any partition deletions tracked within those tier
  * topic partitions. Partition deletion is initiated by the controller by emitting a `TierPartitionDeleteInitiate`
  * message to the tier topic. TierDeletedPartitionsCoordinator picks these messages up by reading the tier topic
  * partitions it is a leader of and initiates garbage collection of tiered segments corresponding to deleted
  * partitions.
  */
class TierDeletedPartitionsCoordinator(scheduler: Scheduler,
                                       replicaManager: ReplicaManager,
                                       tierTopicConsumer: TierTopicConsumer,
                                       tierDeletedPartitionsIntervalMs: Long,
                                       tierNamespace: String,
                                       time: Time) extends Logging with KafkaMetricsGroup {
  private val tierTopicName = TierTopic.topicName(tierNamespace)
  private var listener: DeletedPartitionsChangeListener = _
  private var coordinatorTask: ScheduledFuture[_] = _
  private var lastDeletedPartitionCheckMs = 0L
  private var numInProgress = 0
  private[tier] val immigratedPartitions = mutable.Map[Int, ImmigratedTierTopicPartition]()

  newGauge("TierNumInProgressPartitionDeletions", new Gauge[Long] {
    override def value(): Long = synchronized {
      numInProgress
    }
  })

  newGauge("TierNumQueuedPartitionDeletions", new Gauge[Long] {
    override def value(): Long = synchronized {
      immigratedPartitions.values.map(_.pendingDeletions.size.toLong).sum
    }
  })

  def startup(): Unit = {
    coordinatorTask = scheduler.schedule("tier-deleted-partition-task", () => doWork, delay = 100,
      period = Math.min(60 * 1000, tierDeletedPartitionsIntervalMs), unit = TimeUnit.MILLISECONDS)
  }

  def handleImmigration(tierTopicPartitionId: Int): Unit = synchronized {
    immigratedPartitions += (tierTopicPartitionId -> new ImmigratedTierTopicPartition())
  }

  def handleEmigration(tierTopicPartitionId: Int): Unit = synchronized {
    immigratedPartitions.remove(tierTopicPartitionId).foreach { emigratedPartition =>
      emigratedPartition.inProgressDeletions.values.foreach { inProgressDeletion =>
        info(s"Cancelling segment deletion for ${inProgressDeletion.topicIdPartition} on partition emigration")
        cancelInProgress(inProgressDeletion)
      }
    }
  }

  def registerListener(listener: DeletedPartitionsChangeListener): Unit = {
    this.listener = listener
  }

  def shutdown(): Unit = {
    def removeMetrics(): Unit = {
      removeMetric("TierNumInProgressPartitionDeletions")
      removeMetric("TierNumQueuedPartitionDeletions")
    }

    coordinatorTask.cancel(false)
    removeMetrics()
  }

  private def doWork(): Unit = {
    try {
      val now = time.hiResClockMs()
      if (lastDeletedPartitionCheckMs == 0L || now - lastDeletedPartitionCheckMs >= tierDeletedPartitionsIntervalMs) {
        findDeletedPartitions()
        lastDeletedPartitionCheckMs = now
      }
      makeTransitions()
    } catch {
      case e: Exception => error(s"Caught exception in work loop", e)
    }
  }

  // Find partitions that were deleted
  private def findDeletedPartitions(): Unit = {
    val maxReadSize = 10000
    var allocatedBuffer = ByteBuffer.allocate(maxReadSize)

    tierTopicPartitionsWithPosition.foreach { case (tierTopicPartition, startOffsetOpt) =>
      val (endOffset, buffer) = collectDeletedPartitions(tierTopicPartition, startOffsetOpt.getOrElse(0L), allocatedBuffer)
      updateStartOffset(tierTopicPartition.partition, endOffset)
      allocatedBuffer = buffer
    }
  }

  // Make transitions for tracked deleted partitions. This involves materializing tier partition state and initiating
  // deletion once state has been fully materialized.
  private def makeTransitions(): Unit = {
    maybeBeginMaterialization()
    maybeBeginDeletion()
  }

  private[tier] def collectDeletedPartitions(tierTopicPartition: TopicPartition,
                                             startOffset: Long,
                                             allocatedBuffer: ByteBuffer): (Long, ByteBuffer) = {
    var buffer = allocatedBuffer

    replicaManager.getLog(tierTopicPartition) match {
      case Some(log) =>
        var lastOffset = log.highWatermark
        var currentOffset = startOffset

        while (currentOffset < lastOffset) {
          val fetchDataInfo = log.read(currentOffset,
            maxLength = buffer.capacity,
            isolation = FetchHighWatermark,
            minOneMessage = true) match {
            case localFetchInfo: FetchDataInfo => localFetchInfo
            case _ => throw new IllegalStateException(s"Unexpected tiered segment for tier topic in $tierTopicPartition")
          }

          val records = fetchDataInfo.records match {
            case records: MemoryRecords =>
              records

            case fileRecords: FileRecords =>
              buffer.clear()

              // reallocate the buffer if needed
              if (buffer.capacity < fileRecords.sizeInBytes)
                buffer = ByteBuffer.allocate(fileRecords.sizeInBytes)

              fileRecords.readInto(buffer, 0)
              MemoryRecords.readableRecords(buffer)
          }

          records.batches.asScala.foreach { batch =>
            for (record <- batch.asScala) {
              AbstractTierMetadata.deserialize(record.key, record.value).asScala.foreach {
                case partitionDeleteInitiate: TierPartitionDeleteInitiate =>
                  trackInitiatePartitionDelete(tierTopicPartition.partition, partitionDeleteInitiate.topicIdPartition)

                case partitionDeleteComplete: TierPartitionDeleteComplete =>
                  trackCompletePartitionDelete(tierTopicPartition.partition, partitionDeleteComplete.topicIdPartition)

                case _ =>
              }
            }
            currentOffset = batch.nextOffset
          }
          lastOffset = Math.min(lastOffset, log.highWatermark)
        }
        debug(s"Processed messages in $tierTopicPartition from offset $startOffset to $currentOffset")
        (currentOffset, buffer)

      case None =>
        (startOffset, buffer)
    }
  }

  private[tier] def maybeBeginMaterialization(): Unit = synchronized {
    val immigratedPartitionIt = immigratedPartitions.iterator
    val newDeletions = mutable.Map[TopicIdPartition, ClientCtx]()

    def capacity: Int = TierDeletedPartitionsCoordinator.MaxInProgressPartitions - numInProgress

    while (capacity > 0 && immigratedPartitionIt.hasNext) {
      immigratedPartitionIt.next() match { case (tierTopicPartitionId, partitionState) =>
        if (partitionState.inProgressDeletions.isEmpty) {
          val pendingDeletions = partitionState.pendingDeletions
          val toDelete = pendingDeletions.take(capacity).toList

          // move partitions to inProgress state
          toDelete.foreach { partitionToDelete =>
            val inProgressDeletion = new InProgressDeletion(tierTopicPartitionId, partitionToDelete, tierTopicConsumer)
            partitionState.inProgressDeletions += (partitionToDelete -> inProgressDeletion)
            newDeletions += (partitionToDelete -> inProgressDeletion)
            pendingDeletions.remove(partitionToDelete)
            numInProgress += 1
          }
        }
      }
    }

    // register deletions to begin materialization
    debug(s"Beginning tier partition state materialization for ${newDeletions.map(_._1)}")
    tierTopicConsumer.register(newDeletions.asJava)
  }

  private[tier] def maybeBeginDeletion(): Unit = synchronized {
    immigratedPartitions.values.foreach { immigratedPartition =>
      immigratedPartition.inProgressDeletions.values.foreach { inProgressDeletion =>
        inProgressDeletion.deletionState match {
          case MaterializationComplete =>
            info(s"Beginning segment deletion for ${inProgressDeletion.topicIdPartition}")
            inProgressDeletion.awaitDeleteComplete()
            listener.initiatePartitionDeletion(inProgressDeletion.topicIdPartition, inProgressDeletion.allTieredObjects)

          case _ =>
        }
      }
    }
  }

  // visible for testing
  private[tier] def trackInitiatePartitionDelete(tierTopicPartitionId: Int, deletedPartition: TopicIdPartition): Unit = synchronized {
    debug(s"Processing InitiateDelete for $deletedPartition")
    immigratedPartitions.get(tierTopicPartitionId).foreach(_.pendingDeletions += deletedPartition)
  }

  // visible for testing
  private[tier] def trackCompletePartitionDelete(tierTopicPartitionId: Int, deletedPartition: TopicIdPartition): Unit = synchronized {
    debug(s"Processing CompleteDelete for $deletedPartition")
    immigratedPartitions.get(tierTopicPartitionId).foreach { immigratedTierTopicPartition =>
      immigratedTierTopicPartition.pendingDeletions -= deletedPartition
      immigratedTierTopicPartition.inProgressDeletions.remove(deletedPartition).foreach { inProgress =>
        info(s"Completed deleting segments for $deletedPartition")
        cancelInProgress(inProgress)
      }
    }
  }

  private def updateStartOffset(tierTopicPartitionId: Int, newStartOffset: Long): Unit = synchronized {
    immigratedPartitions.get(tierTopicPartitionId).foreach(_.lastReadOffset = Some(newStartOffset))
  }

  private def tierTopicPartitionsWithPosition: Map[TopicPartition, Option[Long]] = synchronized {
    immigratedPartitions.map { case (tierTopicPartitionId, immigratedPartition) =>
      new TopicPartition(tierTopicName, tierTopicPartitionId) -> immigratedPartition.lastReadOffset
    }.toMap
  }

  private def cancelInProgress(inProgressDeletion: InProgressDeletion): Unit = {
    listener.stopPartitionDeletion(inProgressDeletion.topicIdPartition)
    inProgressDeletion.stopMaterialization()
    numInProgress -= 1
  }
}

private[tier] class ImmigratedTierTopicPartition {
  var lastReadOffset: Option[Long] = None
  val pendingDeletions: mutable.LinkedHashSet[TopicIdPartition] = mutable.LinkedHashSet()
  val inProgressDeletions: mutable.Map[TopicIdPartition, InProgressDeletion] = mutable.Map()
}

/**
  * Deletion of a topic partition follows a state machine. The state machine typically follows this sequence:
  * MaterializingState -> MaterializationComplete -> AwaitingDeleteComplete
  */
sealed trait DeletionState

/**
  * Represents the state where we are materializing state for tiered segments that require deletion. This state is read
  * from the tier topic.
  */
case object MaterializingState extends DeletionState

/**
  * Represents the state where materialization of tiered segments is complete, and we are ready to delete these segments.
  */
case object MaterializationComplete extends DeletionState

/**
  * Represents the state where we have submitted a task to initiate deletion of tiered segments, and are waiting for the
  * task to complete successfully. Once the task is complete, the state machine exits.
  */
case object AwaitingDeleteComplete extends DeletionState

/**
  * Represents the state where deletion for topic partition was aborted prematurely.
  */
case object Aborted extends DeletionState

/**
  * Represents an in-progress partition deletion. Client must explicitly call TierTopicConsumer#register to begin state
  * progression.
  *
  * @param tierTopicPartitionId Tier topic partition id this partition belongs to
  * @param topicIdPartition Topic id partition being deleted
  * @param tierTopicConsumer TierTopicConsumer instance
  * @param tieredObjects Set of tiered objects. This set is constructed when consuming the tier topic and passed to
  *                      the deletion layer once materialization is complete.
  * @param status Current tier partition status, maintained and consumed by TierTopicConsumer
  * @param currentState Current deletion state
  */
private class InProgressDeletion(val tierTopicPartitionId: Int,
                                 val topicIdPartition: TopicIdPartition,
                                 tierTopicConsumer: TierTopicConsumer,
                                 tieredObjects: mutable.Map[UUID, TierObjectStore.ObjectMetadata] = mutable.Map(),
                                 @volatile var status: TierPartitionStatus = TierPartitionStatus.INIT,
                                 private[tier] var currentState: DeletionState = MaterializingState) extends ClientCtx with Logging {
  override def process(metadata: AbstractTierMetadata): TierPartitionState.AppendResult = synchronized {
    metadata match {
      case segmentUpload: TierSegmentUploadInitiate =>
        val objectMetadata = new TierObjectStore.ObjectMetadata(segmentUpload.topicIdPartition,
          segmentUpload.objectId,
          segmentUpload.tierEpoch,
          segmentUpload.baseOffset,
          segmentUpload.hasAbortedTxns)
        tieredObjects += (segmentUpload.objectId -> objectMetadata)

      case segmentDelete: TierSegmentDeleteComplete =>
        tieredObjects -= segmentDelete.objectId

      case _: TierPartitionDeleteInitiate =>
        updateState(MaterializationComplete)

      case _: TierPartitionDeleteComplete =>
        if (tieredObjects.nonEmpty)
          warn(s"Found stray tiered objects after delete completion: $tieredObjects")
        tierTopicConsumer.deregister(topicIdPartition)

      case _ =>
    }
    AppendResult.ACCEPTED
  }

  override def beginCatchup(): Unit = {
    status = TierPartitionStatus.CATCHUP
  }

  override def completeCatchup(): Unit = {
    status = TierPartitionStatus.ONLINE
  }

  def stopMaterialization(): Unit = synchronized {
    tierTopicConsumer.deregister(topicIdPartition)
    currentState = Aborted
  }

  def awaitDeleteComplete(): Unit = synchronized {
    updateState(AwaitingDeleteComplete)
  }

  def deletionState: DeletionState = synchronized {
    currentState
  }

  def updateState(desiredState: DeletionState): Unit = synchronized {
    if (currentState != Aborted)
      currentState = desiredState
  }

  def allTieredObjects: List[TierObjectStore.ObjectMetadata] = synchronized {
    tieredObjects.values.toList
  }
}

trait DeletedPartitionsChangeListener {
  def initiatePartitionDeletion(topicIdPartition: TopicIdPartition, tieredObjects: List[TierObjectStore.ObjectMetadata]): Unit
  def stopPartitionDeletion(topicIdPartition: TopicIdPartition): Unit
}