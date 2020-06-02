/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import java.util.Optional
import java.util.UUID

import com.yammer.metrics.core.Meter
import kafka.log.{AbstractLog, TierLogSegment}
import kafka.server.ReplicaManager
import kafka.tier.domain.{AbstractTierMetadata, TierPartitionDeleteComplete, TierSegmentDeleteComplete, TierSegmentDeleteInitiate}
import kafka.tier.exceptions.{TaskCompletedException, TierDeletionFailedException, TierDeletionFatalException, TierDeletionFencedException, TierDeletionRestoreFencedException, TierDeletionTaskFencedException, TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.TierTask
import kafka.tier.tasks.delete.DeletionTask.State
import kafka.tier.TopicIdPartition
import kafka.tier.state.OffsetAndEpoch
import kafka.tier.state.TierPartitionStatus
import kafka.tier.topic.TierTopicAppender
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters._

object Defaults {
  val FENCED_STATE_EXCEPTION_RETRY_MS = 60000
  val METADATA_EXCEPTION_RETRY_MS = 5000
}

final class DeletionTask(override val ctx: CancellationContext,
                         override val topicIdPartition: TopicIdPartition,
                         private val logCleanupIntervalMs: Long,
                         var state: State,
                         private val retryRateOpt: Option[Meter] = None) extends TierTask[DeletionTask](retryRateOpt) with Logging {
  import DeletionTask._

  var lastProcessedMs: Option[Long] = None

  override def transition(time: Time,
                          tierTopicAppender: TierTopicAppender,
                          tierObjectStore: TierObjectStore,
                          replicaManager: ReplicaManager,
                          maxRetryBackoffMs: Option[Int])(implicit ec: ExecutionContext): Future[DeletionTask] = {
    val nowMs = time.hiResClockMs()

    val newState = {
      // This is just a best effort check, we would like to avoid doing any additional work when
      // we know upfront that the context has been cancelled.
      if (ctx.isCancelled) {
        Future.successful(state)
      } else {
        state.transition(topicIdPartition, replicaManager, tierTopicAppender, tierObjectStore, time)
      }
    }

    newState.map { result =>
      result match {
        case _: CollectDeletableSegments if !result.isDeletedPartition =>
          // Delay check for deletable segments of a partition by `logCleanupIntervalMs`
          onSuccessfulTransitionWithDelay(logCleanupIntervalMs, nowMs)

        case initiateDelete: InitiateDelete =>
          // Delay InitiateDelete based on requested delay for next deletable segment
          val delayMs = initiateDelete.getNextSegmentDelay(nowMs)
          onSuccessfulTransitionWithDelay(delayMs, nowMs)

        case _: Delete if !result.isDeletedPartition =>
          // Delay deletion of object by configured `fileDeleteDelayMs`
          val delayMs = getDelayFromLogConfig(replicaManager.getLog(topicPartition))
          onSuccessfulTransitionWithDelay(delayMs, nowMs)

        case _ =>
          onSuccessfulTransition()
      }

      debug(s"Transitioned $topicIdPartition from $state to $result")
      this.lastProcessedMs = Some(nowMs)
      this.state = result
      this
    }.recover {
      case e @ (_: TierMetadataRetriableException | _: TierObjectStoreRetriableException) =>
        retryTaskLater(maxRetryBackoffMs.getOrElse(Defaults.METADATA_EXCEPTION_RETRY_MS), nowMs, e)
        this
      case e: TierDeletionTaskFencedException =>
        info(s"$topicIdPartition was fenced, stopping deletion process", e)
        ctx.cancel()
        this
      case e: TierDeletionFencedException =>
        info(s"$topicIdPartition was fenced, stopping deletion process", e)
        ctx.cancel()
        this

      case _: TierDeletionRestoreFencedException =>
        debug(s"$topicIdPartition encountered metadata fencing due to state restoration")
        // the TierPartitionState has been restored. We can retry immediately but we must
        // transition to a FailedState so we can re-establish leadership if required
        state = FailedState(state.metadata)
        this
      case e: TierDeletionFailedException =>
        warn(s"$topicIdPartition failed, stopping deletion process and marking $topicIdPartition to be in error", e)
        retryTaskLater(maxRetryBackoffMs.getOrElse(Defaults.FENCED_STATE_EXCEPTION_RETRY_MS), nowMs, e)
        state = FailedState(state.metadata)
        this
      case _: TaskCompletedException =>
        info(s"Stopping deletion process for $topicIdPartition after task completion")
        ctx.cancel()
        this
      case e: Throwable =>
        error(s"$topicIdPartition failed due to unhandled exception, stopping deleting process", e)
        cancelAndSetErrorState(this, e)
        this
    }
  }

  override def toString = s"DeletionTask($topicIdPartition, state=$state, cancelled=${ctx.isCancelled})"
}

object DeletionTask extends Logging {
  override protected def loggerName: String = classOf[DeletionTask].getName
  /**
   * FencedSegmentDeleteDelayMs delay is added for the case where the fenced segments may still have not been uploaded.
   * Storage backend like AWS S3 return success for delete operation if the object is not found, so to address this edge
   * case a practical FencedSegmentDeleteDelayMs is added to the current time in CollectDeletableSegments after which
   * garbage collection is performed on the object with an assumption that broker performing the upload is given
   * sufficient time to complete the fenced upload.
   */
  val FencedSegmentDeleteDelayMs = 10*60*1000

  private[delete] sealed trait StateMetadata {
    def leaderEpoch: Int
  }

  private[delete] case class DeleteAsLeaderMetadata(replicaManager: ReplicaManager, leaderEpoch: Int) extends StateMetadata {
    override def toString: String = s"DeleteAsLeaderMetadata(leaderEpoch: $leaderEpoch)"
  }

  private[delete] case class DeletedPartitionMetadata(tieredObjects: List[TierObjectStore.ObjectMetadata]) extends StateMetadata {
    val leaderEpoch: Int = Int.MaxValue

    override def toString: String = s"DeletedPartitionMetadata(numTieredObjects: ${tieredObjects.size})"
  }

  /**
    * Deletion for a set of segments for a particular topic partition follows a state machine progression. Each call to
    * `transition` can either successfully transition to the next state or remain in the current state on retriable
    * exceptions. The state machine exits (along with all other segments for this topic partition) when an attempt to
    * transition states is fenced or when all segments were successfully deleted.
    *
    * +--------------------------+
    * |                          >----------------------+
    * | CollectDeletableSegments <----+-------------    |
    * |                          |    |            |    |
    * +------------+-------------+    |            |    |
    *              |                  |            |    |
    *              |                  |            |    |
    * +------------v----+             |            |    |
    * |                 >-------------|---------   |    |
    * |  InitiateDelete <----+        |        |   |    |
    * |                 |    |        |        |   |    |
    * +-------+---------+    |        |        |   |    |
    *         |              |        |        |   |    |
    *         |              |        |        |   |    |
    * +-------v---------+    |        |      +-v---^----v-----+
    * |                 |    |        |      |                |
    * |     Delete      |    |        |      |    Failed      |
    - |                 |    |        |      |                |
    * +-------+---------+    |        |      +------^---------+
    *         |              |        |            |
    *         |              |        |            |
    * +-------v---------+    |        |            |
    * |                 |    |        |            |
    * | CompleteDelete  +----+--------+------------+
    * |                 |
    * +-------+---------+
    *         | Deleted Partition
    *         |
    * +-------v------------------+
    * |                          |
    * | PartitionDeleteComplete  |
    * |                          |
    * +--------------------------+
    */
  private[delete] sealed trait State {
    def metadata: StateMetadata

    def transition(topicIdPartition: TopicIdPartition,
                   replicaManager: ReplicaManager,
                   tierTopicAppender: TierTopicAppender,
                   tierObjectStore: TierObjectStore,
                   time: Time)
                  (implicit ec: ExecutionContext): Future[State]

    def leaderEpoch: Int = metadata.leaderEpoch

    def isDeletedPartition: Boolean = {
      metadata match {
        case _: DeletedPartitionMetadata => true
        case _ => false
      }
    }

    override def toString: String = {
      s"State(currentState: ${this.getClass.getName} metadata: $metadata)"
    }
  }

  private[delete] case class CollectDeletableSegments(metadata: StateMetadata) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      Future {
        metadata match {
          case retentionMetadata: DeleteAsLeaderMetadata =>
            val replicaManager = retentionMetadata.replicaManager
            val leaderEpoch = retentionMetadata.leaderEpoch

            replicaManager.getPartitionOrError(topicIdPartition.topicPartition, expectLeader = true) match {
              case Left(error) =>
                throw new TierDeletionTaskFencedException(topicIdPartition, error.exception)

              case Right(partition) =>
                partition.log.map { log =>
                  if (log.tierPartitionState.tierEpoch != leaderEpoch)
                    throw new TierMetadataRetriableException(s"Leadership not established for $topicIdPartition. Backing off.")

                  val currentStateOffset = log.tierPartitionState.lastLocalMaterializedSrcOffsetAndEpoch()
                  val deletableSegments = mutable.Queue.empty ++ collectDeletableSegments(time, log)
                  if (deletableSegments.nonEmpty)
                    InitiateDelete(metadata, Optional.of(currentStateOffset), deletableSegments)
                  else
                    this
                }.getOrElse(this)
            }

          case deletedPartitionMetadata: DeletedPartitionMetadata =>
            val deletableSegments = deletedPartitionMetadata.tieredObjects.map(DeleteObjectMetadata(_, None))
            if (deletableSegments.nonEmpty) {
              // deletion resulting from delete partitions does not have a valid stateOffset and thus empty is passed
              val stateOffset = Optional.empty[OffsetAndEpoch]()
              InitiateDelete(metadata, stateOffset, mutable.Queue.empty ++= deletableSegments)
            } else {
              PartitionDeleteComplete(deletedPartitionMetadata)
            }
        }
      }
    }

    // Collect all deletable segments (retention based and fenced tiered segments) for this log
    private def collectDeletableSegments(time: Time,
                                         log: AbstractLog): List[DeleteObjectMetadata] = {
      // Queue the retention-based segments that can be processed immediately first followed by the fenced segments
      collectRetentionBasedDeletableSegments(time, log) ++ collectFencedSegments(time, log)
    }

    private def collectRetentionBasedDeletableSegments(time: Time, log: AbstractLog): List[DeleteObjectMetadata] = {
      maybeUpdateLogStartOffsetRetentionMsBreachedSegments(time, log)
      maybeUpdateLogStartOffsetRetentionSizeBreachedSegments(log)
      collectLogStartOffsetBreachedSegments(log).map(DeleteObjectMetadata(_, None))
    }

    // Find deletable segments based on the predicate `shouldDelete`. Increments the log start offset and returns the
    // list of deletable segments.
    private def maybeUpdateLogStartOffsetOnDeletePredicate(log: AbstractLog,
                                                           shouldDelete: TierLogSegment => Boolean,
                                                           reason: String): List[TierObjectStore.ObjectMetadata] = {
      val toDelete = mutable.ListBuffer[TierObjectStore.ObjectMetadata]()
      val segmentIterator = log.tieredLogSegments
      try {
        var continue = true
        while (segmentIterator.hasNext && continue) {
          val segment = segmentIterator.next()
          if (shouldDelete(segment)) {
            log.maybeIncrementLogStartOffset(segment.endOffset + 1)
            toDelete += segment.metadata
          } else {
            continue = false
          }
        }
        if (toDelete.nonEmpty)
          info(s"Found deletable tiered segments for ${log.topicPartition} with base offsets " +
            s"[${toDelete.map(_.baseOffset).mkString(",")}] due to $reason")
        toDelete.toList
      } finally {
        segmentIterator.close()
      }
    }

    // Delete segments which are eligible for deletion based on time-based retention and schedule them for deletion
    private def maybeUpdateLogStartOffsetRetentionMsBreachedSegments(time: Time, log: AbstractLog): Unit = {
      val startTime = time.milliseconds
      val retentionMs = log.config.retentionMs

      if (retentionMs < 0)
        return

      def shouldDelete(segment: TierLogSegment): Boolean = startTime - segment.maxTimestamp > retentionMs

      maybeUpdateLogStartOffsetOnDeletePredicate(log, shouldDelete, reason = s"retention time ${retentionMs}ms breach")
    }

    // Delete segments which are eligible for deletion based on size-based retention and schedule them for deletion
    private def maybeUpdateLogStartOffsetRetentionSizeBreachedSegments(log: AbstractLog): Unit = {
      val size = log.size
      val retentionSize = log.config.retentionSize

      if (retentionSize < 0 || size < retentionSize)
        return

      var diff = size - retentionSize
      def shouldDelete(segment: TierLogSegment): Boolean = {
        if (diff - segment.size >= 0) {
          diff -= segment.size
          true
        } else {
          false
        }
      }

      maybeUpdateLogStartOffsetOnDeletePredicate(log, shouldDelete, reason = s"retention size in bytes $retentionSize breach")
    }

    // Collect and delete segments which are below the current log start offset and schedule them for deletion
    private def collectLogStartOffsetBreachedSegments(log: AbstractLog): List[TierObjectStore.ObjectMetadata] = {
      def shouldDelete(segment: TierLogSegment): Boolean = segment.endOffset < log.logStartOffset

      maybeUpdateLogStartOffsetOnDeletePredicate(log, shouldDelete, reason = s"log start offset ${log.logStartOffset} breach")
    }

    /**
     * Collect segments that were fenced during the tier operation. These segments will be deleted by the deletion task
     * after the `FencedSegmentDeleteDelayMs` from current time.
     */
    private def collectFencedSegments(time: Time, log: AbstractLog): List[DeleteObjectMetadata] = {
      val nowMs = time.hiResClockMs()
      val tierObjectMetadataIterator = log.tierPartitionState.fencedSegments.iterator.asScala
      val toDelete = tierObjectMetadataIterator.map { objectMetadata =>
        DeleteObjectMetadata(new TierObjectStore.ObjectMetadata(objectMetadata), Some(Math.addExact(nowMs, FencedSegmentDeleteDelayMs)))
      }.toList
      if (toDelete.nonEmpty)
        info(s"Found deletable tiered segments for ${log.topicPartition} with base offsets " +
          s"[${toDelete.map(_.objectMetadata.baseOffset).mkString(",")}] due to fenced state")
      toDelete
    }
  }

  // This class allows delayed Delete operation on a deletable segment by providing `DeleteObjectMetadata.deleteAfterTimeMs`
  private[delete] case class DeleteObjectMetadata(objectMetadata: TierObjectStore.ObjectMetadata, deleteAfterTimeMs: Option[Long])

  private[delete] case class InitiateDelete(metadata: StateMetadata,
                                            stateOffsetAndEpoch: Optional[OffsetAndEpoch],
                                            toDelete: mutable.Queue[DeleteObjectMetadata]) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      val segment = toDelete.head
      writeDeletionInitiatedMarker(tierTopicAppender, leaderEpoch, stateOffsetAndEpoch, segment.objectMetadata).map { _ =>
        Delete(metadata, stateOffsetAndEpoch, toDelete)
      }
    }

    def getNextSegmentDelay(nowMs: Long): Long = {
      val segment = toDelete.head
      if (!segment.deleteAfterTimeMs.isEmpty)
        Math.subtractExact(segment.deleteAfterTimeMs.get, nowMs)
      else
        0
    }
  }

  private[delete] case class Delete(metadata: StateMetadata,
                                    stateOffsetAndEpoch: Optional[OffsetAndEpoch],
                                    toDelete: mutable.Queue[DeleteObjectMetadata]) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      Future {
        blocking {
          val segment = toDelete.head.objectMetadata
          tierObjectStore.deleteSegment(segment)
          CompleteDelete(metadata, stateOffsetAndEpoch, toDelete)
        }
      }
    }
  }

  private[delete] case class CompleteDelete(metadata: StateMetadata,
                                            stateOffset: Optional[OffsetAndEpoch],
                                            toDelete: mutable.Queue[DeleteObjectMetadata]) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      val segment = toDelete.head.objectMetadata
      writeDeletionCompletedMarker(tierTopicAppender, leaderEpoch, stateOffset, segment).map { _ =>
        toDelete.dequeue()
        if (toDelete.nonEmpty) {
          InitiateDelete(metadata, stateOffset, toDelete)
        } else {
          info(s"Completed segment deletions for $topicIdPartition")
          metadata match {
            case _: DeleteAsLeaderMetadata =>
              CollectDeletableSegments(metadata)

            case deletedPartitionMetadata: DeletedPartitionMetadata =>
              PartitionDeleteComplete(deletedPartitionMetadata)
          }
        }
      }
    }
  }

  private[delete] case class FailedState(metadata: StateMetadata) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      Future {
        replicaManager.getLog(topicIdPartition.topicPartition)
          .map { log =>
            val tierEpoch = log.tierPartitionState.tierEpoch
            val leaderEpoch = metadata.leaderEpoch
            // if we're still in ERROR status, let's throw a fenced exception again and backoff
            if (log.tierPartitionState.status() == TierPartitionStatus.ERROR)
              throw new TierDeletionFailedException(topicIdPartition)
            else if (tierEpoch == leaderEpoch)
              CollectDeletableSegments(DeleteAsLeaderMetadata(replicaManager, leaderEpoch))
            // if the state epoch is higher than us we fence ourselves and cancel
            else if (tierEpoch > leaderEpoch)
              throw new TierDeletionFencedException(topicIdPartition)
            // if the state epoch is higher than us we backoff and let the archiver re-establish leadership
            else if (tierEpoch < leaderEpoch)
              throw new TierDeletionFailedException(topicIdPartition)
            else
              throw new TierDeletionFatalException(s"attempted to transition from Failed for $topicIdPartition while in untransitionable state")
          }.getOrElse(FailedState(metadata))
      }
    }
  }

  private[delete] case class PartitionDeleteComplete(metadata: DeletedPartitionMetadata) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      writePartitionDeletionCompletedMarker(tierTopicAppender, leaderEpoch, topicIdPartition).map { _ =>
        info(s"Completed partition deletion for $topicIdPartition")
        throw new TaskCompletedException(topicIdPartition)
      }
    }
  }

  def getDelayFromLogConfig(log: Option[AbstractLog]): Long = {
    log.map(_.config.fileDeleteDelayMs).map(Long2long).getOrElse(0L)
  }

  def writeDeletionInitiatedMarker(tierTopicAppender: TierTopicAppender,
                                   leaderEpoch: Int,
                                   stateOffset: Optional[OffsetAndEpoch],
                                   segment: TierObjectStore.ObjectMetadata)(implicit ec: ExecutionContext): Future[Unit] = {
    val marker = new TierSegmentDeleteInitiate(segment.topicIdPartition, leaderEpoch, segment.objectId, stateOffset)
    writeMarker(tierTopicAppender, marker)
  }

  def writeDeletionCompletedMarker(tierTopicAppender: TierTopicAppender,
                                   leaderEpoch: Int,
                                   stateOffset: Optional[OffsetAndEpoch],
                                   objectMetadata: TierObjectStore.ObjectMetadata)(implicit ec: ExecutionContext): Future[Unit] = {
    val marker = new TierSegmentDeleteComplete(objectMetadata.topicIdPartition(), leaderEpoch, objectMetadata.objectId, stateOffset)
    writeMarker(tierTopicAppender, marker)
  }

  def writePartitionDeletionCompletedMarker(tierTopicAppender: TierTopicAppender,
                                            leaderEpoch: Int,
                                            topicIdPartition: TopicIdPartition)(implicit ec: ExecutionContext): Future[Unit] = {
    val marker = new TierPartitionDeleteComplete(topicIdPartition, UUID.randomUUID)
    writeMarker(tierTopicAppender, marker)
  }

  def writeMarker(tierTopicAppender: TierTopicAppender,
                  marker: AbstractTierMetadata)(implicit ec: ExecutionContext): Future[Unit] = {
    tierTopicAppender.addMetadata(marker)
      .toScala
      .map {
        appendResult: AppendResult =>
          appendResult match {
            case AppendResult.ACCEPTED =>
              debug(s"Successfully completed $marker")
            case AppendResult.FENCED =>
              info(s"Stopping state machine for ${marker.topicIdPartition()} as attempt to transition was fenced")
              throw new TierDeletionFencedException(marker.topicIdPartition)
            case AppendResult.FAILED =>
              warn(s"Stopping state machine for ${marker.topicIdPartition()} as attempt to transition failed")
              throw new TierDeletionFailedException(marker.topicIdPartition)
            case AppendResult.RESTORE_FENCED =>
              throw new TierDeletionRestoreFencedException(marker.topicIdPartition)
            case _ =>
              throw new IllegalStateException(s"Unexpected append result for ${marker.topicIdPartition()}: $appendResult")
          }
      }
  }
}

