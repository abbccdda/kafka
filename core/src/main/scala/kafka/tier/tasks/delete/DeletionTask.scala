/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import com.yammer.metrics.core.Meter
import kafka.log.{AbstractLog, TierLogSegment}
import kafka.server.ReplicaManager
import kafka.tier.domain.{AbstractTierMetadata, TierSegmentDeleteComplete, TierSegmentDeleteInitiate}
import kafka.tier.exceptions.{TierArchiverFencedException, TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.TierTask
import kafka.tier.tasks.delete.DeletionTask.State
import kafka.tier.TopicIdPartition
import kafka.tier.topic.TierTopicAppender
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.Try

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

    val newState = state match {
      case _ if ctx.isCancelled => Future.successful(state)
      case s: State => s.transition(topicIdPartition, replicaManager, tierTopicAppender, tierObjectStore, time)
    }

    newState.map { result =>
      result match {
        case _: CollectDeletableSegments =>
          // Delay check for deletable segments of a partition by `logCleanupIntervalMs`
          onSuccessfulTransitionWithDelay(logCleanupIntervalMs, nowMs)

        case _: Delete =>
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
        retryTaskLater(maxRetryBackoffMs.getOrElse(5000), nowMs, e)
        this
      case e: TierArchiverFencedException =>
        info(s"$topicIdPartition was fenced, stopping retention process", e)
        ctx.cancel()
        this
      case e: Throwable =>
        ctx.cancel()
        throw e
    }
  }

  override def toString = s"DeletionTask($topicIdPartition, state=${state.getClass.getName}, cancelled=${ctx.isCancelled})"
}

object DeletionTask extends Logging {
  override protected def loggerName: String = classOf[DeletionTask].getName

  /**
    * Deletion for a set of segments for a particular topic partition follows a state machine progression. Each call to
    * `transition` can either successfully transition to the next state or remain in the current state on retriable
    * exceptions. The state machine exits (along with all other segments for this topic partition) when an attempt to
    * transition states is fenced or when all segments were successfully deleted.
    *
    * +--------------------------+
    * |                          |
    * | CollectDeletableSegments <----+
    * |                          |    |
    * +------------+-------------+    |
    *              |                  |
    *              |                  |
    * +------------v----+             |
    * |                 |             |
    * |  InitiateDelete <----+        |
    * |                 |    |        |
    * +-------+---------+    |        |
    *         |              |        |
    *         |              |        |
    * +-------v---------+    |        |
    * |                 |    |        |
    * |     Delete      |    |        |
    * |                 |    |        |
    * +-------+---------+    |        |
    *         |              |        |
    *         |              |        |
    * +-------v---------+    |        |
    * |                 |    |        |
    * | CompleteDelete  +----+--------+
    * |                 |
    * +-----------------+
    */
  private[delete] sealed trait State {
    def leaderEpoch: Int
    def transition(topicIdPartition: TopicIdPartition,
                   replicaManager: ReplicaManager,
                   tierTopicAppender: TierTopicAppender,
                   tierObjectStore: TierObjectStore,
                   time: Time)
                  (implicit ec: ExecutionContext): Future[State]
  }

  private[delete] case class CollectDeletableSegments(leaderEpoch: Int) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      Future {
        val leadershipEstablished = Try(replicaManager
          .tierMetadataManager
          .tierPartitionState(topicIdPartition)
          .asScala
          .exists(_.tierEpoch == leaderEpoch)).getOrElse(false)

        if (!leadershipEstablished)
          throw new TierMetadataRetriableException(s"Leadership not established for $topicIdPartition. Backing off.")

        replicaManager.getLog(topicIdPartition.topicPartition).map { log =>
          val deletableSegments = collectDeletableSegments(time, log, log.tieredLogSegments).to[mutable.Queue]
          if (deletableSegments.nonEmpty)
            InitiateDelete(leaderEpoch, deletableSegments)
          else
            this
        }.getOrElse(this)
      }
    }

    // Collect all deletable segments for this log and schedule them for deletion
    private def collectDeletableSegments(time: Time,
                                         log: AbstractLog,
                                         segments: Iterable[TierLogSegment]): List[TierObjectStore.ObjectMetadata] = {
      if (segments.isEmpty) {
        List.empty
      } else {
        deleteRetentionMsBreachedSegments(time, log, segments)
        deleteRetentionSizeBreachedSegments(log, segments)
        deleteAndCollectLogStartOffsetBreachedSegments(log, segments)
      }
    }

    // Find deletable segments based on the predicate `shouldDelete`. Increments the log start offset and returns the
    // list of deletable segments.
    private def deleteOldSegments(log: AbstractLog,
                                  segments: Iterable[TierLogSegment],
                                  shouldDelete: TierLogSegment => Boolean,
                                  reason: String): List[TierObjectStore.ObjectMetadata] = {
      val toDelete = mutable.ListBuffer[TierObjectStore.ObjectMetadata]()
      val segmentIterator = segments.iterator
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
          s"[${toDelete.map(_.baseOffet).mkString(",")}] due to $reason")
      toDelete.toList
    }

    // Delete segments which are eligible for deletion based on time-based retention and schedule them for deletion
    private def deleteRetentionMsBreachedSegments(time: Time, log: AbstractLog, segments: Iterable[TierLogSegment]): Unit = {
      val startTime = time.milliseconds
      val retentionMs = log.config.retentionMs

      if (retentionMs < 0)
        return

      def shouldDelete(segment: TierLogSegment): Boolean = startTime - segment.maxTimestamp > retentionMs

      deleteOldSegments(log, segments, shouldDelete, reason = s"retention time ${retentionMs}ms breach")
    }

    // Delete segments which are eligible for deletion based on size-based retention and schedule them for deletion
    private def deleteRetentionSizeBreachedSegments(log: AbstractLog, segments: Iterable[TierLogSegment]): Unit = {
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

      deleteOldSegments(log, segments, shouldDelete, reason = s"retention size in bytes $retentionSize breach")
    }

    // Collect and delete segments which are below the current log start offset and schedule them for deletion
    private def deleteAndCollectLogStartOffsetBreachedSegments(log: AbstractLog,
                                                               segments: Iterable[TierLogSegment]): List[TierObjectStore.ObjectMetadata] = {
      def shouldDelete(segment: TierLogSegment): Boolean = segment.endOffset < log.logStartOffset

      deleteOldSegments(log, segments, shouldDelete, reason = s"log start offset ${log.logStartOffset} breach")
    }
  }

  private[delete] case class InitiateDelete(leaderEpoch: Int, toDelete: mutable.Queue[TierObjectStore.ObjectMetadata]) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      val segment = toDelete.head
      writeDeletionInitiatedMarker(tierTopicAppender, leaderEpoch, segment).map { _ =>
        Delete(leaderEpoch, toDelete)
      }
    }
  }

  private[delete] case class Delete(leaderEpoch: Int, toDelete: mutable.Queue[TierObjectStore.ObjectMetadata]) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      Future {
        blocking {
          val segment = toDelete.head
          tierObjectStore.deleteSegment(segment)
          CompleteDelete(leaderEpoch, toDelete)
        }
      }
    }
  }

  private[delete] case class CompleteDelete(leaderEpoch: Int, toDelete: mutable.Queue[TierObjectStore.ObjectMetadata]) extends State {
    override def transition(topicIdPartition: TopicIdPartition,
                            replicaManager: ReplicaManager,
                            tierTopicAppender: TierTopicAppender,
                            tierObjectStore: TierObjectStore,
                            time: Time)(implicit ec: ExecutionContext): Future[State] = {
      val segment = toDelete.head
      writeDeletionCompletedMarker(tierTopicAppender, leaderEpoch, segment).map { _ =>
        toDelete.dequeue()
        if (toDelete.nonEmpty) {
          InitiateDelete(leaderEpoch, toDelete)
        } else {
          info(s"Completed segment deletions for $topicIdPartition")
          CollectDeletableSegments(leaderEpoch)
        }
      }
    }
  }

  def getDelayFromLogConfig(log: Option[AbstractLog]): Long = {
    log.map(_.config.fileDeleteDelayMs).map(Long2long).getOrElse(0L)
  }

  def writeDeletionInitiatedMarker(tierTopicAppender: TierTopicAppender,
                                   leaderEpoch: Int,
                                   segment: TierObjectStore.ObjectMetadata)(implicit ec: ExecutionContext): Future[Unit] = {
    val marker = new TierSegmentDeleteInitiate(segment.topicIdPartition, leaderEpoch, segment.objectId)
    writeMarker(tierTopicAppender, leaderEpoch, marker)
  }

  def writeDeletionCompletedMarker(tierTopicAppender: TierTopicAppender,
                                   leaderEpoch: Int,
                                   objectMetadata: TierObjectStore.ObjectMetadata)(implicit ec: ExecutionContext): Future[Unit] = {
    val marker = new TierSegmentDeleteComplete(objectMetadata.topicIdPartition(), leaderEpoch, objectMetadata.objectId)
    writeMarker(tierTopicAppender, leaderEpoch, marker)
  }

  def writeMarker(tierTopicAppender: TierTopicAppender,
                  leaderEpoch: Int,
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
              throw new TierArchiverFencedException(marker.topicIdPartition)
            case _ =>
              throw new IllegalStateException(s"Unexpected append result for ${marker.topicIdPartition()}: $appendResult")
          }
      }
  }
}