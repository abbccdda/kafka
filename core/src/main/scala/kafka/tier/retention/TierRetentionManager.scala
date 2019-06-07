/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.retention

import java.util
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, Future, TimeUnit}
import java.util.function.Function

import kafka.log.{AbstractLog, TierLogSegment}
import kafka.server.ReplicaManager
import kafka.tier.domain.{AbstractTierMetadata, TierSegmentDeleteComplete, TierSegmentDeleteInitiate}
import kafka.tier.exceptions.TierArchiverFencedException
import kafka.tier.retention.TierRetentionManager.InitiateDelete
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.{TierMetadataManager, TierTopicManager, TopicIdPartition}
import kafka.utils.{Logging, Scheduler, threadsafe}
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionException

/**
  * Responsible for determining segments eligible for deletion in tiered storage due to retention. This module drives
  * deletions for all leader replicas on the broker.
  */
@threadsafe
class TierRetentionManager(scheduler: Scheduler,
                           replicaManager: ReplicaManager,
                           tierMetadataManager: TierMetadataManager,
                           tierTopicManager: TierTopicManager,
                           tierObjectStore: TierObjectStore,
                           retentionCheckMs: Long,
                           time: Time = Time.SYSTEM) extends Logging {
  private val inProgress = new util.HashMap[TopicIdPartition, Future[Option[State]]]()
  private val leaderReplicas = new ConcurrentHashMap[TopicIdPartition, Int]()

  tierMetadataManager.addListener(new TierMetadataManager.ChangeListener {
    override def onBecomeLeader(topicIdPartition: TopicIdPartition, leaderEpoch: Int): Unit = {
      leaderReplicas.put(topicIdPartition, leaderEpoch)
    }

    override def onBecomeFollower(topicIdPartition: TopicIdPartition): Unit = {
      leaderReplicas.remove(topicIdPartition)
    }

    override def onDelete(topicIdPartition: TopicIdPartition): Unit = {
      leaderReplicas.remove(topicIdPartition)
    }
  })

  def startup(): Unit = {
    if (scheduler != null) {
      info(s"Starting tier log deletion with a period of $retentionCheckMs ms.")
      scheduler.schedule("TierRetentionManager_transitions", () => makeTransitions(), delay = 0,
        period = retentionCheckMs, TimeUnit.MILLISECONDS)
    }
  }

  def shutdown(): Unit = {
    info("Shutdown complete")
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

    info(s"Found deletable tiered segments for ${log.topicPartition} with base offsets " +
      s"[${toDelete.map(_.baseOffet).mkString(",")}] due to $reason")
    toDelete.toList
  }

  // Delete segments which are eligible for deletion based on time-based retention and schedule them for deletion
  private def deleteRetentionMsBreachedSegments(log: AbstractLog, segments: Iterable[TierLogSegment]): Unit = {
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

  // Collect all deletable segments for this log and schedule them for deletion
  private[retention] def collectDeletableSegments(log: AbstractLog,
                                                  segments: Iterable[TierLogSegment]): List[TierObjectStore.ObjectMetadata] = {
    deleteRetentionMsBreachedSegments(log, segments)
    deleteRetentionSizeBreachedSegments(log, segments)
    deleteAndCollectLogStartOffsetBreachedSegments(log, segments)
  }

  private def deleteOldSegments(inProgress: util.Map[TopicIdPartition, Future[Option[State]]]): Unit = {
    val leaderReplicasIterator = leaderReplicas.entrySet.iterator

    while (inProgress.size < TierRetentionManager.MaxConcurrentInProgress && leaderReplicasIterator.hasNext) {
      val topicPartitionAndEpoch = leaderReplicasIterator.next()
      val topicIdPartition = topicPartitionAndEpoch.getKey
      val epoch = topicPartitionAndEpoch.getValue

      // If deletion for this topic partition is already in progress, we do not queue any more segments for deletion
      // until we are done with the current set. This is because the segments we found for deletion may already be
      // queued for deletion.
      if (!inProgress.containsKey(topicIdPartition)) {
        replicaManager.getLog(topicIdPartition.topicPartition).foreach { log =>
          val tieredSegments = log.tieredLogSegments
          if (tieredSegments.nonEmpty) {
            val deletable = collectDeletableSegments(log, tieredSegments)
            if (deletable.nonEmpty) {
              val initiateDelete = InitiateDelete(epoch, new util.LinkedList(deletable.asJava))
              val state = initiateDelete.transition(replicaManager, tierTopicManager, tierObjectStore, scheduler)
              inProgress.put(topicIdPartition, state)
            }
          }
        }
      }
    }
  }

  private[retention] def makeTransitions(): Unit = {
    // Check if any in progress deletion states have completed and transition them
    makeTransitions(inProgress)

    // Check if we can add more segments to the state machine
    deleteOldSegments(inProgress)
  }

  private[retention] def makeTransitions(inProgress: util.Map[TopicIdPartition, Future[Option[State]]]): Unit = {
    val inProgressIterator = inProgress.entrySet.iterator
    val transitions = new util.HashMap[TopicIdPartition, Future[Option[State]]]

    // Check for completed transitions and move them to next state
    while (inProgressIterator.hasNext) {
      val inProgress = inProgressIterator.next()
      val topicIdPartition = inProgress.getKey
      val future = inProgress.getValue

      if (future.isDone) {
        inProgressIterator.remove()

        try {
          future.get match {
            case Some(nextState) =>
              // Transition this partition further in the state machine
              transitions.put(topicIdPartition, nextState.transition(replicaManager, tierTopicManager, tierObjectStore, scheduler))

            case None =>
              info(s"Done with deletions for $topicIdPartition")
          }
        } catch {
          case t: Exception =>
            val underlying =
              t match {
                case e: ExecutionException => e.getCause
                case e => e
              }

            underlying match {
              case _: TierArchiverFencedException =>
                info(s"Cancelling deletions for $topicIdPartition on fenced exception")
              case e =>
                throw e
            }
        }
      }
    }
    inProgress.putAll(transitions)
  }
}

/**
  * Deletion for a set of segments for a particular topic partition follows a state machine progression. Each call to
  * `transition` can either successfully transition to the next state or remain in the current state on retriable
  * exceptions. The state machine exits (along with all other segments for this topic partition) when an attempt to
  * transition states is fenced or when all segments were successfully deleted.
  *
  * +-----------------+
  * |                 |
  * |  InitiateDelete <----+
  * |                 |    |
  * +-------+---------+    |
  *         |              |
  *         |              |
  * +-------v---------+    |
  * |                 |    |
  * |     Delete      |    |
  * |                 |    |
  * +-------+---------+    |
  *         |              |
  *         |              |
  * +-------v---------+    |
  * |                 |    |
  * | CompleteDelete  +----+
  * |                 |
  * +-----------------+
  */
private[retention] sealed trait State {
  def leaderEpoch: Int
  def segments: util.Queue[TierObjectStore.ObjectMetadata]
  def currentSegment: Option[TierObjectStore.ObjectMetadata] = Option(segments.peek)
  def next: Option[State]
  def transition(replicaManager: ReplicaManager,
                 tierTopicManager: TierTopicManager,
                 tierObjectStore: TierObjectStore,
                 scheduler: Scheduler): Future[Option[State]]
}

object TierRetentionManager extends Logging {
  override protected def loggerName: String = classOf[TierRetentionManager].getName

  val MaxConcurrentInProgress = 10

  /**
    * This represents the first stage for deleting a particular segment. It signals the start of segment deletion by
    * emitting a DeleteInitiate message to the tier topic and awaits successful materialization.
    */
  private[retention] case class InitiateDelete(leaderEpoch: Int, segments: util.Queue[TierObjectStore.ObjectMetadata]) extends State {
    override def transition(replicaManager: ReplicaManager,
                            tierTopicManager: TierTopicManager,
                            tierObjectStore: TierObjectStore,
                            scheduler: Scheduler): Future[Option[State]] = {
      val metadata = segments.peek
      val initiateDelete = new TierSegmentDeleteInitiate(metadata.topicIdPartition, leaderEpoch, metadata.objectId)
      debug(s"InitiateDelete for $this")
      addMetadata(tierTopicManager, initiateDelete, this)
    }

    override def next: Option[State] = Some(Delete(leaderEpoch, segments))

    override def toString: String = s"InitiateDelete(segment: $currentSegment leaderEpoch: $leaderEpoch)"
  }

  /**
    * This represents the second stage for deleting a particular segment. It deletes the segment and any associated
    * metadata like indices, etc. from tiered storage.
    */
  private[retention] case class Delete(leaderEpoch: Int, segments: util.Queue[TierObjectStore.ObjectMetadata]) extends State {
    override def transition(replicaManager: ReplicaManager,
                            tierTopicManager: TierTopicManager,
                            tierObjectStore: TierObjectStore,
                            scheduler: Scheduler): Future[Option[State]] = {
      val metadata = segments.peek
      val future = new CompletableFuture[Option[State]]()
      val logOpt = replicaManager.getLog(metadata.topicIdPartition.topicPartition)
      val delayMs = logOpt.map(_.config.fileDeleteDelayMs).map(Long2long).getOrElse(0L)
      debug(s"Scheduling Delete for $this after ${delayMs}ms delay")

      scheduler.schedule("TierRetentionManager_DeleteSegment", () => delete(), delay = delayMs, unit = TimeUnit.MILLISECONDS)

      def delete(): Unit = {
        tierObjectStore.deleteSegment(metadata)
        future.complete(next)
      }

      future.exceptionally {
        exceptionFn(this)
      }
    }

    override def next: Option[State] = Some(CompleteDelete(leaderEpoch, segments))

    override def toString: String = s"Delete(segment: $currentSegment leaderEpoch: $leaderEpoch)"
  }

  /**
    * This represents the final stage for deleting a particular segment. It signals successful completion for deleting
    * this segment by emitting a DeleteComplete message to the tier topic and awaits successful materialization.
    */
  private[retention] case class CompleteDelete(leaderEpoch: Int, segments: util.Queue[TierObjectStore.ObjectMetadata]) extends State {
    override val currentSegment: Option[TierObjectStore.ObjectMetadata] = super.currentSegment

    override def transition(replicaManager: ReplicaManager,
                            tierTopicManager: TierTopicManager,
                            tierObjectStore: TierObjectStore,
                            scheduler: Scheduler): Future[Option[State]] = {
      val metadata = segments.peek
      val completeDelete = new TierSegmentDeleteComplete(metadata.topicIdPartition, leaderEpoch, metadata.objectId)
      debug(s"Completing deletion for $this")
      addMetadata(tierTopicManager, completeDelete, this)
    }

    override def next: Option[State] = {
      segments.poll()
      if (segments.isEmpty)
        None
      else
        Some(InitiateDelete(leaderEpoch, segments))
    }

    override def toString: String = s"CompleteDelete(segment: $currentSegment leaderEpoch: $leaderEpoch)"
  }

  private def exceptionFn(currentState: State): Function[Throwable, Option[State]] = {
    new Function[Throwable, Option[State]] {
      override def apply(t: Throwable): Option[State] = {
        t match {
          case e: RetriableException =>
            // Retry current state
            debug(s"Retrying $currentState", e)
            Some(currentState)

          case _ =>
            throw t
        }
      }
    }
  }

  private def addMetadata(tierTopicManager: TierTopicManager, metadata: AbstractTierMetadata, currentState: State): CompletableFuture[Option[State]] = {
    val future: CompletableFuture[Option[State]] =
      tierTopicManager.addMetadata(metadata)
        .thenApply {
          new Function[AppendResult, Option[State]] {
            override def apply(result: AppendResult): Option[State] = {
              result match {
                case AppendResult.ACCEPTED =>
                  debug(s"Successful transition for $currentState")
                  currentState.next
                case AppendResult.FENCED =>
                  // Remove all in-queue deletions for this topic partition, as an attempt to delete a segment for it
                  // was fenced
                  info(s"Stopping state machine for $currentState as attempt to transition was fenced")
                  throw new TierArchiverFencedException(metadata.topicIdPartition)
                case _ =>
                  throw new IllegalStateException(s"Unexpected state for $currentState: $result")
              }
            }
          }
        }
    future.exceptionally {
      exceptionFn(currentState)
    }
  }
}
