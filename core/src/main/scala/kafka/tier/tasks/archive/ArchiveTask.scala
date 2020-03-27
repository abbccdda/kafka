/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.tasks.archive

import java.nio.file.NoSuchFileException
import java.util.UUID

import com.yammer.metrics.core.Meter
import kafka.log.{AbstractLog, LogSegment, UploadableSegment}
import kafka.server.ReplicaManager
import kafka.tier.TopicIdPartition
import kafka.tier.domain.{TierSegmentUploadComplete, TierSegmentUploadInitiate}
import kafka.tier.exceptions.{NotTierablePartitionException, TierArchiverFailedException, TierArchiverFatalException, TierArchiverFencedException, TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.topic.TierTopicAppender
import kafka.tier.tasks.TierTask
import kafka.tier.tasks.archive.ArchiveTask.SegmentDeletedException
import kafka.utils.Logging
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.utils.Time

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.Try

/*
ArchiveTask follows a state machine progression. Each call to `transition` can either successfully transition to the
next state or remain in the current state (after a configurable retry timeout). Unexpected exceptions during state
transitions cause us to exit the state machine, except when we encounter them during BeforeUpload or Upload states,
which causes a retry of the BeforeUpload state if the segment we were trying to upload no longer exists.

        +----------------+
        |                |
        |  BeforeLeader  |
        |                |
        +------+---------+
               |
               |
        +------v--------+
        |               |
        | BeforeUpload  <-----+
        |               |     |
        +------+--------+     |
               |              |
               |              |
        +------v---------+    |
        |                |    |
        |     Upload     +--->+
        |                |    |
        +------+---------+    |
               |              |
               |              |
        +------v-------+      |
        |              |      |
        | AfterUpload  +------+
        |              |
        +--------------+
 */

object Defaults {
  val OBJECT_STORE_EXCEPTION_RETRY_MS = 15000
  val METADATA_EXCEPTION_RETRY_MS = 5000
  val SEGMENT_DELETED_RETRY_MS = 5000
}

sealed trait ArchiveTaskState {
  def leaderEpoch: Int
  def handleSegmentDeletedException(e: SegmentDeletedException): ArchiveTaskState = throw e
}

/**
  * BeforeLeader represents a TopicPartition waiting for a successful fence TierTopic message
  * to go through. Once this has been realized by the TierTopicAppender, it is allowed to progress
  * to BeforeUpload.
  */
case class BeforeLeader(leaderEpoch: Int) extends ArchiveTaskState

/**
  * BeforeUpload represents a TopicPartition checking for eligible segments to upload. If there are no eligible segments,
  * we remain in the current status. If there are eligible segments we transition to Upload after successfully
  * writing out UploadInitiate to the TierTopicManager which marks an attempt to upload a segment to tiered storage.
  */
case class BeforeUpload(leaderEpoch: Int) extends ArchiveTaskState {
  // If the segment was deleted while we were trying to upload, we retry the BeforeUpload state. This allows us to
  // recompute the next tierable segment if there is any.
  override def handleSegmentDeletedException(e: SegmentDeletedException): BeforeUpload = this
}

/**
  * Upload represents writing out the segment data and any associated metadata like indices, epoch state, etc. to tiered
  * storage. On success, we transition to AfterUpload state.
  */
case class Upload(leaderEpoch: Int, uploadInitiate: TierSegmentUploadInitiate, uploadableSegment: UploadableSegment) extends ArchiveTaskState {
  // If the segment was deleted while we were trying to upload, we go back to the BeforeUpload state. This allows us to
  // recompute the next tierable segment if there is any.
  override def handleSegmentDeletedException(e: SegmentDeletedException): BeforeUpload = BeforeUpload(leaderEpoch)
}

/**
  * AfterUpload represents the TopicPartition writing out UploadComplete to the TierTopicManager signalling the end of
  * successful upload to tiered storage. After the TierTopicManager confirms that UploadComplete has been materialized,
  * we transition back to BeforeUpload to find the next segment to upload.
  */
case class AfterUpload(leaderEpoch: Int, uploadInitiate: TierSegmentUploadInitiate, uploadedSize: Long) extends ArchiveTaskState


/**
  * Asynchronous state machine for archiving a topic partition.
  */
final class ArchiveTask(override val ctx: CancellationContext,
                        override val topicIdPartition: TopicIdPartition,
                        var state: ArchiveTaskState,
                        archiverMetrics: ArchiverMetrics) extends TierTask[ArchiveTask](archiverMetrics.retryRateOpt) with Logging {

  override def loggerName: String = classOf[ArchiveTask].getName

  override def transition(time: Time,
                          tierTopicAppender: TierTopicAppender,
                          tierObjectStore: TierObjectStore,
                          replicaManager: ReplicaManager,
                          maxRetryBackoffMs: Option[Int] = None)(implicit ec: ExecutionContext): Future[ArchiveTask] = {

    val newState = {
      // This is just a best effort check, we would like to avoid doing any additional work when
      // we know upfront that the context has been cancelled.
      if (ctx.isCancelled) {
        Future.successful(state)
      } else {
        state match {
          case s: BeforeLeader => ArchiveTask.establishLeadership(s, topicIdPartition, tierTopicAppender)
          case s: BeforeUpload => ArchiveTask.maybeInitiateUpload(s, topicIdPartition, time, tierTopicAppender, tierObjectStore, replicaManager)
          case s: Upload => ArchiveTask.upload(s, topicIdPartition, time, tierObjectStore)
          case s: AfterUpload => ArchiveTask.finalizeUpload(s, topicIdPartition, time, tierTopicAppender, archiverMetrics.byteRateOpt)
        }
      }
    }

    newState.map { result =>
      onSuccessfulTransition()
      state = result
      this
    }.recover {
      case e: TierMetadataRetriableException =>
        retryTaskLater(maxRetryBackoffMs.getOrElse(Defaults.METADATA_EXCEPTION_RETRY_MS), time.hiResClockMs(), e)
        this
      case e: TierObjectStoreRetriableException =>
        retryTaskLater(maxRetryBackoffMs.getOrElse(Defaults.OBJECT_STORE_EXCEPTION_RETRY_MS), time.hiResClockMs(), e)
        this
      case e: TierArchiverFailedException =>
        warn(s"$topicIdPartition failed, stopping archival process and marking $topicIdPartition to be in error", e)
        cancelAndSetErrorState(this, e)
        this
      case e: TierArchiverFencedException =>
        info(s"$topicIdPartition was fenced, stopping archival process", e)
        ctx.cancel()
        this
      case e: NotTierablePartitionException =>
        info(s"$topicIdPartition is not tierable and cannot be archived", e)
        ctx.cancel()
        this
      case e: SegmentDeletedException =>
        state = state.handleSegmentDeletedException(e)
        retryTaskLater(maxRetryBackoffMs.getOrElse(Defaults.SEGMENT_DELETED_RETRY_MS), time.hiResClockMs(), e)
        this
      case t: Throwable =>
        error(s"$topicIdPartition failed due to unhandled exception, stopping archival process and marking $topicIdPartition to be in error", t)
        cancelAndSetErrorState(this, t)
        this
    }
  }

  override def toString = s"ArchiveTask($topicIdPartition, state=${state.getClass.getName}, epoch=${state.leaderEpoch}, cancelled=${ctx.isCancelled})"
}

object ArchiveTask extends Logging {
  override protected def loggerName: String = classOf[ArchiveTask].getName

  def apply(ctx: CancellationContext, topicIdPartition: TopicIdPartition, leaderEpoch: Int, archiverMetrics: ArchiverMetrics): ArchiveTask = {
    new ArchiveTask(ctx, topicIdPartition, BeforeLeader(leaderEpoch), archiverMetrics)
  }

  private[archive] def establishLeadership(state: BeforeLeader,
                                            topicIdPartition: TopicIdPartition,
                                            tierTopicAppender: TierTopicAppender)
                                           (implicit ec: ExecutionContext): Future[BeforeUpload] = {
    Future.fromTry(
      Try(tierTopicAppender.becomeArchiver(topicIdPartition, state.leaderEpoch).toScala))
      .flatMap(identity)
      .map { result: AppendResult =>
        result match {
          case AppendResult.ACCEPTED =>
            info(s"established leadership for $topicIdPartition")
            BeforeUpload(state.leaderEpoch)
          case AppendResult.FAILED =>
            throw new TierArchiverFailedException(topicIdPartition)
          case AppendResult.NOT_TIERABLE =>
            throw new NotTierablePartitionException(topicIdPartition)
          case AppendResult.FENCED =>
            throw new TierArchiverFencedException(topicIdPartition)
          case appendResult =>
            throw new TierArchiverFatalException(s"Unknown AppendResult $appendResult")
        }
      }
  }

  private[archive] def maybeInitiateUpload(state: BeforeUpload,
                                           topicIdPartition: TopicIdPartition,
                                           time: Time,
                                           tierTopicAppender: TierTopicAppender,
                                           tierObjectStore: TierObjectStore,
                                           replicaManager: ReplicaManager)
                                          (implicit ec: ExecutionContext): Future[ArchiveTaskState] = {
    Future {
      replicaManager
        .getLog(topicIdPartition.topicPartition)
        .flatMap { log =>
          if (log.tierPartitionState.tierEpoch != state.leaderEpoch)
            throw new TierArchiverFencedException(topicIdPartition)

          log.tierableLogSegments
            .collectFirst { case logSegment: LogSegment => (log, logSegment) }
        } match {
        case None =>
          // Log has been moved or there is no eligible segment. Retry BeforeUpload state.
          debug(s"Transitioning back to BeforeUpload for $topicIdPartition as log has moved or no tierable segments were found")
          Future(state)

        case Some((log: AbstractLog, logSegment: LogSegment)) =>
          val segment = uploadableSegment(log, logSegment, topicIdPartition)

          // abort early if the log has been deleted
          if (log.isDeleted)
            throw new NotTierablePartitionException(topicIdPartition)

          val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition,
            state.leaderEpoch,
            UUID.randomUUID,
            logSegment.baseOffset,
            segment.nextOffset - 1,
            logSegment.largestTimestamp,
            logSegment.size,
            segment.leaderEpochStateOpt.isDefined,
            segment.abortedTxnIndexOpt.isDefined,
            segment.producerStateOpt.isDefined)

          val startTime = time.milliseconds
          Future.fromTry(Try(tierTopicAppender.addMetadata(uploadInitiate).toScala))
            .flatMap(identity)
            .map {
              case AppendResult.ACCEPTED =>
                info(s"Completed UploadInitiate(objectId: ${uploadInitiate.messageId}, baseOffset: ${uploadInitiate.baseOffset}," +
                  s" endOffset: ${uploadInitiate.endOffset}]) for $topicIdPartition in ${time.milliseconds - startTime}ms")
                Upload(state.leaderEpoch, uploadInitiate, segment)
              case AppendResult.FAILED =>
                throw new TierArchiverFailedException(topicIdPartition)
              case AppendResult.NOT_TIERABLE =>
                throw new NotTierablePartitionException(topicIdPartition)
              case AppendResult.FENCED =>
                throw new TierArchiverFencedException(topicIdPartition)
            }
      }
    }.flatMap(identity)
  }

  private[archive] def upload(state: Upload,
                              topicIdPartition: TopicIdPartition,
                              time: Time,
                              tierObjectStore: TierObjectStore)
                             (implicit ec: ExecutionContext): Future[AfterUpload] = {
    Future {
      val uploadableSegment = state.uploadableSegment
      val uploadInitiate = state.uploadInitiate

      val metadata = new TierObjectStore.ObjectMetadata(uploadInitiate.topicIdPartition,
        uploadInitiate.objectId,
        uploadInitiate.tierEpoch,
        uploadInitiate.baseOffset,
        uploadableSegment.abortedTxnIndexOpt.isDefined,
        uploadableSegment.producerStateOpt.isDefined,
        uploadableSegment.leaderEpochStateOpt.isDefined)

      blocking {
        val startTime = time.milliseconds

        try {
          tierObjectStore.putSegment(metadata,
            uploadableSegment.logSegmentFile,
            uploadableSegment.offsetIndex,
            uploadableSegment.timeIndex,
            uploadableSegment.producerStateOpt.asJava,
            uploadableSegment.abortedTxnIndexOpt.asJava,
            uploadableSegment.leaderEpochStateOpt.asJava)
        } catch {
          case e: Exception if !segmentFilesExist(state.uploadableSegment) =>
            throw SegmentDeletedException(s"Segment ${uploadableSegment.logSegmentFile.getAbsolutePath} of $topicIdPartition deleted when tiering", e)
        }

        info(s"Uploaded segment for $topicIdPartition in ${time.milliseconds - startTime}ms")
        AfterUpload(state.leaderEpoch, uploadInitiate, state.uploadableSegment.uploadedSize)
      }
    }
  }

  private[archive] def finalizeUpload(state: AfterUpload,
                                       topicIdPartition: TopicIdPartition,
                                       time: Time,
                                       tierTopicAppender: TierTopicAppender,
                                       byteRateMetric: Option[Meter])
                                      (implicit ec: ExecutionContext): Future[BeforeUpload] = {
    val uploadComplete = new TierSegmentUploadComplete(state.uploadInitiate)
    val startTime = time.milliseconds

    Future.fromTry(Try(tierTopicAppender.addMetadata(uploadComplete).toScala))
      .flatMap(identity)
      .map {
        case AppendResult.ACCEPTED =>
          info(s"Finalized UploadComplete(${uploadComplete.messageId()}) " +
            s"for $topicIdPartition in ${time.milliseconds - startTime} ms")
          byteRateMetric.foreach(_.mark(state.uploadedSize))
          BeforeUpload(state.leaderEpoch)
        case AppendResult.FAILED =>
          throw new TierArchiverFailedException(topicIdPartition)
        case AppendResult.NOT_TIERABLE =>
          throw new NotTierablePartitionException(topicIdPartition)
        case AppendResult.FENCED =>
          throw new TierArchiverFencedException(topicIdPartition)
      }
  }

  private[archive] def uploadableSegment(log: AbstractLog, logSegment: LogSegment, topicIdPartition: TopicIdPartition): UploadableSegment = {
    try {
      log.createUploadableSegment(logSegment)
    } catch {
      case e: NoSuchFileException =>
        throw SegmentDeletedException(s"Segment $logSegment of $topicIdPartition deleted when tiering", e)
    }
  }

  private def segmentFilesExist(uploadableSegment: UploadableSegment): Boolean = {
    uploadableSegment.allFiles.forall(_.exists)
  }

  case class SegmentDeletedException(msg: String, cause: Throwable) extends RetriableException(msg, cause)
}
