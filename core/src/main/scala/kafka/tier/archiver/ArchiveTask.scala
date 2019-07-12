/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.time.Instant
import java.util.UUID

import com.yammer.metrics.core.Meter
import kafka.log.{AbortedTxn, AbstractLog, LogSegment}
import kafka.server.ReplicaManager
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.tier.archiver.ArchiveTask.SegmentDeletedException
import kafka.tier.{TierTopicAppender, TopicIdPartition}
import kafka.tier.domain.{TierSegmentUploadComplete, TierSegmentUploadInitiate}
import kafka.tier.exceptions.{TierArchiverFatalException, TierArchiverFencedException, TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.utils.Logging
import org.apache.kafka.common.errors.RetriableException
import org.apache.kafka.common.utils.Time

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Random, Try}

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

case class UploadableSegment(log: AbstractLog,
                             logSegment: LogSegment,
                             producerStateOpt: Option[File],
                             leaderEpochStateOpt: Option[File],
                             abortedTxnIndexOpt: Option[ByteBuffer]) {

  val uploadedSize: Long = logSegment.size + Seq(producerStateOpt.map(_.length),
                                                  leaderEpochStateOpt.map(_.length),
                                                  abortedTxnIndexOpt.map(bb => (bb.limit() - bb.position()).toLong)
                                                ).map(_.getOrElse(0L)).sum
}

/**
  * Asynchronous state machine for archiving a topic partition.
  */
final class ArchiveTask(override val ctx: CancellationContext,
                        override val topicIdPartition: TopicIdPartition,
                        var state: ArchiveTaskState) extends ArchiverTaskQueueTask with Logging {

  @volatile var totalRetryCount: Int = 0
  @volatile private var retryCount: Int = 0
  @volatile private var _pausedUntil: Option[Instant] = None

  override def loggerName: String = classOf[ArchiveTask].getName

  override def pausedUntil: Option[Instant] = _pausedUntil

  /**
    * Pause this task for later retry
    */
  private def retryTaskLater(time: Time, maxRetryBackoffMs: Int): Unit = {
    retryCount += 1
    totalRetryCount += 1
    val now = Instant.ofEpochMilli(time.milliseconds())
    val pauseMs = Math.min(maxRetryBackoffMs, Random.nextInt(retryCount) * 1000)
    warn(s"pausing archiving of $topicIdPartition for ${pauseMs}ms")
    _pausedUntil = Some(now.plusMillis(pauseMs))
  }

  def transition(time: Time,
                 tierTopicAppender: TierTopicAppender,
                 tierObjectStore: TierObjectStore,
                 replicaManager: ReplicaManager,
                 byteRateMetric: Option[Meter] = None,
                 maxRetryBackoffMs: Option[Int] = None)(implicit ec: ExecutionContext): Future[ArchiveTask] = {
    val newState = state match {
      case _ if ctx.isCancelled => Future(state)
      case s: BeforeLeader => ArchiveTask.establishLeadership(s, topicIdPartition, tierTopicAppender)
      case s: BeforeUpload => ArchiveTask.maybeInitiateUpload(s, topicIdPartition, time, tierTopicAppender, tierObjectStore, replicaManager)
      case s: Upload => ArchiveTask.upload(s, topicIdPartition, time, tierObjectStore)
      case s: AfterUpload => ArchiveTask.finalizeUpload(s, topicIdPartition, time, tierTopicAppender, byteRateMetric)
    }

    newState.map {
      result =>
        this.retryCount = 0
        this._pausedUntil = None
        this.state = result
        this
    }.recover {
      case _: TierMetadataRetriableException | _: TierObjectStoreRetriableException =>
        info(s"encountered a retriable exception archiving $topicIdPartition")
        retryTaskLater(time, maxRetryBackoffMs.getOrElse(5000))
        this
      case e: TierArchiverFencedException =>
        info(s"$topicIdPartition was fenced, stopping archival process", e)
        ctx.cancel()
        this
      case e: SegmentDeletedException =>
        this.state = state.handleSegmentDeletedException(e)
        retryTaskLater(time, maxRetryBackoffMs.getOrElse(5000))
        info(s"Retrying ${this.state} as segment was deleted", e)
        this
      case e: Throwable =>
        error(s"$topicIdPartition encountered a fatal exception", e)
        ctx.cancel()
        throw e
    }
  }

  override def toString = s"ArchiveTask($topicIdPartition, retries=$totalRetryCount, state=${state.getClass.getName}, epoch=${state.leaderEpoch}, cancelled=${ctx.isCancelled})"
}


object ArchiveTask extends Logging {
  override protected def loggerName: String = classOf[ArchiveTask].getName

  def apply(ctx: CancellationContext, topicIdPartition: TopicIdPartition, leaderEpoch: Int): ArchiveTask = {
    new ArchiveTask(ctx, topicIdPartition, BeforeLeader(leaderEpoch))
  }

  private def assertSegmentFileAccess(uploadableSegment: UploadableSegment): Unit = {
    val logSegment = uploadableSegment.logSegment

    var fileListToCheck: List[File] = List(logSegment.log.file, logSegment.offsetIndex.file, logSegment.timeIndex.file)
    uploadableSegment.producerStateOpt.foreach(fileListToCheck :+= _)
    uploadableSegment.leaderEpochStateOpt.foreach(fileListToCheck :+= _)

    val missing: List[File] =
      fileListToCheck
        .filterNot { f =>
          try {
            f.exists
          } catch {
            case _: SecurityException => false
          }
        }
    if (missing.nonEmpty)
      throw new IOException(s"Tier archiver could not read segment files: ${missing.mkString(", ")}")
  }

  /**
    * Get an uploadable leader epoch state file by cloning state from leader epoch cache and truncating
    * it to the endOffset
    */
  private def uploadableLeaderEpochState(log: AbstractLog, endOffset: Long): Option[File] = {
    val leaderEpochCache = log.leaderEpochCache
    leaderEpochCache.map { cache =>
      val checkpointClone = new LeaderEpochCheckpointFile(new File(cache.file.getAbsolutePath + ".tier"))
      val leaderEpochCacheClone = cache.clone(checkpointClone)
      leaderEpochCacheClone.truncateFromEnd(endOffset)
      leaderEpochCacheClone.file
    }
  }

  /**
    * Collect the set of aborted transactions between baseOffset and endOffset into a ByteBuffer for tiering.
    */
  private def uploadableAbortedTransactionList(log: AbstractLog, baseOffset: Long, endOffset: Long): Option[ByteBuffer] = {
    val abortedTxnsList = log.collectAbortedTransactions(baseOffset, endOffset)
    serializeAbortedTransactions(abortedTxnsList)
  }

  // visible for testing
  /**
    * Serializes the provided AbortedTxns to a ByteBuffer for tiering.
    */
  def serializeAbortedTransactions(abortedTxnsList: Seq[AbortedTxn]): Option[ByteBuffer] = {
    var maybeAbortedTxnsBuf: Option[ByteBuffer] = None
    if (abortedTxnsList.nonEmpty) {
      val buf = ByteBuffer.allocate(abortedTxnsList.length * AbortedTxn.TotalSize)
      for (abortedTxn <- abortedTxnsList) {
        buf.put(abortedTxn.buffer.duplicate())
      }
      buf.flip()
      maybeAbortedTxnsBuf = Some(buf)
    }
    maybeAbortedTxnsBuf
  }

  private[archiver] def establishLeadership(state: BeforeLeader,
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
          case AppendResult.ILLEGAL =>
            throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicIdPartition in illegal status.")
          case AppendResult.FENCED =>
            throw new TierArchiverFencedException(topicIdPartition)
          case appendResult =>
            throw new TierArchiverFatalException(s"Unknown AppendResult $appendResult")
        }
      }
  }

  private[archiver] def maybeInitiateUpload(state: BeforeUpload,
                                            topicIdPartition: TopicIdPartition,
                                            time: Time,
                                            tierTopicAppender: TierTopicAppender,
                                            tierObjectStore: TierObjectStore,
                                            replicaManager: ReplicaManager)
                                           (implicit ec: ExecutionContext): Future[ArchiveTaskState] = {
    Future {
      if (tierTopicAppender.partitionState(topicIdPartition).tierEpoch != state.leaderEpoch) {
        throw new TierArchiverFencedException(topicIdPartition)
      } else {
        replicaManager
          .getLog(topicIdPartition.topicPartition)
          .flatMap { log =>
            log.tierableLogSegments
              .collectFirst { case logSegment: LogSegment => (log, logSegment) }
          } match {
          case None =>
            // Log has been moved or there is no eligible segment. Retry BeforeUpload state.
            debug(s"Transitioning back to BeforeUpload for $topicIdPartition as log has moved or no tierable segments were found")
            Future(state)

          case Some((log: AbstractLog, logSegment: LogSegment)) =>
            val nextOffset: Long = logSegment.readNextOffset
            val segment = uploadableSegment(log, logSegment, nextOffset)
            val uploadInitiate = new TierSegmentUploadInitiate(topicIdPartition,
              state.leaderEpoch,
              UUID.randomUUID,
              logSegment.baseOffset,
              nextOffset - 1,
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
                  info(s"Completed initiateUpload for $topicIdPartition in ${time.milliseconds - startTime}ms")
                  Upload(state.leaderEpoch, uploadInitiate, segment)
                case AppendResult.ILLEGAL =>
                  throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicIdPartition in illegal status")
                case AppendResult.FENCED =>
                  throw new TierArchiverFencedException(topicIdPartition)
              }
        }
      }
    }.flatMap(identity)
  }

  private[archiver] def upload(state: Upload,
                               topicIdPartition: TopicIdPartition,
                               time: Time,
                               tierObjectStore: TierObjectStore)
                              (implicit ec: ExecutionContext): Future[AfterUpload] = {
    Future {
      val logSegment = state.uploadableSegment.logSegment
      val producerStateOpt = state.uploadableSegment.producerStateOpt
      val leaderEpochStateOpt = state.uploadableSegment.leaderEpochStateOpt
      val abortedTransactions = state.uploadableSegment.abortedTxnIndexOpt
      val uploadInitiate = state.uploadInitiate

      val metadata = new TierObjectStore.ObjectMetadata(uploadInitiate.topicIdPartition,
        uploadInitiate.objectId,
        uploadInitiate.tierEpoch,
        uploadInitiate.baseOffset,
        abortedTransactions.isDefined)

      blocking {
        val startTime = time.milliseconds

        try {
          assertSegmentFileAccess(state.uploadableSegment)
          tierObjectStore.putSegment(metadata,
            logSegment.log.file.toPath.toFile,
            logSegment.offsetIndex.file.toPath.toFile,
            logSegment.timeIndex.file.toPath.toFile,
            producerStateOpt.asJava,
            abortedTransactions.asJava,
            leaderEpochStateOpt.asJava)
        } catch {
          case e: Exception if !segmentExists(state.uploadableSegment.log, logSegment) =>
            throw new SegmentDeletedException(s"Segment $logSegment of $topicIdPartition deleted when tiering", e)
        }

        info(s"Uploaded segment for $topicIdPartition in ${time.milliseconds - startTime}ms")
        AfterUpload(state.leaderEpoch, uploadInitiate, state.uploadableSegment.uploadedSize)
      }
    }
  }

  private[archiver] def finalizeUpload(state: AfterUpload,
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
          info(s"Finalized upload segment for $topicIdPartition in ${time.milliseconds - startTime} ms")
          byteRateMetric.foreach(_.mark(state.uploadedSize))
          BeforeUpload(state.leaderEpoch)
        case AppendResult.ILLEGAL =>
          throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicIdPartition in illegal status")
        case AppendResult.FENCED =>
          throw new TierArchiverFencedException(topicIdPartition)
      }
  }

  private[archiver] def uploadableSegment(log: AbstractLog, logSegment: LogSegment, nextOffset: Long): UploadableSegment = {
    try {
      val leaderEpochStateOpt = ArchiveTask.uploadableLeaderEpochState(log, nextOffset)
      // The producer state snapshot for `logSegment` should be named with the next logSegment's base offset
      // Because we never upload the active segment, and a snapshot is created on roll, we expect that either
      // this snapshot file is present, or the snapshot file was deleted.
      val producerStateOpt: Option[File] = log.producerStateManager.snapshotFileForOffset(nextOffset)
      val abortedTransactions = ArchiveTask.uploadableAbortedTransactionList(log, logSegment.baseOffset, nextOffset)
      UploadableSegment(log, logSegment, producerStateOpt, leaderEpochStateOpt, abortedTransactions)
    } catch {
      case e: Exception if !segmentExists(log, logSegment) =>
        throw new SegmentDeletedException(s"Segment $logSegment of ${log.topicPartition} deleted when tiering", e)
    }
  }

  private def segmentExists(log: AbstractLog, segment: LogSegment): Boolean = {
    val baseOffset = segment.baseOffset
    log.localLogSegments(baseOffset, baseOffset + 1).nonEmpty
  }

  case class SegmentDeletedException(msg: String, cause: Throwable) extends RetriableException(msg, cause)
}
