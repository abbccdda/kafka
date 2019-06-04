/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.{File, IOException}
import java.time.Instant

import com.yammer.metrics.core.Meter
import kafka.log.{AbstractLog, LogSegment}
import kafka.server.ReplicaManager
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.tier.{TierTopicAppender, TopicIdPartition}
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.exceptions.{TierArchiverFatalException, TierArchiverFencedException, TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.serdes.State
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Random, Success, Try}

/*
ArchiveTask follows a state machine progression.
Each call to `transition` can either successfully transition
to the next status or remain in the current status
(after a configurable retry timeout).
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
        +------v-------+      |
        |              |      |
        | AfterUpload  +------+
        |              |
        +--------------+
 */

sealed trait ArchiveTaskState {
  val leaderEpoch: Int
}


/**
  * BeforeLeader represents a TopicPartition waiting for a successful fence TierTopic message
  * to go through. Once this has been realized by the TierTopicAppender, it is allowed to progress
  * to BeforeUpload.
  */
case class BeforeLeader(leaderEpoch: Int) extends ArchiveTaskState

/**
  * BeforeUpload represents a TopicPartition checking for eligible segments to upload. If there
  * are no eligible we remain in the current status, if there are eligible segments we transition
  * to AfterUpload on completion of segment (and associated metadata) upload.
  */
case class BeforeUpload(leaderEpoch: Int) extends ArchiveTaskState

/**
  * AfterUpload represents the TopicPartition writing out the TierObjectMetadata to the TierTopicAppender,
  * after the TierTopicAppender confirms that the TierObjectMetadata has been materialized, AfterUpload
  * transitions to BeforeUpload.
  */
case class AfterUpload(leaderEpoch: Int, metadata: TierObjectMetadata, beginUploadTime: Long) extends ArchiveTaskState

/**
  * Asynchronous state machine for archiving a topic partition.
  */
final class ArchiveTask(override val ctx: CancellationContext,
                        override val topicIdPartition: TopicIdPartition,
                        var state: ArchiveTaskState) extends ArchiverTaskQueueTask with Logging {

  @volatile var totalRetryCount: Int = 0
  @volatile private var retryCount: Int = 0
  @volatile private var _pausedUntil: Option[Instant] = None

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
      case s: BeforeUpload => ArchiveTask.tierSegment(s, topicIdPartition, time, tierTopicAppender, tierObjectStore, replicaManager, byteRateMetric)
      case s: AfterUpload => ArchiveTask.finalizeUpload(s, topicIdPartition, time, tierTopicAppender)
    }

    newState.map {
      result =>
        this.retryCount = 0
        this.state = result
        this
    }.recover {
      case e: TierArchiverFatalException =>
        error(s"$topicIdPartition encountered a fatal exception", e)
        ctx.cancel()
        throw e
      case e: TierArchiverFencedException =>
        warn(s"$topicIdPartition was fenced, stopping archival process", e)
        ctx.cancel()
        this
      case _: TierMetadataRetriableException | _: TierObjectStoreRetriableException =>
        warn(s"encountered a retriable exception archiving $topicIdPartition")
        retryTaskLater(time, maxRetryBackoffMs.getOrElse(5000))
        this
    }
  }

  override def toString = s"ArchiveTask($topicIdPartition, retries=$totalRetryCount, state=${state.getClass.getName}, cancelled=${ctx.isCancelled})"
}


object ArchiveTask extends Logging {
  def apply(ctx: CancellationContext, topicIdPartition: TopicIdPartition, leaderEpoch: Int): ArchiveTask = {
    new ArchiveTask(ctx, topicIdPartition, BeforeLeader(leaderEpoch))
  }

  private def createObjectMetadata(topicIdPartition: TopicIdPartition, tierEpoch: Int, logSegment: LogSegment, hasProducerState: Boolean, hasEpochState: Boolean): TierObjectMetadata = {
    val lastStableOffset = logSegment.readNextOffset - 1 // TODO: get from producer status snapshot
    val offsetDelta = lastStableOffset - logSegment.baseOffset
    new TierObjectMetadata(
      topicIdPartition,
      tierEpoch,
      logSegment.baseOffset,
      offsetDelta.intValue(),
      lastStableOffset,
      logSegment.largestTimestamp,
      logSegment.size,
      hasEpochState,
      hasProducerState,
      // TODO: compute whether any tx aborts occurred.
      false,
      State.AVAILABLE)
  }

  private def assertSegmentFileAccess(logSegment: LogSegment, leaderEpochCacheFile: Option[File]): Unit = {
    var fileListToCheck: List[File] = List(logSegment.log.file(),
      logSegment.offsetIndex.file,
      logSegment.timeIndex.file)
    if (leaderEpochCacheFile.isDefined)
      fileListToCheck :+= leaderEpochCacheFile.get

    val missing: List[File] =
      fileListToCheck
        .filterNot { f =>
          try {
            f.exists()
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
    leaderEpochCache.map(cache => {
      val checkpointClone = new LeaderEpochCheckpointFile(new File(cache.file.getAbsolutePath + ".tier"))
      val leaderEpochCacheClone = cache.clone(checkpointClone)
      leaderEpochCacheClone.truncateFromEnd(endOffset)
      leaderEpochCacheClone.file
    })
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

  private[archiver] def finalizeUpload(state: AfterUpload,
                                       topicIdPartition: TopicIdPartition,
                                       time: Time,
                                       tierTopicAppender: TierTopicAppender)
                                      (implicit ec: ExecutionContext): Future[BeforeUpload] = {
    Future.fromTry(Try(tierTopicAppender.addMetadata(state.metadata).toScala))
      .flatMap(identity)
      .map {
        case AppendResult.ACCEPTED =>
          info(s"Tiered log segment for $topicIdPartition in ${time.milliseconds - state.beginUploadTime} ms")
          BeforeUpload(state.leaderEpoch)
        case AppendResult.ILLEGAL =>
          throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicIdPartition in illegal status")
        case AppendResult.FENCED =>
          throw new TierArchiverFencedException(topicIdPartition)
      }
  }

  private[archiver] def tierSegment(state: BeforeUpload,
                                    topicIdPartition: TopicIdPartition,
                                    time: Time,
                                    tierTopicAppender: TierTopicAppender,
                                    tierObjectStore: TierObjectStore,
                                    replicaManager: ReplicaManager,
                                    byteRateMetric: Option[Meter] = None)
                                   (implicit ec: ExecutionContext): Future[ArchiveTaskState] = {
    Future {
      if (tierTopicAppender.partitionState(topicIdPartition).tierEpoch() != state.leaderEpoch) {
        throw new TierArchiverFencedException(topicIdPartition)
      } else {
        replicaManager
          .getLog(topicIdPartition.topicPartition())
          .flatMap { log =>
            log.tierableLogSegments
              .collectFirst { case logSegment: LogSegment => (log, logSegment) }
          } match {
          case None =>
            // Log has been moved or there is no eligible segment. Retry BeforeUpload state.
            debug(s"Transitioning back to BeforeUpload for $topicIdPartition as no log was found")
            Future(state)

          case Some((log: AbstractLog, logSegment: LogSegment)) =>
            // Upload next segment and transition.
            val nextOffset = logSegment.readNextOffset
            val leaderEpochStateFile = ArchiveTask.uploadableLeaderEpochState(log, nextOffset)
            // The producer state snapshot for `logSegment` should be named with the next logSegment's base offset
            // Because we never upload the active segment, and a snapshot is created on roll, we expect that either
            // this snapshot file is present, or the snapshot file was deleted.
            val producerStateFile: Option[File] = log.producerStateManager.snapshotFileForOffset(nextOffset)
            val startTime = time.milliseconds
            blocking {
              putSegment(state,
                topicIdPartition,
                tierObjectStore,
                logSegment,
                producerStateFile,
                leaderEpochStateFile)
                .map(meta => AfterUpload(state.leaderEpoch, meta, startTime))
            }
        }
      }
    }.flatMap(identity)
      .andThen {
        case Success(afterUpload: AfterUpload) =>
          byteRateMetric.foreach(_.mark(afterUpload.metadata.size()))
      }
  }

  private def putSegment(state: BeforeUpload,
                         topicIdPartition: TopicIdPartition,
                         tierObjectStore: TierObjectStore,
                         logSegment: LogSegment,
                         producerStateFile: Option[File],
                         leaderEpochCacheFile: Option[File])
                        (implicit ec: ExecutionContext): Future[TierObjectMetadata] = {
    Future {
      val metadata = createObjectMetadata(topicIdPartition, state.leaderEpoch, logSegment, producerStateFile.isDefined,
        leaderEpochCacheFile.isDefined)
      blocking {
        assertSegmentFileAccess(logSegment, leaderEpochCacheFile)
        tierObjectStore.putSegment(
          metadata,
          logSegment.log.file.toPath.toFile,
          logSegment.offsetIndex.file.toPath.toFile,
          logSegment.timeIndex.file.toPath.toFile,
          producerStateFile.asJava,
          logSegment.timeIndex.file.toPath.toFile, // FIXME transaction index
          leaderEpochCacheFile.asJava)
      }
      metadata
    }
  }
}
