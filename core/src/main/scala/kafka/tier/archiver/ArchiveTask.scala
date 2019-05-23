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
import kafka.tier.TierTopicManager
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.exceptions.{TierArchiverFatalException, TierArchiverFencedException, TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.serdes.State
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
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
  * to go through. Once this has been realized by the TierTopicManager, it is allowed to progress
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
  * AfterUpload represents the TopicPartition writing out the TierObjectMetadata to the TierTopicManager,
  * after the TierTopicManager confirms that the TierObjectMetadata has been materialized, AfterUpload
  * transitions to BeforeUpload.
  */
case class AfterUpload(leaderEpoch: Int, metadata: TierObjectMetadata, beginUploadTime: Long) extends ArchiveTaskState

/**
  * Asynchronous state machine for archiving a topic partition.
  */
final class ArchiveTask(override val ctx: CancellationContext,
                        override val topicPartition: TopicPartition,
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
    warn(s"pausing archiving of $topicPartition for ${pauseMs}ms")
    _pausedUntil = Some(now.plusMillis(pauseMs))
  }

  def transition(time: Time,
                 tierTopicManager: TierTopicManager,
                 tierObjectStore: TierObjectStore,
                 replicaManager: ReplicaManager,
                 byteRateMetric: Option[Meter] = None,
                 maxRetryBackoffMs: Option[Int] = None)(implicit ec: ExecutionContext): Future[ArchiveTask] = {

    val newState = state match {
      case _ if ctx.isCancelled => Future(state)
      case s: BeforeLeader => ArchiveTask.establishLeadership(s, topicPartition, tierTopicManager)
      case s: BeforeUpload => ArchiveTask.tierSegment(s, topicPartition, time, tierTopicManager, tierObjectStore, replicaManager, byteRateMetric)
      case s: AfterUpload => ArchiveTask.finalizeUpload(s, topicPartition, time, tierTopicManager)
    }

    newState.map {
      result =>
        this.retryCount = 0
        this.state = result
        this
    }.recover {
      case e: TierArchiverFatalException =>
        error(s"$topicPartition encountered a fatal exception", e)
        ctx.cancel()
        throw e
      case e: TierArchiverFencedException =>
        warn(s"$topicPartition was fenced, stopping archival process", e)
        ctx.cancel()
        this
      case _: TierMetadataRetriableException | _: TierObjectStoreRetriableException =>
        warn(s"encountered a retriable exception archiving $topicPartition")
        retryTaskLater(time, maxRetryBackoffMs.getOrElse(5000))
        this
    }
  }

  override def toString = s"ArchiveTask($topicPartition, retries=$totalRetryCount, state=${state.getClass.getName}, cancelled=${ctx.isCancelled})"
}


object ArchiveTask extends Logging {
  def apply(ctx: CancellationContext, topicPartition: TopicPartition, leaderEpoch: Int): ArchiveTask = {
    new ArchiveTask(ctx, topicPartition, BeforeLeader(leaderEpoch))
  }

  private def createObjectMetadata(topicPartition: TopicPartition, tierEpoch: Int, logSegment: LogSegment, hasEpochState: Boolean): TierObjectMetadata = {
    val lastStableOffset = logSegment.readNextOffset - 1 // TODO: get from producer status snapshot
    val offsetDelta = lastStableOffset - logSegment.baseOffset
    new TierObjectMetadata(
      topicPartition,
      tierEpoch,
      logSegment.baseOffset,
      offsetDelta.intValue(),
      lastStableOffset,
      logSegment.largestTimestamp,
      logSegment.size,
      hasEpochState,
      // TODO: set when we have producer state
      false,
      // TODO: compute whether any tx aborts occurred.
      false,
      State.AVAILABLE)
  }

  private def assertSegmentFileAccess(logSegment: LogSegment, leaderEpochCacheFile: Option[File]): Unit = {
    var fileListToCheck: List[File] = List(logSegment.log.file(),
      logSegment.offsetIndex.file,
      logSegment.timeIndex.file,
      logSegment.timeIndex.file, // FIXME producer status
      logSegment.timeIndex.file) // FIXME transaction index
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
                                            topicPartition: TopicPartition,
                                            tierTopicManager: TierTopicManager)
                                           (implicit ec: ExecutionContext): Future[BeforeUpload] = {
    Future.fromTry(
      Try(tierTopicManager.becomeArchiver(topicPartition, state.leaderEpoch).toScala))
      .flatMap(identity)
      .map { result: AppendResult =>
        result match {
          case AppendResult.ACCEPTED =>
            info(s"established leadership for $topicPartition")
            BeforeUpload(state.leaderEpoch)
          case AppendResult.ILLEGAL =>
            throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicPartition in illegal status.")
          case AppendResult.FENCED =>
            throw new TierArchiverFencedException(topicPartition)
          case appendResult =>
            throw new TierArchiverFatalException(s"Unknown AppendResult $appendResult")
        }
      }
  }

  private[archiver] def finalizeUpload(state: AfterUpload,
                                       topicPartition: TopicPartition,
                                       time: Time,
                                       tierTopicManager: TierTopicManager)
                                      (implicit ec: ExecutionContext): Future[BeforeUpload] = {
    Future.fromTry(Try(tierTopicManager.addMetadata(state.metadata).toScala))
      .flatMap(identity)
      .map {
        case AppendResult.ACCEPTED =>
          info(s"Tiered log segment for $topicPartition in ${time.milliseconds - state.beginUploadTime} ms")
          BeforeUpload(state.leaderEpoch)
        case AppendResult.ILLEGAL =>
          throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicPartition in illegal status")
        case AppendResult.FENCED =>
          throw new TierArchiverFencedException(topicPartition)
      }
  }

  private[archiver] def tierSegment(state: BeforeUpload,
                                    topicPartition: TopicPartition,
                                    time: Time,
                                    tierTopicManager: TierTopicManager,
                                    tierObjectStore: TierObjectStore,
                                    replicaManager: ReplicaManager,
                                    byteRateMetric: Option[Meter] = None)
                                   (implicit ec: ExecutionContext): Future[ArchiveTaskState] = {
    Future {
      if (tierTopicManager.partitionState(topicPartition).tierEpoch() != state.leaderEpoch) {
        throw new TierArchiverFencedException(topicPartition)
      } else {
        replicaManager
          .getLog(topicPartition)
          .flatMap { log =>
            log.tierableLogSegments
              .collectFirst { case logSegment: LogSegment => (log, logSegment) }
          } match {
          case None =>
            // Log has been moved or there is no eligible segment. Retry BeforeUpload state.
            debug(s"Transitioning back to BeforeUpload for $topicPartition as no log was found")
            Future(state)

          case Some((log: AbstractLog, logSegment: LogSegment)) =>
            // Upload next segment and transition.
            val leaderEpochStateFile = ArchiveTask.uploadableLeaderEpochState(log, logSegment.readNextOffset)
            val startTime = time.milliseconds
            blocking {
              putSegment(state,
                topicPartition,
                tierObjectStore,
                logSegment,
                leaderEpochStateFile
              ).map(meta => AfterUpload(state.leaderEpoch, meta, startTime))
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
                         topicPartition: TopicPartition,
                         tierObjectStore: TierObjectStore,
                         logSegment: LogSegment,
                         leaderEpochCacheFile: Option[File])
                        (implicit ec: ExecutionContext): Future[TierObjectMetadata] = {
    Future {
      val metadata = createObjectMetadata(topicPartition, state.leaderEpoch, logSegment, leaderEpochCacheFile.isDefined)
      blocking {
        assertSegmentFileAccess(logSegment, leaderEpochCacheFile)
        tierObjectStore.putSegment(
          metadata,
          logSegment.log.file.toPath.toFile,
          logSegment.offsetIndex.file.toPath.toFile,
          logSegment.timeIndex.file.toPath.toFile,
          logSegment.timeIndex.file.toPath.toFile, // FIXME producer state
          logSegment.timeIndex.file.toPath.toFile, // FIXME transaction index
          leaderEpochCacheFile.asJava)
      }
      metadata
    }
  }
}
