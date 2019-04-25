/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.io.File
import java.util.concurrent.{Callable, CompletableFuture, ScheduledExecutorService, TimeUnit}
import java.io.IOException
import java.util.Comparator
import java.util.function.Supplier

import kafka.log.AbstractLog
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.log.LogSegment
import kafka.server.ReplicaManager
import kafka.tier.TierTopicManager
import kafka.tier.archiver.CompletableFutureExtensions._
import kafka.tier.archiver.JavaFunctionConversions._
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.exceptions.{TierArchiverFatalException, TierArchiverFencedException, TierMetadataRetriableException, TierObjectStoreRetriableException}
import kafka.tier.serdes.State
import kafka.tier.state.TierPartitionState
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

import scala.compat.java8.OptionConverters._
import scala.util.Random

sealed trait TierArchiverState extends Logging {
  val topicPartition: TopicPartition

  def lag: Long

  def relativePriority(other: TierArchiverState): Int

  def nextState(): CompletableFuture[TierArchiverState]

  override def toString: String = {
    s"${getClass.getSimpleName}(topicPartition=$topicPartition)"
  }
}

/*
TierArchiverState follows a status machine progression.
Each call to `nextState` can either successfully transition
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

object TierArchiverState {
  def calculateLag(topicPartition: TopicPartition,
                   replicaManager: ReplicaManager,
                   tierPartitionState: TierPartitionState): Long = {
    replicaManager.getLog(topicPartition) match {
      case None => 0L
      case Some(log) => log.tierableLogSegments.foldLeft(0L)(_ + _.size)
    }
  }

  object TierArchiverStateComparator extends Comparator[TierArchiverState] with Serializable {
    override def compare(a: TierArchiverState, b: TierArchiverState): Int = {
      a.relativePriority(b)
    }
  }

  object Priority extends Enumeration {
    val Higher: Int = -1
    val Lower: Int = 1
    val Same: Int = 0
  }

  /**
   * Allows running retryable state transitions at some point in the future, based on a configurable
   * backoff.
   */
  sealed abstract class RetriableTierArchiverState(config: TierArchiverConfig) extends TierArchiverState {
    @volatile private var _retryCount: Int = 0

    def retryState(blockingTaskExecutor: ScheduledExecutorService): CompletableFuture[TierArchiverState] = {
      val future: CompletableFuture[TierArchiverState] = new CompletableFuture[TierArchiverState]()
      val self = this
      blockingTaskExecutor.schedule(new Callable[Unit] {
        override def call(): Unit = {
          self._retryCount += 1
          future.complete(self)
        }
      }, backoffMs, TimeUnit.MILLISECONDS)
      future
    }

    def retryCount: Int = _retryCount

    // Calculate a random duration of seconds between zero and the current retry count
    private def backoffMs: Long = {
      if (_retryCount == 0)
        0L
      else
        Math.min(
          config.maxRetryBackoffMs,
          Random.nextInt(_retryCount) * 1000
        )
    }
  }

  // BeforeLeader represents a TopicPartition waiting for a successful fence TierTopic message
  // to go through. Once this has been realized by the TierTopicManager, it is allowed to progress
  // to BeforeUpload.
  final case class BeforeLeader(replicaManager: ReplicaManager,
                                tierTopicManager: TierTopicManager,
                                tierObjectStore: TierObjectStore,
                                metrics: ArchiverStateMetrics,
                                topicPartition: TopicPartition,
                                tierEpoch: Int,
                                blockingTaskExecutor: ScheduledExecutorService,
                                config: TierArchiverConfig) extends RetriableTierArchiverState(config) {

    override def lag: Long = calculateLag(topicPartition, replicaManager, tierTopicManager.partitionState(topicPartition))

    // Priority: BeforeLeader (this) > AfterUpload > BeforeUpload
    override def relativePriority(other: TierArchiverState): Int = {
      other match {
        case _: BeforeLeader => Priority.Same
        case _: BeforeUpload => Priority.Higher
        case _: AfterUpload => Priority.Higher
      }
    }

    override def nextState(): CompletableFuture[TierArchiverState] = {
      tierTopicManager
        .becomeArchiver(topicPartition, tierEpoch)
        .thenApply[TierArchiverState] { result: AppendResult =>
        result match {
          case AppendResult.ACCEPTED =>
            val tierPartitionState = tierTopicManager.partitionState(topicPartition)
            BeforeUpload(
              replicaManager, tierTopicManager, tierObjectStore, metrics,
              topicPartition, tierPartitionState, tierEpoch, blockingTaskExecutor, config
            )
          case AppendResult.ILLEGAL =>
            throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicPartition in illegal status.")
          case AppendResult.FENCED =>
            throw new TierArchiverFencedException(topicPartition)
        }
      }
        .thenComposeExceptionally((ex: Throwable) => {
          if (ex.isInstanceOf[TierMetadataRetriableException])
            retryState(blockingTaskExecutor)
          else
            throw ex
        })
    }
  }

  // BeforeUpload represents a TopicPartition checking for eligible segments to upload. If there
  // are no eligible we remain in the current status, if there are eligible segments we transition
  // to AfterUpload on completion of segment (and associated metadata) upload.
  final case class BeforeUpload(replicaManager: ReplicaManager,
                                tierTopicManager: TierTopicManager,
                                tierObjectStore: TierObjectStore,
                                metrics: ArchiverStateMetrics,
                                topicPartition: TopicPartition,
                                tierPartitionState: TierPartitionState,
                                tierEpoch: Int,
                                blockingTaskExecutor: ScheduledExecutorService,
                                config: TierArchiverConfig) extends RetriableTierArchiverState(config) {

    override def lag: Long = calculateLag(topicPartition, replicaManager, tierPartitionState)

    // Priority: BeforeLeader > AfterUpload > BeforeUpload (this)
    // When comparing two BeforeUpload states, prioritize the state with lower lag higher.
    // Partitions with 0 lag should be deprioritized as they are already fully tiered.
    override def relativePriority(other: TierArchiverState): Int = {
      other match {
        case _: BeforeLeader => Priority.Lower
        case otherBeforeUpload: BeforeUpload =>
          val otherLag = otherBeforeUpload.lag
          val thisLag = this.lag
          if (thisLag == otherLag)
            Priority.Same
          else if (thisLag == 0 || otherLag < thisLag)
            Priority.Lower
          else
            Priority.Higher

        case _: AfterUpload => Priority.Lower
      }
    }

    override def nextState(): CompletableFuture[TierArchiverState] = {
      if (tierPartitionState.tierEpoch() != tierEpoch) {
        CompletableFutureUtil.failed(new TierArchiverFencedException(topicPartition))
      } else {
        val logLogSegment: Option[(AbstractLog, LogSegment)] = replicaManager.getLog(topicPartition).flatMap(log => {
          log.tierableLogSegments.collectFirst { case logSegment: LogSegment => (log, logSegment) }
        })

        logLogSegment match {
          case None =>
            // Log has been moved or there is no eligible segment. Retry BeforeUpload state.
            CompletableFutureUtil.completed(this)

          case Some((log: AbstractLog, logSegment: LogSegment)) =>
            // Upload next segment and transition.
            val leaderEpochStateFile = uploadableLeaderEpochState(log, logSegment.readNextOffset)
            putSegment(logSegment, leaderEpochStateFile, blockingTaskExecutor).thenApply[TierArchiverState] { objectMetadata: TierObjectMetadata =>
              metrics.byteRate.mark(objectMetadata.size())
              AfterUpload(objectMetadata, logSegment, replicaManager, tierTopicManager, tierObjectStore, metrics,
                topicPartition, tierPartitionState, tierEpoch, blockingTaskExecutor, config)
            }.thenComposeExceptionally {
              case ex if ex.getCause.isInstanceOf[IOException] || ex.getCause.isInstanceOf[TierObjectStoreRetriableException] =>
                info(s"Tier archiver failed to upload segment $topicPartition "
                  + s"baseOffset ${logSegment.baseOffset}. Retrying.", ex)
                retryState(blockingTaskExecutor)
              case ex =>
                throw new TierArchiverFatalException(topicPartition, ex)
            }
        }
      }
    }

    // Get an uploadable leader epoch state file by cloning state from leader epoch cache and truncating it to the endOffset
    private def uploadableLeaderEpochState(log: AbstractLog, endOffset: Long): Option[File] = {
      val leaderEpochCache = log.leaderEpochCache
      leaderEpochCache.map(cache => {
        val checkpointClone = new LeaderEpochCheckpointFile(new File(cache.file.getAbsolutePath + ".tier"))
        val leaderEpochCacheClone = cache.clone(checkpointClone)
        leaderEpochCacheClone.truncateFromEnd(endOffset)
        leaderEpochCacheClone.file
      })
    }


    private def putSegment(logSegment: LogSegment, leaderEpochCacheFile: Option[File], blockingTaskExecutor: ScheduledExecutorService): CompletableFuture[TierObjectMetadata] = {

      CompletableFuture.supplyAsync(new Supplier[TierObjectMetadata] {
        override def get(): TierObjectMetadata = {
          val metadata = createObjectMetadata(topicPartition, tierEpoch, logSegment, leaderEpochCacheFile.isDefined)
          assertSegmentFileAccess(logSegment, leaderEpochCacheFile)
          tierObjectStore.putSegment(metadata,
            logSegment.log.file.toPath.toFile,
            logSegment.offsetIndex.file.toPath.toFile,
            logSegment.timeIndex.file.toPath.toFile,
            logSegment.timeIndex.file.toPath.toFile, // FIXME producer state
            logSegment.timeIndex.file.toPath.toFile, // FIXME transaction index
            leaderEpochCacheFile.asJava)
        }
      }, blockingTaskExecutor)
    }

    private def assertSegmentFileAccess(logSegment: LogSegment, leaderEpochCacheFile: Option[File]): Unit = {
      var fileListToCheck: List[File] = List(logSegment.log.file(),
        logSegment.offsetIndex.file,
        logSegment.timeIndex.file,
        logSegment.timeIndex.file, // FIXME producer status
        logSegment.timeIndex.file) // FIXME transaction index
      if (leaderEpochCacheFile.isDefined) {
        fileListToCheck :+= leaderEpochCacheFile.get
      }

      val missing: List[File] =
        fileListToCheck
          .filterNot { f =>
            try {
              f.exists()
            } catch {
              case _: SecurityException => false
            }
          }

      if (missing.nonEmpty) {
        throw new IOException(s"Tier archiver could not read segment files: ${missing.mkString(", ")}")
      }
    }
  }

  // AfterUpload represents the TopicPartition writing out the TierObjectMetadata to the TierTopicManager,
  // after the TierTopicManager confirms that the TierObjectMetadata has been materialized, AfterUpload
  // transitions to BeforeUpload.
  final case class AfterUpload(objectMetadata: TierObjectMetadata,
                               logSegment: LogSegment,
                               replicaManager: ReplicaManager,
                               tierTopicManager: TierTopicManager,
                               tierObjectStore: TierObjectStore,
                               metrics: ArchiverStateMetrics,
                               topicPartition: TopicPartition,
                               tierPartitionState: TierPartitionState,
                               tierEpoch: Int,
                               blockingTaskExecutor: ScheduledExecutorService,
                               config: TierArchiverConfig) extends TierArchiverState {

    override def lag: Long = calculateLag(topicPartition, replicaManager, tierPartitionState)

    // Priority: BeforeLeader > AfterUpload (this) > BeforeUpload
    override def relativePriority(other: TierArchiverState): Int = {
      other match {
        case _: BeforeLeader => Priority.Lower
        case _: BeforeUpload => Priority.Higher
        case _: AfterUpload => Priority.Same
      }
    }

    override def nextState(): CompletableFuture[TierArchiverState] = {
      tierTopicManager
        .addMetadata(objectMetadata)
        .thenApply { t: AppendResult =>
          t match {
            case AppendResult.ACCEPTED =>
              BeforeUpload(replicaManager, tierTopicManager, tierObjectStore, metrics, topicPartition, tierPartitionState, tierEpoch, blockingTaskExecutor, config)
            case AppendResult.ILLEGAL =>
              throw new TierArchiverFatalException(s"Tier archiver found tier partition $topicPartition in illegal status.")
            case AppendResult.FENCED =>
              throw new TierArchiverFencedException(topicPartition)
          }
        }
    }
  }

  private[archiver] def createObjectMetadata(topicPartition: TopicPartition, tierEpoch: Int, logSegment: LogSegment, hasEpochState: Boolean): TierObjectMetadata = {
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
      // TODO: compute whether any tx aborts occurred.
      false,
      State.AVAILABLE)
  }
}

