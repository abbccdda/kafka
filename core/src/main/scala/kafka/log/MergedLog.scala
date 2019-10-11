/*
 Copyright 2018 Confluent Inc.
 */

package kafka.log

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.util

import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.api.ApiVersion
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.server.epoch.LeaderEpochFileCache
import kafka.tier.TierMetadataManager
import kafka.tier.state.{FileTierPartitionStateFactory, TierPartitionState, TierUtils}
import kafka.tier.TierTimestampAndOffset
import kafka.tier.TopicIdPartition
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{KafkaStorageException, OffsetOutOfRangeException}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

/**
  * A merged log which presents a combined view of local and tiered log segments.
  *
  * The log consists of tiered and local segments with the tiered portion of the log being optional. There could be an
  * overlap between the tiered and local segments, which not align one-to-one. The active segment is always guaranteed
  * to be local. If tiered segments are present, they always appear at the head of the log, followed by an optional
  * region of overlap, followed by the local segments including the active segment.
  *
  * A log could be in one of the following states where T is a tiered segment and L is a local segment:
  *
  * (1) No tiered segments
  *
  * | L1 | L2 | L3 | L4 | L5 | L6 | <-- activeSegment
  *   <----- localSegments ----->
  *
  * uniqueLogSegments = {L1, L2, L3, L4, L5, L6}
  *
  * (2) Tiered segments. In the region of overlap, there may not necessarily be a one-to-one correspondence between
  * tiered and local segments.
  *
  *   <-- tieredSegments -->
  * | T1 | T2 | T3 | T4 | T5 |
  *         | L1 | L2 | L3 | L4 | L5 | L6 | <-- activeSegment
  *            <----- localSegments ----->
  *
  * uniqueLogSegments = {T1, T2, L1, L2, L3, L4, L5, L6}
  *
  * @param localLog       The local log
  * @param logStartOffset Start offset of the merged log. This is the earliest offset allowed to be exposed to a Kafka
  *                       client. MergedLog#logStartOffset is the true log start offset of the combined log;
  *                       Log#logStartOffset only maintains a local view of the start offset and could thus diverge from
  *                       the true start offset. Other semantics of the log start offset remain identical.
  *                       The logStartOffset can be updated by:
  *                       - user's DeleteRecordsRequest
  *                       - broker's log retention
  *                       - broker's log truncation
  *                       The logStartOffset is used to decide the following:
  *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
  *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
  *                         we make sure that logStartOffset <= log's highWatermark.
  *                         Other activities such as log cleaning are not affected by logStartOffset.
  * @param tierPartitionState The tier partition instance for this log
  * @param tierMetadataManager Handle to tier metadata manager
  */
class MergedLog(private[log] val localLog: Log,
                @volatile var logStartOffset: Long,
                private val tierPartitionState: TierPartitionState,
                private val tierMetadataManager: TierMetadataManager) extends Logging with KafkaMetricsGroup with AbstractLog {
  /* Protects modification to log start offset */
  private val lock = new Object

  locally {
    logStartOffset = math.max(logStartOffset, firstTieredOffset.getOrElse(localLog.localLogStartOffset))
    localLog.setMergedLogStartOffsetCbk(() => logStartOffset)

    // Log layer uses the checkpointed log start offset to truncate the producer state and leader epoch cache, but the
    // true log start offset could be greater than that after an unclean shutdown. Ensure both these states are truncated
    // up until the true log start offset.
    localLog.loadProducerState(logEndOffset, reloadFromCleanShutdown = localLog.hasCleanShutdownFile)
    leaderEpochCache.foreach(_.truncateFromStart(logStartOffset))

    info(s"Completed load of log with $numberOfSegments segments containing ${localLogSegments.size} local segments and " +
      s"${tieredOffsets.size} tiered segments, tier start offset $logStartOffset, first untiered offset $firstUntieredOffset, " +
      s"local start offset ${localLog.localLogStartOffset}, log end offset $logEndOffset")
  }

  this.logIdent = s"[MergedLog partition=$topicPartition, dir=${dir.getParent}] "

  private val tags = {
    val maybeFutureTag = if (isFuture) Map("is-future" -> "true") else Map.empty[String, String]
    Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++ maybeFutureTag
  }

  // Number of segments in local log
  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = localLog.numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  // Size of local log. For compatibility with tools like ADB and Cruise Control, we continue to report the local size
  // of the log for this metric. Tools that require the total size of the log, including the tiered portion, must use
  // `TotalSize` instead.
  newGauge("Size",
    new Gauge[Long] {
      def value = localLog.size
    },
    tags)

  // Size of tiered portion of the log.
  newGauge("TierSize",
    new Gauge[Long] {
      def value = tierPartitionState.totalSize
    },
    tags)

  // Total size of the log. See AbstractLog#size for details.
  newGauge("TotalSize",
    new Gauge[Long] {
      def value = size
    },
    tags)

  // For compatibility, metrics are defined to be under `Log` class
  override def metricName(name: String, tags: scala.collection.Map[String, String]): MetricName = {
    val klass = localLog.getClass
    val pkg = if (klass.getPackage == null) "" else klass.getPackage.getName
    val simpleName = klass.getSimpleName.replaceAll("\\$$", "")

    explicitMetricName(pkg, simpleName, name, tags)
  }

  override def updateConfig(updatedKeys: collection.Set[String], newConfig: LogConfig): Unit = {
    localLog.updateConfig(updatedKeys, newConfig)
    tierMetadataManager.onConfigChange(topicPartition, newConfig)
  }

  override private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
    removeMetric("TierSize", tags)
    removeMetric("TotalSize", tags)
    localLog.removeLogMetrics()
  }

  override def numberOfSegments: Int = {
    uniqueLogSegments match {
      case (tierLogSegments, localLogSegments) => tierLogSegments.size + localLogSegments.size
    }
  }

  override def renameDir(name: String): Unit = {
    localLog.renameDir(name)
    tierPartitionState.updateDir(new File(dir.getParent, name))
  }

  override def closeHandlers(): Unit = {
    localLog.closeHandlers()
    tierPartitionState.closeHandlers()
  }

  override def maybeIncrementLogStartOffset(newLogStartOffset: Long): Unit = lock synchronized {
    if (newLogStartOffset > logStartOffset) {
      info(s"Incrementing merged log start offset to $newLogStartOffset")
      localLog.maybeIncrementLogStartOffset(newLogStartOffset)
      logStartOffset = newLogStartOffset
    }
  }

  override def read(startOffset: Long,
                    maxLength: Int,
                    isolation: FetchIsolation,
                    minOneMessage: Boolean): AbstractFetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      val logEndOffset = this.logEndOffset
      try {
        readLocal(startOffset, maxLength, isolation, minOneMessage)
      } catch {
        case _: OffsetOutOfRangeException => readTier(startOffset, maxLength, minOneMessage, logEndOffset)
      }
    }
  }

  override def deleteOldSegments(): Int = {
    // Delete all eligible local segments if tiering is disabled. If tiering is enabled, allow deletion for eligible
    // tiered segments only. Local segments that have not been tiered yet must not be deleted.
    if (!config.tierEnable) {
      val deleted = localLog.deleteOldSegments(None)
      maybeIncrementLogStartOffset(localLogStartOffset)
      deleted
    } else {
      val retentionDeleted = localLog.deleteOldSegments(None)  // apply retention: all segments are eligible for deletion

      // Prevent hotset retention until the segment with the highest base offset in the set of deletable segments has a
      // producer state snapshot. This ensures that no deletion will occur until it is guaranteed that all followers will
      // be able to restore a consistent snapshot on OFFSET_TIERED_EXCEPTION.
      def deletionCanProceed(deletableLogSegments: Seq[LogSegment]): Boolean = {
        deletableLogSegments.lastOption.flatMap(lastSegment => {
          // Check if there is a snapshot file for this segment, which would have been created using the
          // next segments base offset on roll
          producerStateManager.snapshotFileForOffset(lastSegment.readNextOffset)
        }).isDefined
      }

      // Apply hotset retention: do not delete any untiered segments
      val deletionUpperBoundOffset = tierPartitionState.committedEndOffset.asScala.map(upperBound => upperBound + 1).getOrElse(0L)
      val hotsetDeleted = localLog.deleteOldSegments(Some(deletionUpperBoundOffset), retentionType = HotsetRetention, deletionCanProceed)

      if (retentionDeleted > 0)
        maybeIncrementLogStartOffset(localLogStartOffset)
      else
        maybeIncrementLogStartOffset(firstTieredOffset.getOrElse(localLogStartOffset))

      if (hotsetDeleted > 0) {
        // maybeIncrementLogStartOffset will set the new local log start offset, delete any
        // producer state snapshot files prior to this.
        localLog.producerStateManager.deleteSnapshotsBefore(localLogStartOffset)
      }

      retentionDeleted + hotsetDeleted
    }
  }

  override def size: Long = {
    // We may overaccount for one of the segments because we do not grab firstUntieredOffset and tierSize atomically.
    // If a segment were uploaded after we read firstUntieredOffset but before we read the total size, we would end up
    // double counting the segment that was uploaded.
    val firstUntieredOffset = this.firstUntieredOffset
    val tierSize = tierPartitionState.totalSize

    val untieredSegments = localLogSegments(firstUntieredOffset, Long.MaxValue)
    val untieredSize = segmentsSize(untieredSegments)

    var size = tierSize + untieredSize

    // there could be an overlap between the last tiered segment and the first untiered local segment
    if (untieredSegments.nonEmpty && tierSize > 0) {
      val firstLocalUntieredSegment = untieredSegments.head

      if (firstLocalUntieredSegment.baseOffset < firstUntieredOffset) {
        // locate the end of overlap in local segment
        val overlapEndPosition = firstLocalUntieredSegment.translateOffset(firstUntieredOffset)
        if (overlapEndPosition != null)
          size -= overlapEndPosition.position  // this is the position corresponding to the first non overlapping offset
      }
    }

    size
  }

  override def firstOffsetMetadata(): LogOffsetMetadata = {
    convertToLocalOffsetMetadata(logStartOffset).getOrElse {
      firstTieredOffset.map { firstOffset =>
        new LogOffsetMetadata(firstOffset, firstOffset, 0)
      }.getOrElse(localLog.firstOffsetMetadata)
    }
  }

  override def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    // Aborted transactions are retrieved by the TierFetcher on READ_COMMITTED, so raise an exception if we attempt
    // to collect aborted transactions from the log layer for a tier fetch.
    unsupportedIfOffsetNotLocal(startOffset)
    localLog.collectAbortedTransactions(startOffset, upperBoundOffset)
  }

  override private[log] def truncateTo(targetOffset: Long): Boolean = lock synchronized {
    if (localLog.truncateTo(targetOffset)) {
      logStartOffset = math.max(logStartOffset, firstTieredOffset.getOrElse(localLog.localLogStartOffset))
      true
    } else {
      false
    }
  }

  override private[log] def truncateFullyAndStartAt(newOffset: Long): Unit = lock synchronized {
    localLog.truncateFullyAndStartAt(newOffset)
    if (config.tierEnable)
      logStartOffset = firstTieredOffset.getOrElse(newOffset)
    else
      logStartOffset = newOffset
  }

  def topicIdPartition: Option[TopicIdPartition] = tierPartitionState.topicIdPartition.asScala

  def onRestoreTierState(proposeLocalLogStart: Long, tierState: TierState): Unit = lock synchronized {
    info(s"restoring tier state for $topicPartition at proposed offset $proposeLocalLogStart")
    if (localLog.leaderEpochCache.isEmpty)
      throw new IllegalStateException("Message format must be upgraded before restoring tier state can be allowed.")

    truncateFullyAndStartAt(proposeLocalLogStart)

    // ProducerStateManager should be fully truncated at this point, making it safe to restore the snapshot
    tierState.producerState match {
      case Some(producerStateBuf: ByteBuffer) =>
        info(s"restoring non-empty producer state snapshot for $topicPartition")
        localLog.producerStateManager.reloadFromTieredSnapshot(logStartOffset, localLog.time.milliseconds(), producerStateBuf, proposeLocalLogStart)
      case None =>
        info(s"restoring empty producer state snapshot for $topicPartition")
        // If there is no producer state to restore, just update the lastMapOffset to the restored segment end offset + 1
        localLog.producerStateManager.updateMapEndOffset(proposeLocalLogStart)
    }
    // We do not currently archive past the first unstable offset.
    // Until we do, we can assume that the first unstable offset is equivalent bounded
    // by the local log start offset after recovery.
    localLog.maybeIncrementFirstUnstableOffset(localLog.localLogStartOffset)

    localLog.leaderEpochCache.get.clear()
    for (entry <- tierState.leaderEpochState)
      localLog.leaderEpochCache.get.assign(entry.epoch, entry.startOffset)
  }

  /**
    * Get the base offset of first segment in log.
    */
  def baseOffsetOfFirstSegment: Long = firstTieredOffset.getOrElse(localLogSegments.head.baseOffset)

  def localLogStartOffset: Long = localLog.localLogStartOffset

  def localLogEndOffset: Long = localLog.logEndOffset

  def tierableLogSegments: Iterable[LogSegment] = {
    // We can tier all segments starting at first untiered offset (or the log start offset if it is greater) until we
    // reach a segment that:
    // 1. contains the first unstable offset: we expect the first unstable offset to always be available locally
    // 2. contains the highwatermark: we only tier messages that have been ack'd by all replicas
    // 3. is the current active segment: we only tier immutable segments (that have been rolled already)
    // 4. the segment end offset is less than the recovery point. This ensures we only upload segments that have been fsync'd.
    val upperBoundOffset = Utils.min(firstUnstableOffset.getOrElse(logEndOffset), highWatermark, recoveryPoint)
    val lowerBoundOffset = Math.max(firstUntieredOffset, logStartOffset)

    // After a leader failover, it is possible that the upperBoundOffset has not moved to the point where the previous
    // leader tiered segments. No segments are tierable until the upperBoundOffset catches up with the tiered segments.
    if (lowerBoundOffset > upperBoundOffset)
      return Iterable.empty

    val candidateSegments = localLogSegments(lowerBoundOffset, upperBoundOffset).toArray
    candidateSegments.lastOption match {
      case Some(lastSegment) =>
        nextLocalLogSegment(lastSegment) match {
          case Some(nextSegment) if (upperBoundOffset >= nextSegment.baseOffset) => candidateSegments   // all segments are tierable
          case _ => candidateSegments.dropRight(1)   // last segment contains `upperBoundOffset` or this is the active segment, so exclude it
        }

      case None =>
        Array.empty[LogSegment]
    }
  }

  def baseOffsetFirstUntierableSegment: Option[Long] = {
    tierableLogSegments.lastOption.flatMap(seg => nextLocalLogSegment(seg).map(_.baseOffset))
  }

  // Attempt to locate "startOffset" in tiered store. If found, returns corresponding metadata about the tiered
  // segment.
  private def readTier(startOffset: Long,
                       maxLength: Int,
                       minOneMessage: Boolean,
                       logEndOffset: Long): TierFetchDataInfo = {
    val tieredSegment = TierUtils.tierLogSegmentForOffset(tierPartitionState, startOffset, tierMetadataManager.tierObjectStore).asScala
    val tierEndOffset = tierPartitionState.endOffset

    if (tieredSegment.isEmpty || startOffset > tierEndOffset.get || startOffset < logStartOffset)
      throw new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
        s"but we only have log segments in the range $logStartOffset to $logEndOffset with tierLogEndOffset: " +
        s"$tierEndOffset and localLogStartOffset: ${localLog.localLogStartOffset}")

    tieredSegment.get.read(startOffset, maxLength, tieredSegment.get.size, minOneMessage)
  }

  // Unique segments of the log. Note that this does not method does not return a point-in-time snapshot. Segments seen
  // by the iterable could change as segments are added or deleted.
  // Visible for testing
  private[log] def uniqueLogSegments: (util.NavigableSet[java.lang.Long], Iterable[LogSegment]) = uniqueLogSegments(0, Long.MaxValue)

  // Unique segments of the log beginning with the segment that includes "from" and ending with the segment that
  // includes up to "to-1" or the end of the log (if to > logEndOffset). Note that this does not method does not return
  // a point-in-time snapshot. Segments seen by the iterable could change as segments are added or deleted.
  // Visible for testing
  private[log] def uniqueLogSegments(from: Long, to: Long): (util.NavigableSet[java.lang.Long], Iterable[LogSegment]) = {
    val localSegments = localLogSegments(from, to)

    // Get tiered segments in range [from, localStartOffset). If localStartOffset does not exist, get tiered segments
    // in range [from, to).
    val firstLocalSegmentOpt = localSegments.headOption
    val tierLastOffsetToInclude = firstLocalSegmentOpt.map(_.baseOffset).getOrElse(to)
    val tieredSegments =
      if (from < tierLastOffsetToInclude)
        tieredOffsets(from, tierLastOffsetToInclude)
      else
        new util.TreeSet[java.lang.Long]

    (tieredSegments, localSegments)
  }

  // tieredLogSegments can be expensive if completely consumed.
  // It will lookup the tier metadata from the TierPartitionState for each TierLogSegment
  def tieredLogSegments: Iterable[TierLogSegment] = tieredLogSegments(0, Long.MaxValue)

  private[log] def tieredLogSegments(from: Long, to: Long): Iterable[TierLogSegment] = {
    TierUtils.tieredSegments(tieredOffsets(from, to), tierPartitionState, tierMetadataManager.tierObjectStore)
      .asScala.toIterable
  }

  // Base offset of all tiered segments in the log
  private def tieredOffsets: util.NavigableSet[java.lang.Long] = tierPartitionState.segmentOffsets(logStartOffset, Long.MaxValue)

  // Base offset of tiered segments beginning with the segment that includes "from" and ending with the segment that
  // includes up to "to-1" or the end of tiered segments if "to" is past the last tiered offset
  private def tieredOffsets(from: Long, to: Long): util.NavigableSet[java.lang.Long] = tierPartitionState.segmentOffsets(from, to)

  // Throw an exception if "offset" has been tiered and has been deleted or is not present in the local log
  private def unsupportedIfOffsetNotLocal(offset: Long): Unit = {
    val firstLocalOffset = localLogSegments.head.baseOffset
    if (!tieredOffsets.isEmpty && offset < firstLocalOffset)
      throw new UnsupportedOperationException(s"Unsupported operation at $offset for log with localStartOffset $firstLocalOffset")
  }

  // First untiered offset, essentially (last_tiered_offset + 1). Returns 0 if no segments have been tiered yet.
  private def firstUntieredOffset: Long = MergedLog.firstUntieredOffset(tierPartitionState)

  // First tiered offset, if there is one
  private def firstTieredOffset: Option[Long] = {
    tierPartitionState.startOffset.asScala.map(Long2long)
  }

  private def segmentsSize(segments: Iterable[LogSegment]): Long = {
    segments.map(_.size.toLong).sum
  }

  // Handle any IOExceptions by taking the log directory offline
  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    try {
      fun
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir.getParent, msg, e)
        throw new KafkaStorageException(msg, e)
    }
  }

  /* --------- Pass-through methods --------- */

  override def dir: File = localLog.dir

  override def config: LogConfig = localLog.config

  override def updateConfig(config: LogConfig): Unit = localLog.updateConfig(config)

  override def recoveryPoint: Long = localLog.recoveryPoint

  override def topicPartition: TopicPartition = localLog.topicPartition

  override def close(): Unit = localLog.close()

  override def readLocal(startOffset: Long,
                         maxLength: Int,
                         isolation: FetchIsolation,
                         minOneMessage: Boolean): FetchDataInfo = {
    localLog.read(startOffset, maxLength, isolation, minOneMessage)
  }

  override def fetchOffsetByTimestamp(targetTimestamp: Long): Option[TimestampAndOffset] = {
    // if we're performing an earliest timestamp or latest timestamp fetch
    // we don't care about tiered data at all, and can use the Log implementation
    if (targetTimestamp.equals(ListOffsetRequest.EARLIEST_TIMESTAMP) || targetTimestamp.equals(ListOffsetRequest.LATEST_TIMESTAMP))
      return localLog.fetchOffsetByTimestamp(targetTimestamp)

    // if the targetTimestamp is within tiered unique log segments,
    // return a TierTimestampAndOffset to indicate a tier fetch request is required
    val (tieredBaseOffsets, _) = uniqueLogSegments(logStartOffset, Long.MaxValue)

    TierUtils.tieredSegments(tieredBaseOffsets, tierPartitionState, tierMetadataManager.tierObjectStore())
      .asScala
      .find(_.maxTimestamp >= targetTimestamp) match {
      case Some(logSegment) =>
        Some(new TierTimestampAndOffset(targetTimestamp, logSegment.metadata))

      // if the offset for timestamp isn't in tiered section, dispatch to local log lookup
      case None => localLog.fetchOffsetByTimestamp(targetTimestamp)
    }
  }

  override def legacyFetchOffsetsBefore(timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val (tieredOffsets, localSegments) = uniqueLogSegments(logStartOffset, Long.MaxValue)
    val tieredSegments = TierUtils.tieredSegments(tieredOffsets, tierPartitionState, tierMetadataManager.tierObjectStore()).asScala
    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val segments = (tieredSegments.map(seg => (seg.baseOffset, seg.maxTimestamp, seg.size))
      ++ localSegments.map(seg => (seg.baseOffset, seg.lastModified, seg.size))).toBuffer
    localLog.legacyFetchOffsetsBefore(timestamp, maxNumOffsets, segments)
  }

  override def convertToLocalOffsetMetadata(offset: Long): Option[LogOffsetMetadata] = localLog.convertToOffsetMetadata(offset)

  override def flush(): Unit = localLog.flush()

  override private[log] def flush(offset: Long): Unit = localLog.flush(offset)

  override def name: String = localLog.name

  override def isFuture: Boolean = localLog.isFuture

  override def leaderEpochCache: Option[LeaderEpochFileCache] = localLog.leaderEpochCache

  override def firstUnstableOffset: Option[Long] = {
    // We guarantee that we never tier past the first unstable offset (see MergedLog#tierableLogSegments), i.e. the first
    // unstable offset must always be in the local portion of the log.
    localLog.firstUnstableOffset
  }

  override def lastStableOffset: Long = localLog.lastStableOffset

  override def lastStableOffsetLag: Long = localLog.lastStableOffsetLag

  override def localLogSegments: Iterable[LogSegment] = localLog.logSegments

  override def localLogSegments(from: Long, to: Long): Iterable[LogSegment] = localLog.logSegments(from, to)

  override def localNonActiveLogSegmentsFrom(from: Long): Iterable[LogSegment] =
    localLog.nonActiveLogSegmentsFrom(from)

  override def activeSegment: LogSegment = localLog.activeSegment

  override def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, isFromClient: Boolean,
                              interBrokerProtocolVersion: ApiVersion): LogAppendInfo = {
    localLog.appendAsLeader(records, leaderEpoch, isFromClient, interBrokerProtocolVersion)
  }

  // Get the segment following the given local segment
  private def nextLocalLogSegment(segment: LogSegment): Option[LogSegment] = localLog.nextLogSegment(segment)

  def latestEpoch: Option[Int] = localLog.latestEpoch

  def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = localLog.endOffsetForEpoch(leaderEpoch)

  def maybeAssignEpochStartOffset(leaderEpoch: Int, startOffset: Long): Unit = {
    localLog.maybeAssignEpochStartOffset(leaderEpoch, startOffset)
  }

  override def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    localLog.appendAsFollower(records)
  }

  override def highWatermark: Long = localLog.highWatermark

  override def updateHighWatermark(hw: Long): Long = localLog.updateHighWatermark(hw)

  override def maybeIncrementHighWatermark(newHighWatermark: LogOffsetMetadata): Option[LogOffsetMetadata] = localLog.maybeIncrementHighWatermark(newHighWatermark)

  override def fetchOffsetSnapshot: LogOffsetSnapshot = localLog.fetchOffsetSnapshot

  override private[log] def lastRecordsOfActiveProducers: Map[Long, LastRecord] = localLog.lastRecordsOfActiveProducers

  override private[log] def activeProducersWithLastSequence = localLog.activeProducersWithLastSequence

  override private[log] def splitOverflowedSegment(segment: LogSegment) = localLog.splitOverflowedSegment(segment)

  override private[log] def replaceSegments(newSegments: Seq[LogSegment],
                                            oldSegments: Seq[LogSegment],
                                            isRecoveredSwapFile: Boolean): Unit = {
    localLog.replaceSegments(newSegments, oldSegments, isRecoveredSwapFile)
  }

  override def logEndOffsetMetadata: LogOffsetMetadata = localLog.logEndOffsetMetadata

  override def logEndOffset: Long = localLog.logEndOffset

  override def lastFlushTime: Long = localLog.lastFlushTime

  override private[log] def delete(): Unit = localLog.delete()

  private def logDirFailureChannel: LogDirFailureChannel = localLog.logDirFailureChannel

  override def producerStateManager: ProducerStateManager = localLog.producerStateManager

  /* --------- End pass-through methods --------- */

  /* --------- Methods exposed for testing --------- */

  override private[log] def latestProducerSnapshotOffset = localLog.latestProducerSnapshotOffset

  override private[log] def oldestProducerSnapshotOffset = localLog.oldestProducerSnapshotOffset

  override private[log] def latestProducerStateEndOffset = localLog.latestProducerStateEndOffset

  override private[log] def producerStateManagerLastEntry(producerId: Long): Option[ProducerStateEntry] = localLog.producerStateManagerLastEntry(producerId)

  override private[log] def takeProducerSnapshot(): Unit = localLog.takeProducerSnapshot()

  override def roll(expectedNextOffset: Option[Long] = None): LogSegment = localLog.roll(expectedNextOffset)

  override private[log] def addSegment(segment: LogSegment): LogSegment = localLog.addSegment(segment)

  /* --------- End methods exposed for testing --------- */
}

object MergedLog {
  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long,
            recoveryPoint: Long,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time = Time.SYSTEM,
            maxProducerIdExpirationMs: Int,
            producerIdExpirationCheckIntervalMs: Int,
            logDirFailureChannel: LogDirFailureChannel,
            tierMetadataManagerOpt: Option[TierMetadataManager] = None): MergedLog = {
    val tierMetadataManager = tierMetadataManagerOpt.getOrElse {
      new TierMetadataManager(new FileTierPartitionStateFactory(), None.asJava, logDirFailureChannel, false)
    }
    val topicPartition = Log.parseTopicPartitionName(dir)
    val tierPartitionState = tierMetadataManager.initState(topicPartition, dir, config)
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)

    // On log startup, all truncation must happen above the last tiered offset, if there is one. The lowest truncation
    // offset puts a lower bound on where truncation can begin from, if needed.
    val localLog = new Log(dir, config, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager, logDirFailureChannel,
      initialUntieredOffset = firstUntieredOffset(tierPartitionState),
      mergedLogStartOffsetCbk = () => logStartOffset)

    new MergedLog(localLog, logStartOffset, tierPartitionState, tierMetadataManager)
  }

  private def firstUntieredOffset(tierPartitionState: TierPartitionState): Long = {
    tierPartitionState.endOffset.asScala.map(endOffset => endOffset + 1).getOrElse(0L)
  }
}

sealed trait AbstractLog {

  /**
    * @return The current active directory where log segments are created
    */
  def dir: File

  /**
    * @return The current log configurations
    */
  def config: LogConfig

  /**
    * @return Log start offset
    */
  def logStartOffset: Long

  /**
   * @return The start offset of the local (disk) log
   */
  def localLogStartOffset: Long

 /**
   * @return The end offset of the local (disk) log
   */
  def localLogEndOffset: Long

  /**
    * @return The current recovery point of the log
    */
  def recoveryPoint: Long

  /**
    * @return Topic-partition of this log
    */
  def topicPartition: TopicPartition

  /**
    * @return Directory name of this log
    */
  def name: String

  /**
    * @return True if this log is for a "future" partition; false otherwise
    */
  def isFuture: Boolean

  /**
    * @return The leader epoch cache file
    */
  def leaderEpochCache: Option[LeaderEpochFileCache]

  /**
    * The earliest offset which is part of an incomplete transaction. This is used to compute the
    * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
    * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
    * will point to the log start offset, which may actually be either part of a completed transaction or not
    * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
    * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
    * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
    * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
    * that this could result in disagreement between replicas depending on when they began replicating the log.
    * In the worst case, the LSO could be seen by a consumer to go backwards.
    *
    * Note that the first unstable offset could be in the tiered portion of the log.
    *
    * @return the first unstable offset
    */
  def firstUnstableOffset: Option[Long]

  def lastStableOffset: Long

  def lastStableOffsetLag: Long

  /**
    * @return The total number of unique segments in this log. "Unique" is defined as the number of non-overlapping
    *         segments across local and tiered storage.
    */
  def numberOfSegments: Int

  /**
    * @return All segments in local store
    */
  def localLogSegments: Iterable[LogSegment]

  /**
    * @param from The start offset (inclusive)
    * @param to The end offset (exclusive)
    * @return A view of local segments beginning with the one containing "from" and up until "to-1" or the end of the log
    *         if "to" is past the end of the log.
    */
  def localLogSegments(from: Long, to: Long): Iterable[LogSegment]

  /**
   * @param from The start offset (inclusive)
   * @return A view of local segments beginning with the one containing "from" and up to the end,
   *         excluding the active segment.
   */
  def localNonActiveLogSegmentsFrom(from: Long): Iterable[LogSegment]

  def tieredLogSegments: Iterable[TierLogSegment]

  /**
    * Get the next set of tierable segments, if any
    * @return Iterator over tierable local segments
    */
  def tierableLogSegments: Iterable[LogSegment]

  /**
   * Get the base offset of the first untiered segment, if one exists
   * @return the base offset.
   */
  def baseOffsetFirstUntierableSegment: Option[Long]

  /**
    * @return The current active segment. The active segment is always local.
    */
  def activeSegment: LogSegment

  /**
    * Close this log. Some log resources like the memory mapped buffers for index files are left open until the log is
    * physically deleted.
    */
  def close(): Unit

  /**
    * Rename the directory of the log
    * @throws KafkaStorageException if rename fails
    */
  def renameDir(name: String): Unit

  /**
    * Close file handlers used by log but don't write to disk. This is called if the log directory is offline
    */
  def closeHandlers(): Unit

  /**
    * Append this message set to the active segment of the log, assigning offsets and Partition Leader Epochs
    *
    * @param records The records to append
    * @param isFromClient Whether or not this append is from a producer
    * @param interBrokerProtocolVersion Inter-broker message protocol version
    * @throws KafkaStorageException If the append fails due to an I/O error.
    * @return Information about the appended messages including the first and last offset.
    */
  def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, isFromClient: Boolean = true,
                     interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion): LogAppendInfo

  /**
    * Append this message set to the active segment of the log without assigning offsets or Partition Leader Epochs
    *
    * @param records The records to append
    * @throws KafkaStorageException If the append fails due to an I/O error.
    * @return Information about the appended messages including the first and last offset.
    */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo

  def latestEpoch: Option[Int]

  def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch]

  def maybeAssignEpochStartOffset(leaderEpoch: Int, startOffset: Long): Unit

  /**
    * The highwatermark puts an upper-bound on segment deletion and tiering. Messages in the log above the highwatermark
    * may not be considered committed from by the replication protocol.
    * @return The current highwatermark of this log
    */
  def highWatermark: Long

  def updateHighWatermark(hw: Long): Long

  def maybeIncrementHighWatermark(newHighWatermark: LogOffsetMetadata): Option[LogOffsetMetadata]

  def fetchOffsetSnapshot: LogOffsetSnapshot

  /**
    * Lookup metadata for the log start offset. This is an expensive call and must be used with caution. The call blocks
    * until the relevant metadata can be read, which might involve reading from tiered storage.
    * @return Metadata for the log start offset
    */
  def firstOffsetMetadata(): LogOffsetMetadata

  /**
    * Increment the log start offset if the provided offset is larger
    * @param newLogStartOffset Proposed log start offset
    */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long): Unit

  /**
    * Locate messages from the log. This method returns a locator to the relevant log data.
    *
    * @param startOffset The offset to begin reading at
    * @param maxLength The maximum number of bytes to read
    * @param isolation The fetch isolation, which controls the maximum offset we are allowed to read
    * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
    * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
    * @return The fetch data information including fetch starting offset metadata and messages read.
    */
  def read(startOffset: Long, maxLength: Int, isolation: FetchIsolation, minOneMessage: Boolean): AbstractFetchDataInfo

  /**
    * Variant of AbstractLog#read that limits read to local store only.
    * Note: this is visible for testing only
    */
  def readLocal(startOffset: Long, maxLength: Int, isolation: FetchIsolation, minOneMessage: Boolean): FetchDataInfo

  /**
    * Collect all aborted transactions between "startOffset" and "upperBoundOffset".
    *
    * @param startOffset Inclusive first offset of the fetch range
    * @param upperBoundOffset Exclusive last offset in the fetch range
    * @return List of all aborted transactions within the range
    */
  def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn]

  /**
    * This function does not acquire Log.lock. The caller has to make sure log segments don't get deleted during
    * this call, and also protects against calling this function on the same segment in parallel.
    *
    * Currently, it is used by LogCleaner threads on log compact non-active segments only with LogCleanerManager's lock
    * to ensure no other logcleaner threads and retention thread can work on the same segment.
    */
  private[log] def getFirstBatchTimestampForSegments(segments: Iterable[LogSegment]): Iterable[Long] = {
    segments.map {
      segment =>
        segment.getFirstBatchTimestamp()
    }
  }

  private[log] def lastRecordsOfActiveProducers: Map[Long, LastRecord]

  /**
    * Collect all active producers along with their last sequence numbers.
    * @return A map of producer id -> last sequence number
    */
  private[log] def activeProducersWithLastSequence: Map[Long, Int]

  /**
    * See Log#splitOverflowedSegment for documentation.
    */
  private[log] def splitOverflowedSegment(segment: LogSegment): List[LogSegment]

  /**
    * See Log#replicaSegments for documentation.
    */
  private[log] def replaceSegments(newSegments: Seq[LogSegment], oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false): Unit

  /**
    * See Log#fetchOffsetByTimestamp for documentation. This performs a best-effort local lookup for the timestamp.
    */
  def fetchOffsetByTimestamp(targetTimestamp: Long): Option[TimestampAndOffset]

  /**
    * See Log#legacyFetchOffsetsBefore for documentation. This performs a best-effort local lookup for the timestamp.
    * Note that for tiered segments the segment's maxTimestamp is used in lieu of segment lastModifiedTime
    * as lastModifiedTime is not maintained.
    */
  def legacyFetchOffsetsBefore(timestamp: Long, maxNumOffsets: Int): Seq[Long]

  /**
    * Locate and return metadata about the given offset including its position in the log. This method supports locating
    * offsets in the local log only.
    */
  def convertToLocalOffsetMetadata(offset: Long): Option[LogOffsetMetadata]

  /**
    * Truncate local log to the specified offset. We are never required to truncate the tiered portion of the log itself
    * but only the local portion. Log state like leader epoch cache and producer state snapshot are truncated as well.
    * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
    * @return True iff targetOffset < logEndOffset
    */
  private[log] def truncateTo(targetOffset: Long): Boolean

  /**
    * Truncate local portion of the log fully and start the local log at the specified offset. We are never required to
    * truncate the tiered portion of the log itself but only the local portion. Log state like leader epoch cache and
    * producer state snapshot are truncated as well.
    * @param newOffset The new offset to start the log with
    */
  private[log] def truncateFullyAndStartAt(newOffset: Long): Unit

  /**
    * Delete old segments in this log, including any tiered segments.
    * @return Number of segments deleted.
    */
  def deleteOldSegments(): Int

  /**
    * @return The size of this log in bytes including tiered segments, if any. If the log consists of tiered segments,
    *         any overlap between between the tiered and local portion of the log is accounted for once only to avoid
    *         double-counting.
    */
  def size: Long

  /**
    * @return The offset metadata of the next message that will be appended to the log
    */
  def logEndOffsetMetadata: LogOffsetMetadata

  /**
    * @return The offset of the next message that will be appended to the log
    */
  def logEndOffset: Long

  /**
    * Flush this log to persistent storage.
    */
  def flush(): Unit

  /**
    * Flush all segments up to offset-1 to persistent storage.
    * @param offset The offset to flush up to (exclusive). This will be the new recovery point.
    */
  private[log] def flush(offset: Long) : Unit

  /**
    * @return The time this log is last known to have been fully flushed to persistent storage
    */
  def lastFlushTime: Long

  /**
    * Update log configurations
    */
  def updateConfig(updatedKeys: scala.collection.Set[String], newConfig: LogConfig): Unit

  /**
    * Get the base offset of first segment in log.
    */
  def baseOffsetOfFirstSegment: Long

  /*
   * Restores tier state for this partition fetched from the tier object store.
   * Initializes the local log to proposedLogStart.
   */
  def onRestoreTierState(proposedLocalLogStart: Long, leaderEpochEntries: TierState): Unit

  /**
    * Remove all log metrics
    */
  private[log] def removeLogMetrics(): Unit

  /**
    * Completely delete this log directory and all contents from the file system with no delay
    */
  private[log] def delete(): Unit

  // visible for testing
  private[log] def latestProducerSnapshotOffset: Option[Long]

  // visible for testing
  private[log] def oldestProducerSnapshotOffset: Option[Long]

  // visible for testing
  private[log] def latestProducerStateEndOffset: Long

  // visible for testing
  private[log] def producerStateManagerLastEntry(producerId: Long): Option[ProducerStateEntry]

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit

  // visible for testing
  private[log] def addSegment(segment: LogSegment): LogSegment

  // visible for testing
  private[log] def updateConfig(config: LogConfig): Unit

  // visible for testing
  def roll(expectedNextOffset: Option[Long] = None): LogSegment

  def producerStateManager: ProducerStateManager
}
