/*
 Copyright 2018 Confluent Inc.
 */

package kafka.server

import java.util
import java.util.Optional

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import kafka.tier.fetcher.PendingOffsetForTimestamp
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.FileTimestampAndOffset

import scala.collection._

/**
 * A delayed list operation that can be created by the replica manager and watched
 * in the list offset operation purgatory
 */
class DelayedListOffsets(delayMs: Long,
                         fetchOnlyFromLeader: Boolean,
                         localLists: util.HashMap[TopicPartition, Option[FileTimestampAndOffset]],
                         pending: PendingOffsetForTimestamp,
                         replicaManager: ReplicaManager,
                         responseCallback: Map[TopicPartition, Option[FileTimestampAndOffset]] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if the TierFetcher has fetched
   * all offset for timestamps from the object store, and completed the pending request.
   *
   * Upon completion, call the responseCallback with all tiered and non-tiered timestamp to offset mappings
   */
  override def tryComplete(): Boolean = {
    val partitionTimestampAndOffsets = localLists.asScala ++
      pending.tierTimestampAndOffsets()
        .asScala
        .map { case (tp, timestampAndOffset) => (tp, Some(timestampAndOffset)) }

    partitionTimestampAndOffsets.foreach { case (tp, timestampAndOffset) =>
      try {
        val partition = replicaManager.getPartitionOrException(tp, expectLeader = fetchOnlyFromLeader)
        val leaderEpoch = timestampAndOffset.get.leaderEpoch()
        partition.localLogWithEpochOrException(leaderEpoch, requireLeader = fetchOnlyFromLeader)
      } catch {
        case e: Exception =>
          pending.completeExceptionally(tp, e)
      }
    }

    if (pending.isDone)
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    pending.cancel()
  }

  /**
   * Upon completion, call response callback with the tier fetched TimestampAndOffsets
   * combined with the disk TimestampAndOffsets.
   */
  override def onComplete() {
    val tierResponses = pending.tierTimestampAndOffsets()
      .asScala
      .map { case (topicPartition, tierTimestampAndOffset) =>
        val timestampAndOffset = pending
          .results
          .getOrDefault(topicPartition,
            Optional.of(new FileTimestampAndOffset(tierTimestampAndOffset.timestamp,
              tierTimestampAndOffset.leaderEpoch(),
              Errors.REQUEST_TIMED_OUT.exception)))
          .asScala

        (topicPartition, timestampAndOffset)
      }
    responseCallback(localLists.asScala ++ tierResponses)
  }
}

