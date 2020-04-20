/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import kafka.tier.fetcher.PendingFetch
import kafka.tier.fetcher.TierFetchResult
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.FetchRequest.PartitionData

import scala.collection._
import scala.collection.JavaConverters._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString: String = {
    "[startOffsetMetadata: " + startOffsetMetadata +
      ", fetchInfo: " + fetchInfo +
      "]"
  }
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchMaxBytes: Int,
                         hardMaxBytesLimit: Boolean,
                         fetchOnlyLeader: Boolean,
                         fetchIsolation: FetchIsolation,
                         isFromFollower: Boolean,
                         replicaId: Int,
                         fetchPartitionStatus: Seq[(TopicPartition, FetchPartitionStatus)]) {

  override def toString = "FetchMetadata(minBytes=" + fetchMinBytes + ", " +
    "maxBytes=" + fetchMaxBytes + ", " +
    "onlyLeader=" + fetchOnlyLeader + ", " +
    "fetchIsolation=" + fetchIsolation + ", " +
    "replicaId=" + replicaId + ", " +
    "partitionStatus=" + fetchPartitionStatus + ")"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 */
class DelayedFetch(delayMs: Long,
                   fetchMetadata: FetchMetadata,
                   replicaManager: ReplicaManager,
                   quota: ReplicaQuota,
                   tierFetchOpt: Option[PendingFetch],
                   clientMetadata: Option[ClientMetadata],
                   brokerTopicStats: BrokerTopicStats,
                   responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: The replica is no longer available on this broker
   * Case C: This broker does not know of some partitions it tries to fetch
   * Case D: The partition is in an offline log directory on this broker
   * Case E: This broker is the leader, but the requested epoch is now fenced
   * Case F: The fetch offset locates not on the last segment of the log
   * Case G: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   * Case H: The high watermark on this broker has changed within a FetchSession, need to propagate to follower (KIP-392)
   * Upon completion, should return whatever data is available for each valid partition
   * Case I: A single partition was fully fetched from tiered storage.
   */
  override def tryComplete(): Boolean = {
    var accumulatedSize = 0
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus: FetchPartitionStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        val fetchLeaderEpoch = fetchStatus.fetchInfo.currentLeaderEpoch
        try {
          // For both tier and non-tier fetches, we attempt to get the partition.
          // this ensures we'll correctly propagate any exceptions for tiered fetches if
          // the leader changes or the replica is not available.
          val partition = replicaManager.getPartitionOrException(topicPartition,
            expectLeader = fetchMetadata.fetchOnlyLeader)
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchLeaderEpoch, fetchMetadata.fetchOnlyLeader)

          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            val endOffset = fetchMetadata.fetchIsolation match {
              case FetchLogEnd => offsetSnapshot.logEndOffset
              case FetchHighWatermark => offsetSnapshot.highWatermark
              case FetchTxnCommitted => offsetSnapshot.lastStableOffset
            }

            // Go directly to the check for Case G if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case F.
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case F, this can happen when the new fetch operation is on a truncated leader
                debug(s"Satisfying fetch $fetchMetadata since it is fetching later segments of partition $topicPartition.")
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case F, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                debug(s"Satisfying fetch $fetchMetadata immediately since it is fetching older segments.")
                // We will not force complete the fetch request if a replica should be throttled.
                if (!replicaManager.shouldLeaderThrottle(quota, partition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!replicaManager.shouldLeaderThrottle(quota, partition, fetchMetadata.replicaId))
                  accumulatedSize += bytesAvailable
              }
            }

            if (fetchMetadata.isFromFollower) {
              // Case H check if the follower has the latest HW from the leader
              if (partition.getReplica(fetchMetadata.replicaId)
                .exists(r => offsetSnapshot.highWatermark.messageOffset > r.lastSentHighWatermark)) {
                return forceComplete()
              }
            }
          }
        } catch {
          case _: NotLeaderForPartitionException =>  // Case A
            debug(s"Broker is no longer the leader of $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: ReplicaNotAvailableException =>  // Case B
            debug(s"Broker no longer has a replica of $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case C
            debug(s"Broker no longer knows of partition $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: KafkaStorageException => // Case D
            debug(s"Partition $topicPartition is in an offline log directory, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: FencedLeaderEpochException => // Case E
            debug(s"Broker is the leader of partition $topicPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $fetchMetadata immediately")
            return forceComplete()
        }
    }

    tierFetchOpt.map { tierFetch =>
      // Case I, our tiered storage fetch request is done.
      if (tierFetch.isComplete)
        return forceComplete()
    }

    // Case G (only if there is no ongoing tier fetch, otherwise we always wait for the tier fetch to complete.
    if (accumulatedSize >= fetchMetadata.fetchMinBytes && tierFetchOpt.isEmpty)
      forceComplete()
    else
      false
  }

  override def onExpiration(): Unit = {
    tierFetchOpt.foreach(_.markFetchExpired())
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
    * For both tiered and local data, collect LogReadResults. For local data, this will include fully-formed
    * Records. For Tiered data, it is expected that the resulting TierLogReadResults will be combined with
    * fetched Record data from the TierFetcher.
    */
  private def collectLogReadResults(): Seq[(TopicPartition, AbstractLogReadResult)] = {
    replicaManager.readFromLocalLog(
      replicaId = fetchMetadata.replicaId,
      fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
      fetchIsolation = fetchMetadata.fetchIsolation,
      fetchMaxBytes = fetchMetadata.fetchMaxBytes,
      hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
      readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
      clientMetadata = clientMetadata,
      quota = quota)
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete(): Unit = {
    // In order to reclaim memory, ensure that DelayedFetch will wait for the
    // thread driving the PendingFetch to conclude. After joining on the future, it's known that
    // the PendingFetch will not attempt to claim any more memory, and ownership of the associated
    // memory lease has transferred to the DelayedFetch/Response.
    val tierFetcherReadResults = tierFetchOpt.map(_.finish().asScala)

    val logReadResults = collectLogReadResults()
    val fetchPartitionData = logReadResults.map { case (tp, logReadResult) =>
      val result = logReadResult match {
        // For data fetched from tiered storage, we combine the tierFetcherReadResults with the TierLogReadResults
        // returned from the Partition/Log layer. This provides us with metadata like leader epoch and high watermark.
        case tierLogReadResult: TierLogReadResult =>
          val tierFetchResult = tierFetcherReadResults.flatMap(_.get(tp)).getOrElse(
            TierFetchResult.emptyFetchResult)
          tierLogReadResult.intoLogReadResult(tierFetchResult, isReadAllowed = !tierFetchResult.isEmpty)
        case localLogReadResult: LogReadResult => localLogReadResult
      }
      FetchLag.maybeRecordConsumerFetchTimeLag(!fetchMetadata.isFromFollower, result, brokerTopicStats)
      tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
        result.lastStableOffset, result.info.abortedTransactions, result.preferredReadReplica,
        fetchMetadata.isFromFollower && replicaManager.isAddingReplica(tp, fetchMetadata.replicaId))
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

