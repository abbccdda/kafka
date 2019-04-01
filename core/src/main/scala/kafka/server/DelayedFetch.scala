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

import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import kafka.tier.fetcher.{TierFetchResult}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors._
import org.apache.kafka.common.requests.FetchRequest.PartitionData

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString = "[startOffsetMetadata: " + startOffsetMetadata + ", " +
                          "fetchInfo: " + fetchInfo + "]"
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
                   tierFetchIdOpt: Option[UUID],
                   responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: This broker does not know of some partitions it tries to fetch
   * Case C: The fetch offset locates not on the last segment of the log
   * Case D: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   * Case E: The partition is in an offline log directory on this broker
   * Case F: This broker is the leader, but the requested epoch is now fenced
   * Case G: A single partition was fully fetched from tiered storage.
   *
   * Upon completion, should return whatever data is available for each valid partition
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

            // Go directly to the check for Case D if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case C.
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case C, this can happen when the new fetch operation is on a truncated leader
                debug(s"Satisfying fetch $fetchMetadata since it is fetching later segments of partition $topicPartition.")
                return forceComplete()
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case C, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                debug(s"Satisfying fetch $fetchMetadata immediately since it is fetching older segments.")
                // We will not force complete the fetch request if a replica should be throttled.
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  return forceComplete()
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  accumulatedSize += bytesAvailable
              }
            }
          }
        } catch {
          case _: KafkaStorageException => // Case E
            debug(s"Partition $topicPartition is in an offline log directory, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: UnknownTopicOrPartitionException => // Case B
            debug(s"Broker no longer knows of partition $topicPartition, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: FencedLeaderEpochException => // Case F
            debug(s"Broker is the leader of partition $topicPartition, but the requested epoch " +
              s"$fetchLeaderEpoch is fenced by the latest leader epoch, satisfy $fetchMetadata immediately")
            return forceComplete()
          case _: NotLeaderForPartitionException =>  // Case A
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
        }
    }

    tierFetchIdOpt.map { tierFetchId =>
      // Case G, our tiered storage fetch request is done.
      if (replicaManager.tierFetchIsComplete(tierFetchId).get)
        return forceComplete()
    }

    // Case D (only if there is no ongoing tier fetch, otherwise we always wait for the tier fetch to complete.
    if (accumulatedSize >= fetchMetadata.fetchMinBytes && tierFetchIdOpt.isEmpty)
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    tierFetchIdOpt.foreach(reqId => replicaManager.cancelTierFetch(reqId))
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
      quota = quota)
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   */
  override def onComplete() {
    val tierFetchCompleted = tierFetchIdOpt.flatMap(reqId => replicaManager.tierFetchIsComplete(reqId)).getOrElse(false)
    // If the tierFetch is not completed, then it's safe to assume that this request timed out. It should have been
    // canceled in `onExpiration()`. In order to prevent blocking the expiration thread, we will only retrieve
    // the results if doing so will not block.
    val tierFetcherReadResults: Option[util.Map[TopicPartition, TierFetchResult]] = if (tierFetchCompleted) {
      tierFetchIdOpt.flatMap(reqId => replicaManager.getTierFetchResults(reqId))
    } else None

    val logReadResults = collectLogReadResults()
    val unifiedReadResults: Seq[(TopicPartition, LogReadResult)] = logReadResults.map {
      // For data fetched from tiered storage, we combine the tierFetcherReadResults with the TierLogReadResults
      // returned from the Partition/Log layer. This provides us with metadata like leader epoch and high watermark.
      case (tp, tierLogReadResult: TierLogReadResult) => {
        // We may have not gotten any results back for our tier fetch if we canceled early,
        // return empty batches in that case.
        val tierFetchResult = tierFetcherReadResults.map(_.get(tp)).getOrElse(TierFetchResult.emptyFetchResult())
        tp -> tierLogReadResult.intoLogReadResult(tierFetchResult)
      }
      // Data returned from local storage does not need to be changed
      case (tp, localLogReadResult: LogReadResult) => tp -> localLogReadResult
    }

    val fetchPartitionData = unifiedReadResults.map {
      case (tp, result) =>
        tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
          result.lastStableOffset, result.info.abortedTransactions)
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

