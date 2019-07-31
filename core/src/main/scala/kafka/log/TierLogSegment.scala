/*
 Copyright 2018 Confluent Inc.
 */

package kafka.log

import kafka.server.TierFetchDataInfo
import kafka.tier.domain.TierObjectMetadata
import kafka.tier.fetcher.TierFetchMetadata
import kafka.tier.store.TierObjectStore

class TierLogSegment private[log] (private val segment: TierObjectMetadata,
                                   val startOffset: Long,
                                   private val tierObjectStore: TierObjectStore) {
  val metadata = new TierObjectStore.ObjectMetadata(segment)

  def baseOffset: Long = segment.baseOffset

  def endOffset: Long = segment.endOffset

  def size: Int = segment.size

  def read(startOffset: Long,
           maxSize: Int,
           maxPosition: Long,
           minOneMessage: Boolean): Option[TierFetchDataInfo] = {
    if (startOffset < baseOffset || startOffset > segment.endOffset) {
      None
    } else {
      val maximumReadableBytes = math.min(maxSize, segment.size)
      val fetchMetadata = TierFetchMetadata(topicPartition = segment.topicIdPartition.topicPartition,
        fetchStartOffset = startOffset,
        maxBytes = maximumReadableBytes,
        maxPosition = maxPosition,
        minOneMessage = minOneMessage,
        segmentMetadata = metadata,
        transactionMetadata = None,
        segmentBaseOffset = baseOffset,
        segmentSize = segment.size)
      Some(TierFetchDataInfo(fetchMetadata, None))
    }
  }

  def nextOffset: Long = segment.endOffset + 1
  def maxTimestamp: Long = segment.maxTimestamp

  override def toString = s"topicPartition: ${segment.topicIdPartition} baseOffset: $baseOffset tierObjectStore: $tierObjectStore"
}

