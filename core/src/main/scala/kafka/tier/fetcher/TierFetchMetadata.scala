/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher

import kafka.tier.domain.TierObjectMetadata
import org.apache.kafka.common.TopicPartition
import kafka.tier.store.TierObjectStore

case class TierFetchMetadata(val topicPartition: TopicPartition,
                             val fetchStartOffset: Long,
                             val maxOffset: Option[Long],
                             val maxBytes: Integer,
                             val maxPosition: Long,
                             val minOneMessage: Boolean,
                             val segmentMetadata: TierObjectStore.ObjectMetadata,
                             val transactionMetadata: Option[List[TierObjectMetadata]],
                             val segmentBaseOffset: Long,
                             val segmentSize: Int)
