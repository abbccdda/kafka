/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher

import kafka.tier.domain.TierObjectMetadata
import org.apache.kafka.common.TopicPartition
import kafka.tier.store.TierObjectStore

case class TierFetchMetadata(topicPartition: TopicPartition,
                             fetchStartOffset: Long,
                             maxBytes: Integer,
                             maxPosition: Long,
                             minOneMessage: Boolean,
                             segmentMetadata: TierObjectStore.ObjectMetadata,
                             transactionMetadata: Option[List[TierObjectMetadata]],
                             segmentBaseOffset: Long,
                             segmentSize: Int)
