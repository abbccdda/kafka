/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions

import kafka.tier.TopicIdPartition

class TierArchiverFencedException(val topicIdPartition: TopicIdPartition, cause: Throwable = null)
  extends RuntimeException(s"Fenced for partition $topicIdPartition", cause)

class TierArchiverFatalException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause) {

  def this(topicIdPartition: TopicIdPartition, cause: Throwable) = {
    this(s"Fatal exception for $topicIdPartition", cause)
  }
}

class NotTierablePartitionException(val topicIdPartition: TopicIdPartition)
  extends RuntimeException(s"Partition $topicIdPartition is closed or deleted, stopping actions")

class TierArchiverFailedException(val topicIdPartition: TopicIdPartition)
  extends RuntimeException(s"Partition $topicIdPartition has failed, stopping actions")