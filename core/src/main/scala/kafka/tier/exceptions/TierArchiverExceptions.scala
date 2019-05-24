/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.exceptions

import kafka.tier.TopicIdPartition

class TierArchiverFencedException(topicIdPartition: TopicIdPartition)
  extends RuntimeException(s"Fenced for partition $topicIdPartition")

class TierArchiverFatalException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause) {

  def this(topicIdPartition: TopicIdPartition, cause: Throwable) {
    this(s"Fatal exception for $topicIdPartition", cause)
  }
}
