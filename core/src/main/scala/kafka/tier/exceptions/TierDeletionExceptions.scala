/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.exceptions

import kafka.tier.TopicIdPartition

class TierDeletionFencedException(val topicIdPartition: TopicIdPartition, cause: Throwable = null)
  extends RuntimeException(s"Fenced for partition $topicIdPartition", cause)

class TierDeletionFatalException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause) {

  def this(topicIdPartition: TopicIdPartition, cause: Throwable) = {
    this(s"Fatal exception for $topicIdPartition", cause)
  }
}

class TierDeletionFailedException(val topicIdPartition: TopicIdPartition)
  extends RuntimeException(s"Partition $topicIdPartition has failed, stopping actions")

class TierDeletionRestoreFencedException(val topicIdPartition: TopicIdPartition)
  extends RuntimeException(s"Partition $topicIdPartition has been restored and all stale metadata has been fenced.")

case class TaskCompletedException(topicIdPartition: TopicIdPartition) extends RuntimeException

class TierDeletionTaskFencedException(val topicIdPartition: TopicIdPartition, cause: Throwable = null)
  extends RuntimeException(s"Fenced for partition $topicIdPartition", cause)