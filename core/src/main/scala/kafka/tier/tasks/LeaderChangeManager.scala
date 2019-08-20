/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks

import kafka.tier.fetcher.CancellationContext
import kafka.tier.{TierMetadataManager, TopicIdPartition}
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

/**
  * Manages leadership changes. Certain tasks like archiving and retention are relevant when we are the leader of a
  * particular topic-partition. This module acts as an intermediary to propagate leadership events down to specific
  * components, through the provided task queues. Changes are propagated synchronously when either one of [[process()]]
  * or [[processAtLeastOne()]] is called.
  * @param ctx Cancellation context associated LeaderChangeManager
  * @param taskQueues Task queues in use. All leadership changes will be propagated down to these.
  * @param time The time instance to use
  */
class LeaderChangeManager(ctx: CancellationContext,
                          taskQueues: Seq[TierTaskQueue[_]],
                          time: Time) extends TierMetadataManager.ChangeListener with Logging with AutoCloseable {
  override def loggerName: String = classOf[LeaderChangeManager].getName

  private val leadershipChangeQueue: UpdatableQueue[LeadershipChange] = new UpdatableQueue()

  override def onBecomeLeader(topicIdPartition: TopicIdPartition, leaderEpoch: Int): Unit = {
    leadershipChangeQueue.push(StartLeadership(topicIdPartition, leaderEpoch))
  }

  override def onBecomeFollower(topicIdPartition: TopicIdPartition): Unit = {
    leadershipChangeQueue.push(StopLeadership(topicIdPartition))
  }

  override def onDelete(topicIdPartition: TopicIdPartition): Unit = {
    leadershipChangeQueue.push(StopLeadership(topicIdPartition))
  }

  /**
    * Process changes, blocking until at least one has been processed. The changes are propagated to underlying task
    * queues.
    */
  def processAtLeastOne(): Unit = {
    val change = leadershipChangeQueue.take()
    processChange(change)
    process()
  }

  /**
    * Process as many changes as available. This is non-blocking and will return as soon as the change queue is fully
    * drained. The changes are propagated to underlying task queues.
    */
  def process(): Unit = {
    var done = false

    while (!ctx.isCancelled && !done) {
      leadershipChangeQueue.poll() match {
        case Some(change) => processChange(change)
        case None => done = true
      }
    }
  }

  override def close(): Unit = {
    ctx.cancel()
    leadershipChangeQueue.close()
  }

  private def processChange(change: LeadershipChange): Unit = {
    change match {
      case StartLeadership(topicIdPartition, leaderEpoch) => taskQueues.foreach(_.addTask(topicIdPartition, leaderEpoch))
      case StopLeadership(topicIdPartition) => taskQueues.foreach(_.removeTask(topicIdPartition))
    }
  }
}

sealed trait LeadershipChange extends UpdatableQueueEntry {
  def topicIdPartition: TopicIdPartition

  override type Key = TopicIdPartition
  override def key: TopicIdPartition = topicIdPartition
}

final case class StartLeadership(topicIdPartition: TopicIdPartition, leaderEpoch: Int) extends LeadershipChange
final case class StopLeadership(topicIdPartition: TopicIdPartition) extends LeadershipChange
