/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks

import kafka.tier.fetcher.CancellationContext
import kafka.tier.store.TierObjectStore
import kafka.tier.{DeletedPartitionsChangeListener, TierReplicaManager, TopicIdPartition}
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

/**
  * Manages partition lifecycle changes for tiered storage. Certain tasks like archiving and retention are relevant
  * when we are the leader of a particular topic-partition. The deletion task also needs to be aware of any partition
  * deletion tasks it needs to start up. This module acts as an intermediary to propagate such events down to specific
  * components, through the provided task queues. Changes are propagated synchronously when either one of [[process()]]
  * or [[processAtLeastOne()]] is called.
  * @param ctx Cancellation context associated LeaderChangeManager
  * @param taskQueues Task queues in use. All leadership changes will be propagated down to these.
  * @param time The time instance to use
  */
class ChangeManager(ctx: CancellationContext,
                    taskQueues: Seq[TierTaskQueue[_]],
                    time: Time) extends TierReplicaManager.ChangeListener with DeletedPartitionsChangeListener with Logging with AutoCloseable {
  override def loggerName: String = classOf[ChangeManager].getName

  private val changeQueue: UpdatableQueue[ChangeMetadata] = new UpdatableQueue()

  override def onBecomeLeader(topicIdPartition: TopicIdPartition, leaderEpoch: Int): Unit = {
    changeQueue.push(StartLeadership(topicIdPartition, leaderEpoch))
  }

  override def onBecomeFollower(topicIdPartition: TopicIdPartition): Unit = {
    changeQueue.push(StopLeadership(topicIdPartition))
  }

  override def onDelete(topicIdPartition: TopicIdPartition): Unit = {
    changeQueue.push(StopLeadership(topicIdPartition))
  }

  override def initiatePartitionDeletion(topicIdPartition: TopicIdPartition,
                                         tieredObjects: List[TierObjectStore.ObjectMetadata]): Unit = {
    changeQueue.push(StartPartitionDeletion(topicIdPartition, tieredObjects))
  }

  override def stopPartitionDeletion(topicIdPartition: TopicIdPartition): Unit = {
    changeQueue.push(StopPartitionDeletion(topicIdPartition))
  }

  /**
    * Process changes, blocking until at least one has been processed. The changes are propagated to underlying task
    * queues.
    */
  def processAtLeastOne(): Unit = {
    val change = changeQueue.take()
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
      changeQueue.poll() match {
        case Some(change) => processChange(change)
        case None => done = true
      }
    }
  }

  override def close(): Unit = {
    ctx.cancel()
    changeQueue.close()
  }

  private def processChange(change: ChangeMetadata): Unit = {
    change match {
      case startChangeMetadata: StartChangeMetadata => taskQueues.foreach(_.maybeAddTask(startChangeMetadata))
      case stopChangeMetadata: StopChangeMetadata => taskQueues.foreach(_.maybeRemoveTask(stopChangeMetadata))
    }
  }
}

sealed trait ChangeMetadata extends UpdatableQueueEntry {
  def topicIdPartition: TopicIdPartition

  override type Key = TopicIdPartition
  override def key: TopicIdPartition = topicIdPartition
}

sealed trait StartChangeMetadata extends ChangeMetadata {
  def topicIdPartition: TopicIdPartition
}

sealed trait StopChangeMetadata extends ChangeMetadata {
  def topicIdPartition: TopicIdPartition
}

sealed trait LeadershipChange extends ChangeMetadata
final case class StartLeadership(topicIdPartition: TopicIdPartition, leaderEpoch: Int) extends LeadershipChange with StartChangeMetadata
final case class StopLeadership(topicIdPartition: TopicIdPartition) extends LeadershipChange with StopChangeMetadata

sealed trait DeletedPartitionChange extends ChangeMetadata
final case class StartPartitionDeletion(topicIdPartition: TopicIdPartition,
                                        tieredObjects: List[TierObjectStore.ObjectMetadata]) extends DeletedPartitionChange with StartChangeMetadata
final case class StopPartitionDeletion(topicIdPartition: TopicIdPartition) extends DeletedPartitionChange with StopChangeMetadata

