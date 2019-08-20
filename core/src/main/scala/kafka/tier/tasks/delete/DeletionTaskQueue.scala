/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import com.yammer.metrics.core.Meter
import kafka.tier.TopicIdPartition
import kafka.tier.fetcher.CancellationContext
import kafka.tier.tasks.TierTaskQueue
import kafka.tier.tasks.delete.DeletionTask.CollectDeletableSegments
import org.apache.kafka.common.utils.Time

import scala.collection.immutable.ListSet

private[delete] class DeletionTaskQueue(ctx: CancellationContext,
                                        maxTasks: Int,
                                        logCleanupIntervalMs: Long,
                                        time: Time,
                                        retryRateOpt: Option[Meter] = None) extends TierTaskQueue[DeletionTask](ctx, maxTasks, time) {
  override protected[tasks] def sortTasks(tasks: ListSet[DeletionTask]): ListSet[DeletionTask] = {
    tasks.toList
      .sortBy { task => taskPriority(task) }
      .to[ListSet]
  }

  override protected[tasks] def newTask(topicIdPartition: TopicIdPartition, epoch: Int): DeletionTask = {
    new DeletionTask(ctx.subContext(), topicIdPartition, logCleanupIntervalMs, CollectDeletableSegments(epoch), retryRateOpt)
  }

  private def taskPriority(task: DeletionTask): Long = {
    task.state match {
      case _: CollectDeletableSegments =>
        task.lastProcessedMs.getOrElse(0L)

      case _ => -1L
    }
  }
}
