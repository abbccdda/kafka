/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import com.yammer.metrics.core.Meter
import kafka.server.ReplicaManager
import kafka.tier.TopicIdPartition
import kafka.tier.fetcher.CancellationContext
import kafka.tier.tasks._
import kafka.tier.tasks.delete.DeletionTask.{CollectDeletableSegments, DeletedPartitionMetadata, DeleteAsLeaderMetadata}
import org.apache.kafka.common.utils.Time

import scala.collection.immutable.ListSet

private[delete] class DeletionTaskQueue(ctx: CancellationContext,
                                        maxTasks: Int,
                                        logCleanupIntervalMs: Long,
                                        time: Time,
                                        replicaManager: ReplicaManager,
                                        retryRateOpt: Option[Meter] = None) extends TierTaskQueue[DeletionTask](ctx, maxTasks, time) {
  override protected[tasks] def sortTasks(tasks: List[DeletionTask]): List[DeletionTask] =
    tasks.sortBy(taskPriority)

  override protected[tasks] def newTask(topicIdPartition: TopicIdPartition, change: StartChangeMetadata): DeletionTask = {
    val stateMetadata = change match {
      case startLeadership: StartLeadership =>
        DeleteAsLeaderMetadata(replicaManager, startLeadership.leaderEpoch)
      case startDeletedPartitionDeletion: StartPartitionDeletion =>
        DeletedPartitionMetadata(startDeletedPartitionDeletion.tieredObjects)
    }

    new DeletionTask(ctx.subContext(), topicIdPartition, logCleanupIntervalMs, CollectDeletableSegments(stateMetadata), retryRateOpt)
  }

  override protected[tasks] def mayProcess(metadata: ChangeMetadata): Boolean = true

  private def taskPriority(task: DeletionTask): Long = {
    task.state match {
      case _: CollectDeletableSegments =>
        task.lastProcessedMs.getOrElse(0L)

      case _ => -1L
    }
  }
}
