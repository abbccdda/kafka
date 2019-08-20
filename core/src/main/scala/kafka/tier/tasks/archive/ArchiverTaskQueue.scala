/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.archive

import kafka.tier.TopicIdPartition
import kafka.tier.fetcher.CancellationContext
import kafka.tier.tasks.TierTaskQueue
import org.apache.kafka.common.utils.Time

import scala.collection.immutable.ListSet

private[tasks] class ArchiverTaskQueue(ctx: CancellationContext,
                                       maxTasks: Int,
                                       time: Time,
                                       lagFn: ArchiveTask => Option[Long],
                                       archiverMetrics: ArchiverMetrics) extends TierTaskQueue[ArchiveTask](ctx, maxTasks, time) {
  protected[tasks] def sortTasks(tasks: ListSet[ArchiveTask]): ListSet[ArchiveTask] = {
    tasks.toList
      .map { task => (task, lagFn(task).getOrElse(0L)) }
      .filterNot { case (_, lag: Long) => lag == 0L }
      .sortBy { case (_, lag: Long) => lag }
      .map { case (task, _) => task }
      .to[ListSet]
  }

  override protected[tasks] def newTask(topicIdPartition: TopicIdPartition, epoch: Int): ArchiveTask = {
    ArchiveTask(ctx.subContext(), topicIdPartition, epoch, archiverMetrics)
  }
}
