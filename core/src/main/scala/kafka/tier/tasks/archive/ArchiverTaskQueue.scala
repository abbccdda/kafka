/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.archive

import kafka.tier.TopicIdPartition
import kafka.tier.fetcher.CancellationContext
import kafka.tier.tasks._
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

  override protected[tasks] def newTask(topicIdPartition: TopicIdPartition, metadata: StartChangeMetadata): ArchiveTask = {
    metadata match {
      case startLeadership: StartLeadership => ArchiveTask(ctx.subContext(), topicIdPartition, startLeadership.leaderEpoch, archiverMetrics)
      case unknown => throw new IllegalStateException(s"Unexpected change $unknown")
    }
  }

  override protected[tasks] def mayProcess(metadata: ChangeMetadata): Boolean = {
    metadata match {
      case _: LeadershipChange => true
      case _ => false
    }
  }
}
