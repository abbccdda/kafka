/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import kafka.server.ReplicaManager
import kafka.tier.TierMetadataManager
import kafka.tier.fetcher.CancellationContext
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.TierTaskWorkingSet
import kafka.tier.topic.TierTopicAppender
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

import scala.concurrent.{ExecutionContext, Future}

/**
  * TierDeletionManager deletes segment files, associated indices and other broker state from the underlying tiered
  * storage. It executes deletions for all topic partitions contained in the exposed [[TierDeletionManager.taskQueue]].
  *
  * The deletion process consists of an asynchronous state machine created for each topic partition contained in the
  * task queue. Transitions for each topic partition can be executed in parallel without blocking another. The actual
  * transitions are run in the context of the supplied [[ec]] so that operations that interact with the object store,
  * etc. do not block the main thread.
  */
final class TierDeletionManager(replicaManager: ReplicaManager,
                                tierMetadataManager: TierMetadataManager,
                                tierTopicAppender: TierTopicAppender,
                                tierObjectStore: TierObjectStore,
                                ctx: CancellationContext,
                                maxTasks: Int,
                                logCleanupIntervalMs: Long,
                                maxRetryBackoffMs: Int,
                                time: Time = Time.SYSTEM)(implicit ec: ExecutionContext) extends Logging with KafkaMetricsGroup {
  override protected def loggerName: String = classOf[TierDeletionManager].getName

  removeMetric("RetriesPerSec")
  private val retryRate = newMeter("RetriesPerSec", "number of retries per second", TimeUnit.SECONDS)

  private[tasks] val taskQueue = new DeletionTaskQueue(ctx.subContext(), maxTasks, logCleanupIntervalMs, time, Some(retryRate))
  private val workingSet = new TierTaskWorkingSet[DeletionTask](taskQueue, replicaManager, tierMetadataManager,
    tierTopicAppender, tierObjectStore, maxRetryBackoffMs, time)

  /**
    * Initiate transitions for tasks and complete transitions if outstanding futures have completed. This method is
    * non-blocking.
    * @return List of outstanding futures
    */
  def doWork(): List[Future[DeletionTask]] = {
    workingSet.doWork()
  }

  def shutdown(): Unit = {
    ctx.cancel()
    taskQueue.close()
  }
}
