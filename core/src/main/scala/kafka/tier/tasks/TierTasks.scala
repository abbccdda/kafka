/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.archive.{ArchiverTaskQueue, TierArchiver}
import kafka.tier.tasks.delete.TierDeletionManager
import kafka.tier.{TierDeletedPartitionsCoordinator, TierReplicaManager}
import kafka.tier.topic.TierTopicAppender
import kafka.utils.{Logging, ShutdownableThread}
import org.apache.kafka.common.utils.{KafkaThread, Time}

import scala.concurrent.duration._
import scala.concurrent._

case class TierTasksConfig(numThreads: Int,
                           logCleanupIntervalMs: Long = 500,
                           updateIntervalMs: Int = 50,
                           mainLoopBackoffMs: Int = 1000,
                           maxRetryBackoffMs: Int = 1000 * 60 * 5)

object TierTasksConfig {
  def apply(kafkaConfig: KafkaConfig): TierTasksConfig = {
    TierTasksConfig(kafkaConfig.tierArchiverNumThreads, kafkaConfig.logCleanupIntervalMs)
  }
}

/**
  * Common infrastructure for executing background tasks for tiered storage. Primary tasks include ability to archive
  * and delete segments.
  */
class TierTasks(config: TierTasksConfig,
                replicaManager: ReplicaManager,
                tierReplicaManager: TierReplicaManager,
                tierDeletedPartitionsCoordinator: TierDeletedPartitionsCoordinator,
                tierTopicAppender: TierTopicAppender,
                tierObjectStore: TierObjectStore,
                time: Time = Time.SYSTEM) extends ShutdownableThread(name = "tier-tasks") with KafkaMetricsGroup with Logging {
  override protected def loggerName: String = classOf[TierTasks].getName

  private val ctx: CancellationContext = CancellationContext.newContext()
  private val executor = Executors.newFixedThreadPool(config.numThreads, new ThreadFactory {
    val threadNum = new AtomicInteger(-1)
    override def newThread(r: Runnable): Thread = {
      val newThreadNum = threadNum.incrementAndGet()
      KafkaThread.nonDaemon(s"TierTask-$newThreadNum", r)
    }
  })

  /**
    * The thread pool used to execute archiver and deletion tasks. All blocking tasks must be run in the context of
    * thread pool, to avoid blocking the polling thread. See [[kafka.tier.tasks.archive.ArchiveTask]] and
    * [[kafka.tier.tasks.delete.DeletionTask]].
    */
  private implicit val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)

  private val tierArchiver = new TierArchiver(config, replicaManager, tierTopicAppender, tierObjectStore,
    ctx.subContext(), maxTasks = config.numThreads, time)
  private val tierDeletionManager = new TierDeletionManager(replicaManager, tierTopicAppender, tierObjectStore,
    ctx.subContext(), maxTasks = config.numThreads, logCleanupIntervalMs = config.logCleanupIntervalMs,
    maxRetryBackoffMs = config.maxRetryBackoffMs, time)

  private val changeManager = new ChangeManager(ctx.subContext(),
    Seq(tierArchiver.taskQueue, tierDeletionManager.taskQueue),
    time)

  removeMetric("CyclesPerSec")
  removeMetric("PartitionsInError")
  private val cycleTimeMetric = newMeter("CyclesPerSec", "tier tasks cycles per second", TimeUnit.SECONDS)
  private val partitionInError = newGauge("NumPartitionsInError",
    new Gauge[Int] {
      def value: Int = {
        tierArchiver.taskQueue.errorPartitionCount()
      }
    },
    Map[String, String]())


  locally {
    tierReplicaManager.addListener(changeManager)
    tierDeletedPartitionsCoordinator.registerListener(changeManager)
  }

  override def doWork(): Unit = {
    if (!tierTopicAppender.isReady) {
      info("TierTopicAppender is not ready. Backing off.")
      while (!tierTopicAppender.isReady && isRunning)
        Thread.sleep(config.mainLoopBackoffMs)

      if (!isRunning)
        return
    }

    cycleTimeMetric.mark()

    changeManager.process()
    val archiverFutures = tierArchiver.doWork()
    val deletionFutures = tierDeletionManager.doWork()
    val futures = archiverFutures ++ deletionFutures

    if (tierArchiver.taskQueue.taskCount == 0 && tierDeletionManager.taskQueue.taskCount == 0) {
      // Both task queues are empty; block until we have at least one task available
      changeManager.processAtLeastOne()
    } else if (futures.isEmpty) {
      // Task queues are not empty but we aren't processing any futures yet; backoff and retry
      Thread.sleep(config.mainLoopBackoffMs)
    } else {
      if (futures.size >= config.numThreads) {
        // We are processing as many futures to saturate the thread pool; wait until at least one of the futures is
        // completed
        debug("working set is full, blocking until a task completes")
        Await.ready(Future.firstCompletedOf(futures), Int.MaxValue.seconds)
      } else {
        // We are processing futures but our thread pool has not been saturated; wait for the update interval and go
        // back early to see if we have more work to do
        debug("working set is not full, attempting to complete at least one future")
        try {
          Await.ready(Future.firstCompletedOf(futures), config.updateIntervalMs.milliseconds)
        } catch {
          case _: TimeoutException =>
        }
      }
    }
  }

  override def shutdown(): Unit = {
    info("shutting down")
    initiateShutdown()
    ctx.cancel()
    changeManager.close()
    tierArchiver.shutdown()
    tierDeletionManager.shutdown()
    executor.shutdownNow()
    awaitShutdown()
  }

  // visible for testing
  def archiverTaskQueue: ArchiverTaskQueue = tierArchiver.taskQueue
}
