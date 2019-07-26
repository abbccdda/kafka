/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CancellationException, Executors, ThreadFactory, TimeUnit}

import com.yammer.metrics.core.Gauge
import kafka.log.AbstractLog
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.store.TierObjectStore
import kafka.tier.topic.TierTopicAppender
import kafka.tier.TierMetadataManager
import kafka.utils.{Logging, ShutdownableThread}
import org.apache.kafka.common.utils.{KafkaThread, Time}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

case class TierArchiverConfig(numThreads: Int = 10,
                              updateIntervalMs: Int = 50,
                              mainLoopBackoffMs: Int = 1000,
                              maxRetryBackoffMs: Int = 1000 * 60 * 5)

object TierArchiverConfig {
  def apply(kafkaConfig: KafkaConfig): TierArchiverConfig = {
    TierArchiverConfig(kafkaConfig.tierArchiverNumThreads)
  }
}

/**
  * Tier Archiver uploads segment files, associated indices and other broker status to blob storage.
  * Segments for a topic partition are eligible for upload if `tierEnable` is set in `logConfig`, and
  * the Tier Archiver is running on the broker that is the current leader for the topic partition.
  *
  * An asynchronous status machine is created from each eligible topic partition, where (potentially blocking)
  * status transitions can be executed serially without blocking status transitions for other topic partitions.
  *
  * The executor thread pool is a limited resource for blocking operations, so there is a `priority()` method
  * on each status, allowing the derivation of relative priority between status transitions. State transitions
  * are scheduled based on this `priority()` method.
  *
  */
final class TierArchiver(config: TierArchiverConfig,
                         replicaManager: ReplicaManager,
                         tierMetadataManager: TierMetadataManager,
                         tierTopicAppender: TierTopicAppender,
                         tierObjectStore: TierObjectStore,
                         time: Time = Time.SYSTEM) extends ShutdownableThread(name = "tier-archiver") with KafkaMetricsGroup with Logging {

  override protected def loggerName: String = classOf[TierArchiver].getName

  private val ctx: CancellationContext = CancellationContext.newContext()
  private val executor = Executors.newFixedThreadPool(config.numThreads, new ThreadFactory {
    val threadNum = new AtomicInteger(-1)
    override def newThread(r: Runnable): Thread = {
      val newThreadNum = threadNum.incrementAndGet()
      KafkaThread.nonDaemon(s"ArchiverThread-$newThreadNum", r)
    }
  })
  private implicit val pool: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
  private[tier] val taskQueue: TaskQueue[ArchiveTask] = new ArchiverTaskQueue[ArchiveTask](ctx.subContext(), time, schedulingLag, ArchiveTask.apply)

  // Register the taskQueue with the metadata listener
  tierMetadataManager.addListener(this.getClass, taskQueue)

  /**
    * Tasks are ordered in the following way,
    *
    *   1. AfterUpload > Upload > BeforeLeader > BeforeUpload
    *
    *   2. A partition in the BeforeUpload state with low
    *      lag always comes before a partition in the BeforeUpload
    *      state with high lag.
    *
    * `schedulingLag()` returns an Option[Long] suitable for sorting
    * tasks using a less-than comparison. That is to say, more important tasks
    * will have a lower return value while less important tasks will
    * have a higher return value. If a lag cannot be found, None is returned.
    */
  private def schedulingLag(task: ArchiveTask): Option[Long] = {
    task.state match {
      case _: BeforeLeader => Some(-1)
      case _: BeforeUpload =>
        replicaManager
          .getLog(task.topicPartition)
          .map(log => TierArchiver.sizeOfTierableSegments(log))
      case _: Upload => Some(-2)
      case _: AfterUpload => Some(-3)
    }
  }

  removeMetric("TotalLag")
  newGauge("TotalLag",
    new Gauge[Long] {
      def value(): Long = TierArchiver.totalLag(replicaManager, tierMetadataManager)
    }
  )

  removeMetric("RetryStateCount")
  newGauge("RetryStateCount",
    new Gauge[Long] {
      def value(): Long = {
        taskQueue.withAllTasks(tasks => {
          tasks.map(_.totalRetryCount).sum
        })
      }
    }
  )

  removeMetric("BytesPerSec")
  private val byteRate = newMeter("BytesPerSec", "bytes per second", TimeUnit.SECONDS)

  removeMetric("CyclesPerSec")
  private val cycleTimeMetric = newMeter("CyclesPerSec", "archiver cycles per second", TimeUnit.SECONDS)

  removeMetric("WorkingSetSaturationPercent")
  newGauge("WorkingSetSaturationPercent", new Gauge[Double] {
    override def value(): Double = {
      workingSet.size / config.numThreads
    }
  })

  /**
    * Contains the set of currently executing ArchiveTasks.
    */
  @volatile var workingSet: List[Future[ArchiveTask]] = List()

  /**
    * Block until there is at least one task item in the workingSet. If there is a task item
    * in the working set already, continue to add to it until filled or poll times out
    */
  private def fillWorkingSet(): Unit = {
    var continue = true
    while (continue && !ctx.isCancelled && workingSet.size < config.numThreads) {
      if (workingSet.isEmpty) {
        debug("working set is empty, blocking until a new task is available")
        val newTask = taskQueue.poll()
        workingSet :+= newTask.transition(time, tierTopicAppender, tierObjectStore, replicaManager,
          Some(byteRate), Some(config.maxRetryBackoffMs))
      } else {
        taskQueue.poll(config.updateIntervalMs, TimeUnit.MILLISECONDS) match {
          case Some(newTask) =>
            workingSet :+= newTask.transition(time, tierTopicAppender, tierObjectStore, replicaManager,
              Some(byteRate), Some(config.maxRetryBackoffMs))
          case None => continue = false
        }
      }
    }
  }

  /**
    * If the working set is full, block until at least one task has completed.
    * If the working set is not full, block for config.updateIntervalMs or until a task
    * has completed. Drain the working set of all completed tasks and re-add them to
    * the work queue.
    */
  private def drainFutures(): Unit = {
    if (workingSet.size >= config.numThreads) {
      debug("working set is full, blocking until a task completes")
      Await.ready(Future.firstCompletedOf(workingSet), Int.MaxValue seconds)
    } else {
      debug("working set is not full, attempting to complete at least one future")
      // Here we just use `Try` to swallow the exception thrown when hitting the `Await.ready` timeout.
      Try(Await.ready(Future.firstCompletedOf(workingSet), config.updateIntervalMs millis))
    }
    val (completed, inProgress) = workingSet.partition(_.isCompleted)
    workingSet = inProgress
    debug(s"${inProgress.size} tasks still in progress")
    debug(s"${completed.size} tasks completed")
    for (taskFuture <- completed) {
      val task = Await.result(taskFuture, 0 seconds)
      debug(s"completing task $task")
      taskQueue.done(task)
    }
  }

  override def doWork(): Unit = {
    try {
      while (!ctx.isCancelled) {
        if (tierTopicAppender.isReady) {
          fillWorkingSet()
          drainFutures()
          cycleTimeMetric.mark()
        } else {
          info("TierTopicAppender is not ready. Backing off.")
          Thread.sleep(1000)
        }
      }
      info("exiting work loop")
    } catch {
      case _: InterruptedException | _: CancellationException =>
        info("archiver shutting down")
      case t: Throwable =>
        fatal("caught fatal exception while archiving", t)
        throw t
    }
  }

  override def shutdown(): Unit = {
    info("shutting down")
    super.shutdown()
    tierMetadataManager.removeListener(this.getClass)
    ctx.cancel()
    taskQueue.close()
    executor.shutdownNow()
  }
}

object TierArchiver {
  private[archiver] def sizeOfTierableSegments(log: AbstractLog): Long = {
    log.tierableLogSegments.map(_.size.toLong).sum
  }

  private[archiver] def totalLag(replicaManager: ReplicaManager, tierMetadataManager: TierMetadataManager): Long = {
    var totalSize = 0L

    tierMetadataManager.tierEnabledLeaderPartitionStateIterator.asScala.foreach { partitionState =>
      if (partitionState.tieringEnabled) {
        replicaManager.getLog(partitionState.topicPartition) match {
          case Some(log) => totalSize += sizeOfTierableSegments(log)
          case None =>
        }
      }
    }
    totalSize
  }
}
