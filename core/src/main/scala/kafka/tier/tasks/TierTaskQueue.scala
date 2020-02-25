/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

import com.yammer.metrics.core.Meter
import kafka.server.ReplicaManager
import kafka.tier.TopicIdPartition
import kafka.tier.fetcher.CancellationContext
import kafka.tier.store.TierObjectStore
import kafka.tier.topic.TierTopicAppender
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.collection.immutable.ListSet
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
  * Represents a task for a particular topic partition. A task can be in one of various states in its state machine.
  */
abstract class TierTask[T <: TierTask[T]](retryRateOpt: Option[Meter]) extends Logging {
  @volatile private[tier] var retryCount: Int = 0
  @volatile private var _pausedUntil: Option[Instant] = None
  @volatile private var _error: Option[Throwable] = None

  def topicIdPartition: TopicIdPartition
  def ctx: CancellationContext

  /**
    * Make an attempt to transition this task. This call is non-blocking.
    * @return Future corresponding to the task transition
    */
  def transition(time: Time,
                 tierTopicAppender: TierTopicAppender,
                 tierObjectStore: TierObjectStore,
                 replicaManager: ReplicaManager,
                 maxRetryBackoffMs: Option[Int] = None)(implicit ec: ExecutionContext): Future[T]

  def pausedUntil: Option[Instant] = _pausedUntil
  def isErrorState: Boolean = _error.isDefined
  def topicPartition: TopicPartition = topicIdPartition.topicPartition

  /**
    * Retry execution of given task after a timeout.
    * @param maxRetryBackoffMs Maximum timeout in milliseconds
    * @param nowMs Current time in milliseconds
    * @param t Exception causing retry
    */
  protected[tasks] def retryTaskLater(maxRetryBackoffMs: Int, nowMs: Long, t: Throwable): Unit = {
    retryCount += 1
    retryRateOpt.foreach(_.mark())
    val now = Instant.ofEpochMilli(nowMs)
    val pauseMs = Math.min(maxRetryBackoffMs, (Random.nextInt(retryCount) + 1) * 1000)
    warn(s"retrying $this after ${pauseMs}ms", t)
    _pausedUntil = Some(now.plusMillis(pauseMs))
  }

  /**
   * Places task into error state. This should only be used for non retriable exceptions
   * @param t Exception to set task to error state for
   */
  protected[tasks] def cancelAndSetErrorState(tierTask: TierTask[T], t: Throwable): Unit = {
    error(s"Partition ${topicIdPartition}, task $tierTask moved to error state due to unhandled exception", t)
    _error = Some(t)
    ctx.cancel()
  }

  /**
    * Signal successful [[transition()]] of the task. Resets the retry state.
    */
  protected def onSuccessfulTransition(): Unit = {
    retryCount = 0
    _pausedUntil = None
  }

  /**
    * Signal successful [[transition()]] of the task and add a delay, so that the next execution of the task is delayed
    * by the specified amount.
    * @param delayMs Time to delay next execution of task by, in milliseconds
    * @param nowMs Current time in milliseconds
    */
  protected def onSuccessfulTransitionWithDelay(delayMs: Long, nowMs: Long): Unit = {
    val now = Instant.ofEpochMilli(nowMs)
    onSuccessfulTransition()
    _pausedUntil = Some(now.plusMillis(delayMs))
  }
}

/**
  * Queue of tiering tasks. Provides abstractions to maintain a prioritized queue of tasks to be executed and retrieving
  * them when needed. This queue is _not_ thread safe.
  * @param ctx The cancellation context associated with this queue
  * @param maxTasks Maximum number of tasks to be processed at a given point in time. This limits the number of tasks
  *                 that can be polled from the queue.
  * @param time The time instance
  */
abstract class TierTaskQueue[T <: TierTask[T]](ctx: CancellationContext, maxTasks: Int, time: Time) extends Logging with AutoCloseable {
  override def loggerName: String = this.getClass.getName

  @volatile private var tasks = ListSet[T]()
  // Track partitions in error so that re-immigrated partitions are remain paused.
  // We will take the conservative approach and assume that these TopicPartitions cannot be safely
  // resumed until manual intervention is made. This can be re-evaluated in the future
  private val partitionsInError = new ConcurrentHashMap[TopicIdPartition, Long]()
  private var processing = ListSet[T]()

  protected[tasks] def errorPartitionCount(): Int = {
    partitionsInError.size()
  }

  /**
    * Sort the tasks in the order they should be processed.
    */
  protected[tasks] def sortTasks(tasks: List[T]): List[T]

  /**
    * Create a new task instance.
    * @param topicIdPartition topic partition
    * @param metadata the StartChangeMetadata for which this new task is being initialized
    * @return New task instance
    */
  protected[tasks] def newTask(topicIdPartition: TopicIdPartition, metadata: StartChangeMetadata): T

  /**
    * Check if this change could be processed. Changes are propagated to the queue only if this method returns `true`.
    * @param metadata Change metadata
    * @return true if the change can be propagated; false otherwise
    */
  protected[tasks] def mayProcess(metadata: ChangeMetadata): Boolean

  /**
    * Add a new task for the given topic partition, replacing any existing task for this partition. The task is added
    * only if the underlying queue allows processing for this change using [[mayProcess()]].
    */
  def maybeAddTask(metadata: StartChangeMetadata): Unit = {
    if (mayProcess(metadata)) {
      remove(metadata.topicIdPartition)
      tasks += newTask(metadata.topicIdPartition, metadata)
    }
  }

  /**
    * Remove task for the given topic partition.
    * @param metadata StopChangeMetadata causing this task to be removed
    */
  def maybeRemoveTask(metadata: StopChangeMetadata): Unit = {
    if (mayProcess(metadata))
      remove(metadata.topicIdPartition)
  }

  /**
    * Get the ordered set of tasks at the top of this queue. Tasks are currently sorted on every call to poll() though
    * we may be able to optimize this in the future. The number of tasks returned is limited by [[maxTasks]].
    * @return Ordered set of tasks if any; None otherwise
    */
  def poll(): Option[List[T]] = synchronized {
    val now = Instant.ofEpochMilli(time.hiResClockMs)
    val processingSpace = maxTasks - processing.size

    // NOTE: poll() will be called frequently for active brokers
    // so care must be taken not to create performance problems when the task set
    // becomes large
    if (processingSpace > 0) {
      val eligibleTasks = tasks
        // convert to a list to avoid rebuilding an immutable set
        // as the eligible task list will already be distinct
        .toList
        .filter { task =>
          !processing(task) &&
            task.pausedUntil.forall(now.isAfter) &&
            !partitionsInError.containsKey(task.topicIdPartition)
        }

      if (eligibleTasks.nonEmpty) {
        val sorted = sortTasks(eligibleTasks)
        if (sorted.nonEmpty) {
          val tasks = sorted.take(processingSpace)
          processing ++= tasks
          return Some(tasks)
        }
      }
    }

    None
  }

  /**
    * Re-enqueue a task for later processing. If the task has since been removed, `done` will cancel the provided task
    * and effectively drop it. `done` will also cleanup a task which has been canceled.
    */
  def done(task: T): Unit = synchronized {
    if (task.isErrorState)
      partitionsInError.put(task.topicIdPartition, time.milliseconds())

    if (!processing.contains(task))
      warn(s"done task $task not found in processing set")
    processing -= task
    if (!tasks.contains(task)) {
      debug(s"cancelling done task $task due to it no longer being in the task set")
      task.ctx.cancel()
    }
    if (task.ctx.isCancelled) {
      debug(s"removing done task $task from the task set")
      tasks -= task
    }
  }

  /**
    * Get the number of tasks in the queue.
    */
  def taskCount: Int = {
    tasks.size
  }

  /**
    * Provides a snapshot view of all tasks which are eligible for processing. Primarily useful for metrics or monitoring.
    */
  def withAllTasks[U](fn: Set[T] => U): U = {
    fn(tasks)
  }

  def close(): Unit = {
    ctx.cancel()
  }

  override def toString: String = {
    s"tasks=$tasks processing=$processing"
  }

  private def remove(topicIdPartition: TopicIdPartition): Unit = {
    findTask(topicIdPartition, tasks).foreach { task =>
      task.ctx.cancel()
      tasks -= task
      processing -= task
    }
  }

  private def findTask(topicIdPartition: TopicIdPartition, queue: ListSet[T]): Option[T] = {
    queue.find(_.topicIdPartition == topicIdPartition)
  }
}
