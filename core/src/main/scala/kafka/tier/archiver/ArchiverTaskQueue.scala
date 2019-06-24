/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.time.{Duration, Instant}
import java.util.concurrent.TimeUnit

import kafka.tier.TierMetadataManager
import kafka.tier.fetcher.CancellationContext
import kafka.tier.TopicIdPartition
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.concurrent.CancellationException

private sealed trait LeadershipChange extends UpdatableQueueEntry

private case class StartLeadership(topicIdPartition: TopicIdPartition, leaderEpoch: Int) extends LeadershipChange {
  override type Key = TopicIdPartition
  override def key: TopicIdPartition = topicIdPartition
}

private final case class StopLeadership(topicIdPartition: TopicIdPartition) extends LeadershipChange {
  override type Key = TopicIdPartition
  override def key: TopicIdPartition = topicIdPartition
}

trait TaskQueue[T] extends TierMetadataManager.ChangeListener with AutoCloseable {
  def poll(): T
  def poll(timeout: Long, timeUnit: TimeUnit): Option[T]
  def done(task: T)
  def taskCount(): Int
  def withAllTasks[U](fn: Set[T] => U): U
}

trait ArchiverTaskQueueTask {
  val topicIdPartition: TopicIdPartition
  val topicPartition: TopicPartition = topicIdPartition.topicPartition
  val ctx: CancellationContext
  def pausedUntil: Option[Instant]
}

/**
  * Leadership-aware task queue. Provides facilities for transforming topic partition leadership events
  * into tasks, and managing the lifecycle of each task during leadership changes and providing an interface
  * to delay re-scheduling of a task.
  *
  * @param ctx root cancellation context for all tasks being managed by this task queue.
  *
  * @param time time source used by the TaskQueue.
  *
  * @param lagFn used to calculate priority. Tasks are prioritized using less-than comparison except for
  *              0 or None tasks.
  * @param taskFactoryFn constructor function for tasks. CancellationContext is a sub-context of the root
  *                      CancellationContext. CancellationContext will be eventually canceled on processing of leadership
  *                      change events.
  * @tparam T Task type
  */
class ArchiverTaskQueue[T <: ArchiverTaskQueueTask](ctx: CancellationContext,
                                                    time: Time,
                                                    lagFn: T => Option[Long],
                                                    taskFactoryFn: (CancellationContext, TopicIdPartition, Int) => T) extends TaskQueue[T] with AutoCloseable with Logging {

  override def loggerName: String = classOf[ArchiverTaskQueue[T]].getName

  private val leadershipChangeQueue: UpdatableQueue[LeadershipChange] = new UpdatableQueue()
  @volatile private var tasks: Set[T] = Set()
  private var processing: Set[T] = Set()

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
    * Returns the set of tasks with non-zero, non-None lag by less-than comparison. Excludes all
    * tasks which are currently being processed.
    */
  private def sortedTasks(): Seq[T] = {
    val now = Instant.ofEpochMilli(time.milliseconds())
    tasks.diff(processing)
      .toList
      .filter {task => task.pausedUntil.forall(now.isAfter) }
      .map {task => (task, lagFn(task).getOrElse(0L)) }
      .filterNot { case (_, lag: Long) => lag == 0L }
      .sortBy { case (_, lag: Long) => lag}
      .map { case (task, _) => task }
  }

  /**
    * Scans the set of all tasks, canceling and removing the first task found with a matching
    * TopicIdPartition.
    * @param topicIdPartition
    */
  private def cancelAndRemoveAll(topicIdPartition: TopicIdPartition): Unit = {
    debug(s"canceling and removing all tasks matching $topicIdPartition")
    val startingTaskCount = tasks.size
    processing.find(_.topicIdPartition == topicIdPartition).foreach(_.ctx.cancel())
    processing = processing.filterNot(_.topicIdPartition == topicIdPartition)
    tasks.find(_.topicIdPartition == topicIdPartition).foreach(_.ctx.cancel())
    tasks = tasks.filterNot(_.topicIdPartition == topicIdPartition)
    debug(s"cancelled ${startingTaskCount - tasks.size} tasks")
  }

  /**
    * Process all entries from the leadership incoming queue. This function will be called normally during `poll()`
    * operations, but is useful for tests where it's necessary to make progress on the leadership change queue
    * without polling any tasks. Each call to processLeadershipQueue will attempt to drain and process the leadershipChangeQueue
    * before hitting the timeout.
    *
    * This method will block until either of the following conditions are met:
    *   1. The provided timeout elapses.
    *   2. This ArchiverTaskQueue has been closed via `close()`.

    * @param timeout time to block on polling the leadershipChangeQueue
    * @param unit unit of time
    * @return true if an entry was processed, false if the timeout expired.
    */
  def processLeadershipQueue(timeout: Long, unit: TimeUnit): Boolean = synchronized {
    val timeoutMillis = unit.toMillis(timeout)
    var remainingWaitDuration = Duration.ofMillis(timeoutMillis)
    var newEntryProcessed = false
    while (!ctx.isCancelled) {
      val timeBeforePoll = time.hiResClockMs() // measure start time so we know when to stop polling
      leadershipChangeQueue.pop(remainingWaitDuration.toMillis, TimeUnit.MILLISECONDS) match {
        case Some(startLeadership: StartLeadership) =>
          debug(s"handling StartLeadership for ${startLeadership.topicIdPartition} for leader epoch ${startLeadership.leaderEpoch}")
          val newTask = taskFactoryFn(ctx.subContext(), startLeadership.topicIdPartition, startLeadership.leaderEpoch)
          cancelAndRemoveAll(newTask.topicIdPartition)
          tasks += newTask
          newEntryProcessed = true

        case Some(stopLeadership: StopLeadership) =>
          debug(s"handling StopLeadership for ${stopLeadership.topicIdPartition}")
          cancelAndRemoveAll(stopLeadership.topicIdPartition)
          newEntryProcessed = true

        case None => // if `leadershipChangeQueue.pop()` returned None, we hit our timeout so there is no work left to do.
          return newEntryProcessed
      }
      // `leadershipChangeQueue.pop()` did not timeout, so we can assume that some work was done processing the `leadershipChangeQueue`.
      // In this case, we should update the `remainingWaitDuration` by subtracting out the time we spent waiting
      // for the item which was just processed.
      val timeWaited = Duration.ofMillis(time.hiResClockMs() - timeBeforePoll)
      remainingWaitDuration = remainingWaitDuration.minus(timeWaited)
    }
    newEntryProcessed
  }

  /**
    * Get the lowest non-zero non-None lag (according to `lagFn`) from the task queue. Will wait up to
    * the specified timeout for leadership change events to make new tasks available. Primarily useful for
    * testing as this will not periodically update lag and re-sort the task list.
    */
  override def poll(timeout: Long, unit: TimeUnit): Option[T] = synchronized {
    val sorted = sortedTasks()
    if (sorted.isEmpty) {
      if(processLeadershipQueue(timeout, unit)) {
        val task = sortedTasks().headOption
        task.foreach(processing += _)
        task
      } else {
        None
      }
    } else {
      val task = sorted.head
      processing += task
      Some(task)
    }
  }

  /**
    * Get the lowest non-zero non-None lag (according to `lagFn`) from the task queue, will periodically
    * re-sort the task list as well as poll for leadership changes until there is a task available.
    */
  override def poll(): T = synchronized {
    try {
      while(!ctx.isCancelled) { // This is a bit of a hack, ideally the log layer would notify the queue
                                // on segment rolls.
        poll(50, TimeUnit.MILLISECONDS) match {
          case Some(task) => return task
          case None =>
        }
      }
      close()
      throw new CancellationException("queue closed")
    } catch {
      case e: Exception =>
        close()
        throw e
    }
  }

  /**
    * Re-enqueue a task for later processing. If a leadership change event has caused this task to be removed,
    * `done` will cancel the provided task and effectively drop it. `done` will also cleanup a task which has been
    * canceled.
    */
  override def done(task: T): Unit = synchronized {
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
    * Get the number of tasks for which there have been leadership change events processed.
    */
  override def taskCount(): Int = {
    tasks.size
  }

  /**
    * Provides a snapshot view of all tasks which are eligible for processing.
    * Primarily Useful for metrics or monitoring.
    */
  override def withAllTasks[U](fn: Set[T] => U): U = {
    fn(tasks)
  }

  override def close(): Unit = {
    ctx.cancel()
    leadershipChangeQueue.close()
  }
}
