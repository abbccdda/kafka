package kafka.tier.tasks

import kafka.server.ReplicaManager
import kafka.tier.store.TierObjectStore
import kafka.tier.topic.TierTopicAppender
import kafka.utils.Logging
import org.apache.kafka.common.utils.Time

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class TierTaskWorkingSet[T <: TierTask[T]](taskQueue: TierTaskQueue[T],
                                           replicaManager: ReplicaManager,
                                           tierTopicAppender: TierTopicAppender,
                                           tierObjectStore: TierObjectStore,
                                           maxRetryBackoffMs: Int,
                                           time: Time)(implicit ec: ExecutionContext) extends Logging {

  private var workingSet: List[Future[T]] = List()

  // Fill the working set by polling the task queue
  private def fillWorkingSet(): Unit = {
    taskQueue.poll() match {
      case Some(newTasks) =>
        newTasks.foreach { newTask =>
          workingSet :+= newTask.transition(time, tierTopicAppender, tierObjectStore, replicaManager, Some(maxRetryBackoffMs))
        }

      case None =>
    }
  }

  // Drain the working set of all completed tasks and re-add them to the task queue
  private def drainFutures(): Unit = {
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

  /**
    * Initiate transitions for tasks and complete transitions if outstanding futures have completed. This method is
    * non-blocking.
    * @return List of outstanding futures
    */
  def doWork(): List[Future[T]] = {
    fillWorkingSet()
    drainFutures()
    workingSet
  }
}
