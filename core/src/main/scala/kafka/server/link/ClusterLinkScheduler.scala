/*
 * Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.function.BiConsumer

import kafka.utils.{KafkaScheduler, Logging}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.internals.KafkaFutureImpl

class ClusterLinkScheduler extends KafkaScheduler(threads = 1, threadNamePrefix = "cluster-link-scheduler") {

  /**
    * Schedules the callback to be run when the future has completed in an asynchronous manner.
    * The callback will be invoked from the scheduler's thread and should not block the thread.
    *
    * @param name name identifying the work
    * @param future the future to wait for completion
    * @param callback the callback to be invoked upon future completion
    */
  def scheduleWhenComplete[T](name: String, future: KafkaFuture[T], callback: () => Unit): Unit = {
    future.whenComplete(new KafkaFuture.BiConsumer[T, Throwable] {
      def accept(obj: T, throwable: Throwable): Unit = {
        scheduleOnce(name, callback)
      }
    })
  }

  /**
   * This overloaded method takes in the type CompletableFuture instead of a KafkaFuture.
   *
   * Schedules the callback to be run when the future has completed in an asynchronous manner.
   * The callback will be invoked from the scheduler's thread and should not block the thread.
   *
   * @param name name identifying the work
   * @param future the future to wait for completion
   * @param callback the callback to be invoked upon future completion
   */
  def scheduleWhenComplete[T](name: String, future: CompletableFuture[T], callback: () => Unit): Unit = {
    future.whenComplete(new BiConsumer[T, Throwable] {
      def accept(obj: T, throwable: Throwable): Unit = {
        scheduleOnce(name, callback)
      }
    })
  }
}

object ClusterLinkScheduler {
  /**
    * A task that's periodically performed on the cluster link's scheduler. The tasks may perform
    * work that takes an indefinite amount of time to complete, and therefore an asynchronous
    * context is required.
    *
    * A task's `run()` will be called from the scheduler when it's reschedule delay has elapsed. From
    * there, the task should start its work and submit any asynchronous callbacks through the
    * `PeriodicTask` (e.g. via `scheduleWhenComplete()`), where the callback should return a boolean
    * indicating whether the task has completed. If an exception is thrown, it's assumed that the task
    * has completed in a failed way, and will be scheduled to run again after its delay has elapsed.
    *
    * @param scheduler the scheduler to run on
    * @param name the name identifying the task's work
    * @param rescheduleDelayMs the delay between subsequent tasks
    */
  abstract class PeriodicTask(val scheduler: ClusterLinkScheduler, val name: String, val rescheduleDelayMs: Int) extends Logging {

    private var runOnceResult: Option[KafkaFutureImpl[Void]] = None
    @volatile private var isShuttingDown = false

    /**
      * Begins the task's execution.
      */
    def startup(): Unit = {
      scheduleRun()
    }

    /**
      * Indicates that the task's execution should complete.
      */
    def shutdown(): Unit = {
      isShuttingDown = true
    }

    /**
      * Used for testing.
      *
      * Runs the task exactly once, returning a future which can be used to wait on the completion
      * which will contain the task's exception, if thrown.
      *
      * The task must *not* have been started via `startup()`.
      *
      * @return a future for the task's result
      */
    def runOnce(): KafkaFuture[Void] = {
      val result = new KafkaFutureImpl[Void]
      runOnceResult = Some(result)
      scheduleOnce(() => run())
      result
    }

    /**
      * Starts running the task, returning whether the task has completed.
      *
      * @return `true` if the task has completed, otherwise `false` if there's outstanding work to be done
      */
    protected def run(): Boolean

    /**
      * Schedules the callback to be run. The callback should return `true` if the task has completed,
      * otherwise `false` if there's outstanding work to be done.
      *
      * @param callback the callback to be invoked
      */
    protected def scheduleOnce(callback: () => Boolean): Unit = {
      scheduler.scheduleOnce(name, wrap(callback))
    }

    /**
      * Schedules the callback to be run when the future has completed. The callback should return `true` if
      * the task has completed, otherwise `false` if there's outstanding work to be done.
      *
      * @param future the future to wait for completion
      * @param callback the callback to be invoked upon future completion
      */
    protected def scheduleWhenComplete[T](future: KafkaFuture[T], callback: () => Boolean): Unit = {
      scheduler.scheduleWhenComplete(name, future, wrap(callback))
    }

    /**
     * This overloaded method takes in the type CompletableFuture instead of a KafkaFuture.
     *
     * Schedules the callback to be run when the future has completed. The callback should return `true` if
     * the task has completed, otherwise `false` if there's outstanding work to be done.
     *
     * @param future the future to wait for completion
     * @param callback the callback to be invoked upon future completion
     */
    protected def scheduleWhenComplete[T](future: CompletableFuture[T], callback: () => Boolean): Unit = {
      scheduler.scheduleWhenComplete(name, future, wrap(callback))
    }

    /**
      * Invokes the callback. This should only be called from the scheduler's thread. If the callback
      * indicates the task has completed, then the task is scheduled to be rescheduled after the
      * delay has elapsed.
      *
      * @param callback the callback to be invoked
      */
    private def invoke(callback: () => Boolean): Unit = {
      if (!isShuttingDown) {
        try {
          if (callback()) {
            runOnceResult match {
              case Some(result) =>
                result.complete(null)
              case None =>
                scheduleRun()
            }
          }
        } catch {
          case e: Throwable =>
            warn(s"Encountered error while performing cluster link task '${name}': ${e}")
            runOnceResult match {
              case Some(result) =>
                result.completeExceptionally(e)
              case None =>
                scheduleRun()
            }
        }
      }
    }

    /**
      * Wraps the provided callback to be called from `invoke()`.
      *
      * @param callback the callback to wrap
      * @return the wrapped callback
      */
    private def wrap(callback: () => Boolean): () => Unit = () => invoke(callback)

    /**
      * Schedules the task to be restarted after the reschedule delay.
      */
    private def scheduleRun() = if (!isShuttingDown) {
      scheduler.schedule(name, wrap(() => run()), delay = rescheduleDelayMs, period = -1L, TimeUnit.MILLISECONDS)
    }
  }

}
