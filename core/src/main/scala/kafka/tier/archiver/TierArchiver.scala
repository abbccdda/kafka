/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.util.PriorityQueue
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, ExecutionException, Executors, TimeUnit}
import java.util.function.Predicate

import com.yammer.metrics.core.Gauge
import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.tier.archiver.TierArchiverState.{BeforeLeader, RetriableTierArchiverState, TierArchiverStateComparator}
import kafka.tier.exceptions.{TierArchiverFatalException, TierArchiverFencedException}
import kafka.tier.store.TierObjectStore
import kafka.tier.{TierMetadataManager, TierTopicManager}
import kafka.utils.{Logging, ShutdownableThread}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

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
class TierArchiver(config: TierArchiverConfig,
                   replicaManager: ReplicaManager,
                   tierMetadataManager: TierMetadataManager,
                   tierTopicManager: TierTopicManager,
                   tierObjectStore: TierObjectStore,
                   time: Time = Time.SYSTEM) extends ShutdownableThread(name = "tier-archiver") with KafkaMetricsGroup with Logging {
  private[tier] val blockingTaskExecutor = Executors.newScheduledThreadPool(config.numThreads)
  private[tier] val immigrationEmigrationQueue = new ConcurrentLinkedQueue[ImmigratingOrEmigratingTopicPartitions]()

  // consists of states between status transitions, and sorts by priority to facilitate scheduling.
  private[tier] val pausedStates = new PriorityQueue[TierArchiverState](11, TierArchiverStateComparator)
  // maps topic partitions to a tuple of state and pending status transition.
  private[tier] val stateTransitionsInProgress = mutable.Map.empty[TopicPartition, State]
  private val lock = new Object

  // set up metrics
  removeMetric("TotalLag")
  newGauge("TotalLag",
    new Gauge[Long] {
      def value(): Long = {
        val (paused, pending) = lock synchronized {
          (pausedStates.toArray(Array.empty[TierArchiverState]), stateTransitionsInProgress.values.map(_.archiverState).toArray)
        }

        (pending ++ paused).foldLeft(0L) { (acc, state) => acc + state.lag }
      }
    }
  )

  removeMetric("RetryStateCount")
  newGauge("RetryStateCount",
    new Gauge[Long] {
      def value(): Long = {
        val (paused, pending) = lock synchronized {
          (pausedStates.toArray(Array.empty[TierArchiverState]), stateTransitionsInProgress.values.map(_.archiverState).toArray)
        }
        (pending ++ paused).collect { case state: RetriableTierArchiverState if state.retryCount > 0 => state }.size
      }
    }
  )

  removeMetric("BytesPerSec")
  private val byteRate = newMeter("BytesPerSec", "bytes", TimeUnit.SECONDS)

  tierMetadataManager.addListener(new TierMetadataManager.ChangeListener {
    override def onBecomeLeader(topicPartition: TopicPartition, leaderEpoch: Int): Unit = handleImmigration(topicPartition, leaderEpoch)
    override def onBecomeFollower(topicPartition: TopicPartition): Unit = handleEmigration(topicPartition)
    override def onDelete(topicPartition: TopicPartition): Unit = handleEmigration(topicPartition)
  })

  def handleImmigration(topicPartition: TopicPartition, leaderEpoch: Int): Unit = {
    immigrationEmigrationQueue.add(ImmigratingTopicPartition(topicPartition, leaderEpoch))
  }

  def handleEmigration(topicPartition: TopicPartition): Unit = {
    immigrationEmigrationQueue.add(EmigratingTopicPartition(topicPartition))
  }

  /**
    * Immigration and emigration requests are queued together in `immigrationEmigrationQueue`, this allows us to
    * preserve ordering of immigration and emigration events to be processed by the archiver, while also not blocking
    * the immigration/emigration handler caller thread.
    *
    * `processImmigrationEmigrationQueue()` empties the `immigrationEmigrationQueue` on each invocation, adding
    * new TopicPartition immigrations to the map of `pausedStates`. An emigration event removes any matching status from
    * `pausedStates`. If there are any `stateTransitionsInProgress` for the emigrating TopicPartition, the status
    * transition is canceled and it is removed from the set of `stateTransitionsInProgress` too.
    *
    * @return true if any new immigration/emigration events were processed
    */
  private def processImmigrationEmigrationQueue(): Boolean = {
    var didWork = false
    while (!immigrationEmigrationQueue.isEmpty) {
      lock synchronized {
        immigrationEmigrationQueue.poll() match {
          case immigrationEvent: ImmigratingTopicPartition =>
            trace(s"Handling immigration for ${immigrationEvent.topicPartition} at epoch ${immigrationEvent.leaderEpoch}")
            val state = BeforeLeader(replicaManager, tierTopicManager, tierObjectStore,
              ArchiverStateMetrics(byteRate), immigrationEvent.topicPartition, immigrationEvent.leaderEpoch,
              blockingTaskExecutor, time, config)
            pausedStates.add(state)
            didWork = true
          case emigrationEvent: EmigratingTopicPartition =>
            trace(s"Handling emigration for ${emigrationEvent.topicPartition}")
            pausedStates.removeIf(new Predicate[TierArchiverState] {
              override def test(t: TierArchiverState): Boolean = {
                t.topicPartition == emigrationEvent.topicPartition
              }
            })
            stateTransitionsInProgress.remove(emigrationEvent.topicPartition).map(_.promise.cancel(true))
            didWork = true
        }
      }
    }
    didWork
  }

  /**
    * Iterates over the map of stateTransitionsInProgress, resolving completed futures and adding them back to
    * `pausedStates`
    * @return True if any states were successfully transitioned or fenced. False if no states were transitioned. Throws
    *         on fatal error.
    */
  private def pauseDoneStates(): Boolean = {
    var didWork = false
    trace(s"Checking completion of transitions in progress (size=${stateTransitionsInProgress.size})")
    for ((topicPartition, state) <- stateTransitionsInProgress) {
      if (state.promise.isDone) {
        lock synchronized {
          Try(state.promise.get) match {
            case Success(nextState: TierArchiverState) =>
              trace(s"Moving $topicPartition from $state to $nextState after successful completion")
              pausedStates.add(nextState)
              didWork = true
              stateTransitionsInProgress.remove(topicPartition)
            case Failure(e: ExecutionException) if e.getCause.isInstanceOf[TierArchiverFencedException] =>
              info(s"Removing fenced partition $topicPartition while in $state state", e)
              didWork = true
              stateTransitionsInProgress.remove(topicPartition)
            case Failure(e) =>
              stateTransitionsInProgress.remove(topicPartition)
              throw new TierArchiverFatalException(s"Unhandled exception for $topicPartition while in $state state", e)
          }
        }
      }
    }
    didWork
  }

  /**
    * If there is room on the executor, try to transition pending states.
    */
  def tryRunPendingStates(): Boolean = {
    var didWork = false
    while (stateTransitionsInProgress.size < config.numThreads && !pausedStates.isEmpty) {
      lock synchronized {
        val state = pausedStates.poll()
        if (state != null) {
          trace(s"Initiating transition for ${state.topicPartition} in state $state")
          stateTransitionsInProgress.put(state.topicPartition, State(state, state.nextState()))
          didWork = true
        }
      }
    }
    didWork
  }

  def processTransitions(): Boolean = {
    val processedImmigrationEmigration = processImmigrationEmigrationQueue()
    val pausedDoneStates = pauseDoneStates()
    val ranPendingStates = tryRunPendingStates()
    processedImmigrationEmigration || ranPendingStates || pausedDoneStates
  }

  override def doWork(): Unit = {
    if (tierTopicManager.isReady) {
      try {
        processTransitions()
      } catch {
        case _: InterruptedException =>
        case e: Throwable =>
          error("Unhandled exception caught in archiver doWork loop. Backing off.", e)
          Thread.sleep(config.mainLoopBackoffMs)
      }
      pause(config.updateIntervalMs, TimeUnit.MILLISECONDS)
    }
  }

  override def shutdown(): Unit = {
    blockingTaskExecutor.shutdown()
    blockingTaskExecutor.awaitTermination(30, TimeUnit.SECONDS)
    super.shutdown()
  }

  private[tier] case class State(archiverState: TierArchiverState, promise: CompletableFuture[TierArchiverState])
}

private[tier] sealed trait ImmigratingOrEmigratingTopicPartitions {
  val topicPartition: TopicPartition
}

private[tier] case class ImmigratingTopicPartition(topicPartition: TopicPartition, leaderEpoch: Integer)
  extends ImmigratingOrEmigratingTopicPartitions

private[tier] case class EmigratingTopicPartition(topicPartition: TopicPartition)
  extends ImmigratingOrEmigratingTopicPartitions

private[tier] case class ArchiverStateMetrics (byteRate: Meter)
