/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.archiver

import java.util.concurrent._
import java.util.function.Predicate

import com.yammer.metrics.core.Gauge
import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.server.ReplicaManager
import kafka.tier.archiver.TierArchiverState.{BeforeLeader, TierArchiverStateComparator}
import kafka.tier.exceptions.{TierArchiverFatalException, TierArchiverFencedException}
import kafka.tier.store.TierObjectStore
import kafka.tier.{TierMetadataManager, TierTopicManager}
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

case class TierArchiverConfig(updateIntervalMs: Int = 50,
                              enableArchiver: Boolean = true,
                              maxConcurrentUploads: Int = 10,
                              maxRetryBackoffMs: Int = 1000 * 60 * 5,
                              archivePartitionsWithGreatestLagFirst: Boolean = false)

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
                   time: Time = Time.SYSTEM) extends ShutdownableThread(name = "tier-archiver") with KafkaMetricsGroup {
  private[tier] val blockingTaskExecutor = Executors.newScheduledThreadPool(config.maxConcurrentUploads)
  private[tier] val immigrationEmigrationQueue = new ConcurrentLinkedQueue[ImmigratingOrEmigratingTopicPartitions]()

  // consists of states between status transitions, and sorts by priority to facilitate scheduling.
  private[tier] val pausedStates = new PriorityBlockingQueue[TierArchiverState](11, TierArchiverStateComparator)
  // maps topic partitions to a tuple of state and pending status transition.
  private[tier] val stateTransitionsInProgress = mutable.Map.empty[TopicPartition, (TierArchiverState, CompletableFuture[TierArchiverState])]
  private[this] val lock = new Object

  // set up metrics
  removeMetric("TotalLag")
  newGauge("TotalLag",
    new Gauge[Long] {
      def value(): Long = {
        lock.synchronized {
          val paused = pausedStates.toArray(Array.empty[TierArchiverState])
          val pending = stateTransitionsInProgress.map { case (_, (state, _)) => state }
          (pending ++ paused)
            .foldLeft(0L) { (acc, state) =>
              acc + state.lag
            }
        }
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
  def processImmigrationEmigrationQueue(): Boolean = {
    var didWork = false
    while (!immigrationEmigrationQueue.isEmpty) {
        immigrationEmigrationQueue.poll() match {
        case immigrationEvent: ImmigratingTopicPartition =>
          val state = BeforeLeader(replicaManager, tierTopicManager, tierObjectStore,
            ArchiverStateMetrics(byteRate), immigrationEvent.topicPartition, immigrationEvent.leaderEpoch,
            blockingTaskExecutor, config)
          pausedStates.put(state)
          didWork = true
        case emigrationEvent: EmigratingTopicPartition =>
          pausedStates.removeIf(new Predicate[TierArchiverState] {
            override def test(t: TierArchiverState): Boolean = {
              t.topicPartition == emigrationEvent.topicPartition
            }
          })
          stateTransitionsInProgress.remove(emigrationEvent.topicPartition).map { case (_, future) =>
            future.cancel(true)
          }
          didWork = true
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
  def pauseDoneStates(): Boolean = {
    var didWork = false
    for ((topicPartition, (_, future)) <- stateTransitionsInProgress) {
      if (future.isDone) {
        Try(future.get()) match {
          case Success(nextState: TierArchiverState) =>
            pausedStates.put(nextState)
            didWork = true
            stateTransitionsInProgress.remove(topicPartition)
          case Failure(ex: ExecutionException) if ex.getCause.isInstanceOf[TierArchiverFencedException] =>
            info(ex.getCause.getMessage)
            didWork = true
            stateTransitionsInProgress.remove(topicPartition)
          case Failure(ex: ExecutionException) if ex.getCause.isInstanceOf[TierArchiverFatalException] =>
            stateTransitionsInProgress.remove(topicPartition)
            throw ex.getCause
          case Failure(ex) =>
            stateTransitionsInProgress.remove(topicPartition)
            throw new TierArchiverFatalException("Unhandled exception", ex)
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
    while (stateTransitionsInProgress.size < config.maxConcurrentUploads && !pausedStates.isEmpty) {
      val state = pausedStates.poll()
      if (state != null) {
        stateTransitionsInProgress.put(state.topicPartition, (state, state.nextState()))
        didWork = true
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
    if (config.enableArchiver && tierTopicManager.isReady) {
      lock.synchronized {
        processTransitions()
      }
    }
    pause(config.updateIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def shutdown(): Unit = {
    blockingTaskExecutor.shutdown()
    blockingTaskExecutor.awaitTermination(30, TimeUnit.SECONDS)
    super.shutdown()
  }
}

private[tier] sealed trait ImmigratingOrEmigratingTopicPartitions {
  val topicPartition: TopicPartition
}

private[tier] case class ImmigratingTopicPartition(topicPartition: TopicPartition, leaderEpoch: Integer)
  extends ImmigratingOrEmigratingTopicPartitions

private[tier] case class EmigratingTopicPartition(topicPartition: TopicPartition)
  extends ImmigratingOrEmigratingTopicPartitions

private[tier] case class ArchiverStateMetrics (byteRate: Meter)
