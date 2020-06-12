/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.io.File
import java.util
import java.util.Optional
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.common.RecordValidationException
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{FetchMetadata => SFetchMetadata}
import kafka.server.HostedPartition.Online
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import kafka.server.link.ClusterLinkFactory
import kafka.tier.{TierReplicaManager, TierTimestampAndOffset}
import kafka.tier.fetcher.{TierFetchResult, TierFetcher, TierStateFetcher}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicPartition}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.{DescribeLogDirsResponseData, LeaderAndIsrResponseData}
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{Errors, MessageUtil}
import org.apache.kafka.common.record.FileRecords.{FileTimestampAndOffset, TimestampAndOffset}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica.{ClientMetadata, _}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{KafkaThread, Time}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, Set, mutable}
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.OptionConverters._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/*
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param hw high watermark of the local replica
 * @param readSize amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param preferredReadReplica the preferred read replica to be used for future fetches
 * @param exception Exception if error encountered while reading from the log
 */
sealed trait AbstractLogReadResult {
  def info: AbstractFetchDataInfo
  def highWatermark: Long
  def leaderLogStartOffset: Long
  def leaderLogEndOffset: Long
  def fetchTimeMs: Long
  def readSize: Int
  def lastStableOffset: Option[Long]
  def exception: Option[Throwable]
  def preferredReadReplica: Option[Int]
  def followerNeedsHwUpdate: Boolean

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/*
 * Result metadata of a local log read operation
 * @param isReadAllowed read operation is not allowed for this partition as fetch request
 *                      maxBytes was already satisfied by previous partitions when
 *                      hardMaxBytesLimitNote in TRUE
 */
case class LogReadResult(info: FetchDataInfo,
                         highWatermark: Long,
                         leaderLogStartOffset: Long,
                         leaderLogEndOffset: Long,
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         readSize: Int,
                         lastStableOffset: Option[Long],
                         isReadAllowed: Boolean,
                         preferredReadReplica: Option[Int] = None,
                         followerNeedsHwUpdate: Boolean = false,
                         exception: Option[Throwable] = None) extends AbstractLogReadResult {

  def withEmptyFetchInfo: LogReadResult =
    copy(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY))

  override def toString =
    s"Fetch Data: [$info], HW: [$highWatermark], leaderLogStartOffset: [$leaderLogStartOffset], leaderLogEndOffset: [$leaderLogEndOffset], " +
    s"followerLogStartOffset: [$followerLogStartOffset], fetchTimeMs: [$fetchTimeMs], readSize: [$readSize], lastStableOffset: [$lastStableOffset], isReadAllowed: [$isReadAllowed] error: [$error]"
}

case class TierLogReadResult(info: TierFetchDataInfo,
                             highWatermark: Long,
                             leaderLogStartOffset: Long,
                             leaderLogEndOffset: Long,
                             followerLogStartOffset: Long,
                             fetchTimeMs: Long,
                             readSize: Int,
                             lastStableOffset: Option[Long],
                             preferredReadReplica: Option[Int] = None,
                             exception: Option[Throwable] = None) extends AbstractLogReadResult {
  /**
    * An attempt to fetch tiered data by the follower will raise an [[OffsetTieredException]]. See [[ReplicaManager.updateFollowerFetchState]].
    */
  val followerNeedsHwUpdate = false

  override def toString =
    s"Tiered Fetch Data: [$info], HW: [$highWatermark], leaderLogStartOffset: [$leaderLogStartOffset], leaderLogEndOffset: [$leaderLogEndOffset], " +
      s"followerLogStartOffset: [$followerLogStartOffset], fetchTimeMs: [$fetchTimeMs], readSize: [$readSize], lastStableOffset: [$lastStableOffset], error: [$error]"

  /**
    * Convert this TierLogReadResult into a LogReadResult to be returned to the client.
    * This requires TierFetchResult's from a completed tier fetch.
    */
  def intoLogReadResult(tierFetchResult: TierFetchResult, isReadAllowed: Boolean): LogReadResult = {
    var newInfo: FetchDataInfo = FetchDataInfo(
      LogOffsetMetadata.UnknownOffsetMetadata,
      tierFetchResult.records,
      firstEntryIncomplete = false,
      this.info.abortedTransactions)

    if (!tierFetchResult.abortedTxns.isEmpty) {
      val abortedTransactionList = tierFetchResult.abortedTxns.asScala.map(_.asAbortedTransaction).toList
      newInfo = newInfo.addAbortedTransactions(abortedTransactionList)
    }

    val exceptionOpt = this.exception.orElse(Option(tierFetchResult.exception))
    LogReadResult(
      info = newInfo,
      highWatermark = this.highWatermark,
      leaderLogStartOffset = this.leaderLogStartOffset,
      leaderLogEndOffset = this.leaderLogEndOffset,
      followerLogStartOffset = this.followerLogStartOffset,
      fetchTimeMs = this.fetchTimeMs,
      readSize = this.readSize,
      isReadAllowed = isReadAllowed,
      lastStableOffset = this.lastStableOffset,
      preferredReadReplica = this.preferredReadReplica,
      followerNeedsHwUpdate = this.followerNeedsHwUpdate,
      exception = exceptionOpt
    )
  }
}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,
                              logStartOffset: Long,
                              records: Records,
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[AbortedTransaction]],
                              preferredReadReplica: Option[Int],
                              isReassignmentFetch: Boolean)


/**
 * Trait to represent the state of hosted partitions. We create a concrete (active) Partition
 * instance when the broker receives a LeaderAndIsr request from the controller indicating
 * that it should be either a leader or follower of a partition.
 */
sealed trait HostedPartition
object HostedPartition {
  /**
   * This broker does not have any state for this partition locally.
   */
  final object None extends HostedPartition

  /**
   * This broker hosts the partition and it is online.
   */
  final case class Online(partition: Partition) extends HostedPartition

  /**
   * This broker hosts the partition, but it is in an offline log directory.
   */
  final object Offline extends HostedPartition
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L

  private[server] def tierFetchPartitionMaxBytesOverride(tierMaxPartitionFetchBytesOverride: Int,
                                                         numPartitionsInFetch: Int,
                                                         fetchMaxBytes: Int,
                                                         maxPartitionFetchBytes: Int,
                                                         localBytesRead: Int): Int = {
    if (tierMaxPartitionFetchBytesOverride > 0) {
      val maxBytes = Math.min(numPartitionsInFetch * maxPartitionFetchBytes.toLong, fetchMaxBytes).toInt
      val maxTierFetchBytes = Math.max(maxBytes - localBytesRead, 0)
      Math.min(maxTierFetchBytes, tierMaxPartitionFetchBytesOverride)
    } else {
      0
    }
  }
}

class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     val zkClient: KafkaZkClient,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     quotaManagers: QuotaManagers,
                     val brokerTopicStats: BrokerTopicStats,
                     val metadataCache: MetadataCache,
                     logDirFailureChannel: LogDirFailureChannel,
                     val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                     val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                     val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                     val delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader],
                     val delayedListOffsetsPurgatory: DelayedOperationPurgatory[DelayedListOffsets],
                     val tierReplicaComponents: TierReplicaComponents,
                     val clusterLinkManager: Option[ClusterLinkFactory.LinkManager],
                     threadNamePrefix: Option[String]) extends Logging with KafkaMetricsGroup {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           zkClient: KafkaZkClient,
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManagers: QuotaManagers,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: MetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           tierReplicaComponents: TierReplicaComponents,
           clusterLinkManager: Option[ClusterLinkFactory.LinkManager],
           threadNamePrefix: Option[String] = None) = {
    this(config, metrics, time, zkClient, scheduler, logManager, isShuttingDown,
      quotaManagers, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedElectLeader](
        purgatoryName = "ElectLeader", brokerId = config.brokerId),
      DelayedOperationPurgatory[DelayedListOffsets](
        purgatoryName = "ListOffsets", brokerId = config.brokerId),
      tierReplicaComponents,
      clusterLinkManager,
      threadNamePrefix)
  }

  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  private val localBrokerId = config.brokerId
  private[server] val allPartitions = new Pool[TopicPartition, HostedPartition](
    valueFactory = Some(tp => HostedPartition.Online(Partition(tp, time, this)))
  )
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)

  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())

  private var logDirFailureHandler: LogDirFailureHandler = null

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  private[kafka] val executor = Executors.newCachedThreadPool(new ThreadFactory {
    val threadNum = new AtomicInteger(-1)
    override def newThread(r: Runnable): Thread = {
      val newThreadNum = threadNum.incrementAndGet()
      KafkaThread.nonDaemon(s"ReplicaManager-$newThreadNum", r)
    }
  })

  // Visible for testing
  private[server] val replicaSelectorOpt: Option[ReplicaSelector] = createReplicaSelector()

  newGauge("LeaderCount", () => leaderPartitionsIterator.size)
  // Visible for testing
  private[kafka] val partitionCount = newGauge("PartitionCount", () => allPartitions.size)
  /**
   * ThrottledReplicasRate track the number of times any given replica was throttled as part of a fetch request.
   * Leader - the fetch response returned no data (throttled) for that replica
   * Follower - the fetch request did not include that replica (throttled)
   */
  private[kafka] val throttledLeaderReplicasRate = newMeter("ThrottledLeaderReplicasPerSec", "replicationThrottle", TimeUnit.SECONDS)
  private[kafka] val throttledFollowerReplicasRate = newMeter("ThrottledFollowerReplicasPerSec", "replicationThrottle", TimeUnit.SECONDS)
  private[kafka] val throttledClusterLinkReplicasRate = newMeter("ThrottledClusterLinkReplicasPerSec", "replicationThrottle", TimeUnit.SECONDS)
  newGauge("OfflineReplicaCount", () => offlinePartitionCount)
  newGauge("UnderReplicatedPartitions", () => underReplicatedPartitionCount)
  newGauge("UnderMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isUnderMinIsr))
  newGauge("AtMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isAtMinIsr))
  newGauge("ReassigningPartitions", () => leaderPartitionsIterator.count(_.isReassigning))
  newGauge("NotCaughtUpPartitionCount", () => leaderPartitionsIterator.count(_.isNotCaughtUp))
  newGauge("MaxLastStableOffsetLag", () => maxLastStableOffsetLag)

  def maxLastStableOffsetLag: Long = {
    var maxLsoLag = 0L
    leaderPartitionsIterator.foreach { partition =>
      val lsoLag = partition.lastStableOffsetLag
      if (lsoLag > maxLsoLag)
        maxLsoLag = lsoLag
    }
    maxLsoLag
  }

  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)

  val isrExpandRate: Meter = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate: Meter = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate: Meter = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def startHighWatermarkCheckPointThread(): Unit = {
    if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks _, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  def recordIsrChange(topicPartition: TopicPartition): Unit = {
    isrChangeSet synchronized {
      isrChangeSet += topicPartition
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  def maybePropagateIsrChanges(): Unit = {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now ||
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
        zkClient.propagateIsrChanges(isrChangeSet)
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }

  // When ReplicaAlterDirThread finishes replacing a current replica with a future replica, it will
  // remove the partition from the partition state map. But it will not close itself even if the
  // partition state map is empty. Thus we need to call shutdownIdleReplicaAlterDirThread() periodically
  // to shutdown idle ReplicaAlterDirThread
  def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def getLog(topicPartition: TopicPartition): Option[AbstractLog] = logManager.getLog(topicPartition)

  def hasDelayedElectionOperations: Boolean = delayedElectLeaderPurgatory.numDelayed != 0

  def tryCompleteElection(key: DelayedOperationKey): Unit = {
    val completed = delayedElectLeaderPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d ElectLeader.".format(key.keyLabel, completed))
  }

  def startup(): Unit = {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", shutdownIdleReplicaAlterLogDirsThread _, period = 10000L, unit = TimeUnit.MILLISECONDS)

    // If inter-broker protocol (IBP) < 1.0, the controller will send LeaderAndIsrRequest V0 which does not include isNew field.
    // In this case, the broker receiving the request cannot determine whether it is safe to create a partition if a log directory has failed.
    // Thus, we choose to halt the broker on any log diretory failure if IBP < 1.0
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasOnlinePartition = allPartitions.values.exists {
      case HostedPartition.Online(partition) => topic == partition.topic
      case HostedPartition.None | HostedPartition.Offline => false
    }
    if (!topicHasOnlinePartition)
      brokerTopicStats.removeMetrics(topic)
  }

  def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean): Unit  = {
    if (deletePartition) {
      getPartition(topicPartition) match {
        case hostedPartition @ HostedPartition.Online(removedPartition) =>
          if (allPartitions.remove(topicPartition, hostedPartition)) {
            maybeRemoveTopicMetrics(topicPartition.topic)
            // this will delete the local log. This call may throw exception if the log is on offline directory
            removedPartition.delete()
          }

        case _ =>
      }

      // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
      // This could happen when topic is being deleted while broker is down and recovers.
      Partition.deleteLog(topicPartition, logManager, tierReplicaComponents.replicaManagerOpt)
    }

    // If we were the leader, we may have some operations still waiting for completion.
    // We force completion to prevent them from timing out.
    completeDelayedRequests(topicPartition)

    stateChangeLogger.trace(s"Finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
  }

  // package private for testing
  private[server] def deleteStrayLogs(): Unit = {
    val allReplicas = nonOfflinePartitionsIterator.map(_.topicPartition).toSet
    logManager.allLogs.map(_.topicPartition).filterNot { topicPartition =>
      allReplicas.contains(topicPartition)
    }.foreach { strayPartition =>
      if (config.strayPartitionDeletionEnabled) {
        warn(s"Deleting stray partition $strayPartition")
        Partition.deleteLog(strayPartition, logManager, tierReplicaComponents.replicaManagerOpt)
      } else {
        warn(s"Found stray partition $strayPartition")
      }
    }
  }

  private def completeDelayedRequests(topicPartition: TopicPartition): Unit = {
    val topicPartitionOperationKey = TopicPartitionOperationKey(topicPartition)
    delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
    // Used for tiered storage
    delayedListOffsetsPurgatory.checkAndComplete(topicPartitionOperationKey)
  }

  def stopReplicas(correlationId: Int,
                   controllerId: Int,
                   controllerEpoch: Int,
                   brokerEpoch: Long,
                   partitionStates: Map[TopicPartition, StopReplicaPartitionState]
                  ): (mutable.Map[TopicPartition, Errors], Errors) = {
    replicaStateChangeLock synchronized {
      stateChangeLogger.info(s"Handling StopReplica request correlationId $correlationId from controller " +
        s"$controllerId for ${partitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        partitionStates.foreach { case (topicPartition, partitionState) =>
          stateChangeLogger.trace(s"Received StopReplica request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch $controllerEpoch for partition $topicPartition")
        }

      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]
      if (controllerEpoch < this.controllerEpoch) {
        stateChangeLogger.warn(s"Ignoring StopReplica request from " +
          s"controller $controllerId with correlation id $correlationId " +
          s"since its controller epoch $controllerEpoch is old. " +
          s"Latest known controller epoch is ${this.controllerEpoch}")
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        this.controllerEpoch = controllerEpoch

        val stoppedPartitions = mutable.Map.empty[TopicPartition, StopReplicaPartitionState]
        partitionStates.foreach { case (topicPartition, partitionState) =>
          val deletePartition = partitionState.deletePartition

          getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)

            case HostedPartition.Online(partition) =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              // When a topic is deleted, the leader epoch is not incremented. To circumvent this,
              // a sentinel value (EpochDuringDelete) overwriting any previous epoch is used.
              // When an older version of the StopReplica request which does not contain the leader
              // epoch, a sentinel value (NoEpoch) is used and bypass the epoch validation.
              if (requestLeaderEpoch == LeaderAndIsr.EpochDuringDelete ||
                  requestLeaderEpoch == LeaderAndIsr.NoEpoch ||
                  requestLeaderEpoch > currentLeaderEpoch) {
                stoppedPartitions += topicPartition -> partitionState
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                stateChangeLogger.warn(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              } else {
                stateChangeLogger.info(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              }

            case HostedPartition.None =>
              // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
              // This could happen when topic is being deleted while broker is down and recovers.
              stoppedPartitions += topicPartition -> partitionState
          }
        }

        // First stop fetchers for all partitions, then stop the corresponding replicas
        val partitions = stoppedPartitions.keySet
        replicaFetcherManager.removeFetcherForPartitions(partitions)
        clusterLinkManager.foreach(_.removePartitionsAndMetadata(partitions))
        replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)

        stoppedPartitions.foreach { case (topicPartition, partitionState) =>
          val deletePartition = partitionState.deletePartition
          try {
            stopReplica(topicPartition, deletePartition)
            responseMap.put(topicPartition, Errors.NONE)
          } catch {
            case e: KafkaStorageException =>
              stateChangeLogger.error(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory", e)
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
          }
        }

        (responseMap, Errors.NONE)
      }
    }
  }

  def getPartition(topicPartition: TopicPartition): HostedPartition = {
    Option(allPartitions.get(topicPartition)).getOrElse(HostedPartition.None)
  }

  def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    getPartition(topicPartition) match {
      case Online(partition) => partition.isAddingReplica(replicaId)
      case _ => false
    }
  }

  // Visible for testing
  def createPartition(topicPartition: TopicPartition): Partition = {
    val partition = Partition(topicPartition, time, this)
    allPartitions.put(topicPartition, HostedPartition.Online(partition))
    partition
  }

  def nonOfflinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) => Some(partition)
      case HostedPartition.None | HostedPartition.Offline => None
    }
  }

  // An iterator over all non offline partitions. This is a weakly consistent iterator; a partition made offline after
  // the iterator has been constructed could still be returned by this iterator.
  private def nonOfflinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.iterator.flatMap {
      case HostedPartition.Online(partition) => Some(partition)
      case HostedPartition.None | HostedPartition.Offline => None
    }
  }

  private def offlinePartitionCount: Int = {
    allPartitions.values.iterator.count(_ == HostedPartition.Offline)
  }

  def getPartitionOrException(topicPartition: TopicPartition, expectLeader: Boolean): Partition = {
    getPartitionOrError(topicPartition, expectLeader) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for $topicPartition")

      case Right(partition) => partition
    }
  }

  def getPartitionOrError(topicPartition: TopicPartition, expectLeader: Boolean): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        Right(partition)

      case HostedPartition.Offline =>
        Left(Errors.KAFKA_STORAGE_ERROR)

      case HostedPartition.None if metadataCache.contains(topicPartition) =>
        if (expectLeader) {
          // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER which
          // forces clients to refresh metadata to find the new location. This can happen, for example,
          // during a partition reassignment if a produce request from the client is sent to a broker after
          // the local replica has been deleted.
          Left(Errors.NOT_LEADER_FOR_PARTITION)
        } else {
          Left(Errors.REPLICA_NOT_AVAILABLE)
        }

      case HostedPartition.None =>
        Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }
  }

  def localLogOrException(topicPartition: TopicPartition): AbstractLog = {
    getPartitionOrException(topicPartition, expectLeader = false).localLogOrException
  }

  def futureLocalLogOrException(topicPartition: TopicPartition): AbstractLog = {
    getPartitionOrException(topicPartition, expectLeader = false).futureLocalLogOrException
  }

  def futureLogExists(topicPartition: TopicPartition): Boolean = {
    getPartitionOrException(topicPartition, expectLeader = false).futureLog.isDefined
  }

  def localLog(topicPartition: TopicPartition): Option[AbstractLog] = {
    nonOfflinePartition(topicPartition).flatMap(_.log)
  }

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    localLog(topicPartition).map(_.parentDir)
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   */
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    origin: AppendOrigin,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                    delayedProduceLock: Option[Lock] = None,
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => (),
                    bufferSupplier: BufferSupplier = BufferSupplier.NO_CACHING): Unit = {
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
        origin, entriesPerPartition, requiredAcks, bufferSupplier)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset
                  new PartitionResponse(result.error, result.info.firstOffset.getOrElse(-1), result.info.logAppendTime,
                    result.info.logStartOffset, result.info.recordErrors.asJava, result.info.errorMessage)) // response status
      }

      recordConversionStatsCallback(localProduceResults.map { case (k, v) => k -> v.info.recordConversionStats })

      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // create delayed produce operation
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        val producerRequestKeys = entriesPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        val produceResponseStatus = produceStatus.map { case (k, status) => k -> status.responseStatus }
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset.getOrElse(-1), RecordBatch.NO_TIMESTAMP, LogAppendInfo.UnknownLogAppendInfo.logStartOffset)
      }
      responseCallback(responseStatus)
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation on internal topics
      if (Topic.isInternal(topicPartition.topic)) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition, expectLeader = true)
          val logDeleteResult = partition.deleteRecordsOnLeader(requestedOffset)
          (topicPartition, logDeleteResult)
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException |
                   _: LeaderNotAvailableException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /**
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          /* If the topic name is exceptionally long, we can't support altering the log directory.
           * See KAFKA-4893 for details.
           * TODO: fix this by implementing topic IDs. */
          if (Log.logFutureDirName(topicPartition).size > 255)
            throw new InvalidTopicException("The topic name is too long.")
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              // Stop current replica movement if the destinationDir is different from the existing destination log directory
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                partition.removeFutureLocalReplica()
              }
            case HostedPartition.Offline =>
              throw new KafkaStorageException(s"Partition $topicPartition is offline")

            case HostedPartition.None => // Do nothing
          }

          // If the log for this partition has not been created yet:
          // 1) Record the destination log directory in the memory so that the partition will be created in this log directory
          //    when broker receives LeaderAndIsrRequest for this partition later.
          // 2) Respond with ReplicaNotAvailableException for this partition in the AlterReplicaLogDirsResponse
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)

          // throw ReplicaNotAvailableException if replica does not exist for the given partition
          val partition = getPartitionOrException(topicPartition, expectLeader = false)
          partition.localLogOrException

          // If the destinationLDir is different from the current log directory of the replica:
          // - If there is no offline log directory, create the future log in the destinationDir (if it does not exist) and
          //   start ReplicaAlterDirThread to move data of this partition from the current log to the future log
          // - Otherwise, return KafkaStorageException. We do not create the future log while there is offline log directory
          //   so that we can avoid creating future log for the same partition in multiple log directories.
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)

            val initialFetchState = InitialFetchState(BrokerEndPoint(config.brokerId, "localhost", -1),
              partition.getLeaderEpoch, futureLog.highWatermark)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> initialFetchState))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: InvalidTopicException |
                  _: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            warn("Unable to alter log dirs for %s".format(topicPartition), e)
            (topicPartition, Errors.forException(e))
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.parentDir)

    config.logDirs.toSet.map { logDir: String =>
      val absolutePath = new File(logDir).getAbsolutePath
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val topicInfos = logs.groupBy(_.topicPartition.topic).map{case (topic, logs) =>
              new DescribeLogDirsResponseData.DescribeLogDirsTopic().setName(topic).setPartitions(
                logs.filter { log =>
                  partitions.contains(log.topicPartition)
                }.map { log =>
                  new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                    .setPartitionSize(log.size)
                    .setPartitionIndex(log.topicPartition.partition)
                    .setOffsetLag(getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture))
                    .setIsFutureKey(log.isFuture)
                }.toList.asJava)
            }.toList.asJava

            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code).setTopics(topicInfos)
          case None =>
            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code)
        }

      } catch {
        case e: KafkaStorageException =>
          warn("Unable to describe replica dirs for %s".format(absolutePath), e)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.forException(t).code)
      }
    }.toList
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    localLog(topicPartition) match {
      case Some(log) =>
        if (isFuture)
          log.logEndOffset - logEndOffset
        else
          math.max(log.highWatermark - logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if the replica is not created or is offline
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit): Unit = {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsPartitionResult()
            .setLowWatermark(result.lowWatermark)
            .setErrorCode(result.error.code)
            .setPartitionIndex(topicPartition.partition)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],
                               requiredAcks: Short,
                               bufferSupplier: BufferSupplier): Map[TopicPartition, LogAppendResult] = {
    val traceEnabled = isTraceEnabled
    def processFailedRecord(topicPartition: TopicPartition, t: Throwable) = {
      val logStartOffset = getPartition(topicPartition) match {
        case HostedPartition.Online(partition) => partition.logStartOffset
        case HostedPartition.None | HostedPartition.Offline => -1L
      }
      brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      error(s"Error processing append operation on partition $topicPartition", t)

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition, expectLeader = true)
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks, bufferSupplier)
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          if (traceEnabled)
            trace(s"${records.sizeInBytes} written to log $topicPartition beginning at offset " +
              s"${info.firstOffset.getOrElse(-1)} and ending at offset ${info.lastOffset}")

          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException |
                   _: LeaderNotAvailableException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(
              logStartOffset, recordErrors, rve.invalidException.getMessage), Some(rve.invalidException)))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicPartition, t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset), Some(t)))
        }
      }
    }
  }

  def fetchTierOffset(topicPartition: TopicPartition,
                      timestamp: Long,
                      currentLeaderEpoch: Option[Integer],
                      fetchOnlyFromLeader: Boolean): Option[Long] = {
    val partition = getPartitionOrException(topicPartition, expectLeader = fetchOnlyFromLeader)
    partition.fetchTierOffsetForType(timestamp, currentLeaderEpoch, fetchOnlyFromLeader)
  }

  /**
   * Fetch offsets for timestamps.
   * If any offsets require fetches from the tiered section of the log, stages a DelayedListOffset request
   * in purgatory.
   * @param lookupMetadata Map of TopicPartition -> (Optional[LeaderEpoch], Timestamp)
   * @param isolationLevel Optional isolation level
   * @param fetchOnlyFromLeader fetchOnlyFromLeader boolean
   * @param responseCallback callback to call with fetched Map of TopicPartition -> FileTimestampAndOffset
   *                         when request is completed or has hit a timeout
   * @param delayMs number of ms to allow the fetch to complete if the request is staged in purgatory
   */
  def fetchOffsetsForTimestamps(lookupMetadata: Map[TopicPartition, (Optional[Integer], Long)],
                                isolationLevel: Option[IsolationLevel],
                                fetchOnlyFromLeader: Boolean,
                                responseCallback: Map[TopicPartition, Option[FileTimestampAndOffset]] => Unit,
                                delayMs: Long): Unit = {
    val tierLists = new util.HashMap[TopicPartition, TierTimestampAndOffset]()
    val localLists = new util.HashMap[TopicPartition, Option[FileTimestampAndOffset]]()
    lookupMetadata.map { case (topicPartition, (leaderAndEpoch, timestamp)) =>
      try {
        fetchOffsetForTimestamp(topicPartition, timestamp, isolationLevel,
          leaderAndEpoch, fetchOnlyFromLeader)
        match {
          case Some(timestampAndOffset: TierTimestampAndOffset) =>
            tierLists.put(topicPartition, timestampAndOffset)
          case Some(timestampAndOffset: FileTimestampAndOffset) =>
            localLists.put(topicPartition, Some(timestampAndOffset))
          case Some(timestampAndOffset) =>
            throw new Exception("Unexpected implementation of TimestampAndOffset " + timestampAndOffset.getClass)
          case None =>
            localLists.put(topicPartition, None)
        }
      } catch {
        case e: Exception =>
          localLists.put(topicPartition, Some(new FileTimestampAndOffset(timestamp, leaderAndEpoch, e)))
      }
    }

    if (tierLists.isEmpty) {
      responseCallback(localLists.asScala)
    } else {
      val completionCallback = (delayedOperationKey: DelayedOperationKey) =>
        delayedListOffsetsPurgatory.checkAndComplete(delayedOperationKey): Unit
      val pending = tierReplicaComponents.fetcherOpt.get.fetchOffsetForTimestamp(tierLists, completionCallback.asJava)
      val delayedListOffsets = new DelayedListOffsets(delayMs, fetchOnlyFromLeader, localLists, pending, this, responseCallback)
      val delayedOperationKeys = pending.delayedOperationKeys.asScala ++ lookupMetadata.keys.map(tp => TopicPartitionOperationKey(tp.topic(), tp.partition()))
      delayedListOffsetsPurgatory.tryCompleteElseWatch(delayedListOffsets, delayedOperationKeys)
    }
  }

  private def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                                      timestamp: Long,
                                      isolationLevel: Option[IsolationLevel],
                                      currentLeaderEpoch: Optional[Integer],
                                      fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = {
    val partition = getPartitionOrException(topicPartition, expectLeader = fetchOnlyFromLeader)
    if (timestamp == ListOffsetRequest.LOCAL_START_OFFSET || timestamp == ListOffsetRequest.LOCAL_END_OFFSET) {
      val offsetOpt = fetchTierOffset(topicPartition, timestamp, currentLeaderEpoch.asScala, fetchOnlyFromLeader = true)
      offsetOpt.map(offset => new FileTimestampAndOffset(timestamp, offset, Optional.empty(): Optional[Integer]))
    } else {
      partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader)
    }
  }

  def legacyFetchOffsetsForTimestamp(topicPartition: TopicPartition,
                                     timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = {
    val partition = getPartitionOrException(topicPartition, expectLeader = fetchOnlyFromLeader)
    partition.legacyFetchOffsetsForTimestamp(timestamp, maxNumOffsets, isFromConsumer, fetchOnlyFromLeader)
  }

  def mergeIntoFetchPartitionStatusList(fetchInfos: Seq[(TopicPartition, PartitionData)],
                                        logReadResultMap: mutable.HashMap[TopicPartition, _ <: AbstractLogReadResult]): Seq[(TopicPartition, FetchPartitionStatus)] = {
    fetchInfos.flatMap { case (topicPartition: TopicPartition, partitionData: PartitionData) =>
      logReadResultMap.get(topicPartition).map {
        case localResult: LogReadResult => topicPartition -> FetchPartitionStatus(localResult.info.fetchOffsetMetadata, partitionData)
        case _: TierLogReadResult => topicPartition -> FetchPartitionStatus(LogOffsetMetadata.UnknownOffsetMetadata, partitionData)
      }
    }
  }

  /**
   * Fetch messages from a replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied.
   * Consumers may fetch from any replica, but followers can only fetch from the leader.
   */
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchMaxBytes: Int,
                    hardMaxBytesLimit: Boolean,
                    fetchInfos: Seq[(TopicPartition, PartitionData)],
                    quota: ReplicaQuota,
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,
                    isolationLevel: IsolationLevel,
                    clientMetadata: Option[ClientMetadata]): Unit = {
    val isFromFollower = Request.isValidBrokerId(replicaId)
    val isFromConsumer = !(isFromFollower || replicaId == Request.FutureLocalReplicaId)
    val fetchIsolation = if (!isFromConsumer)
      FetchLogEnd
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      FetchTxnCommitted
    else
      FetchHighWatermark

    // Restrict fetching to leader if request is from follower or from a client with older version (no ClientMetadata)
    val fetchOnlyFromLeader = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)

    def readFromLog(): Seq[(TopicPartition, AbstractLogReadResult)] = {
      val result = readFromLocalLog(
        replicaId = replicaId,
        fetchOnlyFromLeader = fetchOnlyFromLeader,
        fetchIsolation = fetchIsolation,
        fetchMaxBytes = fetchMaxBytes,
        hardMaxBytesLimit = hardMaxBytesLimit,
        readPartitionInfo = fetchInfos,
        quota = quota,
        clientMetadata = clientMetadata)
      if (isFromFollower) updateFollowerFetchState(replicaId, result)
      else result
    }
    val logReadResults = readFromLog()

    // check if this fetch request can be satisfied right away
    var localReadableBytes: Int = 0
    var errorReadingData = false

    def updateBrokerTopicStats(topic: String): Unit = {
      brokerTopicStats.topicStats(topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()
    }

    val localLogReadResultMap = new mutable.HashMap[TopicPartition, LogReadResult]
    val tierLogReadResultMap = new mutable.HashMap[TopicPartition, TierLogReadResult]
    var anyPartitionsNeedHwUpdate = false

    logReadResults.foreach {
      case (topicPartition: TopicPartition, logReadResult: LogReadResult) =>
        updateBrokerTopicStats(topicPartition.topic)

        if (logReadResult.error != Errors.NONE)
          errorReadingData = true
        localReadableBytes += logReadResult.info.records.sizeInBytes
        localLogReadResultMap.put(topicPartition, logReadResult)
        if (isFromFollower && logReadResult.followerNeedsHwUpdate) {
          anyPartitionsNeedHwUpdate = true
        }

      case (topicPartition: TopicPartition, tierLogReadResult: TierLogReadResult) =>
        updateBrokerTopicStats(topicPartition.topic)

        if (tierLogReadResult.error != Errors.NONE)
          errorReadingData = true
        tierLogReadResultMap.put(topicPartition, tierLogReadResult)
    }

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) fetch request does not require any tiered data and has enough data available in local store to respond
    //                        4) some error happens while reading data
    //                        5) any of the requested partitions need HW update
    if (timeout <= 0 || fetchInfos.isEmpty || (tierLogReadResultMap.isEmpty && localReadableBytes >= fetchMinBytes) || errorReadingData || anyPartitionsNeedHwUpdate) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        val records = result match {
          case logReadResult: LogReadResult =>
            FetchLag.maybeRecordConsumerFetchTimeLag(!isFromFollower, logReadResult, brokerTopicStats)
            logReadResult.info.records
          case _: TierLogReadResult => MemoryRecords.EMPTY
        }
        tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, records,
          result.lastStableOffset, result.info.abortedTransactions, result.preferredReadReplica,
          isFromFollower && isAddingReplica(tp, replicaId))
      }
      responseCallback(fetchPartitionData)
    } else {
      val tierFetchPartitionStatusList = mergeIntoFetchPartitionStatusList(fetchInfos, tierLogReadResultMap)
      val localFetchPartitionStatusList = mergeIntoFetchPartitionStatusList(fetchInfos, localLogReadResultMap)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val localDelayedFetchKeys = localFetchPartitionStatusList.map { case (tp, _) => TopicPartitionOperationKey(tp) }

      if (tierLogReadResultMap.isEmpty) {
        val localFetchMetadata = SFetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,
          fetchIsolation, isFromFollower, replicaId, localFetchPartitionStatusList)

        val delayedFetch = new DelayedFetch(timeout, localFetchMetadata, this, quota, None,
          clientMetadata, brokerTopicStats, responseCallback)

        // try to complete the request immediately, otherwise put it into the purgatory;
        // this is because while the delayed fetch operation is being created, new requests
        // may arrive and hence make this operation completable.
        delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, localDelayedFetchKeys)
      } else {
        // Must use the logReadResult list instead of the tierLogReadResultMap to ensure ordering is maintained.
        val tierFetchMetadataList = logReadResults.collect { case (_, tierLogReadResult: TierLogReadResult) => tierLogReadResult.info.fetchMetadata }
        val completionCallback = (delayedOperationKey: DelayedOperationKey) =>
          delayedFetchPurgatory.checkAndComplete(delayedOperationKey): Unit

        val tierFetcher = tierReplicaComponents.fetcherOpt.getOrElse(throw new IllegalStateException("Attempted to initiate fetch for tiered data but there is no TierFetcher present"))
        val tierFetchBytesOverride = ReplicaManager.tierFetchPartitionMaxBytesOverride(config.tierMaxPartitionFetchBytesOverride,
          fetchInfos.size,
          fetchMaxBytes,
          tierFetchMetadataList.head.maxBytes,
          localReadableBytes)
        val pendingFetch = tierFetcher.fetch(tierFetchMetadataList.asJava, isolationLevel, completionCallback.asJava, tierFetchBytesOverride)

        // Create TopicPartitionOperationKey's for all local partitions included in this fetch. Merge the resulting
        // set of keys with the list of TierFetchOperationKeys returned from initiating the tier fetch.
        val delayedFetchKeys = localDelayedFetchKeys ++ pendingFetch.delayedOperationKeys.asScala

        // For tiered fetches, we set the lower bound on the fetch timeout to 15s, which is half of the default request
        // timeout. This forces all requests with max.wait < 15000 to wait for the tier fetch to complete, or the 15000
        // to elapse. This is only temporary until a solution is found for caching segment data between requests.
        val boundedTimeout = Math.max(timeout, 15000)
        val tierAndLocalFetchMetadata = SFetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,
          fetchIsolation, isFromFollower, replicaId, localFetchPartitionStatusList ++ tierFetchPartitionStatusList)
        val delayedFetch = new DelayedFetch(boundedTimeout, tierAndLocalFetchMetadata, this, quota, Some(pendingFetch),
          clientMetadata, brokerTopicStats, responseCallback)
        // Gather up all of the fetchInfos
        delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
      }
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       fetchIsolation: FetchIsolation,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota,
                       clientMetadata: Option[ClientMetadata]): Seq[(TopicPartition, AbstractLogReadResult)] = {
    val traceEnabled = isTraceEnabled

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): AbstractLogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      try {
        if (traceEnabled)
          trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
            s"remaining response limit $limitBytes" +
            (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        // expect leader if the fetch is from follower
        val partition = getPartitionOrException(tp, expectLeader = fetchOnlyFromLeader)
        val fetchTimeMs = time.milliseconds

        // If we are the leader, determine the preferred read-replica
        val preferredReadReplica = clientMetadata.flatMap(
          metadata => findPreferredReadReplica(partition, metadata, replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorOpt.foreach { selector =>
            debug(s"Replica selector ${selector.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for $clientMetadata")
          }
          // If a preferred read-replica is set, skip the read
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = offsetSnapshot.highWatermark.messageOffset,
            leaderLogStartOffset = offsetSnapshot.logStartOffset,
            leaderLogEndOffset = offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            isReadAllowed = false,
            lastStableOffset = Some(offsetSnapshot.lastStableOffset.messageOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        } else {
          val isFromConsumer = !(Request.isValidBrokerId(replicaId) || replicaId == Request.FutureLocalReplicaId)

          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          val readInfo: LogReadInfo = partition.readRecords(
            fetchOffset = fetchInfo.fetchOffset,
            currentLeaderEpoch = fetchInfo.currentLeaderEpoch,
            maxBytes = adjustedMaxBytes,
            fetchIsolation = fetchIsolation,
            fetchOnlyFromLeader = fetchOnlyFromLeader,
            minOneMessage = minOneMessage,
            permitPreferredTierRead = isFromConsumer)

          // Check if the HW known to the follower is behind the actual HW if a replica selector is defined
          val followerNeedsHwUpdate = replicaSelectorOpt.isDefined &&
            partition.getReplica(replicaId).exists(replica => replica.lastSentHighWatermark < readInfo.highWatermark)

          val (fetchOffsetMetadata, firstEntryIncomplete) = readInfo.fetchedData match {
            case localReadInfo: FetchDataInfo => (localReadInfo.fetchOffsetMetadata, localReadInfo.firstEntryIncomplete)
            case _: TierFetchDataInfo => (LogOffsetMetadata.UnknownOffsetMetadata, false)
          }

          val fetchDataInfo = if (shouldLeaderThrottle(quota, partition, replicaId)) {
            // If the partition is being throttled, simply return an empty set.
            markLeaderReplicaThrottle()
            FetchDataInfo(fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else if (!hardMaxBytesLimit && firstEntryIncomplete) {
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            FetchDataInfo(fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else {
            readInfo.fetchedData
          }

          fetchDataInfo match {
            case info: FetchDataInfo =>
              LogReadResult(info = info,
                highWatermark = readInfo.highWatermark,
                leaderLogStartOffset = readInfo.logStartOffset,
                leaderLogEndOffset = readInfo.logEndOffset,
                followerLogStartOffset = followerLogStartOffset,
                fetchTimeMs = fetchTimeMs,
                readSize = adjustedMaxBytes,
                isReadAllowed = adjustedMaxBytes > 0 || minOneMessage,
                lastStableOffset = Some(readInfo.lastStableOffset),
                preferredReadReplica = preferredReadReplica,
                followerNeedsHwUpdate = followerNeedsHwUpdate,
                exception = None)

            case info: TierFetchDataInfo =>
              TierLogReadResult(info = info,
                highWatermark = readInfo.highWatermark,
                leaderLogStartOffset = readInfo.logStartOffset,
                leaderLogEndOffset = readInfo.logEndOffset,
                followerLogStartOffset = followerLogStartOffset,
                fetchTimeMs = fetchTimeMs,
                readSize = adjustedMaxBytes,
                lastStableOffset = Some(readInfo.lastStableOffset),
                preferredReadReplica = preferredReadReplica,
                exception = None)
          }
        }
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderForPartitionException |
                 _: UnknownLeaderEpochException |
                 _: FencedLeaderEpochException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException |
                 _: LeaderNotAvailableException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            isReadAllowed = false,
            lastStableOffset = None,
            exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = Request.describeReplicaId(replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            isReadAllowed = false,
            lastStableOffset = None,
            exception = Some(e))
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, AbstractLogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize =
        readResult match {
          case localResult: LogReadResult => localResult.info.records.sizeInBytes
          case tierResult: TierLogReadResult => tierResult.info.fetchMetadata.maxBytes.intValue
        }
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }

  /**
    * Using the configured [[ReplicaSelector]], determine the preferred read replica for a partition given the
    * client metadata, the requested offset, and the current set of replicas. If the preferred read replica is the
    * leader, return None
    */
  def findPreferredReadReplica(partition: Partition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    partition.leaderReplicaIdOpt.flatMap { leaderReplicaId =>
      // Don't look up preferred for follower fetches via normal replication
      if (Request.isValidBrokerId(replicaId))
        None
      else if (partition.getIsUncleanLeader) {
        // Partition is marked unclean when an unclean leader has been elected. It stays so until recovery
        // has been performed on the log. Till then, reads are blocked.
        throw new LeaderNotAvailableException(s"Partition $partition is not yet available " +
          s"because it needs to undergo recovery after unclean leader election")
      } else {
        replicaSelectorOpt.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(partition.topicPartition,
            new ListenerName(clientMetadata.listenerName))
          val replicaInfos = partition.remoteReplicas
            // Exclude replicas that don't have the requested offset (whether or not if they're in the ISR)
            .filter(replica => replica.logEndOffset >= fetchOffset && replica.logStartOffset <= fetchOffset)
            .map(replica => new DefaultReplicaView(
              replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
              replica.logEndOffset,
              currentTimeMs - replica.lastCaughtUpTimeMs))

          val leaderReplica = new DefaultReplicaView(
            replicaEndpoints.getOrElse(leaderReplicaId, Node.noNode()),
            partition.localLogOrException.logEndOffset, 0L)
          val replicaInfoSet = mutable.Set[ReplicaView]() ++= replicaInfos += leaderReplica

          val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
          replicaSelector.select(partition.topicPartition, clientMetadata, partitionInfo).asScala.collect {
            // Even though the replica selector can return the leader, we don't want to send it out with the
            // FetchResponse, so we exclude it here
            case selected if !selected.endpoint.isEmpty && selected != leaderReplica => selected.endpoint.id
          }
        }
      }
    }
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, partition: Partition, replicaId: Int): Boolean = {
    val isReplicaInSync = partition.inSyncReplicaIds.contains(replicaId)
    !isReplicaInSync && quota.isThrottled(partition.topicPartition) && quota.isQuotaExceeded
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = localLog(topicPartition).map(_.config)

  def updateLogConfig(topicPartition: TopicPartition, newConfig: LogConfig): Unit = {
    localLog(topicPartition).foreach { log =>
      val tierPartitionState = log.tierPartitionState
      val oldTieringEnabled = tierPartitionState.isTieringEnabled
      log.updateConfig(newConfig)

      // if tiering has now been enabled, propagate replica state to tierReplicaManager
      tierReplicaComponents.replicaManagerOpt.foreach { tierReplicaManager =>
        val newTieringEnabled = tierPartitionState.isTieringEnabled
        if (!oldTieringEnabled && newTieringEnabled) {
          replicaStateChangeLock synchronized {
            getPartition(topicPartition) match {
              case HostedPartition.Online(partition) =>
                if (partition.isLeader)
                  tierReplicaManager.becomeLeader(tierPartitionState, partition.getLeaderEpoch)
                else
                  tierReplicaManager.becomeFollower(tierPartitionState)

              case _ =>
            }
          }
        }
      }
    }
  }

  def getMagic(topicPartition: TopicPartition): Option[Byte] = getLogConfig(topicPartition).map(_.messageFormatVersion.recordVersion.value)

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = s"Received update metadata request with correlation id $correlationId " +
          s"from an old controller ${updateMetadataRequest.controllerId} with epoch ${updateMetadataRequest.controllerEpoch}. " +
          s"Latest known controller epoch is $controllerEpoch"
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateChangeLogger.messageWithPrefix(stateControllerEpochErrorMessage))
      } else {
        val deletedPartitions = metadataCache.updateMetadata(correlationId, updateMetadataRequest)
        controllerEpoch = updateMetadataRequest.controllerEpoch
        deletedPartitions
      }
    }
  }

  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    val startMs = time.milliseconds()
    replicaStateChangeLock synchronized {
      val controllerId = leaderAndIsrRequest.controllerId
      val requestPartitionStates = leaderAndIsrRequest.partitionStates.asScala
      stateChangeLogger.info(s"Handling LeaderAndIsr request correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        requestPartitionStates.foreach { partitionState =>
          stateChangeLogger.trace(s"Received LeaderAndIsr request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch ${leaderAndIsrRequest.controllerEpoch}")
        }

      val response = {
        if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
          stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
            s"correlation id $correlationId since its controller epoch ${leaderAndIsrRequest.controllerEpoch} is old. " +
            s"Latest known controller epoch is $controllerEpoch")
          leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception)
        } else {
          val responseMap = new mutable.HashMap[TopicPartition, Errors]
          controllerEpoch = leaderAndIsrRequest.controllerEpoch

          val partitionStates = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()

          // First create the partition if it doesn't exist already
          requestPartitionStates.foreach { partitionState =>
            val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
            val partitionOpt = getPartition(topicPartition) match {
              case HostedPartition.Offline =>
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                  "partition is in an offline log directory")
                responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
                None

              case HostedPartition.Online(partition) =>
                Some(partition)

              case HostedPartition.None =>
                val partition = Partition(topicPartition, time, this)
                allPartitions.putIfNotExists(topicPartition, HostedPartition.Online(partition))
                Some(partition)
            }

            // Next check partition's leader epoch
            partitionOpt.foreach { partition =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch

              // We propagate the partition state down if:
              // 1. The leader epoch is higher than the current leader epoch of the partition
              // 2. The leader epoch is same as the current leader epoch but a new topic id is being assigned. This is
              //    needed to handle the case where a topic id is assigned for the first time after upgrade.
              def propagatePartitionState(requestLeaderEpoch: Int, currentLeaderEpoch: Int, partition: Partition): Boolean = {
                requestLeaderEpoch > currentLeaderEpoch ||
                  (requestLeaderEpoch == currentLeaderEpoch &&
                    partition.log.flatMap(_.topicIdPartition).isEmpty &&
                    partitionState.topicId != MessageUtil.ZERO_UUID)
              }

              if (propagatePartitionState(requestLeaderEpoch, currentLeaderEpoch, partition)) {
                // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
                // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
                if (partitionState.replicas.contains(localBrokerId))
                  partitionStates.put(partition, partitionState)
                else {
                  stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
                    s"correlation id $correlationId epoch $controllerEpoch for partition $topicPartition as itself is not " +
                    s"in assigned replica list ${partitionState.replicas.asScala.mkString(",")}")
                  responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
                }
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
              } else {
                stateChangeLogger.info(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
              }
            }
          }

          val partitionsToBeLeader = partitionStates.filter { case (_, partitionState) =>
            partitionState.leader == localBrokerId
          }
          val partitionsToBeFollower = partitionStates.filter { case (k, _) => !partitionsToBeLeader.contains(k) }

          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty)
            makeLeaders(controllerId, controllerEpoch, partitionsToBeLeader, correlationId, responseMap,
              highWatermarkCheckpoints)
          else
            Set.empty[Partition]
          val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
            makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap,
              highWatermarkCheckpoints)
          else
            Set.empty[Partition]

          /*
         * KAFKA-8392
         * For topic partitions of which the broker is no longer a leader, delete metrics related to
         * those topics. Note that this means the broker stops being either a replica or a leader of
         * partitions of said topics
         */
          val leaderTopicSet = leaderPartitionsIterator.map(_.topic).toSet
          val followerTopicSet = partitionsBecomeFollower.map(_.topic).toSet
          followerTopicSet.diff(leaderTopicSet).foreach(brokerTopicStats.removeOldLeaderMetrics)

          // remove metrics for brokers which are not followers of a topic
          leaderTopicSet.diff(followerTopicSet).foreach(brokerTopicStats.removeOldFollowerMetrics)

          leaderAndIsrRequest.partitionStates.forEach { partitionState =>
            val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
            /*
           * If there is offline log directory, a Partition object may have been created by getOrCreatePartition()
           * before getOrCreateReplica() failed to create local replica due to KafkaStorageException.
           * In this case ReplicaManager.allPartitions will map this topic-partition to an empty Partition object.
           * we need to map this topic-partition to OfflinePartition instead.
           */
            if (localLog(topicPartition).isEmpty)
              markPartitionOffline(topicPartition)
          }

          // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
          // have been completely populated before starting the checkpointing there by avoiding weird race conditions
          startHighWatermarkCheckPointThread()

          // delete stray logs
          if (leaderAndIsrRequest.containsAllReplicas)
            deleteStrayLogs()

          maybeAddLogDirFetchers(partitionStates.keySet, highWatermarkCheckpoints)

          shutdownIdleFetcherThreads()
          onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
          val responsePartitions = responseMap.iterator.map { case (tp, error) =>
            new LeaderAndIsrPartitionError()
              .setTopicName(tp.topic)
              .setPartitionIndex(tp.partition)
              .setErrorCode(error.code)
          }.toBuffer
          new LeaderAndIsrResponse(
            new LeaderAndIsrResponseData()
              .setErrorCode(Errors.NONE.code)
              .setPartitionErrors(responsePartitions.asJava))
        }
      }
      val endMs = time.milliseconds()
      val elapsedMs = endMs - startMs
      stateChangeLogger.info(s"Finished LeaderAndIsr request in ${elapsedMs}ms correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      response
    }
  }

  private def maybeAddLogDirFetchers(partitions: Set[Partition],
                                     offsetCheckpoints: OffsetCheckpoints): Unit = {
    val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
    for (partition <- partitions) {
      val topicPartition = partition.topicPartition
      if (logManager.getLog(topicPartition, isFuture = true).isDefined) {
        partition.log.foreach { log =>
          val leader = BrokerEndPoint(config.brokerId, "localhost", -1)

          // Add future replica log to partition's map
          partition.createLogIfNotExists(
            isNew = false,
            isFutureReplica = true,
            offsetCheckpoints)

          // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move
          // replica from source dir to destination dir
          logManager.abortAndPauseCleaning(topicPartition)

          futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(leader,
            partition.getLeaderEpoch, log.highWatermark))
        }
      }
    }

    if (futureReplicasAndInitialOffset.nonEmpty)
      replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          controllerEpoch: Int,
                          partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors],
                          highWatermarkCheckpoints: OffsetCheckpoints): Set[Partition] = {
    val traceEnabled = stateChangeLogger.isTraceEnabled
    partitionStates.keys.foreach { partition =>
      if (traceEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from " +
          s"controller $controllerId epoch $controllerEpoch starting the become-leader transition for " +
          s"partition ${partition.topicPartition}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeLeaders = mutable.Set[Partition]()
    val linkedPartitionsToMakeLeaders = mutable.Set[Partition]()

    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      clusterLinkManager.foreach(_.removePartitions(partitionStates))
      stateChangeLogger.info(s"Stopped fetchers as part of LeaderAndIsr request correlationId $correlationId from " +
        s"controller $controllerId epoch $controllerEpoch as part of the become-leader transition for " +
        s"${partitionStates.size} partitions")
      // Update the partition information to be the leader
      partitionStates.foreach { case (partition, partitionState) =>
        try {
          if (partition.makeLeader(partitionState, highWatermarkCheckpoints))
            partitionsToMakeLeaders += partition
          else
            stateChangeLogger.info(s"Skipped the become-leader state change after marking its " +
              s"partition as leader with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} (last update controller epoch ${partitionState.controllerEpoch}) " +
              s"since it is already the leader for the partition.")
          if (partition.isActiveLinkDestinationLeader)
            linkedPartitionsToMakeLeaders += partition
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-leader state change with " +
              s"correlation id $correlationId from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) since " +
              s"the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      clusterLinkManager.foreach(_.addPartitions(linkedPartitionsToMakeLeaders))

    } catch {
      case e: Throwable =>
        partitionStates.keys.foreach { partition =>
          stateChangeLogger.error(s"Error while processing LeaderAndIsr request correlationId $correlationId received " +
            s"from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition}", e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    if (traceEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-leader transition for partition ${partition.topicPartition}")
      }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int,
                            controllerEpoch: Int,
                            partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Errors],
                            highWatermarkCheckpoints: OffsetCheckpoints) : Set[Partition] = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    partitionStates.foreach { case (partition, partitionState) =>
      if (traceLoggingEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch starting the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionState.leader}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
    try {
      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionStates.foreach { case (partition, partitionState) =>
        val newLeaderBrokerId = partitionState.leader
        try {
          metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
            // Only change partition state when the leader is available
            case Some(_) =>
              if (partition.makeFollower(partitionState, highWatermarkCheckpoints))
                partitionsToMakeFollower += partition
              else
                stateChangeLogger.info(s"Skipped the become-follower state change after marking its partition as " +
                  s"follower with correlation id $correlationId from controller $controllerId epoch $controllerEpoch " +
                  s"for partition ${partition.topicPartition} (last update " +
                  s"controller epoch ${partitionState.controllerEpoch}) " +
                  s"since the new leader $newLeaderBrokerId is the same as the old leader")
            case None =>
              // The leader broker should always be present in the metadata cache.
              // If not, we should record the error message and abort the transition process for this partition
              stateChangeLogger.error(s"Received LeaderAndIsrRequest with correlation id $correlationId from " +
                s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
                s"(last update controller epoch ${partitionState.controllerEpoch}) " +
                s"but cannot become follower since the new leader $newLeaderBrokerId is unavailable.")
              // Create the local replica even if the leader is unavailable. This is required to ensure that we include
              // the partition's high watermark in the checkpoint file (see KAFKA-1647)
              partition.createLogIfNotExists(isNew = partitionState.isNew, isFutureReplica = false,
                highWatermarkCheckpoints)
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-follower state change with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) with leader " +
              s"$newLeaderBrokerId since the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the follower for partition $partition with leader " +
              s"$newLeaderBrokerId in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      clusterLinkManager.foreach(_.removePartitionsAndMetadata(partitionsToMakeFollower.map(_.topicPartition)))
      stateChangeLogger.info(s"Stopped fetchers as part of become-follower request from controller $controllerId " +
        s"epoch $controllerEpoch with correlation id $correlationId for ${partitionsToMakeFollower.size} partitions")

      partitionsToMakeFollower.foreach { partition =>
        completeDelayedRequests(partition.topicPartition)
      }

      if (isShuttingDown.get()) {
        if (traceLoggingEnabled) {
          partitionsToMakeFollower.foreach { partition =>
            stateChangeLogger.trace(s"Skipped the adding-fetcher step of the become-follower state " +
              s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} with leader ${partitionStates(partition).leader} " +
              "since it is shutting down")
          }
        }
      } else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map { partition =>
          val leader = metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get
            .brokerEndPoint(config.interBrokerListenerName)
          val fetchOffset = partition.localLogOrException.highWatermark
          partition.topicPartition -> InitialFetchState(leader, partition.getLeaderEpoch, fetchOffset)
       }.toMap

        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    if (traceLoggingEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionStates(partition).leader}")
      }

    partitionsToMakeFollower
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // Shrink ISRs for non offline partitions
    allPartitions.keys.foreach { topicPartition =>
      nonOfflinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  /**
   * Update the follower's fetch state on the leader based on the last fetch request and update `readResult`.
   * If the follower replica is not recognized to be one of the assigned replicas, do not update
   * `readResult` so that log start/end offset and high watermark is consistent with
   * records in fetch response. Log start/end offset and high watermark may change not only due to
   * this fetch request, e.g., rolling new log segment and removing old log segment may move log
   * start offset further than the last offset in the fetched records. The followers will get the
   * updated leader's state in the next fetch response.
   */
  private def updateFollowerFetchState(followerId: Int,
                                       readResults: Seq[(TopicPartition, AbstractLogReadResult)]): Seq[(TopicPartition, AbstractLogReadResult)] = {
    readResults.map { case (topicPartition, readResult) =>
      val updatedReadResult = if (readResult.error != Errors.NONE) {
        debug(s"Skipping update of fetch state for follower $followerId since the " +
          s"log read returned error ${readResult.error}")
        readResult
      } else {
        readResult match {
          case readResult: LogReadResult =>
            nonOfflinePartition(topicPartition) match {
              case Some(partition) =>
                if (partition.updateFollowerFetchState(followerId,
                  followerFetchOffsetMetadata = readResult.info.fetchOffsetMetadata,
                  followerStartOffset = readResult.followerLogStartOffset,
                  followerFetchTimeMs = readResult.fetchTimeMs,
                  leaderEndOffset = readResult.leaderLogEndOffset,
                  lastSentHighwatermark = readResult.highWatermark)) {
                  readResult
                } else {
                  warn(s"Leader $localBrokerId failed to record follower $followerId's position " +
                    s"${readResult.info.fetchOffsetMetadata.messageOffset}, and last sent HW since the replica " +
                    s"is not recognized to be one of the assigned replicas ${partition.assignmentState.replicas.mkString(",")} " +
                    s"for partition $topicPartition. Empty records will be returned for this partition.")
                  readResult.withEmptyFetchInfo
                }
              case None =>
                warn(s"While recording the replica LEO, the partition $topicPartition hasn't been created.")
                readResult
            }
          case readResult: TierLogReadResult =>
            val reason = s"Lagging follower $followerId fetched from the tiered portion of the log at offset " +
              s"${readResult.info.fetchMetadata.fetchStartOffset} for partition $topicPartition"
            info(reason)
            LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
              highWatermark = -1L,
              leaderLogStartOffset = -1L,
              leaderLogEndOffset = -1L,
              followerLogStartOffset = -1L,
              fetchTimeMs = -1L,
              readSize = 0,
              isReadAllowed = false,
              lastStableOffset = None,
              exception = Some(new OffsetTieredException(reason)))
        }
      }
      topicPartition -> updatedReadResult
    }
  }

  def leaderPartitionsIterator: Iterator[Partition] =
    nonOfflinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    nonOfflinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks(): Unit = {
    def putHw(logDirToCheckpoints: mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]],
              log: AbstractLog): Unit = {
      val checkpoints = logDirToCheckpoints.getOrElseUpdate(log.parentDir,
        new mutable.AnyRefMap[TopicPartition, Long]())
      checkpoints.put(log.topicPartition, log.highWatermark)
    }

    val logDirToHws = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]](
      allPartitions.size)
    nonOfflinePartitionsIterator.foreach { partition =>
      partition.log.foreach(putHw(logDirToHws, _))
      partition.futureLog.foreach(putHw(logDirToHws, _))
    }

    for ((logDir, hws) <- logDirToHws) {
      try highWatermarkCheckpoints.get(logDir).foreach(_.write(hws))
      catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $logDir", e)
      }
    }
  }

  // Used only by test
  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.put(tp, HostedPartition.Offline)
    Partition.removeMetrics(tp)
  }

  def markFollowerReplicaThrottle(): Unit = synchronized {
    throttledFollowerReplicasRate.mark()
  }

  def markLeaderReplicaThrottle(): Unit = synchronized {
    throttledLeaderReplicasRate.mark()
  }

  def markClusterLinkReplicaThrottle(): Unit = synchronized {
    throttledClusterLinkReplicasRate.mark()
  }

  // logDir should be an absolute path
  // sendZkNotification is needed for unit test
  def handleLogDirFailure(dir: String, sendZkNotification: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    warn(s"Stopping serving replicas in dir $dir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = nonOfflinePartitionsIterator.filter { partition =>
        partition.log.exists { _.parentDir == dir }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = nonOfflinePartitionsIterator.filter { partition =>
        partition.futureLog.exists { _.parentDir == dir }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      clusterLinkManager.foreach(_.removePartitionsAndMetadata(newOfflinePartitions))
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filter { case (checkpointDir, _) => checkpointDir != dir }

      warn(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)

    if (sendZkNotification)
      zkClient.propagateLogDirEvent(localBrokerId)
    warn(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics(): Unit = {
    removeMetric("LeaderCount")
    removeMetric("PartitionCount")
    removeMetric("OfflineReplicaCount")
    removeMetric("UnderReplicatedPartitions")
    removeMetric("UnderMinIsrPartitionCount")
    removeMetric("AtMinIsrPartitionCount")
    removeMetric("NotCaughtUpPartitionCount")
    removeMetric("MaxLastStableOffsetLag")
    removeMetric("ThrottledLeaderReplicasPerSec")
    removeMetric("ThrottledFollowerReplicasPerSec")
    removeMetric("ThrottledClusterLinkReplicasPerSec")
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true): Unit = {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedElectLeaderPurgatory.shutdown()
    delayedListOffsetsPurgatory.shutdown()
    executor.shutdownNow()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorOpt.foreach(_.close)
    info("Shut down completely")
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager, tierReplicaComponents.stateFetcherOpt)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats)
  }

  protected def createReplicaSelector(): Option[ReplicaSelector] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = CoreUtils.createObject[ReplicaSelector](className)
      tmpReplicaSelector.configure(config.originals())
      tmpReplicaSelector
    }
  }

  def lastOffsetForLeaderEpoch(requestedEpochInfo: Map[TopicPartition, OffsetsForLeaderEpochRequest.PartitionData]): Map[TopicPartition, EpochEndOffset] = {
    requestedEpochInfo.map { case (tp, partitionData) =>
      val epochEndOffset = getPartition(tp) match {
        case HostedPartition.Online(partition) =>
          partition.lastOffsetForLeaderEpoch(partitionData.currentLeaderEpoch, partitionData.leaderEpoch,
            fetchOnlyFromLeader = true)

        case HostedPartition.Offline =>
          new EpochEndOffset(Errors.KAFKA_STORAGE_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

        case HostedPartition.None if metadataCache.contains(tp) =>
          new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

        case HostedPartition.None =>
          new EpochEndOffset(Errors.UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
      tp -> epochEndOffset
    }
  }

  def electLeaders(
    controller: KafkaController,
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    responseCallback: Map[TopicPartition, ApiError] => Unit,
    requestTimeout: Int
  ): Unit = {

    val deadline = time.milliseconds() + requestTimeout

    def electionCallback(results: Map[TopicPartition, Either[ApiError, Int]]): Unit = {
      val expectedLeaders = mutable.Map.empty[TopicPartition, Int]
      val failures = mutable.Map.empty[TopicPartition, ApiError]
      results.foreach {
        case (partition, Right(leader)) => expectedLeaders += partition -> leader
        case (partition, Left(error)) => failures += partition -> error
      }

      if (expectedLeaders.nonEmpty) {
        val watchKeys = expectedLeaders.iterator.map {
          case (tp, _) => TopicPartitionOperationKey(tp)
        }.toBuffer

        delayedElectLeaderPurgatory.tryCompleteElseWatch(
          new DelayedElectLeader(
            math.max(0, deadline - time.milliseconds()),
            expectedLeaders,
            failures,
            this,
            responseCallback
          ),
          watchKeys
        )
      } else {
          // There are no partitions actually being elected, so return immediately
          responseCallback(failures)
      }
    }

    controller.electLeaders(partitions, electionType, electionCallback)
  }

  def shutdownIdleFetcherThreads(): Unit = {
    replicaFetcherManager.shutdownIdleFetcherThreads()
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
    clusterLinkManager.foreach(_.shutdownIdleFetcherThreads())
  }
}

object FetchLag {
  val UnknownFetchLagMs: Long = -1L

  /**
   * Calculate the lag as the difference to the fetch timestamp (ms) which is based on current time  as retention is
   * computed based on current timestamp too.
   * 1. result.isReadAllowed is FALSE then read did not happen; fetch time lag cannot be computed
   * 2. Empty batch indicated zero lag, as the consumer is caught up
   * 3. Non empty record batch with NO_TIMESTAMP indicates messages with older format.
   * 4. lag = result.fetchTimeMs - firstBatchTimestamp
   */
  private def lagInMs(result: LogReadResult): Long = {
    if (!result.isReadAllowed)
      return FetchLag.UnknownFetchLagMs

    val iterator = result.info.records.batches.iterator
    if (!iterator.hasNext())
      return 0L

    val firstBatchTimestamp = iterator.next().maxTimestamp()
    if (firstBatchTimestamp == RecordBatch.NO_TIMESTAMP || result.fetchTimeMs < firstBatchTimestamp)
      return UnknownFetchLagMs

    result.fetchTimeMs - firstBatchTimestamp
  }

  def maybeRecordConsumerFetchTimeLag(isFromConsumer: Boolean,
                                      result: LogReadResult,
                                      brokerTopicStats: BrokerTopicStats): Unit = {
    if (isFromConsumer) {
      val fetchLagMs = FetchLag.lagInMs(result)
      if (fetchLagMs != FetchLag.UnknownFetchLagMs)
        brokerTopicStats.allTopicsStats.consumerFetchLagTimeMs.update(fetchLagMs)
    }
  }
}

/**
  * Replica layer components for tiered storage.
  * @param replicaManagerOpt Replica manager for tiered storage
  * @param fetcherOpt Tier fetcher instance
  * @param stateFetcherOpt Tier state fetcher instance
  * @param logComponents Log components for tiered storage
  */
case class TierReplicaComponents(replicaManagerOpt: Option[TierReplicaManager],
                                 fetcherOpt: Option[TierFetcher],
                                 stateFetcherOpt: Option[TierStateFetcher],
                                 logComponents: TierLogComponents)

object TierReplicaComponents {
  val EMPTY = TierReplicaComponents(None, None, None, TierLogComponents.EMPTY)
}
