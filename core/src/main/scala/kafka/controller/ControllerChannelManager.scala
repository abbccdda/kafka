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
package kafka.controller

import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.{Gauge, Timer}
import kafka.api._
import kafka.cluster.Broker
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{KafkaException, Node, Reconfigurable, TopicPartition}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.HashMap
import scala.collection.{Seq, Set, mutable}

object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize"
  val RequestRateAndQueueTimeMetricName = "RequestRateAndQueueTimeMs"
}

class ControllerChannelManager(controllerContext: ControllerContext,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               stateChangeLogger: StateChangeLogger,
                               threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  import ControllerChannelManager._

  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  newGauge("TotalQueueSize",
    () => brokerLock synchronized {
      brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
    }
  )

  def startup() = {
    controllerContext.liveOrShuttingDownBrokers.foreach(addNewBroker)

    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.toList.foreach(removeExistingBroker)
    }
  }

  /**
   * Send a [[ControlMetadataBatch]] to the broker. Control batches are batched under the
   * hood to minimise the number of control requests sent out to the broker.
   *
   * @param brokerId The broker id to send the batch to.
   * @param batch    The batch to send.
   * @param callback The callback to execute when the response is received. If the batch
   *                 sent is merged into another one, the callback passed here won't be called but the
   *                 callback of the previous batch will. We assume that the same callback is typically
   *                 used when sending batches of a same type.
   */
  def sendControlMetadataBatch(brokerId: Int,
                               batch: ControlMetadataBatch,
                               callback: (ControlMetadataBatch, ControlMetadataBatchResult) => Unit = null): Unit = {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(batch, callback, time.milliseconds()))
        case None =>
          warn(s"Not sending batch $batch to broker $brokerId, since it is offline.")
      }
    }
  }

  def addBroker(broker: Broker): Unit = {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if (!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  def removeBroker(brokerId: Int): Unit = {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  private def addNewBroker(broker: Broker): Unit = {
    val messageQueue = new ControlMetadataAccumulator
    debug(s"Controller ${config.brokerId} trying to connect to broker ${broker.id}")
    val controllerToBrokerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    val controllerToBrokerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
    val brokerNode = broker.node(controllerToBrokerListenerName)
    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")
    val (networkClient, reconfigurableChannelBuilder) = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerToBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerToBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      val reconfigurableChannelBuilder = channelBuilder match {
        case reconfigurable: Reconfigurable =>
          config.addReconfigurable(reconfigurable)
          Some(reconfigurable)
        case _ => None
      }
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> brokerNode.idString).asJava,
        false,
        channelBuilder,
        logContext
      )
      val networkClient = new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        ClientDnsLookup.DEFAULT,
        time,
        false,
        new ApiVersions,
        logContext
      )
      (networkClient, reconfigurableChannelBuilder)
    }
    val threadName = threadNamePrefix match {
      case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
      case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
    }

    val requestRateAndQueueTimeMetrics = newTimer(
      RequestRateAndQueueTimeMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS, brokerMetricTags(broker.id)
    )

    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, requestRateAndQueueTimeMetrics, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    val queueSizeGauge = newGauge(QueueSizeMetricName, () => messageQueue.size, brokerMetricTags(broker.id))

    brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread,
      queueSizeGauge, requestRateAndQueueTimeMetrics, reconfigurableChannelBuilder))
  }

  private def brokerMetricTags(brokerId: Int) = Map("broker-id" -> brokerId.toString)

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo): Unit = {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
      // non-threadsafe classes as described in KAFKA-4959.
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.
      brokerState.reconfigurableChannelBuilder.foreach(config.removeReconfigurable)
      brokerState.requestSendThread.shutdown()
      brokerState.networkClient.close()
      brokerState.messageQueue.close()
      removeMetric(QueueSizeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      removeMetric(RequestRateAndQueueTimeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  protected def startRequestSendThread(brokerId: Int): Unit = {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if (requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: ControlMetadataAccumulator,
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        val requestRateAndQueueTimeMetrics: Timer,
                        val stateChangeLogger: StateChangeLogger,
                        name: String)
  extends ShutdownableThread(name = name) {

  logIdent = s"[RequestSendThread controllerId=$controllerId] "

  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  override def doWork(): Unit = {

    def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

    val item = queue.take()
    if (item == null)
      return

    val QueueItem(batch, callback, enqueueTimeMs) = item
    requestRateAndQueueTimeMetrics.update(time.milliseconds() - enqueueTimeMs, TimeUnit.MILLISECONDS)

    var responseError = Errors.NONE
    val partitionErrors = mutable.Map.empty[TopicPartition, Errors]

    batch.requests(stateChangeLogger).foreach { requestBuilder =>
      var clientResponse: ClientResponse = null
      try {
        var isSendSuccessful = false
        while (isRunning && !isSendSuccessful) {
          // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
          // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
          try {
            if (!brokerReady()) {
              isSendSuccessful = false
              backoff()
            }
            else {
              val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
                time.milliseconds(), true)
              clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
              isSendSuccessful = true
            }
          } catch {
            case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
              warn(s"Controller $controllerId epoch ${controllerContext.epoch} fails to send request $requestBuilder " +
                s"to broker $brokerNode. Reconnecting to broker.", e)
              networkClient.close(brokerNode.idString)
              isSendSuccessful = false
              backoff()
          }
        }

        if (clientResponse != null) {
          val requestHeader = clientResponse.requestHeader
          val api = requestHeader.apiKey
          if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA)
            throw new KafkaException(s"Unexpected apiKey received: ${requestBuilder.apiKey}")

          // Only the last top level error is kept. As requests issued from a same batch have the
          // the same top level metadata, we assume that both requests would have the same error.
          val response = clientResponse.responseBody.asInstanceOf[AbstractControlResponse]
          responseError = response.error
          partitionErrors ++= response.partitionErrors.asScala

          stateChangeLogger.withControllerEpoch(controllerContext.epoch).trace(s"Received response " +
            s"${response.toString(requestHeader.apiVersion)} for request $api with correlation id " +
            s"${requestHeader.correlationId} sent to broker $brokerNode")
        }
      } catch {
        case e: Throwable =>
          error(s"Controller $controllerId fails to send a request to broker $brokerNode", e)
          // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
          networkClient.close(brokerNode.idString)
      }
    }

    if (callback != null) {
      callback(batch, ControlMetadataBatchResult(responseError, partitionErrors))
    }
  }

  private def brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info(s"Controller $controllerId connected to $brokerNode for sending state change requests")
      }

      true
    } catch {
      case e: Throwable =>
        warn(s"Controller $controllerId's connection to broker $brokerNode was unsuccessful", e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

  override def initiateShutdown(): Boolean = {
    if (super.initiateShutdown()) {
      networkClient.initiateClose()
      true
    } else
      false
  }
}

class ControllerBrokerRequestBatch(config: KafkaConfig,
                                   controllerChannelManager: ControllerChannelManager,
                                   controllerEventManager: ControllerEventManager,
                                   controllerContext: ControllerContext,
                                   stateChangeLogger: StateChangeLogger)
  extends AbstractControllerBrokerRequestBatch(config, controllerContext, stateChangeLogger) {

  def sendEvent(event: ControllerEvent): Unit = {
    controllerEventManager.put(event)
  }

  def sendBatch(brokerId: Int,
                batch: ControlMetadataBatch,
                callback: (ControlMetadataBatch, ControlMetadataBatchResult) => Unit = null): Unit = {
    controllerChannelManager.sendControlMetadataBatch(brokerId, batch, callback)
  }
}

abstract class AbstractControllerBrokerRequestBatch(config: KafkaConfig,
                                                    controllerContext: ControllerContext,
                                                    stateChangeLogger: StateChangeLogger) extends Logging {
  val controllerId: Int = config.brokerId
  val leaderAndIsrBatchMap = mutable.Map.empty[Int, LeaderAndIsrBatch]
  val stopReplicaBatchMap = mutable.Map.empty[Int, StopReplicaBatch]
  val updateMetadataBatchMap = mutable.Map.empty[Int, UpdateMetadataBatch]

  def sendEvent(event: ControllerEvent): Unit

  def sendBatch(brokerId: Int,
                batch: ControlMetadataBatch,
                callback: (ControlMetadataBatch, ControlMetadataBatchResult) => Unit = null): Unit

  def newBatch(): Unit = {
    // raise error if the previous batch is not empty
    if (leaderAndIsrBatchMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        s"a new one. Some LeaderAndIsr state changes $leaderAndIsrBatchMap might be lost ")
    if (stopReplicaBatchMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some StopReplica state changes $stopReplicaBatchMap might be lost ")
    if (updateMetadataBatchMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some UpdateMetadata state changes $updateMetadataBatchMap might be lost ")
  }

  def nonEmpty: Boolean = {
    leaderAndIsrBatchMap.nonEmpty || stopReplicaBatchMap.nonEmpty || updateMetadataBatchMap.nonEmpty
  }

  def clear(): Unit = {
    leaderAndIsrBatchMap.clear()
    stopReplicaBatchMap.clear()
    updateMetadataBatchMap.clear()
  }

  private def getOrCreateLeaderAndIsrBatch(brokerId: Int): LeaderAndIsrBatch = {
    leaderAndIsrBatchMap.getOrElseUpdate(brokerId, new LeaderAndIsrBatch(brokerId))
  }

  private def getOrCreateStopReplicaBatch(brokerId: Int): StopReplicaBatch = {
    stopReplicaBatchMap.getOrElseUpdate(brokerId, new StopReplicaBatch(brokerId))
  }

  private def getOrCreateUpdateMetadataBatch(brokerId: Int): UpdateMetadataBatch = {
    updateMetadataBatchMap.getOrElseUpdate(brokerId, new UpdateMetadataBatch(brokerId))
  }

  def setContainsAllReplicas(brokerIds: Set[Int]): Unit = {
    brokerIds.foreach { brokerId =>
      getOrCreateLeaderAndIsrBatch(brokerId).setContainsAllReplicas()
    }
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int],
                                       topicPartition: TopicPartition,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicaAssignment: ReplicaAssignment,
                                       isNew: Boolean): Unit = {
    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      val clusterLink = controllerContext.linkedTopics.get(topicPartition.topic)

      val partitionState = new LeaderAndIsrPartitionState()
        .setTopicName(topicPartition.topic)
        .setPartitionIndex(topicPartition.partition)
        .setControllerEpoch(leaderIsrAndControllerEpoch.controllerEpoch)
        .setLeader(leaderAndIsr.leader)
        .setLeaderEpoch(leaderAndIsr.leaderEpoch)
        .setConfluentIsUncleanLeader(leaderAndIsr.isUnclean)
        .setClusterLinkId(clusterLink.map(_.linkId.toString).orNull)
        .setClusterLinkTopicState(clusterLink.map(_.state.name).orNull)
        .setLinkedLeaderEpoch(leaderAndIsr.clusterLinkState.map(_.linkedLeaderEpoch).getOrElse(-1))
        .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava)
        .setZkVersion(leaderAndIsr.zkVersion)
        .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava)
        .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
        .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
        .setObservers(replicaAssignment.effectiveObservers.map(Integer.valueOf).asJava)
        .setIsNew(isNew)

      if (config.tierFeature) {
        partitionState.setTopicId(controllerContext.topicIds(partitionState.topicName))
      }

      getOrCreateLeaderAndIsrBatch(brokerId).addPartitionState(topicPartition, partitionState)
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int],
                                      topicPartition: TopicPartition,
                                      deletePartition: Boolean): Unit = {
    // A sentinel (-2) is used as an epoch if the topic is queued for deletion. It overrides
    // any existing epoch.
    val leaderEpoch = if (controllerContext.isTopicQueuedUpForDeletion(topicPartition.topic)) {
      LeaderAndIsr.EpochDuringDelete
    } else {
      controllerContext.partitionLeadershipInfo(topicPartition)
        .map(_.leaderAndIsr.leaderEpoch)
        .getOrElse(LeaderAndIsr.NoEpoch)
    }

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      getOrCreateStopReplicaBatch(brokerId).addPartitionState(topicPartition,
        new StopReplicaPartitionState()
          .setPartitionIndex(topicPartition.partition())
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(deletePartition))
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicPartition]): Unit = {
    val partitionStates = mutable.Map.empty[TopicPartition, UpdateMetadataPartitionState]
    partitions.foreach { partition =>
      controllerContext.partitionLeadershipInfo(partition) match {
        case Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          val beingDeleted = controllerContext.topicsToBeDeleted.contains(partition.topic)
          val replicaAssignment = controllerContext.partitionFullReplicaAssignment(partition)
          val replicas = replicaAssignment.replicas
          val offlineReplicas = replicas.filter(!controllerContext.isReplicaOnline(_, partition))
          val updatedLeaderAndIsr =
            if (beingDeleted) LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            else leaderAndIsr

          partitionStates.put(partition, new UpdateMetadataPartitionState()
            .setTopicName(partition.topic)
            .setPartitionIndex(partition.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(updatedLeaderAndIsr.leader)
            .setLeaderEpoch(updatedLeaderAndIsr.leaderEpoch)
            .setIsr(updatedLeaderAndIsr.isr.map(Integer.valueOf).asJava)
            .setZkVersion(updatedLeaderAndIsr.zkVersion)
            .setReplicas(replicas.map(Integer.valueOf).asJava)
            .setOfflineReplicas(offlineReplicas.map(Integer.valueOf).asJava)
            .setObservers(replicaAssignment.effectiveObservers.map(Integer.valueOf).asJava))

        case None =>
          info(s"Leader not yet assigned for partition $partition. Skip sending UpdateMetadataRequest.")
      }
    }

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val batch = getOrCreateUpdateMetadataBatch(brokerId)
      partitionStates.foreach { case (topicPartition, partitionState) =>
        batch.addPartitionState(topicPartition, partitionState)
      }
    }
  }

  private def sendLeaderAndIsrRequest(controllerEpoch: Int): Unit = {
    val leaderAndIsrRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 1
      else 0

    leaderAndIsrBatchMap.foreach { case (brokerId, leaderAndIsrBatch) =>
      if (controllerContext.liveOrShuttingDownBrokerIds.contains(brokerId)) {
        leaderAndIsrBatch
          .setVersion(leaderAndIsrRequestVersion)
          .setControllerId(controllerId)
          .setControllerEpoch(controllerEpoch)
          .setBrokerEpoch(controllerContext.liveBrokerIdAndEpochs(brokerId))

        val leaderIds = leaderAndIsrBatch.partitions.map(_._2.leader).toSet
        controllerContext.liveOrShuttingDownBrokers.foreach { broker =>
          if (leaderIds.contains(broker.id))
            leaderAndIsrBatch.addLiveLeader(broker.node(config.interBrokerListenerName))
        }

        sendBatch(brokerId, leaderAndIsrBatch, (_, result) =>
          sendEvent(LeaderAndIsrResponseReceived(result.error, result.partitionErrors, brokerId))
        )
      }
    }

    leaderAndIsrBatchMap.clear()
  }

  private def sendUpdateMetadataRequests(controllerEpoch: Int): Unit = {
    val updateMetadataRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 6
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 5
      else if (config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
      else 0

    updateMetadataBatchMap.foreach { case (brokerId, updateMetadataBatch) =>
      if (controllerContext.liveOrShuttingDownBrokerIds.contains(brokerId)) {
        updateMetadataBatch
          .setVersion(updateMetadataRequestVersion)
          .setControllerId(controllerId)
          .setControllerEpoch(controllerEpoch)
          .setBrokerEpoch(controllerContext.liveBrokerIdAndEpochs(brokerId))
          .setLiveBrokers(controllerContext.liveOrShuttingDownBrokers.toSet)

        sendBatch(brokerId, updateMetadataBatch, (_, result) =>
          sendEvent(UpdateMetadataResponseReceived(result.error, brokerId))
        )
      }
    }

    updateMetadataBatchMap.clear()
  }

  private def sendStopReplicaRequests(controllerEpoch: Int): Unit = {
    val stopReplicaRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_6_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 1
      else 0

    def callback(brokerId: Int)(batch: ControlMetadataBatch, result: ControlMetadataBatchResult): Unit = {
      val stopReplicaBatch = batch.asInstanceOf[StopReplicaBatch]
      val partitionErrorsForDeletingTopics = mutable.Map.empty[TopicPartition, Errors]
      result.partitionErrors.foreach { case (topicPartition, partitionError) =>
        // We consider only the partitions that were deleted in the batch sent out.
        if (controllerContext.isTopicDeletionInProgress(topicPartition.topic) &&
          stopReplicaBatch.partitions.get(topicPartition).exists(_.deletePartition)) {
          partitionErrorsForDeletingTopics += topicPartition -> partitionError
        }
      }
      if (partitionErrorsForDeletingTopics.nonEmpty)
        sendEvent(TopicDeletionStopReplicaResponseReceived(brokerId, result.error,
          partitionErrorsForDeletingTopics))
    }

    stopReplicaBatchMap.foreach { case (brokerId, stopReplicaBatch) =>
      if (controllerContext.liveOrShuttingDownBrokerIds.contains(brokerId)) {
        stopReplicaBatch
          .setVersion(stopReplicaRequestVersion)
          .setControllerId(controllerId)
          .setControllerEpoch(controllerEpoch)
          .setBrokerEpoch(controllerContext.liveBrokerIdAndEpochs(brokerId))

        sendBatch(brokerId, stopReplicaBatch, callback(brokerId))
      }
    }

    stopReplicaBatchMap.clear()
  }

  def sendRequestsToBrokers(controllerEpoch: Int): Unit = {
    try {
      sendLeaderAndIsrRequest(controllerEpoch)
      sendStopReplicaRequests(controllerEpoch)
      sendUpdateMetadataRequests(controllerEpoch)
    } catch {
      case e: Throwable =>
        if (leaderAndIsrBatchMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +
            s"the map is $leaderAndIsrBatchMap. Exception message: $e")
        }
        if (updateMetadataBatchMap.nonEmpty) {
          error("Haven't been able to send metadata update requests, current state of " +
            s"the map is $updateMetadataBatchMap. Exception message: $e")
        }
        if (stopReplicaBatchMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +
            s"the map is $stopReplicaBatchMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
    }
  }
}

case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: ControlMetadataAccumulator,
                                     requestSendThread: RequestSendThread,
                                     queueSizeGauge: Gauge[Int],
                                     requestRateAndTimeMetrics: Timer,
                                     reconfigurableChannelBuilder: Option[Reconfigurable])
