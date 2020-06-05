// (Copyright) [2020 - 2020] Confluent, Inc.
package kafka.controller

import java.util

import kafka.api.LeaderAndIsr
import kafka.cluster.Broker
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractControlRequest, LeaderAndIsrRequest, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

/**
 * The Status trait is used to create an enumeration of states that are
 * used by [[ControlMetadataBatch#process]]. See the documentation of both
 * [[ControlMetadataBatch#process]] and [[ControlMetadataAccumulator]] for
 * details.
 */
sealed trait Status

/**
 * Continue indicates that the [[ControlMetadataAccumulator]] can move on to
 * the next item in the queue. The [[ControlMetadataBatch]] may have invalidated
 * metadata in the one being processed.
 */
case object Continue extends Status

/**
 * ContinueMerged indicates that the [[ControlMetadataAccumulator]] can move on
 * to the next item in the queue AND that the [[ControlMetadataBatch]] has been
 * merged in the one being processed.
 */
case object ContinueMerged extends Status

/**
 * Block indicates that the [[ControlMetadataAccumulator]] must not move beyond
 * the current item in the queue to preserve a coherent ordering of the metadata updates.
 * This can happen if the [[ControlMetadataBatch]] can't be modified nor invalidated.
 */
case object Block extends Status

/**
 * ControlMetadataBatch is the base trait used by all the control metadata batches. The
 * trait has two purposed:
 * 1. It is used by [[ControlMetadataAccumulator]] to accumulate metadata of a same type
 * together if possible.
 * 2. It is used to turn metadata into [[AbstractControlRequest.Builder]].
 */
trait ControlMetadataBatch {

  /**
   * Indicates if the batch is empty. This is used by the [[ControlMetadataAccumulator]] to
   * avoid sending out empty batches.
   *
   * @return True if the batch is empty, False otherwise.
   */
  def isEmpty: Boolean

  /**
   * Process the provided batch. The current batch can either accumulate the metadata of the
   * provided batch with its own metadata; or use the metadata of the provided batch to invalidate
   * its own metadata.
   *
   * The current batch can also decides if the provided metadata could be sent before itself or
   * not. If not, the [[ControlMetadataAccumulator]] will ensure that the metadata of the provided
   * batch stay behind the current one in the queue.
   *
   * @param batch The batch to process.
   * @return The status of the processing.
   */
  def process(batch: ControlMetadataBatch): Status

  /**
   * Turn the metadata into one or more requests.
   *
   * @param stateChangeLogger The state change logger used to log the requests
   * @return The requests that have been built.
   */
  def requests(stateChangeLogger: StateChangeLogger): Seq[AbstractControlRequest.Builder[_ <: AbstractControlRequest]]
}

/**
 * ControlMetadataBatchResult represents the response got after having sent a batch. If the
 * batch yields multiples requests, responses are aggregated. The top level response of the
 * last one is kept.
 *
 * @param error The top level error of the response.
 * @param partitionErrors The partition level errors of the response.
 */
case class ControlMetadataBatchResult(error: Errors,
                                      partitionErrors: mutable.Map[TopicPartition, Errors])

/**
 * Small wrapper object.
 *
 * @param batch The batch to be sent.
 * @param callback The callback to be called when all responses are received.
 * @param enqueueTimeMs The time at which the batch was enqueued.
 */
case class QueueItem(batch: ControlMetadataBatch,
                     callback: (ControlMetadataBatch, ControlMetadataBatchResult) => Unit,
                     enqueueTimeMs: Long)

/**
 * ControlMetadataAccumulator is a purpose built queue which accumulates batches implementing
 * the [[ControlMetadataBatch]] trait. The main goals of the accumulator are to merge batches of
 * a similar type together to minimize the number of requests sent out by the controller; and
 * to invalidate stale pending metadata to avoid sending out stale information to the brokers.
 *
 * The accumulator also guarantee the ordering in two ways. First, batches are kept in their
 * inserting order. So batch A will go out before batch B if it was inserted before. Batch A
 * could carry one more metadata if batches were accumulated in between. Second, batches can
 * decide if newly inserted batches can move in front of them or not. This is used by batches
 * that can't be modified (e.g. KIP-550) or by batches that must guarantee that its metadata is
 * send before others (e.g. LeaderAndIsr before UpdateMetadata).
 *
 * If a batch becomes empty due to all its metadata being invalidated, it is removed from the
 * queue.
 */
class ControlMetadataAccumulator {
  private var closed = false
  private val deque: util.Deque[QueueItem] = new util.ArrayDeque[QueueItem]()

  /**
   * Put the provided item in the queue. First, it iterates over the batches in the queue
   * to try accumulating the batch into another one and to invalidate state metadata. It
   * does so until it reaches the end or encounter a batch which blocks the progress. The
   * number of iterations is guarantee to be small, usually one batch per type.
   *
   * If the batch wasn't merged during the iteration, it is inserted at the end of the queue.
   *
   * @param item The item to insert in the queue.
   */
  def put(item: QueueItem): Unit = {
    synchronized {
      val iterator = deque.descendingIterator()
      val batch = item.batch
      var merged = false
      var break = false
      while (iterator.hasNext && !break) {
        val next = iterator.next
        next.batch.process(batch) match {
          // break the loop as the new batch violates the constraints of
          // the current batch. we can't go further.
          case Block =>
            break = true

          // continue processing items but remember that the batch has
          // been merged already.
          case ContinueMerged =>
            merged = true

          // continue processing items. delete batch is empty.
          case Continue =>
            if (next.batch.isEmpty)
              iterator.remove()
        }
      }
      if (!merged)
        deque.addLast(item)
      notify()
    }
  }

  /**
   * Take an item out of the queue. The method blocks if the queue is
   * empty until an item is available.
   *
   * @return An item or null if the queue was closed.
   */
  def take(): QueueItem = {
    synchronized {
      var item = deque.pollFirst()
      while (!closed && item == null) {
        wait()
        item = deque.pollFirst()
      }
      if (deque.size() > 0) {
        notify()
      }
      item
    }
  }

  /**
   * Return the size of the queue.
   */
  def size(): Int = {
    synchronized {
      deque.size()
    }
  }

  /**
   * Close the queue.
   */
  def close(): Unit = {
    synchronized {
      closed = true
      deque.clear()
      notifyAll()
    }
  }
}

/**
 * StopReplicaBatch holds all the metadata necessary to build StopReplicaRequests. It
 * accumulate any other StopReplicaBatches and invalidates its metadata based on
 * LeaderAndIsrBatches. StopReplicaBatch prevents any UpdateMetadataBatch to jump in
 * front in the accumulator.
 *
 * A StopReplicaBatch yields two StopReplicaRequests prior to KAFKA_2_6_IV0. Only one
 * afterwards.
 */
class StopReplicaBatch(brokerId: Int) extends ControlMetadataBatch {
  var version: Short = ApiKeys.STOP_REPLICA.latestVersion
  var controllerId: Int = 0
  var controllerEpoch: Int = 0
  var brokerEpoch: Long = 0
  var partitions = mutable.Map.empty[TopicPartition, StopReplicaPartitionState]

  def setVersion(version: Short): StopReplicaBatch = {
    this.version = version
    this
  }

  def setControllerId(controllerId: Int): StopReplicaBatch = {
    this.controllerId = controllerId
    this
  }

  def setControllerEpoch(controllerEpoch: Int): StopReplicaBatch = {
    this.controllerEpoch = controllerEpoch
    this
  }

  def setBrokerEpoch(brokerEpoch: Long): StopReplicaBatch = {
    this.brokerEpoch = brokerEpoch
    this
  }

  def addPartitionState(topicPartition: TopicPartition,
                        partitionState: StopReplicaPartitionState): StopReplicaBatch = {
    partitions.get(topicPartition) match {
      // If there is no accumulated partition state yet, we store it.
      case None => partitions.put(topicPartition, partitionState)

      // If there is already an accumulated partition state, the new state supersedes the
      // stored one if its leader epoch is newer or if it contains the delete sentinel. If
      // the new state has the same epoch as the accumulated one, it only supersedes it if
      // the delete partition flag is set.
      case Some(existingPartitionState) =>
        if (partitionState.leaderEpoch == LeaderAndIsr.EpochDuringDelete ||
            partitionState.leaderEpoch > existingPartitionState.leaderEpoch) {
          partitions.put(topicPartition, partitionState)
        } else if (partitionState.leaderEpoch == existingPartitionState.leaderEpoch &&
                   partitionState.deletePartition) {
          partitions.put(topicPartition, partitionState)
        }
    }
    this
  }

  def maybeInvalidatePartitionState(topicPartition: TopicPartition,
                                    leaderEpoch: Int): StopReplicaBatch = {
    // The accumulated partition state is invalidated if it has a smaller epoch.
    partitions.get(topicPartition).foreach { partitionState =>
      if (leaderEpoch > partitionState.leaderEpoch) {
        partitions.remove(topicPartition)
      }
    }
    this
  }

  override def isEmpty: Boolean = partitions.isEmpty

  override def process(batch: ControlMetadataBatch): Status = {
    batch match {
      // A new StopReplica batch is merge into the current one.
      case stopReplicaBatch: StopReplicaBatch =>
        version = stopReplicaBatch.version
        controllerId = stopReplicaBatch.controllerId
        controllerEpoch = stopReplicaBatch.controllerEpoch
        brokerEpoch = stopReplicaBatch.brokerEpoch

        stopReplicaBatch.partitions.foreach { case (topicPartition, partitionState) =>
          addPartitionState(topicPartition, partitionState)
        }

        ContinueMerged

      // A LeaderAndIsr batch is used to invalidate accumulated partition states if the
      // LeaderAndIsr batch has newer information.
      case leaderAndIsrBatch: LeaderAndIsrBatch =>
        leaderAndIsrBatch.partitions.foreach { case (topicPartition, partitionState) =>
          maybeInvalidatePartitionState(topicPartition, partitionState.leaderEpoch)
        }

        Continue

      // Prevent UpdateMetadata to jump in front of this batch because we want to
      // give priority to batches which updates the replica states.
      case _: UpdateMetadataBatch =>
        Block
    }
  }

  override def requests(stateChangeLogger: StateChangeLogger): Seq[AbstractControlRequest.Builder[_ <: AbstractControlRequest]] = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)
    val isTraceEnabled = stateChangeLog.isTraceEnabled

    if (version >= 3) {
      // Starting from version 3, only one request is sent combining the replicas to be
      // stopped and the replicas to be deleted.
      val stopReplicaTopicState = mutable.Map.empty[String, StopReplicaTopicState]

      partitions.foreach { case (topicPartition, partitionState) =>
        if (isTraceEnabled)
          stateChangeLog.trace(s"Sending StopReplica request $partitionState to " +
            s"broker $brokerId for partition $topicPartition")

        val topicState = stopReplicaTopicState.getOrElseUpdate(topicPartition.topic,
          new StopReplicaTopicState().setTopicName(topicPartition.topic))
        topicState.partitionStates().add(partitionState)
      }

      stateChangeLog.info(s"Sending StopReplica request for ${partitions.size} " +
        s"replicas to broker $brokerId")
      Seq(new StopReplicaRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
        false, stopReplicaTopicState.values.toBuffer.asJava))
    } else {
      // Prior to version 3, one request with all the replicas to be deleted and one request
      // with all the replicas to be stopped are sent out.
      var numPartitionStateWithDelete = 0
      var numPartitionStateWithoutDelete = 0
      val topicStatesWithDelete = mutable.Map.empty[String, StopReplicaTopicState]
      val topicStatesWithoutDelete = mutable.Map.empty[String, StopReplicaTopicState]

      partitions.foreach { case (topicPartition, partitionState) =>
        if (isTraceEnabled)
          stateChangeLog.trace(s"Sending StopReplica request (delete = ${partitionState.deletePartition}) " +
            s"$partitionState to broker $brokerId for partition $topicPartition")

        val topicStates = if (partitionState.deletePartition) {
          numPartitionStateWithDelete += 1
          topicStatesWithDelete
        } else {
          numPartitionStateWithoutDelete += 1
          topicStatesWithoutDelete
        }

        val topicState = topicStates.getOrElseUpdate(topicPartition.topic,
          new StopReplicaTopicState().setTopicName(topicPartition.topic))
        topicState.partitionStates().add(partitionState)
      }

      val buffer = mutable.Buffer.empty[AbstractControlRequest.Builder[_ <: AbstractControlRequest]]
      def addRequest(topicStates: mutable.Map[String, StopReplicaTopicState],
                     deletePartitions: Boolean, numPartitions: Int): Unit = {
        if (topicStates.nonEmpty) {
          stateChangeLog.info(s"Sending StopReplica request (delete = $deletePartitions) for " +
            s"$numPartitions partitions to broker $brokerId")
          buffer += new StopReplicaRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
            deletePartitions, topicStates.values.toBuffer.asJava)
        }
      }
      addRequest(topicStatesWithDelete, true, numPartitionStateWithDelete)
      addRequest(topicStatesWithoutDelete, false, numPartitionStateWithoutDelete)
      buffer
    }
  }

  override def toString: String = "StopReplicaBatch(" +
    s"brokerId=$brokerId, " +
    s"version=$version, " +
    s"controllerId=$controllerId, " +
    s"controllerEpoch=$controllerEpoch, " +
    s"brokerEpoch=$brokerEpoch, " +
    s"partitions=${partitions.mkString(",")})"
}

/**
 * LeaderAndIsrBatch holds all the metadata necessary to build LeaderAndIsrRequests. It
 * accumulate any other LeaderAndIsrBatches and invalidates its metadata based on
 * StopReplicaBatches. LeaderAndIsrBatch prevents any UpdateMetadataBatch to jump in
 * front in the accumulator.
 */
class LeaderAndIsrBatch(brokerId: Int) extends ControlMetadataBatch {
  var version: Short = ApiKeys.LEADER_AND_ISR.latestVersion
  var controllerId: Int = 0
  var controllerEpoch: Int = 0
  var brokerEpoch: Long = 0
  var partitions = mutable.Map.empty[TopicPartition, LeaderAndIsrPartitionState]
  var liveLeaders = mutable.Set.empty[Node]
  var containsAllReplicas = false

  private def clear(): Unit = {
    partitions.clear()
    liveLeaders.clear()
  }

  def setVersion(version: Short): LeaderAndIsrBatch = {
    this.version = version
    this
  }

  def setControllerId(controllerId: Int): LeaderAndIsrBatch = {
    this.controllerId = controllerId
    this
  }

  def setControllerEpoch(controllerEpoch: Int): LeaderAndIsrBatch = {
    this.controllerEpoch = controllerEpoch
    this
  }

  def setBrokerEpoch(brokerEpoch: Long): LeaderAndIsrBatch = {
    this.brokerEpoch = brokerEpoch
    this
  }

  def addPartitionState(topicPartition: TopicPartition,
                        partitionState: LeaderAndIsrPartitionState): LeaderAndIsrBatch = {
    partitions.get(topicPartition) match {
      // If there is no accumulated partition state yet, we store it.
      case None =>
        partitions.put(topicPartition, partitionState)

      // If there is already an accumulated partition state, the new state supersedes the
      // existing one if it has a newer epoch. If the previous state is overwritten, we
      // ensure that the `isNew` flag is kept.
      case Some(existingPartitionState) =>
        if (partitionState.leaderEpoch > existingPartitionState.leaderEpoch) {
          if (existingPartitionState.isNew)
            partitionState.setIsNew(existingPartitionState.isNew)
          partitions.put(topicPartition, partitionState)
        }
    }
    this
  }

  def maybeInvalidatePartitionState(topicPartition: TopicPartition,
                                    leaderEpoch: Int): LeaderAndIsrBatch = {
    // The accumulated partition state is invalidated if it has a smaller epoch than
    // the current leader epoch or if the delete sentinel is used.
    partitions.get(topicPartition).foreach { partitionState =>
      if (leaderEpoch == LeaderAndIsr.EpochDuringDelete ||
          leaderEpoch > partitionState.leaderEpoch) {
        partitions.remove(topicPartition)
      }
    }
    this
  }

  def addLiveLeader(leader: Node): LeaderAndIsrBatch = {
    liveLeaders += leader
    this
  }

  def addLiveLeaders(leaders: Set[Node]): LeaderAndIsrBatch = {
    liveLeaders ++= leaders
    this
  }

  def setLiveLeaders(leaders: Set[Node]): LeaderAndIsrBatch = {
    liveLeaders.clear()
    addLiveLeaders(leaders)
  }

  def setContainsAllReplicas(): LeaderAndIsrBatch = {
    containsAllReplicas = true
    this
  }

  override def isEmpty: Boolean = partitions.isEmpty

  override def process(batch: ControlMetadataBatch): Status = {
    batch match {
      // A new LeaderAndIsr batch is merge into the current one.
      case leaderAndIsrBatch: LeaderAndIsrBatch =>
        version = leaderAndIsrBatch.version
        controllerId = leaderAndIsrBatch.controllerId
        controllerEpoch = leaderAndIsrBatch.controllerEpoch
        brokerEpoch = leaderAndIsrBatch.brokerEpoch

        // This should never happen but if it does at some point, a LeaderAndIsr
        // request with all the partitions/replicas supersedes the current one.
        if (leaderAndIsrBatch.containsAllReplicas) {
          clear()
          setContainsAllReplicas()
        }

        leaderAndIsrBatch.partitions.foreach { case (topicPartition, partitionState) =>
          addPartitionState(topicPartition, partitionState)
        }

        // While accumulating, all the live leaders are kept but only the relevant ones
        // are sent out at the end.
        addLiveLeaders(leaderAndIsrBatch.liveLeaders.toSet)

        ContinueMerged

      // A StopReplica batch is used to invalidate accumulated partition states if the
      // StopReplica batch has newer information.
      case stopReplicaBatch: StopReplicaBatch =>
        // The very first LeaderAndIsr request sent by the controller to the broker
        // contains all the partitions/replicas and sets the containsAllReplica flag.
        // The broker uses that information to delete stray partitions that are not
        // in the LeaderAndIsr request. We don't invalidate it in order to not consider
        // partitions that may have been deleted before this request is sent as stray
        // partitions nor we let it pass.
        if (containsAllReplicas)
          return Block

        stopReplicaBatch.partitions.foreach { case (topicPartition, partitionState) =>
          maybeInvalidatePartitionState(topicPartition, partitionState.leaderEpoch)
        }

        Continue

      // Prevent UpdateMetadata to jump in front of this batch because we want to
      // give priority to batches which updates the replica states.
      case _: UpdateMetadataBatch =>
        Block
    }
  }

  override def requests(stateChangeLogger: StateChangeLogger): Seq[AbstractControlRequest.Builder[_ <: AbstractControlRequest]] = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)
    val isTraceEnabled = stateChangeLog.isTraceEnabled

    // Only the live leaders of the accumulated partition states are sent out. Stale live
    // leaders may have been left while accumulating.
    val leaderIds = mutable.Set.empty[Int]
    var numBecomeLeaders = 0
    partitions.foreach { case (topicPartition, partitionState) =>
      val typeOfRequest = if (partitionState.leader == brokerId) {
        numBecomeLeaders += 1
        "become-leader"
      } else {
        "become-follower"
      }
      if (isTraceEnabled) {
        stateChangeLog.trace(s"Sending $typeOfRequest LeaderAndIsr request $partitionState to " +
          s"broker $brokerId for partition $topicPartition")
      }
      leaderIds += partitionState.leader
    }
    val leaders = liveLeaders.filter(node => leaderIds.contains(node.id))

    stateChangeLog.info(s"Sending LeaderAndIsr request to broker $brokerId with $numBecomeLeaders " +
      s"become-leader and ${partitions.size - numBecomeLeaders} become-follower partitions")
    Seq(new LeaderAndIsrRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitions.values.toBuffer.asJava, leaders.asJava, containsAllReplicas))
  }

  override def toString: String = "LeaderAndIsrBatch(" +
    s"brokerId=$brokerId, " +
    s"version=$version, " +
    s"controllerId=$controllerId, " +
    s"controllerEpoch=$controllerEpoch, " +
    s"brokerEpoch=$brokerEpoch, " +
    s"partitions=${partitions.mkString(",")}, " +
    s"liveLeaders=${liveLeaders.mkString(",")}, " +
    s"containsAllReplicas=$containsAllReplicas)"
}

/**
 * UpdateMetadataBatch holds all the metadata necessary to build UpdateMetadataRequests. It
 * accumulate any other UpdateMetadataBatch but is never invalidated. We assume that the
 * controller will send another UpdateMetadataBatch which will invalidate state metadata
 * by accumulating it.
 */
class UpdateMetadataBatch(brokerId: Int) extends ControlMetadataBatch {
  var version: Short = ApiKeys.UPDATE_METADATA.latestVersion
  var controllerId: Int = 0
  var controllerEpoch: Int = 0
  var brokerEpoch: Long = 0
  var partitions = mutable.Map.empty[TopicPartition, UpdateMetadataPartitionState]
  var liveBrokers = mutable.Set.empty[Broker]

  def setVersion(version: Short): UpdateMetadataBatch = {
    this.version = version
    this
  }

  def setControllerId(controllerId: Int): UpdateMetadataBatch = {
    this.controllerId = controllerId
    this
  }

  def setControllerEpoch(controllerEpoch: Int): UpdateMetadataBatch = {
    this.controllerEpoch = controllerEpoch
    this
  }

  def setBrokerEpoch(brokerEpoch: Long): UpdateMetadataBatch = {
    this.brokerEpoch = brokerEpoch
    this
  }

  def addPartitionState(topicPartition: TopicPartition,
                        partitionState: UpdateMetadataPartitionState): UpdateMetadataBatch = {
    // We assume that any new UpdateMetadata batch will have newer information so we
    // always overwrite any previous state.
    partitions.put(topicPartition, partitionState)
    this
  }

  def addLiveBrokers(brokers: Set[Broker]): UpdateMetadataBatch = {
    liveBrokers ++= brokers
    this
  }

  def setLiveBrokers(brokers: Set[Broker]): UpdateMetadataBatch = {
    liveBrokers.clear()
    addLiveBrokers(brokers)
  }

  override def isEmpty: Boolean = partitions.isEmpty && liveBrokers.isEmpty

  override def process(batch: ControlMetadataBatch): Status = {
    batch match {
      // A new UpdateMetadata batch is merge into the current one.
      case updateMetadataBatch: UpdateMetadataBatch =>
        version = updateMetadataBatch.version
        controllerId = updateMetadataBatch.controllerId
        controllerEpoch = updateMetadataBatch.controllerEpoch
        brokerEpoch = updateMetadataBatch.brokerEpoch

        updateMetadataBatch.partitions.foreach { case (topicPartition, partitionState) =>
          addPartitionState(topicPartition, partitionState)
        }

        // Every batch comes with all the current live brokers in the cluster, therefore
        // we overwrite any previous view.
        setLiveBrokers(updateMetadataBatch.liveBrokers.toSet)

        ContinueMerged

      // We give priority to updating the replica states over updating the metadata cache so
      // we let LeaderAndIsr and StopReplica batch jump in front of a UpdateMetadata batch.
      // We never invalidate UpdateMetadata batch because we assume that a new UpdateMetadata
      // batch will always follow any LeaderAndIsr or StopReplica batches.
      case _ =>
        Continue
    }
  }

  override def requests(stateChangeLogger: StateChangeLogger): Seq[AbstractControlRequest.Builder[_ <: AbstractControlRequest]] = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)
    stateChangeLog.info(s"Sending UpdateMetadata request to broker $brokerId " +
      s"for ${partitions.size} partitions")

    val liveBrokers = this.liveBrokers.iterator.map { broker =>
      val endpoints = if (version == 0) {
        // Version 0 of UpdateMetadataRequest only supports PLAINTEXT
        val securityProtocol = SecurityProtocol.PLAINTEXT
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val node = broker.node(listenerName)
        Seq(new UpdateMetadataEndpoint()
          .setHost(node.host)
          .setPort(node.port)
          .setSecurityProtocol(securityProtocol.id)
          .setListener(listenerName.value))
      } else {
        broker.endPoints.map { endpoint =>
          new UpdateMetadataEndpoint()
            .setHost(endpoint.host)
            .setPort(endpoint.port)
            .setSecurityProtocol(endpoint.securityProtocol.id)
            .setListener(endpoint.listenerName.value)
        }
      }
      new UpdateMetadataBroker()
        .setId(broker.id)
        .setEndpoints(endpoints.asJava)
        .setRack(broker.rack.orNull)
    }.toSet

    Seq(new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, brokerEpoch,
      partitions.values.toBuffer.asJava, liveBrokers.toBuffer.asJava))
  }

  override def toString: String = "UpdateMetadataBatch(" +
    s"brokerId=$brokerId, " +
    s"version=$version, " +
    s"controllerId=$controllerId, " +
    s"controllerEpoch=$controllerEpoch, " +
    s"brokerEpoch=$brokerEpoch, " +
    s"partitions=${partitions.mkString(",")}, " +
    s"liveBrokers=${liveBrokers.mkString(",")})"
}
