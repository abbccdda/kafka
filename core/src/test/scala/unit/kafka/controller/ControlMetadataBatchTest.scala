// (Copyright) [2020 - 2020] Confluent, Inc.
package kafka.controller

import java.util.concurrent.{Executors, TimeUnit}

import kafka.api.LeaderAndIsr
import kafka.cluster.{Broker, EndPoint}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.junit.{After, Test}
import org.junit.Assert._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Random

class ControlMetadataAccumulatorTest {
  val nThreads = 5
  val executor = Executors.newFixedThreadPool(nThreads)

  @After
  def tearDown(): Unit = {
    if (executor != null)
      executor.shutdownNow()
  }

  @Test
  def testCanPutAndTake(): Unit = {
    val accumulator = new ControlMetadataAccumulator
    val stopReplica = new StopReplicaBatch(0)
      .addPartitionState(new TopicPartition("topic", 0),
        new StopReplicaPartitionState()
          .setPartitionIndex(0)
          .setLeaderEpoch(0))
    accumulator.put(newQueueItem(stopReplica))

    val item = accumulator.take()
    assertEquals(item.batch, stopReplica)
  }

  @Test
  def testPutMergeAndInvalidate(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val accumulator = new ControlMetadataAccumulator

    val leaderAndIsr = new LeaderAndIsrBatch(0)
      .addPartitionState(tp0, new LeaderAndIsrPartitionState().setLeaderEpoch(1))
      .addPartitionState(tp1, new LeaderAndIsrPartitionState().setLeaderEpoch(1))
    accumulator.put(newQueueItem(leaderAndIsr))

    val stopReplica1 = new StopReplicaBatch(0)
      .addPartitionState(tp0, new StopReplicaPartitionState().setLeaderEpoch(2))
    accumulator.put(newQueueItem(stopReplica1))

    val stopReplica2 = new StopReplicaBatch(0)
      .addPartitionState(tp0, new StopReplicaPartitionState().setLeaderEpoch(3))
    accumulator.put(newQueueItem(stopReplica2))

    assertEquals(2, accumulator.size())

    val firstItem = accumulator.take()
    assertTrue(firstItem.batch.isInstanceOf[LeaderAndIsrBatch])
    val takenLeaderAndIsr = firstItem.batch.asInstanceOf[LeaderAndIsrBatch]
    assertEquals(1, takenLeaderAndIsr.partitions.size)
    assertNotNull(takenLeaderAndIsr.partitions.get(tp1).orNull)

    val secondItem = accumulator.take()
    assertTrue(secondItem.batch.isInstanceOf[StopReplicaBatch])
    val takenStopReplica = secondItem.batch.asInstanceOf[StopReplicaBatch]
    assertEquals(1, takenStopReplica.partitions.size)
    assertNotNull(takenStopReplica.partitions.get(tp0).orNull)
  }

  @Test
  def testPutRespectBlockStatus(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val tp2 = new TopicPartition("topic", 3)
    val accumulator = new ControlMetadataAccumulator

    // Add a StopReplica batch
    val stopReplica1 = new StopReplicaBatch(0)
      .addPartitionState(tp0, new StopReplicaPartitionState().setLeaderEpoch(0))
    accumulator.put(newQueueItem(stopReplica1))

    // Add a LeaderAndIsr batch with containsAllReplicas = true
    val leaderAndIsr1 = new LeaderAndIsrBatch(0)
      .addPartitionState(tp1, new LeaderAndIsrPartitionState().setLeaderEpoch(1))
      .addPartitionState(tp2, new LeaderAndIsrPartitionState().setLeaderEpoch(1))
      .setContainsAllReplicas() // Block StopReplicaBatch
    accumulator.put(newQueueItem(leaderAndIsr1))

    // Add another LeaderAndIsr batch. It must be merged into the previous one.
    val leaderAndIsr2 = new LeaderAndIsrBatch(0)
      .addPartitionState(tp1, new LeaderAndIsrPartitionState().setLeaderEpoch(3))
      .addPartitionState(tp2, new LeaderAndIsrPartitionState().setLeaderEpoch(3))
    accumulator.put(newQueueItem(leaderAndIsr2))

    // Add another StopReplica batch. It won't invalidate the previous LeaderAndIsr
    // due to containsAllReplicas AND it won't be able to move past it neither for
    // the same reason.
    val stopReplica2 = new StopReplicaBatch(0)
      .addPartitionState(tp1, new StopReplicaPartitionState().setLeaderEpoch(4))
    accumulator.put(newQueueItem(stopReplica2))

    assertEquals(3, accumulator.size())

    val takenStopReplica1 = accumulator.take().batch.asInstanceOf[StopReplicaBatch]
    assertEquals(1, takenStopReplica1.partitions.size)
    assertNotNull(takenStopReplica1.partitions.get(tp0).orNull)

    val takenLeaderAndIsr1 = accumulator.take().batch.asInstanceOf[LeaderAndIsrBatch]
    assertEquals(2, takenLeaderAndIsr1.partitions.size)
    assertNotNull(takenLeaderAndIsr1.partitions.get(tp1).orNull)
    assertNotNull(takenLeaderAndIsr1.partitions.get(tp2).orNull)

    val takenStopReplica2 = accumulator.take().batch.asInstanceOf[StopReplicaBatch]
    assertEquals(1, takenStopReplica2.partitions.size)
    assertNotNull(takenStopReplica2.partitions.get(tp1).orNull)
  }

  @Test
  def testPutRemoveEmptyBatches(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val accumulator = new ControlMetadataAccumulator

    val leaderAndIsr1 = new LeaderAndIsrBatch(0)
      .addPartitionState(tp0, new LeaderAndIsrPartitionState().setLeaderEpoch(1))
    accumulator.put(newQueueItem(leaderAndIsr1))

    val stopReplica1 = new StopReplicaBatch(0)
      .addPartitionState(tp0, new StopReplicaPartitionState().setLeaderEpoch(2))
    accumulator.put(newQueueItem(stopReplica1))

    assertEquals(1, accumulator.size())

    val item1 = accumulator.take()
    assertTrue(item1.batch.isInstanceOf[StopReplicaBatch])
    val takenStopReplica1 = item1.batch.asInstanceOf[StopReplicaBatch]
    assertEquals(1, takenStopReplica1.partitions.size)
    assertNotNull(takenStopReplica1.partitions.get(tp0).orNull)

    assertEquals(0, accumulator.size())
  }

  @Test
  def testTakeBlocks(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val tp2 = new TopicPartition("topic", 2)
    val accumulator = new ControlMetadataAccumulator

    val futures = (0 until 3).map(_ => executor.submit(() => accumulator.take()))
    assertTrue(futures.forall(!_.isDone))

    val leaderAndIsr1 = new LeaderAndIsrBatch(0)
      .addPartitionState(tp0, new LeaderAndIsrPartitionState().setLeaderEpoch(1))
    accumulator.put(newQueueItem(leaderAndIsr1))

    val stopReplica1 = new StopReplicaBatch(0)
      .addPartitionState(tp1, new StopReplicaPartitionState().setLeaderEpoch(1))
    accumulator.put(newQueueItem(stopReplica1))

    val updateMetadata1 = new UpdateMetadataBatch(0)
      .addPartitionState(tp2, new UpdateMetadataPartitionState().setLeaderEpoch(1))
    accumulator.put(newQueueItem(updateMetadata1))

    assertTrue(futures.forall(_.get.isInstanceOf[QueueItem]))

    accumulator.close()
  }

  @Test
  def testConcurrentPutsAndTakesInRandomOrder(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val tp2 = new TopicPartition("topic", 2)
    val accumulator = new ControlMetadataAccumulator

    val leaderAndIsr1 = new LeaderAndIsrBatch(0)
      .addPartitionState(tp0, new LeaderAndIsrPartitionState().setLeaderEpoch(1))
    val stopReplica1 = new StopReplicaBatch(0)
      .addPartitionState(tp1, new StopReplicaPartitionState().setLeaderEpoch(1))
    val updateMetadata1 = new UpdateMetadataBatch(0)
      .addPartitionState(tp2, new UpdateMetadataPartitionState().setLeaderEpoch(1))

    val puts = List(
      new Put(accumulator, leaderAndIsr1),
      new Put(accumulator, stopReplica1),
      new Put(accumulator, updateMetadata1)
    )

    val takes = puts.indices.map(_ => new Take(accumulator))

    Random.shuffle(puts ++ takes).map(executor.submit)

    assertEquals(Set(true), puts.map(_.await()).toSet)
    assertEquals(Set(leaderAndIsr1, stopReplica1, updateMetadata1), takes.map(_.await()).toSet)
  }

  @Test
  def testClose(): Unit = {
    val accumulator = new ControlMetadataAccumulator
    val futures = (0 until 3).map(_ => executor.submit(() => accumulator.take()))
    assertTrue(futures.forall(!_.isDone))
    accumulator.close()
    assertTrue(futures.forall(_.get == null))
  }

  def newQueueItem(batch: ControlMetadataBatch): QueueItem = {
    QueueItem(batch, (_, _) => (), Time.SYSTEM.milliseconds)
  }

  trait Action[T] extends Runnable {
    def await(): T
  }

  class Put(val queue: ControlMetadataAccumulator,
            val batch: ControlMetadataBatch) extends Action[Boolean] {
    val promise = Promise[Boolean]
    def run(): Unit = {
      queue.put(newQueueItem(batch))
      promise.success(true)
    }
    def await(): Boolean = {
      Await.result(promise.future, Duration(10, TimeUnit.SECONDS))
    }
  }

  class Take(val queue: ControlMetadataAccumulator) extends Action[ControlMetadataBatch] {
    val promise = Promise[ControlMetadataBatch]
    def run(): Unit = {
      val item = queue.take()
      if (item == null)
        promise.success(null)
      else
        promise.success(item.batch)
    }
    def await(): ControlMetadataBatch = {
      Await.result(promise.future, Duration(10, TimeUnit.SECONDS))
    }
  }
}

class StopReplicaBatchTest {
  val stateChangeLogger = new StateChangeLogger(0, true, None)

  @Test
  def testBasicOperations(): Unit = {
    val batch = new StopReplicaBatch(0)

    assertEquals(ApiKeys.STOP_REPLICA.latestVersion, batch.version)
    assertEquals(0, batch.controllerId)
    assertEquals(0, batch.controllerEpoch)
    assertEquals(0, batch.brokerEpoch)
    assertTrue(batch.partitions.isEmpty)
    assertTrue(batch.isEmpty)

    val tp = new TopicPartition("topic", 0)
    val tpState = new StopReplicaPartitionState()
      .setPartitionIndex(0)
      .setLeaderEpoch(1)
      .setDeletePartition(true)

    batch
      .setVersion(ApiKeys.STOP_REPLICA.oldestVersion)
      .setControllerId(1)
      .setControllerEpoch(2)
      .setBrokerEpoch(3)
      .addPartitionState(tp, tpState)

    assertEquals(ApiKeys.STOP_REPLICA.oldestVersion, batch.version)
    assertEquals(1, batch.controllerId)
    assertEquals(2, batch.controllerEpoch)
    assertEquals(3, batch.brokerEpoch)
    assertFalse(batch.partitions.isEmpty)
    assertEquals(Map(tp -> tpState), batch.partitions)
    assertFalse(batch.isEmpty)
  }

  @Test
  def testAddPartitionState(): Unit = {
    val batch = new StopReplicaBatch(0)
    val tp = new TopicPartition("topic", 0)

    // state is added
    batch.addPartitionState(tp, makePartitionState(0, 0, false))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(0, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).deletePartition)

    // state with same leader epoch and delete partition = true overwrite the precedent
    batch.addPartitionState(tp, makePartitionState(0, 0, true))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(0, batch.partitions(tp).leaderEpoch)
    assertTrue(batch.partitions(tp).deletePartition)

    // state with same leader epoch and delete partition = false is ignored
    batch.addPartitionState(tp, makePartitionState(0, 0, false))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(0, batch.partitions(tp).leaderEpoch)
    assertTrue(batch.partitions(tp).deletePartition)

    // state with larger leader epoch overwrite the precedent
    batch.addPartitionState(tp, makePartitionState(0, 1, false))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(1, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).deletePartition)

    // state with smaller leader epoch is ignored
    batch.addPartitionState(tp, makePartitionState(0, 0, false))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(1, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).deletePartition)

    // state with sentinel epoch overwrite the precedent
    batch.addPartitionState(tp, makePartitionState(0, LeaderAndIsr.EpochDuringDelete, false))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(LeaderAndIsr.EpochDuringDelete, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).deletePartition)
  }

  @Test
  def testInvalidatePartitionState(): Unit = {
    val batch = new StopReplicaBatch(0)
    val tp = new TopicPartition("topic", 0)

    // state is added
    batch.addPartitionState(tp, makePartitionState(0, 5, false))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(5, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).deletePartition)

    // a smaller leader epoch does not invalidate
    batch.maybeInvalidatePartitionState(tp, 4)
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(5, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).deletePartition)

    // an equal leader epoch does not invalidate
    batch.maybeInvalidatePartitionState(tp, 5)
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(5, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).deletePartition)

    // a larger leader epoch does invalidate
    batch.maybeInvalidatePartitionState(tp, 6)
    assertNull(batch.partitions.get(tp).orNull)
  }

  @Test
  def testProcessWithStopReplicaBatch(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)

    val batch1 = new StopReplicaBatch(0)
      .setVersion((ApiKeys.STOP_REPLICA.latestVersion - 1).asInstanceOf[Short])
      .setControllerId(0)
      .setControllerEpoch(0)
      .setBrokerEpoch(0)
      .addPartitionState(tp0, makePartitionState(0, 1, false))

    val batch2 = new StopReplicaBatch(0)
      .setVersion(ApiKeys.STOP_REPLICA.latestVersion)
      .setControllerId(1)
      .setControllerEpoch(1)
      .setBrokerEpoch(1)
      .addPartitionState(tp0, makePartitionState(0, 2, true))
      .addPartitionState(tp1, makePartitionState(1, 1, false))

    // batch2 is merged into batch1
    assertEquals(ContinueMerged, batch1.process(batch2))

    assertEquals(ApiKeys.STOP_REPLICA.latestVersion, batch1.version)
    assertEquals(1, batch1.controllerId)
    assertEquals(1, batch1.controllerEpoch)
    assertEquals(1, batch1.brokerEpoch)
    assertEquals(Map(
      tp0 -> makePartitionState(0, 2, true),
      tp1 -> makePartitionState(1, 1, false)
    ), batch1.partitions)
  }

  @Test
  def testProcessWithLeaderAndIsrBatch(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)

    val stopReplicaBatch = new StopReplicaBatch(0)
      .addPartitionState(tp0, makePartitionState(0, 2, true))
      .addPartitionState(tp1, makePartitionState(1, 3, false))

    val leaderAndIsrBatch = new LeaderAndIsrBatch(0)
      .addPartitionState(tp0, new LeaderAndIsrPartitionState().setLeaderEpoch(3))

    // a larger leader epoch invalidates the previous one
    assertEquals(Continue, stopReplicaBatch.process(leaderAndIsrBatch))

    // tp0 is gone
    assertEquals(Map(tp1 -> makePartitionState(1, 3, false)),
      stopReplicaBatch.partitions)
  }

  @Test
  def testProcessWithUpdateMetadataBatch(): Unit = {
    assertEquals(Block, new StopReplicaBatch(0).process(new UpdateMetadataBatch(0)))
  }

  @Test
  def testBuildRequests(): Unit = {
    for (version <- ApiKeys.STOP_REPLICA.oldestVersion to ApiKeys.STOP_REPLICA.latestVersion) {
      testBuildRequests(version.asInstanceOf[Short])
    }
  }

  def testBuildRequests(version: Short): Unit = {
    val controllerId = 0
    val controllerEpoch = 1
    val brokerEpoch = 2
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val state0 = makePartitionState(0, 2, true)
    val state1 = makePartitionState(1, 3, false)

    val batch = new StopReplicaBatch(0)
      .setVersion(version)
      .setControllerId(controllerId)
      .setControllerEpoch(controllerEpoch)
      .setBrokerEpoch(brokerEpoch)
      .addPartitionState(tp0, state0)
      .addPartitionState(tp1, state1)

    val requests = batch.requests(stateChangeLogger)

    if (version >= 3) {
      assertEquals(1, requests.size)
      val stopReplicaRequest = requests.head.build().asInstanceOf[StopReplicaRequest]
      assertEquals(version, stopReplicaRequest.version)
      assertEquals(controllerId, stopReplicaRequest.controllerId)
      assertEquals(controllerEpoch, stopReplicaRequest.controllerEpoch)
      assertEquals(brokerEpoch, stopReplicaRequest.brokerEpoch)
      assertEquals(Map(tp0 -> state0, tp1 -> state1), stopReplicaRequest.partitionStates.asScala)
    } else {
      assertEquals(2, requests.size)

      val stopReplicaRequestWithDelete = requests(0).build().asInstanceOf[StopReplicaRequest]
      assertEquals(version, stopReplicaRequestWithDelete.version)
      assertEquals(controllerId, stopReplicaRequestWithDelete.controllerId)
      assertEquals(controllerEpoch, stopReplicaRequestWithDelete.controllerEpoch)
      assertEquals(brokerEpoch, stopReplicaRequestWithDelete.brokerEpoch)
      assertEquals(Map(tp0 -> makePartitionState(0, -1, true)),
        stopReplicaRequestWithDelete.partitionStates.asScala)

      val stopReplicaRequestWithoutDelete = requests(1).build().asInstanceOf[StopReplicaRequest]
      assertEquals(version, stopReplicaRequestWithoutDelete.version)
      assertEquals(controllerId, stopReplicaRequestWithoutDelete.controllerId)
      assertEquals(controllerEpoch, stopReplicaRequestWithoutDelete.controllerEpoch)
      assertEquals(brokerEpoch, stopReplicaRequestWithoutDelete.brokerEpoch)
      assertEquals(Map(tp1 -> makePartitionState(1, -1, false)),
        stopReplicaRequestWithoutDelete.partitionStates.asScala)
    }
  }

  def makePartitionState(partitionIndex: Int,
                         leaderEpoch: Int,
                         deletePartition: Boolean): StopReplicaPartitionState = {
    new StopReplicaPartitionState()
      .setPartitionIndex(partitionIndex)
      .setLeaderEpoch(leaderEpoch)
      .setDeletePartition(deletePartition)
  }
}

class LeaderAndIsrBatchTest {
  val stateChangeLogger = new StateChangeLogger(0, true, None)
  val Node0 = new Node(0, "", 9090)
  val Node1 = new Node(1, "", 9091)
  val Node2 = new Node(2, "", 9092)

  @Test
  def testBasicOperations(): Unit = {
    val batch = new LeaderAndIsrBatch(0)

    assertEquals(ApiKeys.LEADER_AND_ISR.latestVersion, batch.version)
    assertEquals(0, batch.controllerId)
    assertEquals(0, batch.controllerEpoch)
    assertEquals(0, batch.brokerEpoch)
    assertTrue(batch.partitions.isEmpty)
    assertTrue(batch.liveLeaders.isEmpty)
    assertFalse(batch.containsAllReplicas)
    assertTrue(batch.isEmpty)

    val tp = new TopicPartition("topic", 0)
    val tpState = new LeaderAndIsrPartitionState()
      .setLeader(0)
      .setLeaderEpoch(1)
      .setIsNew(false)

    batch
      .setVersion(ApiKeys.LEADER_AND_ISR.oldestVersion)
      .setControllerId(1)
      .setControllerEpoch(2)
      .setBrokerEpoch(3)
      .addPartitionState(tp, tpState)
      .addLiveLeaders(Set(Node0))
      .addLiveLeader(Node1)
      .setContainsAllReplicas()

    assertEquals(ApiKeys.STOP_REPLICA.oldestVersion, batch.version)
    assertEquals(1, batch.controllerId)
    assertEquals(2, batch.controllerEpoch)
    assertEquals(3, batch.brokerEpoch)
    assertFalse(batch.partitions.isEmpty)
    assertEquals(Map(tp -> tpState), batch.partitions)
    assertEquals(Set(Node0, Node1), batch.liveLeaders)
    assertTrue(batch.containsAllReplicas)
    assertFalse(batch.isEmpty)
  }

  @Test
  def testAddPartitionState(): Unit = {
    val batch = new LeaderAndIsrBatch(0)
    val tp = new TopicPartition("topic", 0)

    // state is added
    batch.addPartitionState(tp, makePartitionState(0, 0, false))
    assertEquals(0, batch.partitions(tp).leader)
    assertEquals(0, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).isNew)

    // state with same leader epoch is ignored
    batch.addPartitionState(tp, makePartitionState(0, 0, true))
    assertEquals(0, batch.partitions(tp).leader)
    assertEquals(0, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).isNew)

    // state with larger leader epoch overwrite precedent
    batch.addPartitionState(tp, makePartitionState(0, 1, true))
    assertEquals(0, batch.partitions(tp).leader)
    assertEquals(1, batch.partitions(tp).leaderEpoch)
    assertTrue(batch.partitions(tp).isNew)

    // once isNew is set, it is kept until the state is sent out
    batch.addPartitionState(tp, makePartitionState(0, 2, false))
    assertEquals(0, batch.partitions(tp).leader)
    assertEquals(2, batch.partitions(tp).leaderEpoch)
    assertTrue(batch.partitions(tp).isNew)
  }

  @Test
  def testInvalidatePartitionState(): Unit = {
    val batch = new LeaderAndIsrBatch(0)
    val tp = new TopicPartition("topic", 0)

    // state is added
    batch.addPartitionState(tp, makePartitionState(0, 5, false))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(5, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).isNew)

    // a smaller leader epoch does not invalidate
    batch.maybeInvalidatePartitionState(tp, 4)
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(5, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).isNew)

    // an equal leader epoch does not invalidate
    batch.maybeInvalidatePartitionState(tp, 5)
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(5, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).isNew)

    // a larger leader epoch does invalidate
    batch.maybeInvalidatePartitionState(tp, 6)
    assertNull(batch.partitions.get(tp).orNull)

    // state is re-added
    batch.addPartitionState(tp, makePartitionState(0, 5, false))
    assertEquals(0, batch.partitions(tp).partitionIndex)
    assertEquals(5, batch.partitions(tp).leaderEpoch)
    assertFalse(batch.partitions(tp).isNew)

    // a sentinel epoch does invalidate
    batch.maybeInvalidatePartitionState(tp, LeaderAndIsr.EpochDuringDelete)
    assertNull(batch.partitions.get(tp).orNull)
  }

  @Test
  def testProcessWithLeaderAndIsrBatch(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)

    val batch1 = new LeaderAndIsrBatch(0)
      .setVersion((ApiKeys.LEADER_AND_ISR.latestVersion - 1).asInstanceOf[Short])
      .setControllerId(0)
      .setControllerEpoch(0)
      .setBrokerEpoch(0)
      .addPartitionState(tp0, makePartitionState(0, 1, false))
      .addLiveLeaders(Set(Node0))

    val batch2 = new LeaderAndIsrBatch(0)
      .setVersion(ApiKeys.LEADER_AND_ISR.latestVersion)
      .setControllerId(1)
      .setControllerEpoch(1)
      .setBrokerEpoch(1)
      .addPartitionState(tp0, makePartitionState(1, 2, true))
      .addPartitionState(tp1, makePartitionState(1, 1, false))
      .addLiveLeaders(Set(Node1))

    // batch2 is merged into batch1
    assertEquals(ContinueMerged, batch1.process(batch2))

    assertEquals(ApiKeys.LEADER_AND_ISR.latestVersion, batch1.version)
    assertEquals(1, batch1.controllerId)
    assertEquals(1, batch1.controllerEpoch)
    assertEquals(1, batch1.brokerEpoch)
    assertEquals(Map(
      tp0 -> makePartitionState(1, 2, true),
      tp1 -> makePartitionState(1, 1, false)
    ), batch1.partitions)
    assertEquals(Set(Node0, Node1), batch1.liveLeaders)
  }

  @Test
  def testProcessWithLeaderAndIsrBatchWhenBatchContainsAllReplicas(): Unit = {
    assertEquals(ContinueMerged, new LeaderAndIsrBatch(0).setContainsAllReplicas()
      .process(new LeaderAndIsrBatch(0)))

    assertEquals(ContinueMerged, new LeaderAndIsrBatch(0).setContainsAllReplicas()
      .process(new LeaderAndIsrBatch(0).setContainsAllReplicas()))

    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val tp2 = new TopicPartition("topic", 2)

    val batch1 = new LeaderAndIsrBatch(0)
        .addPartitionState(tp0, makePartitionState(0, 1, false))
        .addLiveLeaders(Set(Node0))

    val batch2 = new LeaderAndIsrBatch(0)
        .setContainsAllReplicas()
        .addPartitionState(tp1, makePartitionState(1, 1, false))
        .addPartitionState(tp2, makePartitionState(2, 1, false))
        .addLiveLeaders(Set(Node1, Node2))

    assertEquals(ContinueMerged, batch1.process(batch2))

    assertTrue(batch1.containsAllReplicas)
    assertEquals(Map(
      tp1 -> makePartitionState(1, 1, false),
      tp2 -> makePartitionState(2, 1, false)
    ), batch1.partitions)
    assertEquals(Set(Node1, Node2), batch1.liveLeaders)
  }

  @Test
  def testProcessWithStopReplicaBatch(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val tp2 = new TopicPartition("topic", 2)

    val leaderAndIsrBatch = new LeaderAndIsrBatch(0)
      .addPartitionState(tp0, makePartitionState(0, 2, true))
      .addPartitionState(tp1, makePartitionState(1, 3, false))
      .addPartitionState(tp2, makePartitionState(2, 4, false))

    val stopReplicaBatch = new StopReplicaBatch(0)
      .addPartitionState(tp0, new StopReplicaPartitionState()
        .setLeaderEpoch(3))
      .addPartitionState(tp1, new StopReplicaPartitionState()
        .setLeaderEpoch(LeaderAndIsr.EpochDuringDelete))

    // larger leader epochs invalidates the previous ones
    assertEquals(Continue, leaderAndIsrBatch.process(stopReplicaBatch))

    // tp0 and tp1 are gone
    assertEquals(Map(tp2 -> makePartitionState(2, 4, false)),
      leaderAndIsrBatch.partitions)
  }

  @Test
  def testProcessWithStopReplicaBatchWhenBatchContainsAllReplicas(): Unit = {
    assertEquals(Block, new LeaderAndIsrBatch(0).setContainsAllReplicas()
      .process(new StopReplicaBatch(0)))
  }

  @Test
  def testProcessWithUpdateMetadataBatch(): Unit = {
    assertEquals(Block, new LeaderAndIsrBatch(0).process(new UpdateMetadataBatch(0)))
  }

  @Test
  def testBuildRequests(): Unit = {
    for (version <- ApiKeys.LEADER_AND_ISR.oldestVersion to ApiKeys.LEADER_AND_ISR.latestVersion) {
      testBuildRequests(version.asInstanceOf[Short])
    }
  }

  def testBuildRequests(version: Short): Unit = {
    val controllerId = 0
    val controllerEpoch = 1
    val brokerEpoch = 2
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val state0 = makePartitionState(0, 2, true)
    val state1 = makePartitionState(1, 3, false)

    val batch = new LeaderAndIsrBatch(0)
      .setVersion(version)
      .setControllerId(controllerId)
      .setControllerEpoch(controllerEpoch)
      .setBrokerEpoch(brokerEpoch)
      .addPartitionState(tp0, state0)
      .addPartitionState(tp1, state1)
      .addLiveLeaders(Set(Node0, Node1, Node2)) // Node2 is filtered out when the request is created

    val requests = batch.requests(stateChangeLogger)
    assertEquals(1, requests.size)
    val leaderAndIsrRequest = requests.head.build().asInstanceOf[LeaderAndIsrRequest]
    assertEquals(version, leaderAndIsrRequest.version)
    assertEquals(controllerId, leaderAndIsrRequest.controllerId)
    assertEquals(controllerEpoch, leaderAndIsrRequest.controllerEpoch)
    assertEquals(brokerEpoch, leaderAndIsrRequest.brokerEpoch)
    assertEquals(Set(state0, state1), leaderAndIsrRequest.partitionStates.asScala.toSet)
    assertEquals(Set(Node0.id, Node1.id), leaderAndIsrRequest.liveLeaders.asScala.map(_.brokerId).toSet)
  }

  def makePartitionState(leader: Int,
                         leaderEpoch: Int,
                         isNew: Boolean): LeaderAndIsrPartitionState = {
    new LeaderAndIsrPartitionState()
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
      .setIsNew(isNew)
  }
}

class UpdateMetadataBatchTest {
  val stateChangeLogger = new StateChangeLogger(0, true, None)
  val Broker0 = Broker(0, Seq(
    new EndPoint("localhost", 9090, new ListenerName("PLAINTEXT"), SecurityProtocol.PLAINTEXT),
    new EndPoint("localhost", 9190, new ListenerName("SASL_SSL"), SecurityProtocol.SASL_SSL)
  ), None)
  val Broker1 = Broker(1, Seq(
    new EndPoint("localhost", 9091, new ListenerName("PLAINTEXT"), SecurityProtocol.PLAINTEXT),
    new EndPoint("localhost", 9191, new ListenerName("SASL_SSL"), SecurityProtocol.SASL_SSL)
  ), None)

  @Test
  def testBasicOperations(): Unit = {
    val batch = new UpdateMetadataBatch(0)

    assertEquals(ApiKeys.UPDATE_METADATA.latestVersion, batch.version)
    assertEquals(0, batch.controllerId)
    assertEquals(0, batch.controllerEpoch)
    assertEquals(0, batch.brokerEpoch)
    assertTrue(batch.partitions.isEmpty)
    assertTrue(batch.liveBrokers.isEmpty)
    assertTrue(batch.isEmpty)

    val tp = new TopicPartition("topic", 0)
    val tpState = new UpdateMetadataPartitionState()
        .setLeader(0)
        .setLeaderEpoch(0)

    batch
      .setVersion(ApiKeys.UPDATE_METADATA.oldestVersion)
      .setControllerId(1)
      .setControllerEpoch(2)
      .setBrokerEpoch(3)
      .addPartitionState(tp, tpState)
      .setLiveBrokers(Set(Broker0))

    assertEquals(ApiKeys.UPDATE_METADATA.oldestVersion, batch.version)
    assertEquals(1, batch.controllerId)
    assertEquals(2, batch.controllerEpoch)
    assertEquals(3, batch.brokerEpoch)
    assertFalse(batch.partitions.isEmpty)
    assertEquals(Map(tp -> tpState), batch.partitions)
    assertEquals(Set(Broker0), batch.liveBrokers)
    assertFalse(batch.isEmpty)
  }

  @Test
  def testAddPartitionState(): Unit = {
    val batch = new UpdateMetadataBatch(0)
    val tp = new TopicPartition("topic", 0)

    // state is added
    batch.addPartitionState(tp, makePartitionState(0, 0))
    assertEquals(0, batch.partitions(tp).leader)
    assertEquals(0, batch.partitions(tp).leaderEpoch)

    // new state overwrite previous one
    batch.addPartitionState(tp, makePartitionState(0, 1))
    assertEquals(0, batch.partitions(tp).leader)
    assertEquals(1, batch.partitions(tp).leaderEpoch)

    // new state overwrite previous one
    batch.addPartitionState(tp, makePartitionState(0, 0))
    assertEquals(0, batch.partitions(tp).leader)
    assertEquals(0, batch.partitions(tp).leaderEpoch)
  }

  @Test
  def testProcessWithUpdateMetadataBatch(): Unit = {
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)

    val batch1 = new UpdateMetadataBatch(0)
      .setVersion((ApiKeys.UPDATE_METADATA.latestVersion - 1).asInstanceOf[Short])
      .setControllerId(0)
      .setControllerEpoch(0)
      .setBrokerEpoch(0)
      .addPartitionState(tp0, makePartitionState(0, 1))
      .setLiveBrokers(Set(Broker0))

    val batch2 = new UpdateMetadataBatch(0)
      .setVersion(ApiKeys.UPDATE_METADATA.latestVersion)
      .setControllerId(1)
      .setControllerEpoch(1)
      .setBrokerEpoch(1)
      .addPartitionState(tp0, makePartitionState(1, 2))
      .addPartitionState(tp1, makePartitionState(1, 1))
      .setLiveBrokers(Set(Broker1))

    // batch2 is merged into batch1
    assertEquals(ContinueMerged, batch1.process(batch2))

    assertEquals(ApiKeys.UPDATE_METADATA.latestVersion, batch1.version)
    assertEquals(1, batch1.controllerId)
    assertEquals(1, batch1.controllerEpoch)
    assertEquals(1, batch1.brokerEpoch)
    assertEquals(Map(
      tp0 -> makePartitionState(1, 2),
      tp1 -> makePartitionState(1, 1)
    ), batch1.partitions)
    assertEquals(Set(Broker1), batch1.liveBrokers)
  }

  @Test
  def testProcessWithLeaderAndIsrBatch(): Unit = {
    assertEquals(Continue, new UpdateMetadataBatch(0).process(new LeaderAndIsrBatch(0)))
  }

  @Test
  def testProcessWithStopReplicaBatch(): Unit = {
    assertEquals(Continue, new UpdateMetadataBatch(0).process(new StopReplicaBatch(0)))
  }

  @Test
  def testBuildRequests(): Unit = {
    for (version <- ApiKeys.UPDATE_METADATA.oldestVersion to ApiKeys.UPDATE_METADATA.latestVersion) {
      testBuildRequests(version.asInstanceOf[Short])
    }
  }

  def testBuildRequests(version: Short): Unit = {
    val controllerId = 0
    val controllerEpoch = 1
    val brokerEpoch = 2
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)
    val state0 = makePartitionState(0, 2)
    val state1 = makePartitionState(1, 3)

    val batch = new UpdateMetadataBatch(0)
      .setVersion(version)
      .setControllerId(controllerId)
      .setControllerEpoch(controllerEpoch)
      .setBrokerEpoch(brokerEpoch)
      .addPartitionState(tp0, state0)
      .addPartitionState(tp1, state1)
      .setLiveBrokers(Set(Broker0, Broker1))

    val requests = batch.requests(stateChangeLogger)
    assertEquals(1, requests.size)
    val updateMetadataRequest = requests.head.build().asInstanceOf[UpdateMetadataRequest]
    assertEquals(version, updateMetadataRequest.version)
    assertEquals(controllerId, updateMetadataRequest.controllerId)
    assertEquals(controllerEpoch, updateMetadataRequest.controllerEpoch)
    assertEquals(brokerEpoch, updateMetadataRequest.brokerEpoch)
    assertEquals(Set(state0, state1), updateMetadataRequest.partitionStates().asScala.toSet)
    assertEquals(Set(Broker0.id, Broker1.id), updateMetadataRequest.liveBrokers.asScala.map(_.id).toSet)

    val ports = updateMetadataRequest.liveBrokers.asScala.flatMap(_.endpoints.asScala.map(_.port)).toSet
    if (version == 0) {
      val securityProtocol = SecurityProtocol.PLAINTEXT
      val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
      assertEquals(Set(Broker0, Broker1).map(_.node(listenerName).port), ports)
    } else {
      assertEquals(Set(Broker0, Broker1).flatMap(_.endPoints.map(_.port)), ports)
    }
  }

  def makePartitionState(leader: Int, leaderEpoch: Int): UpdateMetadataPartitionState = {
    new UpdateMetadataPartitionState()
      .setLeader(leader)
      .setLeaderEpoch(leaderEpoch)
  }
}
