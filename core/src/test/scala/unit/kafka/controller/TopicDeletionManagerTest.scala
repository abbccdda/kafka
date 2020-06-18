/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.concurrent.CompletableFuture
import java.util.{Properties, UUID}

import kafka.cluster.{Broker, EndPoint}
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.tier.exceptions.TierMetadataRetriableException
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.topic.TierTopicManager
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.Assert._
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.mutable
import scala.concurrent.CancellationException

class TopicDeletionManagerTest {

  private val brokerId = 1
  private val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(brokerId, "zkConnect"))
  private val deletionClient = mock(classOf[DeletionClient])

  @Before
  def setup(): Unit = {
  }

  @Test
  def testInitialization(): Unit = {
    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar", "baz"),
      numPartitions = 2,
      replicationFactor = 3)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient, None)

    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(initialTopicsToBeDeleted = Set("foo", "bar"), initialTopicsIneligibleForDeletion = Set("bar", "baz"))

    assertEquals(Set("foo", "bar"), controllerContext.topicsToBeDeleted.toSet)
    assertEquals(Set("bar"), controllerContext.topicsIneligibleForDeletion.toSet)
  }

  @Test
  def testBasicDeletion(): Unit = {
    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3)
    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient, None)
    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet
    val barPartitions = controllerContext.partitionsForTopic("bar")
    val barReplicas = controllerContext.replicasForPartition(barPartitions).toSet

    // Clean up state changes before starting the deletion
    replicaStateMachine.clear()
    partitionStateMachine.clear()

    // Queue the topic for deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo", "bar"))

    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(fooReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    assertEquals(barPartitions, controllerContext.partitionsInState("bar", NonExistentPartition))
    assertEquals(barReplicas, controllerContext.replicasInState("bar", ReplicaDeletionStarted))
    verify(deletionClient).sendMetadataUpdate(fooPartitions ++ barPartitions)
    assertEquals(Set("foo", "bar"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo", "bar"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    // Complete the deletion
    deletionManager.completeReplicaDeletion(fooReplicas ++ barReplicas)

    assertEquals(Set.empty, controllerContext.partitionsForTopic("foo"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "foo"))
    assertEquals(Set.empty, controllerContext.partitionsForTopic("bar"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "bar"))
    assertEquals(Set(), controllerContext.topicsToBeDeleted)
    assertEquals(Set(), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    assertEquals(1, partitionStateMachine.stateChangesCalls(OfflinePartition))
    assertEquals(1, partitionStateMachine.stateChangesCalls(NonExistentPartition))

    assertEquals(1, replicaStateMachine.stateChangesCalls(ReplicaDeletionIneligible))
    assertEquals(1, replicaStateMachine.stateChangesCalls(OfflineReplica))
    assertEquals(1, replicaStateMachine.stateChangesCalls(ReplicaDeletionStarted))
    assertEquals(1, replicaStateMachine.stateChangesCalls(ReplicaDeletionSuccessful))
  }

  @Test
  def testBasicDeletionOfTieredTopic(): Unit = {
    val brokerProperties = TestUtils.createBrokerConfig(brokerId, "zkConnect")
    brokerProperties.put(KafkaConfig.TierFeatureProp, "true")
    val config = KafkaConfig.fromProps(brokerProperties)

    val topicProperties = new Properties()
    topicProperties.setProperty(LogConfig.TierEnableProp, "true")

    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3,
      addTopicId = true)
    val tierTopicManager = mock(classOf[TierTopicManager])
    val future = new CompletableFuture[AppendResult]()
    when(tierTopicManager.addMetadata(any())).thenReturn(future)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient, Some(tierTopicManager))
    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet

    // Queue the topic for deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo"))

    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(fooReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    verify(deletionClient).sendMetadataUpdate(fooPartitions)
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    // Attempt to complete deletion will not succeed until future is completed successfully
    deletionManager.completeReplicaDeletion(fooReplicas)
    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    var finishInvoked = false
    when(deletionClient.completeDeleteTopic(any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        deletionManager.finishTopicDelete(invocation.getArgument(0))
        finishInvoked = true
      }
    })

    // Completing the future will complete the deletion
    future.complete(AppendResult.ACCEPTED)
    TestUtils.waitUntilTrue(() => finishInvoked == true, "Timed out waiting for deletion to be completed")
    assertEquals(Set.empty, controllerContext.partitionsForTopic("foo"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "foo"))
    assertEquals(Set(), controllerContext.topicsToBeDeleted)
    assertEquals(Set(), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
  }

  @Test
  def testExceptionWhenDeletingTieredTopic(): Unit = {
    val brokerProperties = TestUtils.createBrokerConfig(brokerId, "zkConnect")
    brokerProperties.put(KafkaConfig.TierFeatureProp, "true")
    val config = KafkaConfig.fromProps(brokerProperties)

    val topicProperties = new Properties()
    topicProperties.setProperty(LogConfig.TierEnableProp, "true")

    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3,
      addTopicId = true)

    val futureWithException = new CompletableFuture[AppendResult]()
    futureWithException.completeExceptionally(new TierMetadataRetriableException("exception on tier topic append"))

    val successfulFuture = CompletableFuture.completedFuture(AppendResult.ACCEPTED)

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.addMetadata(any())).thenReturn(futureWithException)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient, Some(tierTopicManager))
    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet

    // Queue the topic for deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo"))

    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(fooReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    verify(deletionClient).sendMetadataUpdate(fooPartitions)
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    // Complete deletion. Deletion will fail because of failed tier topic append.
    deletionManager.completeReplicaDeletion(fooReplicas)
    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    when(deletionClient.completeDeleteTopic(any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        deletionManager.finishTopicDelete(invocation.getArgument(0))
      }
    })

    // Retry deletion
    when(tierTopicManager.addMetadata(any())).thenReturn(successfulFuture)
    deletionManager.enqueueTopicsForDeletion(Set("foo"))
    deletionManager.completeReplicaDeletion(fooReplicas)
    assertEquals(Set.empty, controllerContext.partitionsForTopic("foo"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "foo"))
    assertEquals(Set(), controllerContext.topicsToBeDeleted)
    assertEquals(Set(), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
  }

  @Test
  def testDeletionDuringShutdownProcess(): Unit = {
    // Tests deletion manager handling during shutdown
    // CPKAFKA-4272 shuts down the TierTopicManager in advance of the controller
    // After doing so, addMetadata calls will result in futures completed with CancellationException(s)
    // This test is very similar to testExceptionWhenDeletingTieredTopic, however it tests CancellationException(s)
    // for correct behavior as these exceptions are non retriable
    val brokerProperties = TestUtils.createBrokerConfig(brokerId, "zkConnect")
    brokerProperties.put(KafkaConfig.TierFeatureProp, "true")
    val config = KafkaConfig.fromProps(brokerProperties)

    val topicProperties = new Properties()
    topicProperties.setProperty(LogConfig.TierEnableProp, "true")

    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3,
      addTopicId = true)

    val futureWithException = new CompletableFuture[AppendResult]()
    futureWithException.completeExceptionally(new CancellationException("TierTopicManager is shutting down"))

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.addMetadata(any())).thenReturn(futureWithException)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient, Some(tierTopicManager))
    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet

    // Queue the topic for deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo"))

    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(fooReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    verify(deletionClient).sendMetadataUpdate(fooPartitions)
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)

    // Complete deletion. Deletion will fail as a result of the cancellation exception in the add metadata call
    deletionManager.completeReplicaDeletion(fooReplicas)
    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
  }

  @Test
  def testDeletionWithBrokerOffline(): Unit = {
    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient, None)
    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet

    // Broker 2 is taken offline
    val failedBrokerId = 2
    val offlineBroker = controllerContext.liveOrShuttingDownBroker(failedBrokerId).get
    val lastEpoch = controllerContext.liveBrokerIdAndEpochs(failedBrokerId)
    controllerContext.removeLiveBrokers(Set(failedBrokerId))
    assertEquals(Set(1, 3), controllerContext.liveBrokerIds)

    val (offlineReplicas, onlineReplicas) = fooReplicas.partition(_.replica == failedBrokerId)
    replicaStateMachine.handleStateChanges(offlineReplicas.toSeq, OfflineReplica)

    // Start topic deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo"))
    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    verify(deletionClient).sendMetadataUpdate(fooPartitions)
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionIneligible))

    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set("foo"), controllerContext.topicsIneligibleForDeletion)

    // Deletion succeeds for online replicas
    deletionManager.completeReplicaDeletion(onlineReplicas)

    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set("foo"), controllerContext.topicsIneligibleForDeletion)
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionSuccessful))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", OfflineReplica))

    // Broker 2 comes back online and deletion is resumed
    controllerContext.addLiveBrokers(Map(offlineBroker -> (lastEpoch + 1L)))
    deletionManager.resumeDeletionForTopics(Set("foo"))

    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionSuccessful))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))

    deletionManager.completeReplicaDeletion(offlineReplicas)
    assertEquals(Set.empty, controllerContext.partitionsForTopic("foo"))
    assertEquals(Set.empty[PartitionAndReplica], controllerContext.replicaStates.keySet.filter(_.topic == "foo"))
    assertEquals(Set(), controllerContext.topicsToBeDeleted)
    assertEquals(Set(), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
  }

  @Test
  def testBrokerFailureAfterDeletionStarted(): Unit = {
    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3)

    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient, None)
    deletionManager.init(Set.empty, Set.empty)

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet

    // Queue the topic for deletion
    deletionManager.enqueueTopicsForDeletion(Set("foo"))
    assertEquals(fooPartitions, controllerContext.partitionsInState("foo", NonExistentPartition))
    assertEquals(fooReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))

    // Broker 2 fails
    val failedBrokerId = 2
    val offlineBroker = controllerContext.liveOrShuttingDownBroker(failedBrokerId).get
    val lastEpoch = controllerContext.liveBrokerIdAndEpochs(failedBrokerId)
    controllerContext.removeLiveBrokers(Set(failedBrokerId))
    assertEquals(Set(1, 3), controllerContext.liveBrokerIds)
    val (offlineReplicas, onlineReplicas) = fooReplicas.partition(_.replica == failedBrokerId)

    // Fail replica deletion
    deletionManager.failReplicaDeletion(offlineReplicas)
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set("foo"), controllerContext.topicsIneligibleForDeletion)
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionIneligible))
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))

    // Broker 2 is restarted. The offline replicas remain ineligable
    // (TODO: this is probably not desired)
    controllerContext.addLiveBrokers(Map(offlineBroker -> (lastEpoch + 1L)))
    deletionManager.resumeDeletionForTopics(Set("foo"))
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionIneligible))

    // When deletion completes for the replicas which started, then deletion begins for the remaining ones
    deletionManager.completeReplicaDeletion(onlineReplicas)
    assertEquals(Set("foo"), controllerContext.topicsToBeDeleted)
    assertEquals(Set("foo"), controllerContext.topicsWithDeletionStarted)
    assertEquals(Set(), controllerContext.topicsIneligibleForDeletion)
    assertEquals(onlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionSuccessful))
    assertEquals(offlineReplicas, controllerContext.replicasInState("foo", ReplicaDeletionStarted))

  }

  @Test
  def testMultipleTieredTopicDeletions(): Unit = {
    val brokerProperties = TestUtils.createBrokerConfig(brokerId, "zkConnect")
    brokerProperties.put(KafkaConfig.TierFeatureProp, "true")
    val config = KafkaConfig.fromProps(brokerProperties)

    val topicProperties = new Properties()
    topicProperties.setProperty(LogConfig.TierEnableProp, "true")

    val tierTopicManager = mock(classOf[TierTopicManager])
    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    val controllerContext = initContext(
      brokers = Seq(1, 2, 3),
      topics = Set("foo", "bar"),
      numPartitions = 2,
      replicationFactor = 3,
      addTopicId = true)
    val replicaStateMachine = new MockReplicaStateMachine(controllerContext)
    replicaStateMachine.startup()

    val partitionStateMachine = new MockPartitionStateMachine(controllerContext, uncleanLeaderElectionEnabled = false)
    partitionStateMachine.startup()

    val deletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
      partitionStateMachine, deletionClient, Some(tierTopicManager))
    assertTrue(deletionManager.isDeleteTopicEnabled)
    deletionManager.init(Set.empty, Set.empty)

    val deletionsBeingCompleted = mutable.ListBuffer[String]()

    when(deletionClient.completeDeleteTopic(any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        deletionsBeingCompleted += invocation.getArgument(0)
      }
    })

    val fooPartitions = controllerContext.partitionsForTopic("foo")
    val fooReplicas = controllerContext.replicasForPartition(fooPartitions).toSet

    val barPartitions = controllerContext.partitionsForTopic("bar")
    val barReplicas = controllerContext.replicasForPartition(barPartitions).toSet

    // complete deletion for "foo"
    deletionManager.enqueueTopicsForDeletion(Set("foo"))
    deletionManager.completeReplicaDeletion(fooReplicas)

    assertEquals(List("foo"), deletionsBeingCompleted)

    // complete deletion for "bar"
    deletionManager.enqueueTopicsForDeletion(Set("bar"))
    deletionManager.completeReplicaDeletion(barReplicas)

    assertEquals(List("foo", "bar"), deletionsBeingCompleted)
  }

  def initContext(brokers: Seq[Int],
                  topics: Set[String],
                  numPartitions: Int,
                  replicationFactor: Int,
                  addTopicId: Boolean = false): ControllerContext = {
    val context = new ControllerContext
    val brokerEpochs = brokers.map { brokerId =>
      val endpoint = new EndPoint("localhost", 9900 + brokerId, new ListenerName("blah"),
        SecurityProtocol.PLAINTEXT)
      Broker(brokerId, Seq(endpoint), rack = None) -> 1L
    }.toMap
    context.setLiveBrokers(brokerEpochs)

    // Simple round-robin replica assignment
    var leaderIndex = 0
    for (topic <- topics; partitionId <- 0 until numPartitions) {
      val partition = new TopicPartition(topic, partitionId)
      val replicas = (0 until replicationFactor).map { i =>
        val replica = brokers((i + leaderIndex) % brokers.size)
        replica
      }
      context.updatePartitionFullReplicaAssignment(partition, ReplicaAssignment(replicas, Seq.empty))
      leaderIndex += 1
    }

    if (addTopicId) {
      topics.foreach { topic =>
        context.addTopicId(topic, UUID.randomUUID)
      }
    }

    context
  }

}
