package kafka.tier.retention

import java.util
import java.util.UUID
import java.util.concurrent.{CompletableFuture, ExecutionException, Future}

import kafka.log.{AbstractLog, LogConfig}
import kafka.server.ReplicaManager
import kafka.tier.domain.{TierSegmentDeleteComplete, TierSegmentDeleteInitiate}
import kafka.tier.retention.TierRetentionManager.InitiateDelete
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.{TierMetadataManager, TierTopicManager, TopicIdPartition}
import kafka.utils.{MockTime, TestUtils}
import org.junit.Assert.{assertThrows, assertTrue}
import org.junit.function.ThrowingRunnable
import org.junit.{Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}

import scala.collection.JavaConverters._

class TierRetentionManagerStateTest {
  val time = new MockTime()
  val scheduler = time.scheduler
  val replicaManager = mock(classOf[ReplicaManager])
  val tierTopicManager = mock(classOf[TierTopicManager])
  val tierMetadataManager = mock(classOf[TierMetadataManager])
  val tierObjectStore = mock(classOf[TierObjectStore])
  val log = mock(classOf[AbstractLog])
  val logConfig = mock(classOf[LogConfig])
  val tierRetentionManager = new TierRetentionManager(scheduler, replicaManager, tierMetadataManager, tierTopicManager,
    tierObjectStore, retentionCheckMs = 10L, time)

  @Before
  def setup(): Unit = {
    when(replicaManager.getLog(any())).thenReturn(Some(log))
    when(log.config).thenReturn(logConfig)
    when(logConfig.fileDeleteDelayMs).thenReturn(1L)
  }

  @Test
  def testSuccessfulTransitions(): Unit = {
    val epoch = 0
    val topicIdPartition = randomTopicIdPartition
    val segments = List(randomObjectMetadata(topicIdPartition), randomObjectMetadata(topicIdPartition), randomObjectMetadata(topicIdPartition))
    val expectedInitiateDelete = segments.map { metadata =>
      new TierSegmentDeleteInitiate(metadata.topicIdPartition, epoch, metadata.objectId)
    }
    val expectedCompleteDelete = segments.map { metadata =>
      new TierSegmentDeleteComplete(metadata.topicIdPartition, epoch, metadata.objectId)
    }
    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))

    val initiateDelete: State = InitiateDelete(epoch, new util.LinkedList(segments.asJava))
    val future = initiateDelete.transition(replicaManager, tierTopicManager, tierObjectStore, scheduler)
    val inProgress = new util.HashMap[TopicIdPartition, Future[Option[State]]]()
    inProgress.put(topicIdPartition, future)

    while (inProgress.size > 0) {
      tierRetentionManager.makeTransitions(inProgress)
      time.sleep(10)
    }

    expectedInitiateDelete.foreach { initiateDelete =>
      verify(tierTopicManager, times(1)).addMetadata(initiateDelete)
    }
    segments.foreach { segment =>
      verify(tierObjectStore, times(1)).deleteSegment(segment)
    }
    expectedCompleteDelete.foreach { completeDelete =>
      verify(tierTopicManager, times(1)).addMetadata(completeDelete)
    }
  }

  @Test
  def testFencedAppendExitsStateMachine(): Unit = {
    val epoch = 0
    val topicIdPartition = randomTopicIdPartition
    val segments = List(randomObjectMetadata(topicIdPartition), randomObjectMetadata(topicIdPartition), randomObjectMetadata(topicIdPartition))
    val initiateDelete: State = InitiateDelete(epoch, new util.LinkedList(segments.asJava))
    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.FENCED))

    val inProgress = new util.HashMap[TopicIdPartition, Future[Option[State]]]()
    val future = initiateDelete.transition(replicaManager, tierTopicManager, tierObjectStore, scheduler)

    inProgress.put(topicIdPartition, future)
    tierRetentionManager.makeTransitions(inProgress)
    assertTrue(inProgress.isEmpty)
    assertThrows(classOf[ExecutionException], new ThrowingRunnable {
      override def run(): Unit = future.get
    })
  }

  private def randomTopicIdPartition: TopicIdPartition = new TopicIdPartition(TestUtils.tempTopic, UUID.randomUUID, TestUtils.random.nextInt)

  private def randomObjectMetadata(topicIdPartition: TopicIdPartition): TierObjectStore.ObjectMetadata = {
    val random = TestUtils.random
    new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID, random.nextInt, random.nextLong)
  }
}
