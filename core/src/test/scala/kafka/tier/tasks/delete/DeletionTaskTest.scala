/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.tasks.delete

import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.{Properties, UUID}

import kafka.log._
import kafka.server.ReplicaManager
import kafka.tier.domain.{TierObjectMetadata, TierSegmentDeleteComplete, TierSegmentDeleteInitiate}
import kafka.tier.fetcher.CancellationContext
import kafka.tier.state.TierPartitionState
import kafka.tier.state.TierPartitionState.AppendResult
import kafka.tier.store.TierObjectStore
import kafka.tier.tasks.delete.DeletionTask._
import kafka.tier.topic.TierTopicManager
import kafka.tier.TopicIdPartition
import kafka.utils.TestUtils
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.MockTime
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DeletionTaskTest {
  val topicIdPartition_1 = new TopicIdPartition("foo-0", UUID.randomUUID, 0)
  val tierPartitionState_1 = mockTierPartitionState(0)

  val topicIdPartition_2 = new TopicIdPartition("foo-1", UUID.randomUUID, 0)
  val tierPartitionState_2 = mockTierPartitionState(0)

  val ctx = CancellationContext.newContext()
  val tierTopicManager = mock(classOf[TierTopicManager])
  val tierObjectStore = mock(classOf[TierObjectStore])
  val replicaManager = mock(classOf[ReplicaManager])
  val time = new MockTime()
  val tmpFile = TestUtils.tempFile()
  var logWithTieredSegments: AbstractLog = _
  var emptyLog: AbstractLog = _

  @Before
  def setup(): Unit = {
    var baseOffset = 100L
    val tieredSegments = mutable.ListBuffer[TierLogSegment]()
    val localSegments = mutable.ListBuffer[LogSegment]()

    for (_ <- 0 to 15) {
      val segment = tieredLogSegment(topicIdPartition_1, baseOffset, baseOffset + 50)
      baseOffset += 51
      tieredSegments += segment
      time.sleep(50)
    }

    for (_ <- 0 to 5) {
      val segment = mockLogSegment(tmpFile, baseOffset, baseOffset + 50)
      baseOffset += 51
      localSegments += segment
    }

    val properties = new Properties()
    properties.put(LogConfig.RetentionMsProp, "300")
    val logConfig = LogConfig(properties)

    logWithTieredSegments = mockAbstractLog(tieredSegments.toList, localSegments.toList)
    when(logWithTieredSegments.config).thenReturn(logConfig)
    when(logWithTieredSegments.logStartOffset).thenReturn(0L)
    doAnswer(new Answer[Any] {
      override def answer(invocation: InvocationOnMock): Any = {
        val newStartOffset = invocation.getArgument(0).asInstanceOf[Long]
        if (logWithTieredSegments.logStartOffset < newStartOffset)
          when(logWithTieredSegments.logStartOffset).thenReturn(newStartOffset)
      }
    }).when(logWithTieredSegments).maybeIncrementLogStartOffset(any())

    emptyLog = mockAbstractLog(List.empty, List.empty)
    when(emptyLog.config).thenReturn(logConfig)

    when(replicaManager.getLog(topicIdPartition_1.topicPartition)).thenReturn(Some(logWithTieredSegments))
    when(replicaManager.getLog(topicIdPartition_2.topicPartition)).thenReturn(Some(emptyLog))

    when(logWithTieredSegments.tierPartitionState).thenReturn(tierPartitionState_1)
    when(emptyLog.tierPartitionState).thenReturn(tierPartitionState_2)

    when(tierTopicManager.addMetadata(any())).thenReturn(CompletableFuture.completedFuture(AppendResult.ACCEPTED))
  }

  @After
  def teardown(): Unit = {
    tmpFile.delete()
  }

  @Test
  def testCollectDeletableSegments(): Unit = {
    val state = CollectDeletableSegments(RetentionMetadata(replicaManager, leaderEpoch = 0))
    val future = state.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[InitiateDelete], nextState.getClass)

    val initiateDelete = nextState.asInstanceOf[InitiateDelete]
    assertTrue(initiateDelete.toDelete.nonEmpty)
  }

  @Test
  def testCollectDeletableSegmentsEmptyLog(): Unit = {
    val state = CollectDeletableSegments(RetentionMetadata(replicaManager, leaderEpoch = 0))
    val future = state.transition(topicIdPartition_2, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[CollectDeletableSegments], nextState.getClass)
  }

  @Test
  def testInitiateDelete(): Unit = {
    val toDelete = logWithTieredSegments.tieredLogSegments.take(3).map(_.metadata)
    val initiateDelete = InitiateDelete(RetentionMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])
    val future = initiateDelete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[Delete], nextState.getClass)

    val delete = nextState.asInstanceOf[Delete]
    assertEquals(toDelete.size, delete.toDelete.size)

    val firstSegment = toDelete.head
    val expected = new TierSegmentDeleteInitiate(topicIdPartition_1, 0, firstSegment.objectId)
    verify(tierTopicManager, times(1)).addMetadata(expected)
  }

  @Test
  def testDelete(): Unit = {
    val toDelete = logWithTieredSegments.tieredLogSegments.take(3).map(_.metadata)
    val delete = Delete(RetentionMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])
    val future = delete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[CompleteDelete], nextState.getClass)

    val completeDelete = nextState.asInstanceOf[CompleteDelete]
    assertEquals(toDelete.size, completeDelete.toDelete.size)

    verify(tierObjectStore, times(1)).deleteSegment(toDelete.head)
  }

  @Test
  def testCompleteDelete(): Unit = {
    val toDelete = logWithTieredSegments.tieredLogSegments.take(3).map(_.metadata)
    val completeDelete = CompleteDelete(RetentionMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])
    val future = completeDelete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[InitiateDelete], nextState.getClass)

    val initiateDelete = nextState.asInstanceOf[InitiateDelete]
    assertEquals(toDelete.tail, initiateDelete.toDelete)

    val firstSegment = toDelete.head
    val expected = new TierSegmentDeleteComplete(topicIdPartition_1, 0, firstSegment.objectId)
    verify(tierTopicManager, times(1)).addMetadata(expected)
  }

  @Test
  def testCompleteAllDeletes(): Unit = {
    val toDelete = logWithTieredSegments.tieredLogSegments.take(1).map(_.metadata)
    val completeDelete = CompleteDelete(RetentionMetadata(replicaManager, leaderEpoch = 0), toDelete.to[mutable.Queue])
    val future = completeDelete.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[CollectDeletableSegments], nextState.getClass)
  }

  @Test
  def testDeletePartitionWithNoTieredSegments(): Unit = {
    val collectDeletableSegments = CollectDeletableSegments(DeletedPartitionMetadata(List.empty))
    val future = collectDeletableSegments.transition(topicIdPartition_1, replicaManager, tierTopicManager, tierObjectStore, time)
    val result = Await.ready(future, 1 second)
    val nextState = result.value.get.get

    assertEquals(classOf[PartitionDeleteComplete], nextState.getClass)
  }

  private def tieredLogSegment(topicIdPartition: TopicIdPartition, baseOffset: Long, endOffset: Long): TierLogSegment = {
    val segment = mock(classOf[TierLogSegment])
    val size = 200
    val state = TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE
    val metadata = new TierObjectMetadata(topicIdPartition, 0, UUID.randomUUID,
      baseOffset, endOffset, time.milliseconds(), size, state, true, false, true)

    when(segment.startOffset).thenReturn(baseOffset)
    when(segment.endOffset).thenReturn(endOffset)
    when(segment.nextOffset).thenReturn(endOffset + 1)
    when(segment.metadata).thenReturn(new TierObjectStore.ObjectMetadata(metadata))
    when(segment.maxTimestamp).thenReturn(time.milliseconds())

    segment
  }

  private def mockLogSegment(tmpFile: File, baseOffset: Long, endOffset: Long): LogSegment = {
    val offsetIndex = mock(classOf[OffsetIndex])
    when(offsetIndex.file).thenReturn(tmpFile)

    val timeIndex = mock(classOf[TimeIndex])
    when(timeIndex.file).thenReturn(tmpFile)

    val fileRecords = mock(classOf[FileRecords])
    when(fileRecords.file).thenReturn(tmpFile)

    val logSegment = mock(classOf[LogSegment])
    when(logSegment.readNextOffset).thenReturn(endOffset + 1)
    when(logSegment.baseOffset).thenReturn(baseOffset)
    when(logSegment.largestTimestamp).thenReturn(time.milliseconds())
    when(logSegment.size).thenReturn(1000)
    when(logSegment.log).thenReturn(fileRecords)
    when(logSegment.offsetIndex).thenReturn(offsetIndex)
    when(logSegment.timeIndex).thenReturn(timeIndex)

    logSegment
  }

  private def mockTierPartitionState(leaderEpoch: Int): TierPartitionState = {
    val tierPartitionState = mock(classOf[TierPartitionState])
    when(tierPartitionState.tierEpoch).thenReturn(leaderEpoch)
    tierPartitionState
  }

  private def mockAbstractLog(tieredSegments: List[TierLogSegment], localSegments: List[LogSegment]): AbstractLog = {
    val log = mock(classOf[AbstractLog])
    when(log.tieredLogSegments).thenReturn(tieredSegments)
    when(log.tierableLogSegments).thenReturn(localSegments)
    log
  }
}
