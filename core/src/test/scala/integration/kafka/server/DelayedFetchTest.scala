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
package kafka.server

import java.util.concurrent.TimeUnit
import java.util.{Collections, Optional}

import kafka.cluster.{Partition, Replica}
import kafka.log.LogOffsetSnapshot
import kafka.tier.fetcher.{PendingFetch, TierFetchResult}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{FencedLeaderEpochException, UnknownServerException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.easymock.{EasyMock, EasyMockSupport}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

class DelayedFetchTest extends EasyMockSupport {
  private val maxBytes = 1024
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val replicaQuota: ReplicaQuota = mock(classOf[ReplicaQuota])

  /**
    * Test that DelayedFetch.onComplete() merges TierFetcher results with
    * metadata returned from the log layer.
    */
  @Test
  def testMixedTierFetch(): Unit = {
    val topicPartition0 = new TopicPartition("topic", 0) // local
    val topicPartition1 = new TopicPartition("topic", 1) // tier

    val replicaId = 1
    val fetchOffset = 500L
    val highWatermark = 50
    val fetchMetadata = buildMultiPartitionFetchMetadata(
      replicaId,
      Seq(
        (topicPartition0, buildFetchPartitionStatus(fetchOffset, LogOffsetMetadata(0, 0))),
        (topicPartition1, buildFetchPartitionStatus(fetchOffset, LogOffsetMetadata.UnknownOffsetMetadata))
      ))

    val pendingFetch : PendingFetch = mock(classOf[PendingFetch])
    EasyMock.expect(pendingFetch.isComplete).andReturn(true)

    val callbackPromise: Promise[Seq[(TopicPartition, FetchPartitionData)]] = Promise[Seq[(TopicPartition, FetchPartitionData)]]()
    val delayedFetch = new DelayedFetch(
      delayMs = 500, fetchMetadata = fetchMetadata, replicaManager = replicaManager, replicaQuota, Some(pendingFetch),
      clientMetadata = None, callbackPromise.success
    )
    expectGetTierFetchResults(pendingFetch, Seq((topicPartition1, None)))
    expectReadFromLocalLog(replicaManager, Seq(
      (topicPartition0, FetchDataInfo(LogOffsetMetadata(0,0), MemoryRecords.EMPTY, firstEntryIncomplete = false, None), None),
      (topicPartition1, TierFetchDataInfo(null, None), None)
    ), highWatermark = highWatermark)

    replayAll()
    delayedFetch.forceComplete()
    assertTrue("Expected forceComplete to complete the request", callbackPromise.isCompleted)
    val results = Await.result(callbackPromise.future, Duration(1, TimeUnit.SECONDS))
    assertTrue("Expected both a tiered and non-tiered fetch result", results.size == 2)
    assertTrue("Expected HWM to be set for both tiered and non-tiered results", results.forall { case (tp, result) => result.highWatermark == highWatermark})
  }

  /**
    * Test that exceptions returned from the TierFetcher are propagated to the DelayedFetch callback.
    * It's excepted that both log layer and tier fetcher exceptions will be included in FetchPartitionData,
    * but log layer exceptions take precedence.
    */
  @Test
  def testTierFetcherException(): Unit = {
    val topicPartition0 = new TopicPartition("topic", 0) // throws FencedLeaderEpoch exception
    val topicPartition1 = new TopicPartition("topic", 1) // throws UnknownServerException exception
    val topicPartition2 = new TopicPartition("topic", 2) // throws FencedLeaderEpoch exception and UnknownServerException

    val replicaId = 1
    val fetchOffset = 500L
    val highWatermark = 50
    val fetchMetadata = buildMultiPartitionFetchMetadata(
      replicaId,
      Seq(
        (topicPartition0, buildFetchPartitionStatus(fetchOffset, LogOffsetMetadata.UnknownOffsetMetadata)),
        (topicPartition1, buildFetchPartitionStatus(fetchOffset, LogOffsetMetadata.UnknownOffsetMetadata)),
        (topicPartition2, buildFetchPartitionStatus(fetchOffset, LogOffsetMetadata.UnknownOffsetMetadata)))
    )

    val pendingFetch : PendingFetch = mock(classOf[PendingFetch])
    EasyMock.expect(pendingFetch.isComplete).andReturn(true)

    val callbackPromise: Promise[Seq[(TopicPartition, FetchPartitionData)]] = Promise[Seq[(TopicPartition, FetchPartitionData)]]()
    val delayedFetch = new DelayedFetch(
      delayMs = 500, fetchMetadata = fetchMetadata, replicaManager = replicaManager, replicaQuota, Some(pendingFetch),
      clientMetadata = None, callbackPromise.success
    )

    expectGetTierFetchResults(
      pendingFetch,
      Seq(
        (topicPartition0, None),
        (topicPartition1, Some(new UnknownServerException)),
        (topicPartition2, Some(new UnknownServerException))
      ))

    expectReadFromLocalLog(
      replicaManager,
      Seq(
        (topicPartition0, TierFetchDataInfo(null, None), Some(new FencedLeaderEpochException(""))),
        (topicPartition1, TierFetchDataInfo(null, None), None),
        (topicPartition2, TierFetchDataInfo(null, None), Some(new FencedLeaderEpochException("")))
      ),
      highWatermark = highWatermark
    )

    replayAll()
    delayedFetch.forceComplete()
    assertTrue("Expected forceComplete to complete the request", callbackPromise.isCompleted)
    val results = Await.result(callbackPromise.future, Duration(1, TimeUnit.SECONDS)).toMap

    assertTrue("Expected 3 fetch results", results.size == 3)
    assertEquals("Expected topicPartition0 to return a FencedLeaderException", results(topicPartition0).error, Errors.FENCED_LEADER_EPOCH)
    assertEquals("Expected topicPartition1 to return a UnknownServerErrorException", results(topicPartition1).error, Errors.UNKNOWN_SERVER_ERROR)
    assertEquals("Expected topicPartition2 to return a FencedLeaderException as it takes precedence over TierFetcher exceptions", results(topicPartition2).error, Errors.FENCED_LEADER_EPOCH)
  }

  @Test
  def testFetchWithFencedEpoch(): Unit = {
    val topicPartition = new TopicPartition("topic", 0)
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchMetadata = buildFetchMetadata(replicaId, topicPartition, fetchStatus)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      delayMs = 500,
      fetchMetadata = fetchMetadata,
      replicaManager = replicaManager,
      quota = replicaQuota,
      None,
      clientMetadata = None,
      responseCallback = callback)

    val partition: Partition = mock(classOf[Partition])

    EasyMock.expect(replicaManager.getPartitionOrException(topicPartition, expectLeader = true))
        .andReturn(partition)
    EasyMock.expect(partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = true))
        .andThrow(new FencedLeaderEpochException("Requested epoch has been fenced"))

    expectReadFromReplicaWithError(replicaId, topicPartition, fetchStatus.fetchInfo, Errors.FENCED_LEADER_EPOCH)

    replayAll()

    assertTrue(delayedFetch.tryComplete())
    assertTrue(delayedFetch.isCompleted)
    assertTrue(fetchResultOpt.isDefined)

    val fetchResult = fetchResultOpt.get
    assertEquals(Errors.FENCED_LEADER_EPOCH, fetchResult.error)
  }

  def checkCompleteWhenFollowerLaggingHW(followerHW: Option[Long], checkResult: DelayedFetch => Unit): Unit = {
    val topicPartition = new TopicPartition("topic", 0)
    val fetchOffset = 500L
    val logStartOffset = 0L
    val currentLeaderEpoch = Optional.of[Integer](10)
    val replicaId = 1

    val fetchStatus = FetchPartitionStatus(
      startOffsetMetadata = LogOffsetMetadata(fetchOffset),
      fetchInfo = new FetchRequest.PartitionData(fetchOffset, logStartOffset, maxBytes, currentLeaderEpoch))
    val fetchMetadata = buildFetchMetadata(replicaId, topicPartition, fetchStatus)

    var fetchResultOpt: Option[FetchPartitionData] = None
    def callback(responses: Seq[(TopicPartition, FetchPartitionData)]): Unit = {
      fetchResultOpt = Some(responses.head._2)
    }

    val delayedFetch = new DelayedFetch(
      delayMs = 500,
      fetchMetadata = fetchMetadata,
      replicaManager = replicaManager,
      quota = replicaQuota,
      tierFetchOpt = None,
      clientMetadata = None,
      responseCallback = callback
    )

    val partition: Partition = mock(classOf[Partition])

    EasyMock.expect(replicaManager.getPartitionOrException(topicPartition, expectLeader = true))
      .andReturn(partition)
    EasyMock.expect(partition.fetchOffsetSnapshot(currentLeaderEpoch, fetchOnlyFromLeader = true))
      .andReturn(
        LogOffsetSnapshot(
          logStartOffset = 0,
          logEndOffset = new LogOffsetMetadata(500L),
          highWatermark = new LogOffsetMetadata(480L),
          lastStableOffset = new LogOffsetMetadata(400L)))

    expectReadFromReplica(replicaId, topicPartition, fetchStatus.fetchInfo)

    val follower = new Replica(replicaId, topicPartition)
    followerHW.foreach(hw => {
      follower.updateFetchState(LogOffsetMetadata.UnknownOffsetMetadata, 0L, 0L, 0L)
      follower.updateLastSentHighWatermark(hw)
    })
    EasyMock.expect(partition.getReplica(replicaId))
        .andReturn(Some(follower))

    replayAll()
    checkResult.apply(delayedFetch)
  }

  @Test
  def testCompleteWhenFollowerLaggingHW(): Unit = {
    // No HW from the follower, should complete
    resetAll
    checkCompleteWhenFollowerLaggingHW(None, delayedFetch => {
      assertTrue(delayedFetch.tryComplete())
      assertTrue(delayedFetch.isCompleted)
    })

    // A higher HW from the follower (shouldn't actually be possible)
    resetAll
    checkCompleteWhenFollowerLaggingHW(Some(500), delayedFetch => {
      assertFalse(delayedFetch.tryComplete())
      assertFalse(delayedFetch.isCompleted)
    })

    // An equal HW from follower
    resetAll
    checkCompleteWhenFollowerLaggingHW(Some(480), delayedFetch => {
      assertFalse(delayedFetch.tryComplete())
      assertFalse(delayedFetch.isCompleted)
    })

    // A lower HW from follower, should complete the fetch
    resetAll
    checkCompleteWhenFollowerLaggingHW(Some(470), delayedFetch => {
      assertTrue(delayedFetch.tryComplete())
      assertTrue(delayedFetch.isCompleted)
    })
  }

  private def buildMultiPartitionFetchMetadata(replicaId: Int,
                                               fetchPartitionStatus: Seq[(TopicPartition, FetchPartitionStatus)]): FetchMetadata = {
    FetchMetadata(fetchMinBytes = 1,
      fetchMaxBytes = maxBytes,
      hardMaxBytesLimit = false,
      fetchOnlyLeader = true,
      fetchIsolation = FetchLogEnd,
      isFromFollower = true,
      replicaId = replicaId,
      fetchPartitionStatus = fetchPartitionStatus)
  }

  private def buildFetchMetadata(replicaId: Int,
                                 topicPartition: TopicPartition,
                                 fetchPartitionStatus: FetchPartitionStatus): FetchMetadata = {
    buildMultiPartitionFetchMetadata(replicaId, Seq((topicPartition, fetchPartitionStatus)))
  }

  private def expectReadFromReplicaWithError(replicaId: Int,
                                             topicPartition: TopicPartition,
                                             fetchPartitionData: FetchRequest.PartitionData,
                                             error: Errors): Unit = {
    EasyMock.expect(replicaManager.readFromLocalLog(
      replicaId = replicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchLogEnd,
      fetchMaxBytes = maxBytes,
      hardMaxBytesLimit = false,
      readPartitionInfo = Seq((topicPartition, fetchPartitionData)),
      clientMetadata = None,
      quota = replicaQuota))
      .andReturn(Seq((topicPartition, buildReadResultWithError(error))))
  }

  private def expectReadFromReplica(replicaId: Int,
                                    topicPartition: TopicPartition,
                                    fetchPartitionData: FetchRequest.PartitionData): Unit = {
    val result = LogReadResult(
      exception = None,
      info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
      highWatermark = -1L,
      leaderLogStartOffset = -1L,
      leaderLogEndOffset = -1L,
      followerLogStartOffset = -1L,
      fetchTimeMs = -1L,
      readSize = -1,
      lastStableOffset = None)


    EasyMock.expect(replicaManager.readFromLocalLog(
      replicaId = replicaId,
      fetchOnlyFromLeader = true,
      fetchIsolation = FetchLogEnd,
      fetchMaxBytes = maxBytes,
      hardMaxBytesLimit = false,
      readPartitionInfo = Seq((topicPartition, fetchPartitionData)),
      clientMetadata = None,
      quota = replicaQuota))
      .andReturn(Seq((topicPartition, result))).anyTimes()
  }

  private def buildReadResultWithError(error: Errors): LogReadResult = {
    LogReadResult(
      exception = Some(error.exception),
      info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
      highWatermark = -1L,
      leaderLogStartOffset = -1L,
      leaderLogEndOffset = -1L,
      followerLogStartOffset = -1L,
      fetchTimeMs = -1L,
      readSize = -1,
      lastStableOffset = None)
  }

  private def expectGetTierFetchResults(pendingFetch: PendingFetch,
                                        topicPartitionException: Seq[(TopicPartition, Option[Throwable])]): Unit = {
    val results = topicPartitionException
      .map { case (topicPartition: TopicPartition, exceptionOpt: Option[Throwable]) =>
        (topicPartition, new TierFetchResult(MemoryRecords.EMPTY, Collections.emptyList(), exceptionOpt.orNull))
      }.toMap.asJava
    EasyMock
      .expect(pendingFetch.finish())
      .andReturn(results)
  }

  private def expectReadFromLocalLog(replicaManager: ReplicaManager,
                                     fetchDataInfos: Seq[(TopicPartition, AbstractFetchDataInfo, Option[Throwable])],
                                     highWatermark: Long = 0): Unit = {
    val readResults = fetchDataInfos.map {
      case (tp, tierFetchDataInfo: TierFetchDataInfo, exceptionOpt: Option[Throwable]) =>
        (tp, TierLogReadResult(info = tierFetchDataInfo, highWatermark, 0, 0, 0, 0, 0, None, None, exceptionOpt))
      case (tp, fetchDataInfo: FetchDataInfo, exceptionOpt: Option[Throwable]) =>
        (tp, LogReadResult(info = fetchDataInfo, highWatermark, 0, 0, 0, 0, 0, None, None, false, exceptionOpt))
    }
    EasyMock.expect(replicaManager.readFromLocalLog(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
      EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
      .andReturn(readResults)
  }

  private def buildFetchPartitionStatus(fetchOffset: Long, logOffsetMetadata: LogOffsetMetadata): FetchPartitionStatus = {
    FetchPartitionStatus(startOffsetMetadata = logOffsetMetadata, new FetchRequest.PartitionData(fetchOffset, 0, Int.MaxValue, Optional.empty()))
  }
}