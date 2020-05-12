/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server.epoch

import java.io.File

import scala.collection.Seq
import scala.collection.mutable.ListBuffer

import kafka.server.checkpoints.{LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import org.apache.kafka.common.requests.EpochEndOffset.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{After, Test}

/**
  * Unit test for the LeaderEpochFileCache.
  */
class LeaderEpochFileCacheTest {
  val tp = new TopicPartition("TestTopic", 5)
  private var logEndOffset = 0L
  private val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
    private var epochs: Seq[EpochEntry] = Seq()
    override val file = TestUtils.tempFile()
    override def write(epochs: Seq[EpochEntry]): Unit = this.epochs = epochs
    override def read(): Seq[EpochEntry] = this.epochs
  }
  private val cache = new LeaderEpochFileCache(tp, () => logEndOffset, checkpoint)

  @After
  def tearDown(): Unit = {
    Utils.delete(checkpoint.file)
  }

  @Test
  def shouldAddEpochAndMessageOffsetToCache() = {
    //When
    cache.assign(epoch = 2, startOffset = 10)
    logEndOffset = 11

    //Then
    assertEquals(Some(2), cache.latestEpoch)
    assertEquals(EpochEntry(2, 10), cache.epochEntries(0))
    assertEquals((2, logEndOffset), cache.endOffsetFor(2)) //should match logEndOffset
  }

  @Test
  def shouldReturnLogEndOffsetIfLatestEpochRequested() = {
    //When just one epoch
    cache.assign(epoch = 2, startOffset = 11)
    cache.assign(epoch = 2, startOffset = 12)
    logEndOffset = 14

    //Then
    assertEquals((2, logEndOffset), cache.endOffsetFor(2))
  }

  @Test
  def shouldReturnUndefinedOffsetIfUndefinedEpochRequested() = {
    val expectedEpochEndOffset = (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

    // assign couple of epochs
    cache.assign(epoch = 2, startOffset = 11)
    cache.assign(epoch = 3, startOffset = 12)

    //When (say a bootstraping follower) sends request for UNDEFINED_EPOCH
    val epochAndOffsetFor = cache.endOffsetFor(UNDEFINED_EPOCH)

    //Then
    assertEquals("Expected undefined epoch and offset if undefined epoch requested. Cache not empty.",
                 expectedEpochEndOffset, epochAndOffsetFor)
  }

  @Test
  def shouldNotOverwriteLogEndOffsetForALeaderEpochOnceItHasBeenAssigned() = {
    //Given
    logEndOffset = 9

    cache.assign(2, logEndOffset)

    //When called again later
    cache.assign(2, 10)

    //Then the offset should NOT have been updated
    assertEquals(logEndOffset, cache.epochEntries(0).startOffset)
    assertEquals(ListBuffer(EpochEntry(2, 9)), cache.epochEntries)
  }

  @Test
  def shouldEnforceMonotonicallyIncreasingStartOffsets() = {
    //Given
    cache.assign(2, 9)

    //When update epoch new epoch but same offset
    cache.assign(3, 9)

    //Then epoch should have been updated
    assertEquals(ListBuffer(EpochEntry(3, 9)), cache.epochEntries)
  }
  
  @Test
  def shouldNotOverwriteOffsetForALeaderEpochOnceItHasBeenAssigned() = {
    cache.assign(2, 6)

    //When called again later with a greater offset
    cache.assign(2, 10)

    //Then later update should have been ignored
    assertEquals(6, cache.epochEntries(0).startOffset)
  }

  @Test
  def shouldReturnUnsupportedIfNoEpochRecorded(): Unit = {
    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(0))
  }

  @Test
  def shouldReturnUnsupportedIfNoEpochRecordedAndUndefinedEpochRequested(): Unit = {
    logEndOffset = 73

    //When (say a follower on older message format version) sends request for UNDEFINED_EPOCH
    val offsetFor = cache.endOffsetFor(UNDEFINED_EPOCH)

    //Then
    assertEquals("Expected undefined epoch and offset if undefined epoch requested. Empty cache.",
                 (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), offsetFor)
  }

  @Test
  def shouldReturnFirstEpochIfRequestedEpochLessThanFirstEpoch(): Unit = {
    cache.assign(epoch = 5, startOffset = 11)
    cache.assign(epoch = 6, startOffset = 12)
    cache.assign(epoch = 7, startOffset = 13)

    //When
    val epochAndOffset = cache.endOffsetFor(4)

    //Then
    assertEquals((4, 11), epochAndOffset)
  }

  @Test
  def shouldTruncateIfMatchingEpochButEarlierStartingOffset(): Unit = {
    cache.assign(epoch = 5, startOffset = 11)
    cache.assign(epoch = 6, startOffset = 12)
    cache.assign(epoch = 7, startOffset = 13)

    // epoch 7 starts at an earlier offset
    cache.assign(epoch = 7, startOffset = 12)

    assertEquals((5, 12), cache.endOffsetFor(5))
    assertEquals((5, 12), cache.endOffsetFor(6))
  }

  @Test
  def shouldGetFirstOffsetOfSubsequentEpochWhenOffsetRequestedForPreviousEpoch() = {
    //When several epochs
    cache.assign(epoch = 1, startOffset = 11)
    cache.assign(epoch = 1, startOffset = 12)
    cache.assign(epoch = 2, startOffset = 13)
    cache.assign(epoch = 2, startOffset = 14)
    cache.assign(epoch = 3, startOffset = 15)
    cache.assign(epoch = 3, startOffset = 16)
    logEndOffset = 17

    //Then get the start offset of the next epoch
    assertEquals((2, 15), cache.endOffsetFor(2))
  }

  @Test
  def shouldReturnNextAvailableEpochIfThereIsNoExactEpochForTheOneRequested(): Unit = {
    //When
    cache.assign(epoch = 0, startOffset = 10)
    cache.assign(epoch = 2, startOffset = 13)
    cache.assign(epoch = 4, startOffset = 17)

    //Then
    assertEquals((0, 13), cache.endOffsetFor(requestedEpoch = 1))
    assertEquals((2, 17), cache.endOffsetFor(requestedEpoch = 2))
    assertEquals((2, 17), cache.endOffsetFor(requestedEpoch = 3))
  }

  @Test
  def shouldNotUpdateEpochAndStartOffsetIfItDidNotChange() = {
    //When
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 2, startOffset = 7)

    //Then
    assertEquals(1, cache.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache.epochEntries.toList(0))
  }

  @Test
  def shouldReturnInvalidOffsetIfEpochIsRequestedWhichIsNotCurrentlyTracked(): Unit = {
    logEndOffset = 100

    //When
    cache.assign(epoch = 2, startOffset = 100)

    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(3))
  }

  @Test
  def shouldSupportEpochsThatDoNotStartFromZero(): Unit = {
    //When
    cache.assign(epoch = 2, startOffset = 6)
    logEndOffset = 7

    //Then
    assertEquals((2, logEndOffset), cache.endOffsetFor(2))
    assertEquals(1, cache.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache.epochEntries(0))
  }

  @Test
  def shouldPersistEpochsBetweenInstances(): Unit = {
    val checkpointPath = TestUtils.tempFile().getAbsolutePath
    val checkpoint = new LeaderEpochCheckpointFile(new File(checkpointPath))

    //Given
    val cache = new LeaderEpochFileCache(tp, () => logEndOffset, checkpoint)
    cache.assign(epoch = 2, startOffset = 6)

    //When
    val checkpoint2 = new LeaderEpochCheckpointFile(new File(checkpointPath))
    val cache2 = new LeaderEpochFileCache(tp, () => logEndOffset, checkpoint2)

    //Then
    assertEquals(1, cache2.epochEntries.size)
    assertEquals(EpochEntry(2, 6), cache2.epochEntries.toList(0))
  }

  @Test
  def shouldEnforceMonotonicallyIncreasingEpochs(): Unit = {
    //Given
    cache.assign(epoch = 1, startOffset = 5); logEndOffset = 6
    cache.assign(epoch = 2, startOffset = 6); logEndOffset = 7

    //When we update an epoch in the past with a different offset, the log has already reached
    //an inconsistent state. Our options are either to raise an error, ignore the new append,
    //or truncate the cached epochs to the point of conflict. We take this latter approach in
    //order to guarantee that epochs and offsets in the cache increase monotonically, which makes
    //the search logic simpler to reason about.
    cache.assign(epoch = 1, startOffset = 7); logEndOffset = 8

    //Then later epochs will be removed
    assertEquals(Some(1), cache.latestEpoch)

    //Then end offset for epoch 1 will have changed
    assertEquals((1, 8), cache.endOffsetFor(1))

    //Then end offset for epoch 2 is now undefined
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(2))
    assertEquals(EpochEntry(1, 7), cache.epochEntries(0))
  }

  @Test
  def shouldEnforceOffsetsIncreaseMonotonically() = {
    //When epoch goes forward but offset goes backwards
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 5)

    //The last assignment wins and the conflicting one is removed from the log
    assertEquals(EpochEntry(3, 5), cache.epochEntries.toList(0))
  }

  @Test
  def shouldIncreaseAndTrackEpochsAsLeadersChangeManyTimes(): Unit = {
    //Given
    cache.assign(epoch = 0, startOffset = 0) //logEndOffset=0

    //When
    cache.assign(epoch = 1, startOffset = 0) //logEndOffset=0

    //Then epoch should go up
    assertEquals(Some(1), cache.latestEpoch)
    //offset for 1 should still be 0
    assertEquals((1, 0), cache.endOffsetFor(1))
    //offset for epoch 0 should still be 0
    assertEquals((0, 0), cache.endOffsetFor(0))

    //When we write 5 messages as epoch 1
    logEndOffset = 5

    //Then end offset for epoch(1) should be logEndOffset => 5
    assertEquals((1, 5), cache.endOffsetFor(1))
    //Epoch 0 should still be at offset 0
    assertEquals((0, 0), cache.endOffsetFor(0))

    //When
    cache.assign(epoch = 2, startOffset = 5) //logEndOffset=5

    logEndOffset = 10 //write another 5 messages

    //Then end offset for epoch(2) should be logEndOffset => 10
    assertEquals((2, 10), cache.endOffsetFor(2))

    //end offset for epoch(1) should be the start offset of epoch(2) => 5
    assertEquals((1, 5), cache.endOffsetFor(1))

    //epoch (0) should still be 0
    assertEquals((0, 0), cache.endOffsetFor(0))
  }

  @Test
  def shouldIncreaseAndTrackEpochsAsFollowerReceivesManyMessages(): Unit = {
    //When Messages come in
    cache.assign(epoch = 0, startOffset = 0); logEndOffset = 1
    cache.assign(epoch = 0, startOffset = 1); logEndOffset = 2
    cache.assign(epoch = 0, startOffset = 2); logEndOffset = 3

    //Then epoch should stay, offsets should grow
    assertEquals(Some(0), cache.latestEpoch)
    assertEquals((0, logEndOffset), cache.endOffsetFor(0))

    //When messages arrive with greater epoch
    cache.assign(epoch = 1, startOffset = 3); logEndOffset = 4
    cache.assign(epoch = 1, startOffset = 4); logEndOffset = 5
    cache.assign(epoch = 1, startOffset = 5); logEndOffset = 6

    assertEquals(Some(1), cache.latestEpoch)
    assertEquals((1, logEndOffset), cache.endOffsetFor(1))

    //When
    cache.assign(epoch = 2, startOffset = 6); logEndOffset = 7
    cache.assign(epoch = 2, startOffset = 7); logEndOffset = 8
    cache.assign(epoch = 2, startOffset = 8); logEndOffset = 9

    assertEquals(Some(2), cache.latestEpoch)
    assertEquals((2, logEndOffset), cache.endOffsetFor(2))

    //Older epochs should return the start offset of the first message in the subsequent epoch.
    assertEquals((0, 3), cache.endOffsetFor(0))
    assertEquals((1, 6), cache.endOffsetFor(1))
  }

  @Test
  def shouldDropEntriesOnEpochBoundaryWhenRemovingLatestEntries(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When clear latest on epoch boundary
    cache.truncateFromEnd(endOffset = 8)

    //Then should remove two latest epochs (remove is inclusive)
    assertEquals(ListBuffer(EpochEntry(2, 6)), cache.epochEntries)
  }

  @Test
  def shouldPreserveResetOffsetOnClearEarliestIfOneExists(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset ON epoch boundary
    cache.truncateFromStart(startOffset = 8)

    //Then should preserve (3, 8)
    assertEquals(ListBuffer(EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateSavedOffsetWhenOffsetToClearToIsBetweenEpochs(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset BETWEEN epoch boundaries
    cache.truncateFromStart(startOffset = 9)

    //Then we should retain epoch 3, but update it's offset to 9 as 8 has been removed
    assertEquals(ListBuffer(EpochEntry(3, 9), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldNotClearAnythingIfOffsetToEarly(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset before first epoch offset
    cache.truncateFromStart(startOffset = 1)

    //Then nothing should change
    assertEquals(ListBuffer(EpochEntry(2, 6),EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldNotClearAnythingIfOffsetToFirstOffset(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset on earliest epoch boundary
    cache.truncateFromStart(startOffset = 6)

    //Then nothing should change
    assertEquals(ListBuffer(EpochEntry(2, 6),EpochEntry(3, 8), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldRetainLatestEpochOnClearAllEarliest(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When
    cache.truncateFromStart(startOffset = 11)

    //Then retain the last
    assertEquals(ListBuffer(EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateOffsetBetweenEpochBoundariesOnClearEarliest(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When we clear from a postition between offset 8 & offset 11
    cache.truncateFromStart(startOffset = 9)

    //Then we should update the middle epoch entry's offset
    assertEquals(ListBuffer(EpochEntry(3, 9), EpochEntry(4, 11)), cache.epochEntries)
  }

  @Test
  def shouldUpdateOffsetBetweenEpochBoundariesOnClearEarliest2(): Unit = {
    //Given
    cache.assign(epoch = 0, startOffset = 0)
    cache.assign(epoch = 1, startOffset = 7)
    cache.assign(epoch = 2, startOffset = 10)

    //When we clear from a postition between offset 0 & offset 7
    cache.truncateFromStart(startOffset = 5)

    //Then we should keeep epoch 0 but update the offset appropriately
    assertEquals(ListBuffer(EpochEntry(0,5), EpochEntry(1, 7), EpochEntry(2, 10)), cache.epochEntries)
  }

  @Test
  def shouldRetainLatestEpochOnClearAllEarliestAndUpdateItsOffset(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset beyond last epoch
    cache.truncateFromStart(startOffset = 15)

    //Then update the last
    assertEquals(ListBuffer(EpochEntry(4, 15)), cache.epochEntries)
  }

  @Test
  def shouldDropEntriesBetweenEpochBoundaryWhenRemovingNewest(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset BETWEEN epoch boundaries
    cache.truncateFromEnd(endOffset = 9)

    //Then should keep the preceding epochs
    assertEquals(Some(3), cache.latestEpoch)
    assertEquals(ListBuffer(EpochEntry(2, 6), EpochEntry(3, 8)), cache.epochEntries)
  }

  @Test
  def shouldClearAllEntries(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When
    cache.clearAndFlush()

    //Then
    assertEquals(0, cache.epochEntries.size)
  }

  @Test
  def shouldNotResetEpochHistoryHeadIfUndefinedPassed(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset on epoch boundary
    cache.truncateFromEnd(endOffset = UNDEFINED_EPOCH_OFFSET)

    //Then should do nothing
    assertEquals(3, cache.epochEntries.size)
  }

  @Test
  def shouldNotResetEpochHistoryTailIfUndefinedPassed(): Unit = {
    //Given
    cache.assign(epoch = 2, startOffset = 6)
    cache.assign(epoch = 3, startOffset = 8)
    cache.assign(epoch = 4, startOffset = 11)

    //When reset to offset on epoch boundary
    cache.truncateFromEnd(endOffset = UNDEFINED_EPOCH_OFFSET)

    //Then should do nothing
    assertEquals(3, cache.epochEntries.size)
  }

  @Test
  def shouldFetchLatestEpochOfEmptyCache(): Unit = {
    //Then
    assertEquals(None, cache.latestEpoch)
  }

  @Test
  def shouldFetchEndOffsetOfEmptyCache(): Unit = {
    //Then
    assertEquals((UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET), cache.endOffsetFor(7))
  }

  @Test
  def shouldClearEarliestOnEmptyCache(): Unit = {
    //Then
    cache.truncateFromStart(7)
  }

  @Test
  def shouldClearLatestOnEmptyCache(): Unit = {
    //Then
    cache.truncateFromEnd(7)
  }

  @Test
  def shouldReturnCorrectStartOffsetForEpoch(): Unit = {
    cache.assign(1, 10)
    cache.assign(2, 20)
    cache.assign(3, 30)
    // non existent epoch
    var requestedEpoch = 0
    assertEquals(s"Returned wrong start offset for epoch: $requestedEpoch",
      UNDEFINED_EPOCH_OFFSET, cache.offsetForEpoch(requestedEpoch))
    requestedEpoch = 4
    assertEquals(s"Returned wrong start offset for epoch: $requestedEpoch",
      UNDEFINED_EPOCH_OFFSET, cache.offsetForEpoch(requestedEpoch))
    // valid epoch
    requestedEpoch = 1
    assertEquals(s"Returned wrong start offset for epoch: $requestedEpoch",
      10, cache.offsetForEpoch(requestedEpoch))
    // epoch cache empty
    cache.clearAndFlush()
    assertEquals(s"Returned wrong start offset for epoch: $requestedEpoch",
      UNDEFINED_EPOCH_OFFSET, cache.offsetForEpoch(requestedEpoch))
  }

  @Test
  def shouldNotReportDivergenceWhenNoDivergence(): Unit = {
    // comparison with identical tier state
    // tieredEpochState: ((5,50)(6,60)(7,70)(8,80)) firstTieredOffset: 50 lastTieredOffset: 89 localCache: ((5,50)(6,60)(7,70)(8,80)) firstLocalOffset: 50 lastLocalOffset: 89
    cache.assign(5, 50)
    cache.assign(6, 60)
    cache.assign(7, 70)
    cache.assign(8, 80)
    var tieredEpochState = cache.epochEntries.toList
    assertEquals("False positive for divergence while comparing with identical tier state", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 50L, 89L, 50L, 89L))

    // comparison with a superset tier state with no divergence
    // tieredEpochState: ((5,50)(6,60)(7,70)(8,80)) firstTieredOffset: 50 lastTieredOffset: 89 localCache: ((6,60)(7,70)) firstLocalOffset: 60 lastLocalOffset: 79
    cache.clearAndFlush()
    cache.assign(6, 60)
    cache.assign(7, 70)
    assertEquals("False positive for divergence while comparing with an identical but superset tier state", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 50L, 89L, 60L, 79L))

    // empty epoch cache
    // tieredEpochState: ((5,50)(6,60)(7,70)(8,80)) firstTieredOffset: 50 lastTieredOffset: 89 localCache: () firstLocalOffset: 0 lastLocalOffset: -1
    cache.clearAndFlush()
    assertEquals("False positive for divergence when epoch cache is empty", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 50L, 89L, 0L, -1L))

    // empty tiered state
    // tieredEpochState: () firstTieredOffset: 0 lastTieredOffset: -1 localCache: ((10,100)(11,110)) firstLocalOffset: 100 lastLocalOffset: 119
    tieredEpochState = List[EpochEntry]()
    cache.clearAndFlush()
    cache.assign(10, 100)
    cache.assign(11, 110)
    assertEquals("False positive for divergence when epoch cache is empty", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, -1L, 100L, 119L))

    // comparison with disjointed tier state
    // tieredEpochState: ((5,50)(6,60)(7,70)(8,80)) firstTieredOffset: 50 lastTieredOffset: 89 localCache: ((10,100)(11,110)(12,120)) firstLocalOffset: 100 lastLocalOffset: 129
    tieredEpochState = List(EpochEntry(5, 50), EpochEntry(6, 60), EpochEntry(7, 70), EpochEntry(8, 80))
    cache.clearAndFlush()
    cache.assign(10, 100)
    cache.assign(11, 110)
    cache.assign(12, 120)
    assertEquals("False positive for divergence while comparing with a disjointed epoch cache", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 50L, 89L, 100L, 129L))

    // tieredEpochState: ((5,50)(6,60)(7,70)(8,80)) firstTieredOffset: 50 lastTieredOffset: 89 localCache: ((0,0)(1,10)(2,20)(3,30)) firstLocalOffset: 0 lastLocalOffset: 39
    cache.clearAndFlush()
    cache.assign(0, 0)
    cache.assign(1, 10)
    cache.assign(2, 20)
    cache.assign(3, 30)
    assertEquals("False positive for divergence while comparing with a disjointed epoch cache", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 50L, 89L, 0L, 39L))

    // Leader replicas update epoch cache upon receiving LeaderAndIsr request, while followers update epoch cache
    // only when a new leader appends a message to the log. Therefore, epoch cache may differ at leader and follower replicas in
    // a way that follower will not make an entry for a leader generation that did not append any messages. This should not be
    // treated as a divergence.
    // tieredEpochState: ((0,0)(1,10)(2,20)(3,30)(4,40)(5,40)(6,40)) firstTieredOffset: 0 lastTieredOffset: 49 localCache: ((0,0)(1,10)(2,20)(3,30)(6,40)) firstLocalOffset: 0 lastLocalOffset: 49
    tieredEpochState = List(EpochEntry(0, 0), EpochEntry(1, 10), EpochEntry(2, 20), EpochEntry(3, 30),
      EpochEntry(4, 40),  EpochEntry(5, 40), EpochEntry(6, 40))  // epochs 4 and 5 have not appended any message
    cache.clearAndFlush()
    cache.assign(0, 0)
    cache.assign(1, 10)
    cache.assign(2, 20)
    cache.assign(3, 30)
    cache.assign(6, 40) // two offsets are not recorded after epoch 3 as they did not append any message
    assertEquals("False positive for divergence when follower had not recorded leader with no messages", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 49L, 0L, 49L))

    // If the local log is incremented, offset for the first local epoch may be different, although it must still
    // be a valid offset for this epoch. Such cases should not be treated as a divergence.
    // tieredEpochState: ((0,0)(1,10)(2,20)(3,30)) firstTieredOffset: 0 lastTieredOffset: 39 localCache: ((0,5)(1,10)(2,20)) firstLocalOffset: 5 lastLocalOffset: 29
    tieredEpochState = List(EpochEntry(0, 0), EpochEntry(1, 10), EpochEntry(2, 20), EpochEntry(3, 30))
    cache.clearAndFlush()
    cache.assign(0, 5) // start offset for epoch 0 is changed to 5 to simulate increment in log start offset
    cache.assign(1, 10)
    cache.assign(2, 20)
    assertEquals("False positive for divergence when local log had been incremented", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 39L, 5L, 29L))

    // only one entry in tierState and localCache. local log has been incremented and local log end offset != tiered log end offset
    // tieredEpochState: (0,0) firstTieredOffset: 0 lastTieredOffset: 899 localCache: (0,100) firstLocalOffset: 100 lastLocalOffset: 999
    tieredEpochState = List(EpochEntry(0, 0))
    cache.clearAndFlush()
    cache.assign(0, 100)
    assertEquals("False positive for divergence when single entry in leaderCache, local log incremented and lastLocalOffset != lastTieredOffset", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 899L, 100L, 999L))

    // only one entry in tierState and localCache. local log end offset != tiered log end offset
    // tieredEpochState: (0,0) firstTieredOffset: 0 lastTieredOffset: 899 localCache: (0,0) firstLocalOffset: 0 lastLocalOffset: 999
    tieredEpochState = List(EpochEntry(0, 0))
    cache.clearAndFlush()
    cache.assign(0, 0)
    assertEquals("False positive for divergence when end offset for an epoch mismatch due to lastLocalOffset != lastTieredOffset", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 899L, 0L, 999L))

    // last offset for an epoch at tiered state < last offset for the epoch at local cache && tiered log ends at this offset
    // tieredEpochState: ((0,0)) firstTieredOffset: 0 lastTieredOffset: 49 localCache: ((0,0)(1,100)) firstLocalOffset: 0 lastLocalOffset: 149
    tieredEpochState = List(EpochEntry(0, 0))
    cache.clearAndFlush()
    cache.assign(0, 0)
    cache.assign(1, 100)
    assertEquals("False negative when end offset for an epoch mismatches", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 49L, 0L, 149L))

    // last offset for an epoch at tiered state > last offset for the epoch at local cache && local log ends at this offset
    // tieredEpochState: ((0,0)(1,100)) firstTieredOffset: 0 lastTieredOffset: 199 localCache: ((0,0)) firstLocalOffset: 0 lastLocalOffset: 49
    tieredEpochState = List(EpochEntry(0, 0), EpochEntry(1, 100))
    cache.clearAndFlush()
    cache.assign(0, 0)
    assertEquals("False negative when end offset for an epoch mismatches", -1,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 199L, 0L, 49L))
  }

  @Test
  def shouldReportDivergenceWhenDiverging(): Unit = {
    // comparison with overlapping and diverging tier state
    // tieredEpochState: ((0,0)(1,10)(2,20)(3,30)) firstTieredOffset: 0 lastTieredOffset: 39 localCache:((2,20)(3,25)(4,40)(5,50)) firstLocalOffset: 20 lastLocalOffset: 59
    var tieredEpochState = List(EpochEntry(0, 0), EpochEntry(1, 10), EpochEntry(2, 20), EpochEntry(3, 30))
    cache.assign(2, 20)
    cache.assign(3, 25)
    cache.assign(4, 40)
    cache.assign(5, 50)
    assertEquals("False negative for an overlapping but diverging tier state", 25,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 39L, 20L, 59L))

    // comparison with overlapping and diverging tier state
    // tieredEpochState: ((0,0)(1,10)(2,20)(3,30)) firstTieredOffset: 0 lastTieredOffset: 39 localCache:((2,20)(3,35)(4,40)(5,50)) firstLocalOffset: 20 lastLocalOffset: 59
    cache.clearAndFlush()
    cache.assign(2, 20)
    cache.assign(3, 35)
    cache.assign(4, 40)
    cache.assign(5, 50)
    assertEquals("False negative for an overlapping but diverging tier state", 30,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 39L, 20L, 59L))

    // Offset for the first local epoch may be different, although it must still be a greater than tiered offset for this epoch.
    // tieredEpochState: ((1,10)(2,20)(3,30)) firstTieredOffset: 10 lastTieredOffset: 39 localCache: ((1,5)(2,20)(3,30)) firstLocalOffset: 5 lastLocalOffset: 39
    tieredEpochState = List(EpochEntry(1, 10), EpochEntry(2, 20), EpochEntry(3, 30))
    cache.clearAndFlush()
    cache.assign(1, 5)
    cache.assign(2, 20)
    cache.assign(3, 30)
    assertEquals("False negative when first local epoch has offset lower than tiered offset for the same epoch", 5,
      cache.findDivergenceInEpochCache(tieredEpochState, 10L, 39L, 5L, 39L))

    // If a tiered epoch is missing at local cache but the local log starts at an offset that must have been written to by the missing epoch,
    // then local log has diverged at the start offset for the missing epoch.
    // tieredEpochState: ((0,0)(1,10)(2,20)(3,30)) firstTieredOffset: 0 lastTieredOffset: 39 localCache:((3,25)(4,40)(5,50)) firstLocalOffset: 25 lastLocalOffset: 59
    tieredEpochState = List(EpochEntry(0, 0), EpochEntry(1, 10), EpochEntry(2, 20), EpochEntry(3, 30))
    cache.clearAndFlush()
    cache.assign(3, 25)
    cache.assign(4, 40)
    cache.assign(5, 50)
    assertEquals("False negative when local cache misses an epoch but includes the corresponding offset", 20,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 39L, 25L, 59L))

    // mismatch in the start offset for an epoch which is not the first local epoch
    // tieredEpochState: ((3,30)(4,40)(5,50)) firstTieredOffset: 30 lastTieredOffset: 59 localCache:((2,20)(3,35)(4,40)) firstLocalOffset: 20 lastLocalOffset: 49
    tieredEpochState = List(EpochEntry(3, 30), EpochEntry(4, 40), EpochEntry(5, 50))
    cache.clearAndFlush()
    cache.assign(2, 20) // local cache has some older data
    cache.assign(3, 35) // first epoch to match with tiered state (but not first epoch in local cache)
    cache.assign(4, 40)
    assertEquals("False negative when divergence at first matching epoch but it is not the first local epoch", 30,
      cache.findDivergenceInEpochCache(tieredEpochState, 30L, 59L, 20L, 49L))

    // mismatch in the start offset for an epoch which is not the first local epoch
    // tieredEpochState: ((3,30)(4,40)(5,50)) firstTieredOffset: 30 lastTieredOffset: 59 localCache:((2,20)(3,25)(4,40)) firstLocalOffset: 20 lastLocalOffset: 49
    cache.clearAndFlush()
    cache.assign(2, 20) // local cache has some older data
    cache.assign(3, 25) // first epoch to match with tiered state (but not first epoch in local cache)
    cache.assign(4, 40)
    assertEquals("False negative when divergence at first matching epoch but it is not the first local epoch", 25,
      cache.findDivergenceInEpochCache(tieredEpochState, 30L, 59L, 20L, 49L))

    // Even when tiered leader state and localCache are disjointed sets(wrt leader epoch), offsets must increase monotonically
    // across the two sets
    // tieredEpochState: ((5,50)(6,60)(7,70)) firstTieredOffset: 50 lastTieredOffset: 79 localCache:((2,60)(3,70)(4,80)) firstLocalOffset: 60 lastLocalOffset: 89
    tieredEpochState = List(EpochEntry(5, 50), EpochEntry(6, 60), EpochEntry(7, 70))
    cache.clearAndFlush()
    cache.assign(2, 60)
    cache.assign(3, 70)
    cache.assign(4, 80)
    assertEquals("False negative when offsets at tieredEpochState and localCache do not increase monotonically", 60,
      cache.findDivergenceInEpochCache(tieredEpochState, 50L, 79L, 60L, 89L))

    // tieredEpochState: ((1,100)(2,150)) firstTieredOffset: 100 lastTieredOffset: 179 localCache: ((1,100)) firstLocalOffset: 100 lastLocalOffset: 199
    tieredEpochState = List(EpochEntry(1, 100), EpochEntry(2, 150))
    cache.clearAndFlush()
    cache.assign(1, 100)
    assertEquals("False negative when localCache is missing an epoch but the corresponding offsets are " +
      "written to by a different epoch", 150,
      cache.findDivergenceInEpochCache(tieredEpochState, 100L, 179L, 100L, 199L))

    // local log has been incremented but end offsets for the start epoch do not match
    // tieredEpochState: ((0,0)(1,100)) firstTieredOffset: 0 lastTieredOffset: 199 localCache: ((0,50)(1,75)) firstLocalOffset: 50 lastLocalOffset: 199
    tieredEpochState = List(EpochEntry(0, 0), EpochEntry(1, 100))
    cache.clearAndFlush()
    cache.assign(0, 50)
    cache.assign(1, 75)
    assertEquals("False negative when end offset for start epoch mismatches", 75,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 199L, 50L, 199L))

    // last offset for an epoch at tiered state < last offset for the epoch at local cache && tiered log does not end at this offset
    // tieredEpochState: ((0,0)(1,100)) firstTieredOffset: 0 lastTieredOffset: 149 localCache: ((0,0)) firstLocalOffset: 0 lastLocalOffset: 149
    tieredEpochState = List(EpochEntry(0, 0), EpochEntry(1, 100))
    cache.clearAndFlush()
    cache.assign(0, 0)
    assertEquals("False negative when end offset for an epoch mismatches", 100,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 149L, 0L, 149L))

    // last offset for an epoch at tiered state > last offset for the epoch at local cache && local log does not end at this offset but tiered state does
    // tieredEpochState: ((0,0)) firstTieredOffset: 0 lastTieredOffset: 99 localCache: ((0,0)(1,50)) firstLocalOffset: 0 lastLocalOffset: 199
    tieredEpochState = List(EpochEntry(0, 0))
    cache.clearAndFlush()
    cache.assign(0, 0)
    cache.assign(1, 50)
    assertEquals("False negative when end offset for an epoch mismatches", 50,
      cache.findDivergenceInEpochCache(tieredEpochState, 0L, 99L, 0L, 199L))
  }
}
