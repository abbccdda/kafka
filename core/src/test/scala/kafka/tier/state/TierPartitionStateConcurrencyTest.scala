/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state

import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import kafka.server.LogDirFailureChannel
import kafka.tier.domain.TierTopicInitLeader
import kafka.tier.TierTestUtils
import kafka.tier.TopicIdPartition
import kafka.utils.TestUtils
import java.nio.ByteBuffer
import java.util.Optional

import kafka.tier.domain.TierPartitionForceRestore
import org.junit.Assert._
import org.junit.Test

class StateSeek(state: FileTierPartitionState,
                shutdown: AtomicBoolean,
                error: AtomicReference[Throwable],
                latestStartOffset: AtomicLong)
  extends Runnable {
  def run(): Unit = {
    while (!shutdown.get()) {
      try {
        val offset = latestStartOffset.get()
        if (offset >= 0) {
          while (!state.metadata(offset).isPresent) {}
          val found = state
            .metadata(offset)
            .get()
            .baseOffset()
          if (offset != found) {
            error.set(new Exception("Unexpected offset found expected: " + offset + " found: " + found))
          }
        }
      } catch {
        case e: Throwable =>
          shutdown.set(true)
          error.set(e)
      }
    }
  }
}

class StateScan(state: FileTierPartitionState,
                shutdown: AtomicBoolean,
                error: AtomicReference[Throwable],
                latestStartOffset: AtomicLong)
  extends Runnable {
  def run(): Unit = {
    var accum = 0L
    var prevSize = 0L
    while (!shutdown.get()) {
      try {
        state.flush()
        val newSize = state.totalSize
        accum += newSize
        if (prevSize > newSize) {
          throw new Exception("Size decreased between reads. This should not occur as we are appending.")
        }
        prevSize = newSize
      } catch {
        case e: Throwable =>
          shutdown.set(true)
          error.set(e)
      }
    }
  }
}

class TierPartitionStateConcurrencyTest {
  @Test
  def readWriteHotswapConcurrencyTest(): Unit = {
    val baseDir = TestUtils.tempDir
    val topic = UUID.randomUUID().toString
    val partition = 0
    val topicId = UUID.randomUUID()
    val tpid = new TopicIdPartition(topic, topicId, partition)
    val tp = tpid.topicPartition()
    val runLengthMs = 500
    val nThreads = 8
    val epoch = 0

    val state = new FileTierPartitionState(baseDir, new LogDirFailureChannel(5), tp, true)
    state.setTopicId(tpid.topicId)
    state.beginCatchup()
    state.onCatchUpComplete()
    val startTime = System.currentTimeMillis()
    // initialize readOffset to -1, as we will not want to advance
    // the read offset until we have swapped a state in and out
    val readOffset = new AtomicLong(-1)
    val exception = new AtomicReference[Throwable]()
    val shutdown = new AtomicBoolean(false)

    for (i <- 0 to nThreads / 2) {
      new Thread(new StateSeek(state, shutdown, exception, readOffset))
        .start()
    }

    try {
      state.append(
        new TierTopicInitLeader(tpid,
          epoch,
          java.util.UUID.randomUUID(),
          0), TierTestUtils.nextTierTopicOffsetAndEpoch())
      state.flush()
      var i = 0
      while (System.currentTimeMillis() < startTime + runLengthMs) {
        // take a backup before application
        val copyBytes = Files.readAllBytes(Paths.get(state.flushedPath()))
        val originalRestore = new TierPartitionForceRestore(tpid, UUID.randomUUID(), state.startOffset().orElse(-1L), state.endOffset(), TierTestUtils.nextTierTopicOffsetAndEpoch(), "contenthash")

        TierTestUtils.uploadWithMetadata(state,
          tpid,
          epoch,
          UUID.randomUUID,
          i * 2,
          i * 2 + 1,
          i,
          i,
          i,
          false,
          true,
          false,
          new OffsetAndEpoch(0L, Optional.empty()))
        state.flush()
        val revertedRestore = new TierPartitionForceRestore(tpid, UUID.randomUUID(), state.startOffset().orElse(-1L), state.endOffset(), TierTestUtils.nextTierTopicOffsetAndEpoch(), "contenthash")

        val correctBytes = Files.readAllBytes(Paths.get(state.flushedPath()))

        // swap state back and forth, make sure it doesn't die
        state.restoreState(originalRestore, ByteBuffer.wrap(copyBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch())
        state.restoreState(revertedRestore, ByteBuffer.wrap(correctBytes), TierPartitionStatus.ONLINE, TierTestUtils.nextTierTopicOffsetAndEpoch())

        // lag one read behind because of the way we hotswap
        readOffset.set((i-1) * 2)
        i += 1
      }

      shutdown.set(true)

      Thread.sleep(10)
      if (exception.get() != null) {
        exception.get().printStackTrace()
      }

      assertNull(exception.get())
    } finally {
      state.delete()
    }
  }

  @Test
  def readWriteConcurrencyTest(): Unit = {
    val baseDir = TestUtils.tempDir
    val topic = UUID.randomUUID().toString
    val partition = 0
    val topicId = UUID.randomUUID()
    val tpid = new TopicIdPartition(topic, topicId, partition)
    val tp = tpid.topicPartition()
    val runLengthMs = 500
    val nThreads = 8
    val epoch = 0

    val state = new FileTierPartitionState(baseDir, new LogDirFailureChannel(5), tp, true)
    state.setTopicId(tpid.topicId)
    state.beginCatchup()
    state.onCatchUpComplete()
    val startTime = System.currentTimeMillis()
    val latestStartOffset = new AtomicLong(0)
    val exception = new AtomicReference[Throwable]()
    val shutdown = new AtomicBoolean(false)

    for (i <- 0 to nThreads / 2) {
      new Thread(new StateSeek(state, shutdown, exception, latestStartOffset))
        .start()
    }

    for (i <- 0 to nThreads / 2) {
      new Thread(new StateScan(state, shutdown, exception, latestStartOffset))
        .start()
    }

    try {
      state.append(
        new TierTopicInitLeader(tpid,
          epoch,
          java.util.UUID.randomUUID(),
          0), TierTestUtils.nextTierTopicOffsetAndEpoch())
      var size = 0
      var i = 0
      while (System.currentTimeMillis() < startTime + runLengthMs) {
        TierTestUtils.uploadWithMetadata(state,
          tpid,
          epoch,
          UUID.randomUUID,
          i * 2,
          i * 2 + 1,
          i,
          i,
          i,
          false,
          true)
        state.flush()
        latestStartOffset.set(i * 2)
        size += i
        i += 1
      }

      shutdown.set(true)

      Thread.sleep(10)
      if (exception.get() != null) {
        exception.get().printStackTrace()
      }

      assertNull(exception.get())
    } finally {
      state.delete()
    }
  }
}
