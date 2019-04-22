/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import kafka.tier.domain.{TierObjectMetadata, TierTopicInitLeader}
import kafka.tier.serdes.State
import kafka.tier.state.FileTierPartitionState
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test

class StateSeek(state: FileTierPartitionState,
                shutdown: AtomicBoolean,
                error: AtomicReference[Throwable],
                latestStartOffset: AtomicLong)
  extends Runnable {
  def run() {
    while (!shutdown.get()) {
      try {
        val offset = latestStartOffset.get()
        // read until file update is available
        while (!state.metadata(offset).isPresent) {}
        val found = state
          .metadata(offset)
          .get()
          .startOffset()
        if (offset != found) {
          error.set(new Exception("Unexpected offset found expected: " + offset + " found: " + found))
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
  def run() {
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
  def readWriteConcurrencyTest(): Unit = {
    val baseDir = TestUtils.tempDir
    val topic = UUID.randomUUID().toString
    val partition = 0
    val tp = new TopicPartition(topic, partition)
    val runLengthMs = 500
    val nThreads = 8
    val epoch = 0

    val state = new FileTierPartitionState(baseDir, tp, true, false)
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
        new TierTopicInitLeader(tp,
          epoch,
          java.util.UUID.randomUUID(),
          0))
      var size = 0
      var i = 0
      while (System.currentTimeMillis() < startTime + runLengthMs) {
        state.append(
          new TierObjectMetadata(tp,
            epoch,
            i * 2,
            1,
            1,
            i,
            i,
            true,
            false,
            State.AVAILABLE))
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
