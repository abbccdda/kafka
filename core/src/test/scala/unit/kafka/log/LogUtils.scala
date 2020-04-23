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

package kafka.log

import java.io.File
import java.util.concurrent.Semaphore

import kafka.api.ApiVersion
import org.apache.kafka.common.record.{BufferSupplier, FileRecords, MemoryRecords}
import org.apache.kafka.common.utils.Time

object LogUtils {
  /**
    *  Create a segment with the given base offset
    */
  def createSegment(offset: Long,
                    logDir: File,
                    indexIntervalBytes: Int = 10,
                    time: Time = Time.SYSTEM): LogSegment = {
    val ms = FileRecords.open(Log.logFile(logDir, offset))
    val idx = LazyIndex.forOffset(Log.offsetIndexFile(logDir, offset), offset, maxIndexSize = 1000)
    val timeIdx = LazyIndex.forTime(Log.timeIndexFile(logDir, offset), offset, maxIndexSize = 1500)
    val txnIndex = new TransactionIndex(offset, Log.transactionIndexFile(logDir, offset))

    new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time)
  }

  class SlowAppendAsFollowerLog(log: MergedLog,
                                tierLogComponents: TierLogComponents,
                                appendSemaphore: Semaphore) extends MergedLog(
    localLog = log.localLog,
    logStartOffset = log.localLogStartOffset,
    tierPartitionState = log.tierPartitionState,
    tierLogComponents = tierLogComponents) {

    override def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
      appendSemaphore.acquire()
      val appendInfo = super.appendAsFollower(records)
      appendInfo
    }
  }

  class SlowAppendAsLeaderLog(log: MergedLog,
                              tierLogComponents: TierLogComponents,
                              appendSemaphore: Semaphore) extends MergedLog(
    localLog = log.localLog,
    logStartOffset = log.localLogStartOffset,
    tierPartitionState = log.tierPartitionState,
    tierLogComponents = tierLogComponents) {

    override def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, origin: AppendOrigin,
                                interBrokerProtocolVersion: ApiVersion,
                                bufferSupplier: BufferSupplier = BufferSupplier.NO_CACHING): LogAppendInfo = {
      val appendInfo = super.appendAsLeader(records, leaderEpoch, origin, interBrokerProtocolVersion,
        bufferSupplier)
      appendSemaphore.acquire()
      appendInfo
    }
  }
}
