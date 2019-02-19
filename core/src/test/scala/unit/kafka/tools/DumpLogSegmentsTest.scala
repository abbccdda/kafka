/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.io.{File, ByteArrayOutputStream}
import java.util.Properties

import scala.collection.mutable
import kafka.log.{LogConfig, LogManager, Log, MergedLog}
import kafka.server.{LogDirFailureChannel, BrokerTopicStats}
import kafka.utils.{TestUtils, MockTime}
import org.apache.kafka.common.record.{MemoryRecords, CompressionType, SimpleRecord}
import org.apache.kafka.common.utils.Utils
import org.junit.Assert._
import org.junit.{Before, After, Test}

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

class DumpLogSegmentsTest {

  val tmpDir = TestUtils.tempDir()
  val logDir = TestUtils.randomPartitionLogDir(tmpDir)
  val segmentName = "00000000000000000000"
  val logFilePath = s"$logDir/$segmentName.log"
  val indexFilePath = s"$logDir/$segmentName.index"
  val timeIndexFilePath = s"$logDir/$segmentName.timeindex"
  val time = new MockTime(0, 0)

  val batches = new ArrayBuffer[Seq[SimpleRecord]]
  var log: Log = _

  @Before
  def setUp(): Unit = {
    val props = new Properties
    props.setProperty(LogConfig.IndexIntervalBytesProp, "128")
    val log = MergedLog(logDir, LogConfig(props), logStartOffset = 0L, recoveryPoint = 0L, scheduler = time.scheduler,
      brokerTopicStats = new BrokerTopicStats, time = time, maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      logDirFailureChannel = new LogDirFailureChannel(10),
      tierMetadataManagerOpt = Some(TestUtils.createTierMetadataManager(Seq(logDir))))

    val now = System.currentTimeMillis()
    val firstBatchRecords = (0 until 10).map { i => new SimpleRecord(now + i * 2, s"hello there $i".getBytes)}
    batches += firstBatchRecords
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, 0, firstBatchRecords: _*),
      leaderEpoch = 0)
    val secondBatchRecords = (10 until 30).map { i => new SimpleRecord(now + i * 3, s"hello there again $i".getBytes)}
    batches += secondBatchRecords
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, 0, secondBatchRecords: _*),
      leaderEpoch = 0)
    val thirdBatchRecords = (30 until 50).map { i => new SimpleRecord(now + i * 5, s"hello there one more time $i".getBytes)}
    batches += thirdBatchRecords
    log.appendAsLeader(MemoryRecords.withRecords(CompressionType.NONE, 0, thirdBatchRecords: _*),
      leaderEpoch = 0)

    // Flush, but don't close so that the indexes are not trimmed and contain some zero entries
    log.flush()
  }

  @After
  def tearDown(): Unit = {
    log.close()
    Utils.delete(tmpDir)
  }

  @Test
  def testPrintDataLog(): Unit = {

    def verifyRecordsInOutput(args: Array[String]): Unit = {
      def isBatch(index: Int): Boolean = {
        var i = 0
        batches.zipWithIndex.foreach { case (batch, batchIndex) =>
          if (i == index)
            return true

          i += 1

          batch.indices.foreach { recordIndex =>
            if (i == index)
              return false
            i += 1
          }
        }
        TestUtils.fail(s"No match for index $index")
      }

      val output = runDumpLogSegments(args)
      val lines = output.split("\n")
      assertTrue(s"Data not printed: $output", lines.length > 2)
      val totalRecords = batches.map(_.size).sum
      var offset = 0
      (0 until totalRecords + batches.size).foreach { index =>
        val line = lines(lines.length - totalRecords - batches.size + index)
        // The base offset of the batch is the offset of the first record in the batch, so we
        // only increment the offset if it's not a batch
        if (isBatch(index))
          assertTrue(s"Not a valid batch-level message record: $line", line.startsWith(s"baseOffset: $offset lastOffset: "))
        else {
          assertTrue(s"Not a valid message record: $line", line.startsWith(s"${DumpLogSegments.RecordIndent} offset: $offset"))
          offset += 1
        }
      }
    }

    def verifyNoRecordsInOutput(args: Array[String]): Unit = {
      val output = runDumpLogSegments(args)
      assertFalse(s"Data should not have been printed: $output", output.matches("(?s).*offset: [0-9]* isvalid.*"))
    }

    // Verify that records are printed with --print-data-log even if --deep-iteration is not specified
    verifyRecordsInOutput(Array("--print-data-log", "--files", logFilePath))
    // Verify that records are printed with --print-data-log if --deep-iteration is also specified
    verifyRecordsInOutput(Array("--print-data-log", "--deep-iteration", "--files", logFilePath))
    // Verify that records are printed with --value-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(Array("--value-decoder-class", "kafka.serializer.StringDecoder", "--files", logFilePath))
    // Verify that records are printed with --key-decoder even if --print-data-log is not specified
    verifyRecordsInOutput(Array("--key-decoder-class", "kafka.serializer.StringDecoder", "--files", logFilePath))
    // Verify that records are printed with --deep-iteration even if --print-data-log is not specified
    verifyRecordsInOutput(Array("--deep-iteration", "--files", logFilePath))

    // Verify that records are not printed by default
    verifyNoRecordsInOutput(Array("--files", logFilePath))
  }

  @Test
  def testDumpIndexMismatches(): Unit = {
    val offsetMismatches = mutable.Map[String, List[(Long, Long)]]()
    DumpLogSegments.dumpIndex(new File(indexFilePath), indexSanityOnly = false, verifyOnly = true, offsetMismatches,
      Int.MaxValue)
    assertEquals(Map.empty, offsetMismatches)
  }

  @Test
  def testDumpTimeIndexErrors(): Unit = {
    val errors = new TimeIndexDumpErrors
    DumpLogSegments.dumpTimeIndex(new File(timeIndexFilePath), indexSanityOnly = false, verifyOnly = true, errors,
      Int.MaxValue)
    assertEquals(Map.empty, errors.misMatchesForTimeIndexFilesMap)
    assertEquals(Map.empty, errors.outOfOrderTimestamp)
    assertEquals(Map.empty, errors.shallowOffsetNotFound)
  }

  private def runDumpLogSegments(args: Array[String]): String = {
    val outContent = new ByteArrayOutputStream
    Console.withOut(outContent) {
      DumpLogSegments.main(args)
    }
    outContent.toString
  }
}
