/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
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

import java.nio.ByteBuffer
import java.util.Properties

import kafka.log.LogConfig
import kafka.message.ZStdCompressionCodec
import kafka.metrics.KafkaYammerMetrics
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.apache.kafka.test.InterceptorUtils.{AnotherMockRecordInterceptor, MockRecordInterceptor}
import org.junit.Assert._
import org.junit.Test
import org.scalatest.Assertions.fail

import scala.jdk.CollectionConverters._

/**
  * Subclasses of `BaseProduceSendRequestTest` exercise the producer and produce request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class ProduceRequestTest extends BaseRequestTest {

  val metricsKeySet = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala

  @Test
  def testSimpleProduceRequest(): Unit = {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    def sendAndCheck(memoryRecords: MemoryRecords, expectedOffset: Long): ProduceResponse.PartitionResponse = {
      val topicPartition = new TopicPartition("topic", partition)
      val partitionRecords = Map(topicPartition -> memoryRecords)
      val produceResponse = sendProduceRequest(leader,
          ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
      assertEquals(1, produceResponse.responses.size)
      val (tp, partitionResponse) = produceResponse.responses.asScala.head
      assertEquals(topicPartition, tp)
      assertEquals(Errors.NONE, partitionResponse.error)
      assertEquals(expectedOffset, partitionResponse.baseOffset)
      assertEquals(-1, partitionResponse.logAppendTime)
      assertTrue(partitionResponse.recordErrors.isEmpty)
      partitionResponse
    }

    sendAndCheck(MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes)), 0)

    sendAndCheck(MemoryRecords.withRecords(CompressionType.GZIP,
      new SimpleRecord(System.currentTimeMillis(), "key1".getBytes, "value1".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key2".getBytes, "value2".getBytes)), 1)
  }

  @Test
  def testProduceRequestDuringPartitionRecoveryAfterUncleanLeaderElection(): Unit = {
    // PRODUCE requests will return LEADER_NOT_AVAILABLE exception when the partition is under recovery
    // after unclean leader election. Partition is flagged as unclean when broker receives LeaderAndIsr request
    // with unclean leader flag set. It is flagged as clean once the recovery completes.
    val topic = "test-topic"
    val TopicConfig = new Properties()
    TopicConfig.put(LogConfig.MessageTimestampTypeProp, "LogAppendTime")
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 2, servers, TopicConfig)
    val partition = new TopicPartition(topic, 0)
    val leader = partitionToLeader(partition.partition)
    val replicas = zkClient.getReplicasForPartition(partition).toSet
    val follower = replicas.find(_ != leader).get
    val nonReplica = servers.map(_.config.brokerId).find(!replicas.contains(_)).get

    def produceRecordsAndValidateResponse(brokerId: Int, error: Errors, expectedOffset: Int): Unit = {
      val records = MemoryRecords.withRecords(CompressionType.NONE,
        new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
      val partitionRecords = Map(partition -> records)
      val produceResponse = sendProduceRequest(brokerId,
        ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
      // validate response
      assertEquals(s"Unexpected response size", 1, produceResponse.responses.size)
      val (tp, partitionResponse) = produceResponse.responses.asScala.head
      assertEquals(s"Unexpected TopicPartition", partition, tp)
      assertEquals(s"Unexpected base Offset", expectedOffset, partitionResponse.baseOffset)
      assertEquals(s"Unexpected error", error, partitionResponse.error)
      error match {
        case Errors.NONE =>
          assertNotEquals(s"Unexpected logAppendTime", RecordBatch.NO_TIMESTAMP, partitionResponse.logAppendTime)
          assertNotEquals(s"No error; Unexpected logStartOffset", -1, partitionResponse.logStartOffset)
        case _ =>
          assertEquals(s"Unexpected logAppendTime during error condition", RecordBatch.NO_TIMESTAMP, partitionResponse.logAppendTime)
          assertEquals(s"Unexpected logStartOffset during error condition", -1, partitionResponse.logStartOffset)
      }
      assertTrue(partitionResponse.recordErrors.isEmpty)
    }

    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(partition, expectLeader = true).setUncleanLeaderFlagTo(false)
    produceRecordsAndValidateResponse(leader, Errors.NONE, 0)
    produceRecordsAndValidateResponse(follower, Errors.NOT_LEADER_FOR_PARTITION, -1)
    produceRecordsAndValidateResponse(nonReplica, Errors.NOT_LEADER_FOR_PARTITION, -1)
    // Mark the partition unclean (indicating it needs recovery due to unclean leader election)
    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(partition, expectLeader = true).setUncleanLeaderFlagTo(true)
    produceRecordsAndValidateResponse(leader, Errors.LEADER_NOT_AVAILABLE, -1)
    produceRecordsAndValidateResponse(follower, Errors.NOT_LEADER_FOR_PARTITION, -1)
    produceRecordsAndValidateResponse(nonReplica, Errors.NOT_LEADER_FOR_PARTITION, -1)
    // Mark the partition clean. This should enable produce requests to be processed
    servers.find(_.config.brokerId == leader).get.replicaManager.getPartitionOrException(partition, expectLeader = true).setUncleanLeaderFlagTo(false)
    produceRecordsAndValidateResponse(leader, Errors.NONE, 1)
    produceRecordsAndValidateResponse(follower, Errors.NOT_LEADER_FOR_PARTITION, -1)
    produceRecordsAndValidateResponse(nonReplica, Errors.NOT_LEADER_FOR_PARTITION, -1)
  }

  @Test
  def testProduceWithInvalidTimestamp(): Unit = {
    val topic = "topic"
    val partition = 0
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MessageTimestampDifferenceMaxMsProp, "1000")
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)

    def createRecords(magicValue: Byte, timestamp: Long, codec: CompressionType): MemoryRecords = {
      val buf = ByteBuffer.allocate(512)
      val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L)
      builder.appendWithOffset(0, timestamp, null, "hello".getBytes)
      builder.appendWithOffset(1, timestamp, null, "there".getBytes)
      builder.appendWithOffset(2, timestamp, null, "beautiful".getBytes)
      builder.build()
    }

    val records = createRecords(RecordBatch.MAGIC_VALUE_V2, System.currentTimeMillis() - 1001L, CompressionType.GZIP)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> records)
    val produceResponse = sendProduceRequest(leader, ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.INVALID_TIMESTAMP, partitionResponse.error)
    // there are 3 records with InvalidTimestampException created from inner function createRecords
    assertEquals(3, partitionResponse.recordErrors.size())
    assertEquals(0, partitionResponse.recordErrors.get(0).batchIndex)
    assertEquals(1, partitionResponse.recordErrors.get(1).batchIndex)
    assertEquals(2, partitionResponse.recordErrors.get(2).batchIndex)
    for (recordError <- partitionResponse.recordErrors.asScala) {
      assertNotNull(recordError.message)
    }
    assertEquals("One or more records have been rejected due to invalid timestamp", partitionResponse.errorMessage)
  }

  @Test
  def testProduceToNonReplica(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, 1, servers)
    val leader = partitionToLeader(partition)
    val nonReplicaOpt = servers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId =  nonReplicaOpt.get.config.brokerId

    // Send the produce request to the non-replica
    val records = MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("key".getBytes, "value".getBytes))
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> records)
    val produceRequest = ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build()

    val produceResponse = sendProduceRequest(nonReplicaId, produceRequest)
    assertEquals(1, produceResponse.responses.size)
    assertEquals(Errors.NOT_LEADER_FOR_PARTITION, produceResponse.responses.asScala.head._2.error)
  }

  /* returns a pair of partition id and leader id */
  private def createTopicAndFindPartitionWithLeader(topic: String): (Int, Int) = {
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 3, 2, servers)
    partitionToLeader.collectFirst {
      case (partition, leader) if leader != -1 => (partition, leader)
    }.getOrElse(fail(s"No leader elected for topic $topic"))
  }

  @Test
  def testCorruptLz4ProduceRequest(): Unit = {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val timestamp = 1000000
    val memoryRecords = MemoryRecords.withRecords(CompressionType.LZ4,
      new SimpleRecord(timestamp, "key".getBytes, "value".getBytes))
    // Change the lz4 checksum value (not the kafka record crc) so that it doesn't match the contents
    val lz4ChecksumOffset = 6
    memoryRecords.buffer.array.update(DefaultRecordBatch.RECORD_BATCH_OVERHEAD + lz4ChecksumOffset, 0)
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)
    val produceResponse = sendProduceRequest(leader, 
      ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE, partitionResponse.error)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.logAppendTime)
    assertEquals(metricsKeySet.count(_.getMBeanName.endsWith(s"${BrokerTopicStats.InvalidMessageCrcRecordsPerSec}")), 1)
    assertTrue(TestUtils.meterCount(s"${BrokerTopicStats.InvalidMessageCrcRecordsPerSec}") > 0)
  }

  @Test
  def testZSTDProduceRequest(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic compressed with ZSTD
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.CompressionTypeProp, ZStdCompressionCodec.name)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.ZSTD,
      new SimpleRecord(System.currentTimeMillis(), "key".getBytes, "value".getBytes))
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)

    // produce request with v7: works fine!
    val res1 = sendProduceRequest(leader,
      new ProduceRequest.Builder(7, 7, -1, 3000, partitionRecords.asJava, null).build())
    val (tp1, partitionResponse1) = res1.responses.asScala.head
    assertEquals(topicPartition, tp1)
    assertEquals(Errors.NONE, partitionResponse1.error)
    assertEquals(0, partitionResponse1.baseOffset)
    assertEquals(-1, partitionResponse1.logAppendTime)

    // produce request with v3: returns Errors.UNSUPPORTED_COMPRESSION_TYPE.
    val res2 = sendProduceRequest(leader,
      new ProduceRequest.Builder(3, 3, -1, 3000, partitionRecords.asJava, null)
        .buildUnsafe(3))
    val (tp2, partitionResponse2) = res2.responses.asScala.head
    assertEquals(topicPartition, tp2)
    assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, partitionResponse2.error)
  }

  @Test
  def testProduceRequestIncludesRecordsRejectedByTheSameInterceptor(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.AppendRecordInterceptorClassesProp, classOf[MockRecordInterceptor].getName)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key0".getBytes, "reject me".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key1".getBytes, "reject me".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key2".getBytes, "value".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key3".getBytes, "value".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key4".getBytes, "reject me".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key5".getBytes, "value".getBytes))

    val topicPartition = new TopicPartition(topic, partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)

    val produceResponse = sendProduceRequest(leader,
      ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.INVALID_RECORD, partitionResponse.error)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.logAppendTime)
    assertNotNull(partitionResponse.recordErrors)
    val recordErrors = partitionResponse.recordErrors
    assertEquals(3, recordErrors.size)
    assertEquals(0, recordErrors.get(0).batchIndex)
    assertTrue(recordErrors.get(0).message.endsWith(s"rejected by the record interceptor ${classOf[MockRecordInterceptor].getName}"))
    assertEquals(1, recordErrors.get(1).batchIndex)
    assertTrue(recordErrors.get(1).message.endsWith(s"rejected by the record interceptor ${classOf[MockRecordInterceptor].getName}"))
    assertEquals(4, recordErrors.get(2).batchIndex)
    assertTrue(recordErrors.get(2).message.endsWith(s"rejected by the record interceptor ${classOf[MockRecordInterceptor].getName}"))
    assertEquals("One or more records have been rejected", partitionResponse.errorMessage)
  }

  @Test
  def testProduceRequestIncludesRecordsRejectedByDifferentInterceptors(): Unit = {
    val topic = "topic"
    val partition = 0

    // Create a single-partition topic
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.AppendRecordInterceptorClassesProp, s"${classOf[MockRecordInterceptor].getName},${classOf[AnotherMockRecordInterceptor].getName}")
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)
    val memoryRecords = MemoryRecords.withRecords(CompressionType.NONE,
      new SimpleRecord(System.currentTimeMillis(), "key0".getBytes, "reject me".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key1".getBytes, "value".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key2".getBytes, "reject me please".getBytes),
      new SimpleRecord(System.currentTimeMillis(), "key3".getBytes, "value".getBytes))

    val topicPartition = new TopicPartition(topic, partition)
    val partitionRecords = Map(topicPartition -> memoryRecords)

    val produceResponse = sendProduceRequest(leader,
      ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.INVALID_RECORD, partitionResponse.error)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.logAppendTime)
    assertNotNull(partitionResponse.recordErrors)
    val recordErrors = partitionResponse.recordErrors
    assertEquals(2, recordErrors.size)
    assertEquals(0, recordErrors.get(0).batchIndex)
    assertTrue(partitionResponse.recordErrors.get(0).message.endsWith(s"rejected by the record interceptor ${classOf[MockRecordInterceptor].getName}"))
    assertEquals(2, recordErrors.get(1).batchIndex)
    assertTrue(partitionResponse.recordErrors.get(1).message.endsWith(s"rejected by the record interceptor ${classOf[AnotherMockRecordInterceptor].getName}"))
    assertEquals("One or more records have been rejected", partitionResponse.errorMessage)
  }

  /**
   * If a batch contains records both rejected because of interceptors and of invalid timestamp
   * then test that the error code is INVALID_TIMESTAMP
   */
  @Test
  def testProduceRequestIncludesInvalidTimestampAndInterceptedRecords(): Unit = {
    val topic = "topic"
    val partition = 0
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MessageTimestampDifferenceMaxMsProp, "1000")
    topicConfig.setProperty(LogConfig.AppendRecordInterceptorClassesProp, classOf[MockRecordInterceptor].getName)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, 1, 1, servers, topicConfig)
    val leader = partitionToLeader(partition)

    val timestamp = System.currentTimeMillis() - 1001L
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryRecords.builder(buf, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, 0L)
    builder.appendWithOffset(0, timestamp, null, "hello".getBytes)
    builder.appendWithOffset(1, timestamp, null, "there".getBytes)
    builder.appendWithOffset(2, timestamp, null, "beautiful".getBytes)
    // we want this record to fail because of interceptor
    builder.appendWithOffset(3, System.currentTimeMillis(), null, "reject me".getBytes)
    val records = builder.build()

    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> records)
    val produceResponse = sendProduceRequest(leader, ProduceRequest.Builder.forCurrentMagic(-1, 3000, partitionRecords.asJava).build())
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.INVALID_TIMESTAMP, partitionResponse.error)
    assertEquals(4, partitionResponse.recordErrors.size())
    assertTrue(partitionResponse.recordErrors.get(3).message.endsWith(s"rejected by the record interceptor ${classOf[MockRecordInterceptor].getName}"))
    // the global error message is still related to timestamp
    assertEquals("One or more records have been rejected due to invalid timestamp", partitionResponse.errorMessage)
  }

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    connectAndReceive[ProduceResponse](request, destination = brokerSocketServer(leaderId))
  }

}
