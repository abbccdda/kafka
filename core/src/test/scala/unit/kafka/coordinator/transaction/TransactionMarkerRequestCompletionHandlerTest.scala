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
package kafka.coordinator.transaction

import java.lang.management.ManagementFactory
import java.{lang, util}
import java.util.Arrays.asList

import javax.management.ObjectName
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, Sensor}
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.{RequestHeader, TransactionResult, WriteTxnMarkersRequest, WriteTxnMarkersResponse}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TransactionMarkerRequestCompletionHandlerTest {

  private val brokerId = 0
  private val txnTopicPartition = 0
  private val transactionalId = "txnId1"
  private val producerId = 0.asInstanceOf[Long]
  private val producerEpoch = 0.asInstanceOf[Short]
  private val lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH
  private val txnTimeoutMs = 0
  private val coordinatorEpoch = 0
  private val txnResult = TransactionResult.COMMIT
  private val topicPartition = new TopicPartition("topic1", 0)
  private val txnIdAndMarkers = asList(
      TxnIdAndMarkerEntry(transactionalId, new WriteTxnMarkersRequest.TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, txnResult, asList(topicPartition))))

  private val txnMetadata = new TransactionMetadata(transactionalId, producerId, producerId, producerEpoch, lastProducerEpoch,
    txnTimeoutMs, PrepareCommit, mutable.Set[TopicPartition](topicPartition), 0L, 0L)

  private val markerChannelManager: TransactionMarkerChannelManager =
    EasyMock.createNiceMock(classOf[TransactionMarkerChannelManager])

  private val txnStateManager: TransactionStateManager = EasyMock.createNiceMock(classOf[TransactionStateManager])

  private val handler = new TransactionMarkerRequestCompletionHandler(brokerId, txnStateManager, markerChannelManager, txnIdAndMarkers)

  private val metrics = new Metrics()
  val stateErrorRateMetricName = metrics.metricName("transaction-state-error-rate", TransactionStateManager.MetricsGroup,
    "The rate at which state errors occur within the transaction coordinator")
  val stateErrorCountMetricName = metrics.metricName("transaction-state-error-count", TransactionStateManager.MetricsGroup,
    "The total count of state errors that have occurred within the transaction coordinator")

  private def mockCache(): Unit = {
    EasyMock.expect(txnStateManager.partitionFor(transactionalId))
      .andReturn(txnTopicPartition)
      .anyTimes()
    EasyMock.expect(txnStateManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
      .anyTimes()
    EasyMock.replay(txnStateManager)
  }

  @Test
  def shouldReEnqueuePartitionsWhenBrokerDisconnected(): Unit = {
    mockCache()

    EasyMock.expect(markerChannelManager.addTxnMarkersToBrokerQueue(transactionalId,
      producerId, producerEpoch, txnResult, coordinatorEpoch, Set[TopicPartition](topicPartition)))
    EasyMock.replay(markerChannelManager)

    handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
      null, null, 0, 0, true, null, null, null))

    EasyMock.verify(markerChannelManager)
  }

  @Test
  def shouldThrowIllegalStateExceptionIfErrorCodeNotAvailableForPid(): Unit = {
    EasyMock.expect(txnStateManager.stateErrorSensor).andReturn(setupStateErrorSensor())

    mockCache()
    EasyMock.replay(markerChannelManager)

    val response = new WriteTxnMarkersResponse(new java.util.HashMap[java.lang.Long, java.util.Map[TopicPartition, Errors]]())

    try {
      handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
        null, null, 0, 0, false, null, null, response))
      fail("should have thrown illegal argument exception")
    } catch {
      case _: IllegalStateException => // ok
    }

    assertEquals(1.0, metrics.metric(stateErrorCountMetricName).metricValue().asInstanceOf[Double], 0.0)
    assertTrue(metrics.metric(stateErrorRateMetricName).metricValue().asInstanceOf[Double] > 0)
  }

  @Test
  def shouldCompleteDelayedOperationWhenNoErrors(): Unit = {
    mockCache()

    verifyCompleteDelayedOperationOnError(Errors.NONE)
  }

  @Test
  def shouldCompleteDelayedOperationWhenNotCoordinator(): Unit = {
    EasyMock.expect(txnStateManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.NOT_COORDINATOR))
      .anyTimes()
    EasyMock.replay(txnStateManager)

    verifyRemoveDelayedOperationOnError(Errors.NONE)
  }

  @Test
  def shouldCompleteDelayedOperationWhenCoordinatorLoading(): Unit = {
    EasyMock.expect(txnStateManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Left(Errors.COORDINATOR_LOAD_IN_PROGRESS))
      .anyTimes()
    EasyMock.replay(txnStateManager)

    verifyRemoveDelayedOperationOnError(Errors.NONE)
  }

  @Test
  def shouldCompleteDelayedOperationWhenCoordinatorEpochChanged(): Unit = {
    EasyMock.expect(txnStateManager.getTransactionState(EasyMock.eq(transactionalId)))
      .andReturn(Right(Some(CoordinatorEpochAndTxnMetadata(coordinatorEpoch+1, txnMetadata))))
      .anyTimes()
    EasyMock.replay(txnStateManager)

    verifyRemoveDelayedOperationOnError(Errors.NONE)
  }

  @Test
  def shouldCompleteDelayedOperationWhenInvalidProducerEpoch(): Unit = {
    mockCache()

    verifyRemoveDelayedOperationOnError(Errors.INVALID_PRODUCER_EPOCH)
  }

  @Test
  def shouldCompleteDelayedOperationWheCoordinatorEpochFenced(): Unit = {
    mockCache()

    verifyRemoveDelayedOperationOnError(Errors.TRANSACTION_COORDINATOR_FENCED)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenUnknownError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.UNKNOWN_SERVER_ERROR)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenCorruptMessageError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.CORRUPT_MESSAGE)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenMessageTooLargeError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.MESSAGE_TOO_LARGE)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenRecordListTooLargeError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.RECORD_LIST_TOO_LARGE)
  }

  @Test
  def shouldThrowIllegalStateExceptionWhenInvalidRequiredAcksError(): Unit = {
    verifyThrowIllegalStateExceptionOnError(Errors.INVALID_REQUIRED_ACKS)
  }

  @Test
  def shouldRetryPartitionWhenUnknownTopicOrPartitionError(): Unit = {
    verifyRetriesPartitionOnError(Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def shouldRetryPartitionWhenNotLeaderForPartitionError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_LEADER_FOR_PARTITION)
  }

  @Test
  def shouldRetryPartitionWhenNotEnoughReplicasError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_ENOUGH_REPLICAS)
  }

  @Test
  def shouldRetryPartitionWhenNotEnoughReplicasAfterAppendError(): Unit = {
    verifyRetriesPartitionOnError(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
  }

  @Test
  def shouldRetryPartitionWhenKafkaStorageError(): Unit = {
    verifyRetriesPartitionOnError(Errors.KAFKA_STORAGE_ERROR)
  }

  @Test
  def shouldRemoveTopicPartitionFromWaitingSetOnUnsupportedForMessageFormat(): Unit = {
    mockCache()
    verifyCompleteDelayedOperationOnError(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT)
  }

  private def verifyRetriesPartitionOnError(error: Errors) = {
    mockCache()

    EasyMock.expect(markerChannelManager.addTxnMarkersToBrokerQueue(transactionalId,
      producerId, producerEpoch, txnResult, coordinatorEpoch, Set[TopicPartition](topicPartition)))
    EasyMock.replay(markerChannelManager)

    val response = new WriteTxnMarkersResponse(createProducerIdErrorMap(error))
    handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
      null, null, 0, 0, false, null, null, response))

    assertEquals(txnMetadata.topicPartitions, mutable.Set[TopicPartition](topicPartition))
    EasyMock.verify(markerChannelManager)
  }

  private def verifyThrowIllegalStateExceptionOnError(error: Errors) = {
    EasyMock.expect(txnStateManager.stateErrorSensor).andReturn(setupStateErrorSensor())

    val server = ManagementFactory.getPlatformMBeanServer
    val mBeanName = "kafka.server:type=transaction-coordinator-metrics"
    def getStateErrorCount(): Double = {
      server.getAttribute(new ObjectName(mBeanName), "transaction-state-error-count").asInstanceOf[Double]
    }

    mockCache()

    val response = new WriteTxnMarkersResponse(createProducerIdErrorMap(error))
    try {
      handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
        null, null, 0, 0, false, null, null, response))
      fail("should have thrown illegal state exception")
    } catch {
      case _: IllegalStateException => // ok
    }

    assertEquals(1.0, metrics.metric(stateErrorCountMetricName).metricValue().asInstanceOf[Double], 0.0)
    assertTrue(metrics.metric(stateErrorRateMetricName).metricValue().asInstanceOf[Double] > 0)
  }

  private def verifyCompleteDelayedOperationOnError(error: Errors): Unit = {

    var completed = false
    EasyMock.expect(markerChannelManager.completeSendMarkersForTxnId(transactionalId))
      .andAnswer(() => completed = true)
      .once()
    EasyMock.replay(markerChannelManager)

    val response = new WriteTxnMarkersResponse(createProducerIdErrorMap(error))
    handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
      null, null, 0, 0, false, null, null, response))

    assertTrue(txnMetadata.topicPartitions.isEmpty)
    assertTrue(completed)
  }

  private def verifyRemoveDelayedOperationOnError(error: Errors): Unit = {

    var removed = false
    EasyMock.expect(markerChannelManager.removeMarkersForTxnId(transactionalId))
      .andAnswer(() => removed = true)
      .once()
    EasyMock.replay(markerChannelManager)

    val response = new WriteTxnMarkersResponse(createProducerIdErrorMap(error))
    handler.onComplete(new ClientResponse(new RequestHeader(ApiKeys.PRODUCE, 0, "client", 1),
      null, null, 0, 0, false, null, null, response))

    assertTrue(removed)
  }


  private def createProducerIdErrorMap(errors: Errors) = {
    val pidMap = new java.util.HashMap[lang.Long, util.Map[TopicPartition, Errors]]()
    val errorsMap = new util.HashMap[TopicPartition, Errors]()
    errorsMap.put(topicPartition, errors)
    pidMap.put(producerId, errorsMap)
    pidMap
  }

  private def setupStateErrorSensor(): Sensor = {
    val reporter = new JmxReporter("kafka.server")
    metrics.addReporter(reporter)

    val sensor = metrics.sensor("TransactionStateErrors")
    sensor.add(new Meter(stateErrorRateMetricName, stateErrorCountMetricName))
    sensor
  }
}
