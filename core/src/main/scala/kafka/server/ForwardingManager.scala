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

import java.util.concurrent.atomic.AtomicInteger

import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, EnvelopeRequest, EnvelopeResponse}
import org.apache.kafka.common.utils.Time

class ForwardingManager(metadataCache: kafka.server.MetadataCache,
                        time: Time,
                        metrics: Metrics,
                        config: KafkaConfig,
                        channelType: BrokerToControllerChannelType,
                        threadNamePrefix: Option[String] = None) extends
  BrokerToControllerChannelManagerImpl(metadataCache, time, metrics, config, channelType, threadNamePrefix) with KafkaMetricsGroup {

  private val forwardingMetricName = "NumRequestsForwardingToControllerPerSec"
  private val numRequestsInForwarding = new AtomicInteger(0)

  def forwardRequest(responseToOriginalClient: (RequestChannel.Request, Int => AbstractResponse) => Unit,
                     request: RequestChannel.Request): Unit = {
    val serializedPrincipal = request.principalSerde.get.serialize(request.context.principal)
    val forwardRequestBuffer = request.buffer.duplicate()
    forwardRequestBuffer.flip()
    val envelopeRequest = new EnvelopeRequest.Builder(
      forwardRequestBuffer,
      serializedPrincipal,
      request.context.clientAddress.getAddress
    )

    numRequestsInForwarding.incrementAndGet()

    sendRequest(envelopeRequest, (response: ClientResponse) => responseToOriginalClient(
      request, _ => {
        val envelopeResponse = response.responseBody.asInstanceOf[EnvelopeResponse]
        val internalError = envelopeResponse.error()
        numRequestsInForwarding.decrementAndGet()

        if (internalError!= Errors.NONE) {
          debug(s"Encountered error $internalError during request forwarding, returning unknown server " +
            s"error to the client to indicate that the failure is caused by the inter-broker communication.")
          request.body[AbstractRequest].getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception())
        } else {
          AbstractResponse.deserializeBody(envelopeResponse.embedResponseData, request.header)
        }
      }))
  }

  override def start(): Unit = {
    super.shutdown()
    newGauge(forwardingMetricName, () => numRequestsInForwarding)
  }

  override def shutdown(): Unit = {
    super.shutdown()
    removeMetric(forwardingMetricName)
  }
}
