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
package kafka.server

import java.util.concurrent.TimeUnit

import kafka.network.RequestChannel
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.quota.ClientQuotaCallback

import scala.jdk.CollectionConverters._


class ClientRequestQuotaManager(private val config: ClientQuotaManagerConfig,
                                private val metrics: Metrics,
                                private val time: Time,
                                threadNamePrefix: String,
                                quotaCallback: Option[ClientQuotaCallback],
                                activeTenantsManager: Option[ActiveTenantsManager])
                                extends ClientQuotaManager(config, metrics, QuotaType.Request, time, threadNamePrefix, quotaCallback, activeTenantsManager) {
  private val threadUsageSensors = new ThreadUsageSensors(metrics, ClientQuotaManagerConfig.InactiveSensorExpirationTimeSeconds)
  // tracks the decrease in broker request quota limit due to request overload, if any
  // dynamic broker request quota limit is the percent of time available on IO and network threads (based on number
  // of threads and their current usage) minus 'lastLimitCorrection' which gets increased/decreased dynamically based
  // on the load of the data-plane request queue
  private var lastLimitCorrection: Double = 0.0

  val maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(this.config.quotaWindowSizeSeconds)
  def exemptSensor = getOrCreateSensor(exemptSensorName, exemptMetricName)

  // sensor for broker-wide limit on percentage of time requests can spend on network + request
  // handler threads; recorded and emitted when quotas are enabled, and used for request
  // backpressure when request backpressure is enabled
  def nonExemptCapacitySensor = getOrCreateValueSensor(
    "non-exempt-capacity", BrokerBackpressureMetrics.nonExemptRequestCapacityMetricName(metrics))

  def recordExemptNetworkThread(value: Double, listenerName: String, timeMs: Long): Unit = {
    exemptSensor.record(value, timeMs)
    recordNetworkUsage(value, listenerName, ExemptRequest, timeMs)
  }

  def recordExemptIoThread(value: Double, timeMs: Long): Unit = {
    exemptSensor.record(value, timeMs)
  }

  def removeListenerMetrics(listenerName: String): Unit = {
    threadUsageSensors.removeListenerMetrics(listenerName)
  }

  /**
    * Records that a user/clientId changed request processing time being throttled. If quota has been violated, return
    * throttle time in milliseconds. Throttle time calculation may be overridden by sub-classes.
    * @param request client request
    * @return Number of milliseconds to throttle in case of quota violation. Zero otherwise
    */
  def maybeRecordAndGetThrottleTimeMs(request: RequestChannel.Request): Int = {
    if (request.apiRemoteCompleteTimeNanos == -1) {
      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeNanos = time.nanoseconds
    }

    val currentTimeMs = time.milliseconds()
    val reqIoThreadPercentage = nanosToPercentage(request.requestThreadTimeNanos)
    val listenerName = request.context.listenerName.value
    threadUsageSensors.recordIoThreadUsage(reqIoThreadPercentage, currentTimeMs)

    if (quotasEnabled) {
      threadUsageSensors.recordIoThreadUsage(reqIoThreadPercentage, currentTimeMs, Some(NonExemptRequest))

      request.recordNetworkThreadTimeCallback = Some(timeNanos => {
        recordNoThrottle(getOrCreateQuotaSensors(request.session, request.header.clientId), nanosToPercentage(timeNanos))
        recordNetworkUsage(nanosToPercentage(timeNanos), listenerName, NonExemptRequest, time.milliseconds())
      })
      recordAndGetThrottleTimeMs(request.session, request.header.clientId, reqIoThreadPercentage, currentTimeMs)
    } else {
      request.recordNetworkThreadTimeCallback = Some(timeNanos => recordNetworkUsage(
        nanosToPercentage(timeNanos), listenerName, NonExemptRequest, time.milliseconds()))
      0
    }
  }

  def maybeRecordExempt(request: RequestChannel.Request): Unit = {
    val currentTimeMs = time.milliseconds()
    val reqIoThreadPercentage = nanosToPercentage(request.requestThreadTimeNanos)
    val listenerName = request.context.listenerName.value
    threadUsageSensors.recordIoThreadUsage(reqIoThreadPercentage, currentTimeMs)

    if (quotasEnabled) {
      request.recordNetworkThreadTimeCallback = Some(timeNanos => {
        recordExemptNetworkThread(nanosToPercentage(timeNanos), listenerName, time.milliseconds())
      })
      recordExemptIoThread(reqIoThreadPercentage, currentTimeMs)
    } else {
      request.recordNetworkThreadTimeCallback = Some(timeNanos => recordNetworkUsage(
        nanosToPercentage(timeNanos), listenerName, ExemptRequest, time.milliseconds()))
    }
  }

  override def backpressureEnabled: Boolean =
    dynamicBackpressureConfig.backpressureEnabledInConfig &&
    dynamicBackpressureConfig.tenantEndpointListenerNames.nonEmpty

  override protected def throttleTime(clientMetric: KafkaMetric): Long = {
    math.min(super.throttleTime(clientMetric), maxThrottleTimeMs)
  }

  override protected def clientRateMetricName(quotaMetricTags: Map[String, String]): MetricName = {
    metrics.metricName("request-time", QuotaType.Request.toString,
      "Tracking request-time per user/client-id",
      quotaMetricTags.asJava)
  }

  private def exemptMetricName: MetricName = {
    metrics.metricName("exempt-request-time", QuotaType.Request.toString,
                   "Tracking exempt-request-time utilization percentage")
  }

  private def exemptSensorName: String = "exempt-" + QuotaType.Request

  private def nanosToPercentage(nanos: Long): Double = nanos * ClientQuotaManagerConfig.NanosToPercentagePerSecond

  /**
    * This method returns broker quota limit when tenant-level quotas are enabled. Broker quota
    * limit is used by backpressure algorithm to auto-tune tenant quotas such that aggregate
    * tenant quotas stay below the totak broker limit. When backpressure is disabled, but
    * tenant-level quotas are enabled, broker quota limit is still updated and emitted as a JMX
    * metric. Initially, broker quota limit is "unlimited" (max value)
    *
    * @return broker quota limit or max value if tenant-level quotas are disabled
    */
  override def getBrokerQuotaLimit: Double = {
    val metricOpt = Option(metrics.metric(BrokerBackpressureMetrics.nonExemptRequestCapacityMetricName(metrics)))
    metricOpt match {
      case Some(metric) => metric.metricValue().asInstanceOf[Double]
      case _ => Double.MaxValue   // metric has not been recorded (unlimited broker limit)
    }
  }

  private def recordNetworkUsage(value: Double,
                                 listenerName: String,
                                 throttleType: RequestThrottleType,
                                 timeMs: Long): Unit = {
    threadUsageSensors.recordNetworkThreadUsage(value, timeMs, listenerName)
    if (quotasEnabled) {
      throttleType match {
        case NonExemptRequest =>
          threadUsageSensors.recordNetworkThreadUsage(value, timeMs, listenerName, Some(NonExemptRequest))
        case _ => // we do not record exempt usage, since it is network usage minus non-exempt usage
      }
    }
  }

  /**
    * This method is called periodically to update broker-wide limit on percentage of time
    * requests can spend on network + request handler threads. Broker quota limit is recorded
    * into the 'type=backpressure-metrics,name=non-exempt-request-time-capacity' metric.
    *
    * Broker request quota limit is used to limit aggregate dynamic tenant quotas when broker
    * backpressure is enabled for limiting total usage of network and request handler threads by
    * requests ('confluent.backpressure.types' config contains 'request'). Broker request quota
    * limit is applied only to requests non exempt from throttling.
    *
    * Broker request quota limit is set based on both (1) threads usage and (2) request load:
    * 1. Threads usage of IO and network threads determines the upper limit on Broker Request Quota Limit, since we
    *    we cannot use any more threads than available. There are three goals when setting broker request quota limit
    *    based on usage : 1) Keep total utilization below 100% (the desired percent is set in ClientQuotaManagerConfig
    *    .DefaultMaxResourceUtilization); 2) Allocate some headroom to requests exempt from
    *    throttling (since we cannot control their usage); 3) Make sure that utilization of each
    *    type of threads also stays below 100% (controlled by same config ClientQuotaManagerConfig
    *    .DefaultMaxResourceUtilization).
    *
    *    In common case, broker request quota limit is set to ClientQuotaManagerConfig
    *    .DefaultMaxResourceUtilization of total available time on network and request handler
    *    threads, minus current usage of requests exempt from throttling. As soon as current usage of
    *    one type of threads exceeds non-exempt capacity, the total broker limit is set to the current
    *    usage (to ensure that over-utilized threads do not get used even more).
    * 2. If combined number of IO and network is larger than number of cores, there will be more thread capacity available
    *    than capacity that is available to use. If requests are not processed fast enough, requests queues build up.
    *    If 95th percentile of request queue size exceeds threshold, broker request quota limit is further decreased.
    *    Once request queues stop building up, broker request quota limit is lifted up again back to (1) estimate based
    *    thread capacity and current thread usage.
    */
  override protected[server] def updateBrokerQuotaLimit(): Unit = {

    // this method is used to calculate the upper limit for non-exempt request usage of a threadpool
    // max(maxAvailableCapacity - exempt usage, minLimit), where
    //    maxAvailableCapacity = DefaultMaxResourceUtilization of total capacity
    //    minLimit = DefaultMinNonExemptRequestUtilization of total capacity
    def nonExemptThreadUsageLimit(nonExemptUsage: Double, totalUsage: Double, totalCapacity: Double): Double = {
      val exemptUsage = totalUsage - nonExemptUsage
      val nonExemptCapacity = totalCapacity * BrokerBackpressureConfig.DefaultMaxResourceUtilization - exemptUsage
      val minNonExemptCapacity = totalCapacity * BrokerBackpressureConfig.DefaultMinNonExemptRequestUtilization
      math.max(nonExemptCapacity, minNonExemptCapacity)
    }

    val tenantEndpointsListenerNames = dynamicBackpressureConfig.tenantEndpointListenerNames
    if (quotasEnabled && tenantEndpointsListenerNames.nonEmpty) {
      val nonExemptIoThreadUsage = ThreadUsageMetrics.ioThreadsUsage(metrics, Some(NonExemptRequest))
      val ioThreadUsage = ThreadUsageMetrics.ioThreadsUsage(metrics)
      val nonExemptNetworkThreadUsage = ThreadUsageMetrics.networkThreadsUsage(
        metrics, tenantEndpointsListenerNames, Some(NonExemptRequest))
      val networkThreadUsage = ThreadUsageMetrics.networkThreadsUsage(metrics, tenantEndpointsListenerNames)

      // get per-thread-type limit on percentage of time non-exempt requests can spend on
      // network/IO threads (incorporates headroom for requests exempt from throttling)
      val nonExemptIoThreadLimit = nonExemptThreadUsageLimit(
        nonExemptIoThreadUsage, ioThreadUsage, ThreadUsageMetrics.ioThreadsCapacity(metrics))
      val nonExemptNetworkThreadLimit = nonExemptThreadUsageLimit(
        nonExemptNetworkThreadUsage, networkThreadUsage, ThreadUsageMetrics.networkThreadsCapacity(metrics, tenantEndpointsListenerNames))

      // as soon as actual usage of one type of threads exceeds capacity available for non-exempt
      // requests, total broker request quota limit is set to the current usage, to ensure that
      // over-utilized thread type does not get even more overloaded
      val brokerRequestQuotaLimit = if (((ioThreadUsage >= nonExemptIoThreadLimit) && (networkThreadUsage >= nonExemptNetworkThreadLimit)) ||
                                        (ioThreadUsage < nonExemptIoThreadLimit) && (networkThreadUsage < nonExemptNetworkThreadLimit))
        nonExemptIoThreadLimit + nonExemptNetworkThreadLimit
      else
        math.min(networkThreadUsage, nonExemptNetworkThreadLimit) + math.min(ioThreadUsage, nonExemptIoThreadLimit)

      // step 2: in setups where number of threads >> cores, broker may get overloaded on
      // requests while there are enough "threads" available but no CPU to process the requests
      // Additionally decrease Request Limit if queues are getting close to full
      val correctedLimit = updateAdjustedCapacity(brokerRequestQuotaLimit)
      nonExemptCapacitySensor.record(correctedLimit)
    }
  }

  /**
    * This method checks whether requests queues are overloaded, and further reduces Broker
    * Request Quota Limit if queues are very close to being filled up.
    * Backpressure is increased/decreased in linear constant steps. This approach may be a bit slow during sudden overload.
    */
  def updateAdjustedCapacity(brokerRequestLimit: Double): Double = {
    val queueSize = RequestQueueSizePercentiles.dataPlaneQueueSize(metrics, "p95")
    val minCap = BrokerBackpressureConfig.DefaultMinRequestQuotaLimit
    lastLimitCorrection = if (queueSize >= dynamicBackpressureConfig.queueSizeCap) {
      val maxAdjustmentLimit = math.max(brokerRequestLimit - minCap, 0.0)
      if (lastLimitCorrection < maxAdjustmentLimit)
        lastLimitCorrection + BrokerBackpressureConfig.DefaultRequestQuotaAdjustment
      else
        lastLimitCorrection
    } else
      math.max(0.0, lastLimitCorrection - BrokerBackpressureConfig.DefaultRequestQuotaAdjustment)

    debug(s"queueSize(p95)=$queueSize,  lastLimitCorrection=$lastLimitCorrection")
    math.max(brokerRequestLimit - lastLimitCorrection, minCap)
  }
}
