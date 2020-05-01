/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server


import java.util.concurrent.TimeUnit

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.apache.kafka.common.metrics._

import scala.collection.Seq


/**
  * Configuration settings for broker backpressure management
  * @param backpressureEnabledInConfig    true if broker backpressure is enabled, otherwise false;
  *                                       note that Request backpressure also requires
  *                                       tenantEndpointListenerName to be defined.
  * @param backpressureCheckFrequencyMs   frequency of updating broker quota limit; also, if
  *                                       backpressureEnabled is true, frequency of auto-tuning quotas
  * @param tenantEndpointListenerNames    names of the listeners for the endpoints receiving
  *                                       requests from tenants
  * @param maxQueueSize                   the maximum number of queued requests allowed for data-plane, before blocking
  *                                       the network threads, which corresponds to "queued.max.requests" broker config
  * @param minBrokerRequestQuota          The minimum broker request quota; request backpressure would not reduce
  *                                       the broker request quota any further even if the overload is still detected
  * @param queueSizePercentile            request queue size percentile used by request backpressure as string of format "pN",
  *                                       where N is the percentile value. Example is "p95".
  */
case class BrokerBackpressureConfig(backpressureEnabledInConfig: Boolean = false,
                                    backpressureCheckFrequencyMs: Long = BrokerBackpressureConfig.DefaultBackpressureCheckFrequencyMs,
                                    tenantEndpointListenerNames: Seq[String] = Seq(),
                                    maxQueueSize: Double = Double.MaxValue,
                                    minBrokerRequestQuota: Double = ConfluentConfigs.BACKPRESSURE_REQUEST_MIN_BROKER_LIMIT_DEFAULT.toDouble,
                                    queueSizePercentile: String = ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_DEFAULT) {

  /**
   * Maximum queue size threshold for the data-plane request queue, used by request backpressure to decide whether to
   * throttle requests more or less
   */
  def queueSizeCap: Double =
    if (maxQueueSize < Double.MaxValue) maxQueueSize * BrokerBackpressureConfig.DefaultMaxResourceUtilization
    else Double.MaxValue

  override def toString: String  = {
    s"BrokerBackpressureConfig(backpressureEnabledInConfig=$backpressureEnabledInConfig" +
      s", backpressureCheckFrequencyMs=$backpressureCheckFrequencyMs" +
      s", tenantEndpointListenerNames=$tenantEndpointListenerNames" +
      s", minBrokerRequestQuota=$minBrokerRequestQuota" +
      s", queueSizePercentile=$queueSizePercentile)"
  }
}


object BrokerBackpressureConfig {
  // Time window in which a tenant is considered active
  val DefaultActiveWindowMs = TimeUnit.MINUTES.toMillis(1)
  // How often to update broker quota limit (same frequency for bandwidth and request quotas);
  // If broker backpressure is enabled, also used as frequency to auto-tune tenants' quotas
  val DefaultBackpressureCheckFrequencyMs = TimeUnit.SECONDS.toMillis(30)

  // default maximum utilization of resource to which quotas are applied
  // for request time on threads, both network and IO threads are kept at or below max utilization
  val DefaultMaxResourceUtilization = 0.8
  // default minimum utilization of network threads and IO threads (separately, or in aggregate)
  // represented as percent of total; non-exempt requests will be always given at least
  // DefaultMinNonExemptRequestUtilization * total capacity
  val DefaultMinNonExemptRequestUtilization = 0.3

  // the absolute minimum for the broker request quota that limits combined request quota for all active tenants.
  // used only when the value in ConfluentConfigs.BACKPRESSURE_REQUEST_QUEUE_SIZE_PERCENTILE_CONFIG is smaller than this
  val MinBrokerRequestQuota = 10.0

  // step to increase / decrease request limit when increasing/decreasing request backpressure
  val DefaultRequestQuotaAdjustment = 25.0
}

object BrokerBackpressureMetrics {
  val MetricGroup = "backpressure-metrics"

  def nonExemptRequestCapacityMetricName(metrics: Metrics): MetricName = {
    metrics.metricName("non-exempt-request-time-capacity", MetricGroup,
                       "Tracking maximum utilization percentage of IO and network threads available to non-exempt requests")
  }
}


