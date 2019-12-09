/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server


import java.util.concurrent.TimeUnit

import org.apache.kafka.common.MetricName
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
  */
case class BrokerBackpressureConfig(backpressureEnabledInConfig: Boolean = false,
                                    backpressureCheckFrequencyMs: Long = BrokerBackpressureConfig.DefaultBackpressureCheckFrequencyMs,
                                    tenantEndpointListenerNames: Seq[String] = Seq(),
                                    maxQueueSize: Double = Double.MaxValue) {

  /**
   * Maximum queue size threshold for the data-plane request queue, used by request backpressure to decide whether to
   * throttle requests more or less
   */
  def queueSizeCap: Double =
    if (maxQueueSize < Double.MaxValue) maxQueueSize * BrokerBackpressureConfig.DefaultMaxResourceUtilization
    else Double.MaxValue
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

  // minimum combined request quota for all tenants. Even if request queues are over-loaded,
  // the broker will not backpressure requests any further than 200 total request quota
  // if time on threads was exactly CPU, then 200 corresponds to two cores. Our default cloud
  // deployments have 4 virtual cores (aws, gcp, azure); time on threads are usually not exactly
  // CPU time (especially when the time is taken to read from disk). During various experiments
  // on AWS, total measured IO + network thread usage often got up to 700.
  val DefaultMinRequestQuotaLimit = 200.0

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


