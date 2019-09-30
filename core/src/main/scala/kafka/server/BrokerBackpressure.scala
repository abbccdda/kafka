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
  *
  */
case class BrokerBackpressureConfig(backpressureEnabledInConfig: Boolean = false,
                                    backpressureCheckFrequencyMs: Long = BrokerBackpressureConfig.DefaultBackpressureCheckFrequencyMs,
                                    tenantEndpointListenerNames: Seq[String] = Seq())


object BrokerBackpressureConfig {
  // Time window in which a tenant is considered active
  val DefaultActiveWindowMs = 1 * TimeUnit.MINUTES.toMillis(1)
  // How often to update broker quota limit (same frequency for bandwidth and request quotas);
  // If broker backpressure is enabled, also used as frequency to auto-tune tenants' quotas
  val DefaultBackpressureCheckFrequencyMs = 1 * TimeUnit.MINUTES.toMillis(1)

  // default maximum utilization of resource to which quotas are applied
  // for request time on threads, both network and IO threads are kept at or below max utilization
  val DefaultMaxResourceUtilization = 0.9
  // default minimum utilization of network threads and IO threads (separately, or in aggregate)
  // represented as percent of total; non-exempt requests will be always given at least
  // DefaultMinNonExemptRequestUtilization * total capacity
  val DefaultMinNonExemptRequestUtilization = 0.3
}

object BrokerBackpressureMetrics {
  val MetricGroup = "backpressure-metrics"

  def nonExemptRequestCapacityMetricName(metrics: Metrics): MetricName = {
    metrics.metricName("non-exempt-request-time-capacity", MetricGroup,
                       "Tracking maximum utilization percentage of IO and network threads available to non-exempt requests")
  }
}


