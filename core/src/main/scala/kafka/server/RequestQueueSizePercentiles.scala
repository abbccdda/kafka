/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server

import kafka.network.SocketServer
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{JmxReporter, Metrics}
import org.apache.kafka.common.metrics.stats.{Percentile, Percentiles}
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing

import scala.collection.JavaConverters._

object RequestQueueSizePercentiles {
  val MetricGroup = "request-queue-metrics"
  val PercentileNamePrefix = "request-queue-size-"
  val Buckets = 1000
  val Tags: Map[String, String] = Map(JmxReporter.JMX_IGNORE_TAG -> "")

  def createPercentiles(metrics: Metrics, queueSize: Int, metricNamePrefix : String): Percentiles = {
    val prefix = metricNamePrefix.concat(PercentileNamePrefix)
    new Percentiles(Buckets * 4, 0.0, queueSize, BucketSizing.CONSTANT,
                    new Percentile(queueSizeMetricName(metrics, prefix + "p80"), 80.0),
                    new Percentile(queueSizeMetricName(metrics, prefix + "p90"), 90.0),
                    new Percentile(queueSizeMetricName(metrics, prefix + "p95"), 95.0),
                    new Percentile(queueSizeMetricName(metrics, prefix + "p99"), 99.0)
    )
  }

  def dataPlaneQueueSize(metrics: Metrics, percentileSuffix: String): Double = {
    val name = SocketServer.DataPlaneMetricPrefix.concat(PercentileNamePrefix).concat(percentileSuffix)
    val metricOpt = Option(metrics.metric(queueSizeMetricName(metrics, name)))
    metricOpt match {
      case Some(metric) => metric.metricValue().asInstanceOf[Double]
      case _ => 0.0 // no metric means metric has not been recorded (no requests?)
    }
  }

  def queueSizeMetricName(metrics: Metrics, name: String): MetricName =
    metrics.metricName(name, MetricGroup, Tags.asJava)
}
