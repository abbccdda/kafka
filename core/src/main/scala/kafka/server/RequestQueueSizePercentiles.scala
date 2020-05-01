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

  private val percentileSuffixToValue = Map("p90" -> 90.0, "p95" -> 95.0, "p98" -> 98.0, "p99" -> 99.0)

  def createPercentiles(metrics: Metrics, queueSize: Int, metricNamePrefix : String): Percentiles = {
    val prefix = metricNamePrefix.concat(PercentileNamePrefix)
    val percentiles = percentileSuffixToValue.map{ case (suffix, value) =>
      new Percentile(queueSizeMetricName(metrics, prefix + suffix), value) }.toList
    // the number of buckets is calculated in the Percentiles constructor as the first argument divided by 4
    new Percentiles(Buckets * 4, 0.0, queueSize, BucketSizing.CONSTANT, percentiles: _*)
  }

  def dataPlaneQueueSize(metrics: Metrics, percentileSuffix: String): Double = {
    val name = SocketServer.DataPlaneMetricPrefix.concat(PercentileNamePrefix).concat(percentileSuffix)
    val metricOpt = Option(metrics.metric(queueSizeMetricName(metrics, name)))
    metricOpt match {
      case Some(metric) => metric.metricValue().asInstanceOf[Double]
      case _ => 0.0 // no metric means metric has not been recorded (no requests?)
    }
  }

  def valid(suffix: String): Boolean = percentileSuffixToValue.keySet.contains(suffix)

  def queueSizeMetricName(metrics: Metrics, name: String): MetricName =
    metrics.metricName(name, MetricGroup, Tags.asJava)
}
