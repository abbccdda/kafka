/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.Rate

import scala.collection.JavaConverters._


sealed trait ThreadType { def name: String }
case object NetworkThread extends ThreadType { val name = "network" }
case object IoThread extends ThreadType { val name = "io" }

sealed trait RequestThrottleType { def name: String }
// requests exempt from throttling
case object ExemptRequest extends RequestThrottleType { val name = "exempt" }
// requests non exempt from throttling when quotas enabled
case object NonExemptRequest extends RequestThrottleType { val name = "non-exempt" }


object ThreadUsageMetrics {
  val MetricGroup = "threads-usage-metrics"
  val ListenerMetricTag = "listener"

  /**
    * Returns measured percentage of time requests (either exempt only, non-exempt
    * only, or all requests as specified by 'throttleTypeOpt') spent on 'threadType' threads.
    * @param threadType      either network or request handler thread type
    * @param metricTags      metrics tags, use 'networkThreadUsageMetricTags()' or
    *                        'ioThreadUsageMetricTags()' to get metrics tags for a specific thread type
    * @param throttleTypeOpt None means total thread usage of all requests (exempt and
    *                        non-exempt), otherwise exempt of non-exempt request type
    */
  private def threadUsage(metrics: Metrics, threadType: ThreadType,
                  metricTags: Map[String, String],
                  throttleTypeOpt: Option[RequestThrottleType] = None): Double = {
    val metricOpt = Option(metrics.metric(threadUsageMetricName(metrics, threadType, metricTags, throttleTypeOpt)))
    metricOpt match {
      case Some(metric) => metric.metricValue().asInstanceOf[Double]
      case _ => 0.0   // no metric means metric has not been recorded (no usage)
    }
  }

  def ioThreadsUsage(metrics: Metrics, throttleTypeOpt: Option[RequestThrottleType] = None): Double =
    threadUsage(metrics, IoThread, ioThreadUsageMetricTags, throttleTypeOpt)

  /**
    * Returns combined usage of all network threads responsible for serving endpoints that
    * receive requests from tenants. This method models set of network thread pools (threadpool
    * per endpoint) as one combined threadpool (usage and capacity is additive).
    *
    * @param listeners listener names of endpoints that accept requests from tenants
    */
  def networkThreadsUsage(metrics: Metrics, listeners: Seq[String],
                          throttleTypeOpt: Option[RequestThrottleType] = None): Double =
    listeners.foldLeft(0.0) { case (usage, listener) =>
      usage + threadUsage(metrics, NetworkThread, listenerNetworkThreadUsageMetricTags(listener), throttleTypeOpt)}

  /**
    * Returns metrics name for the percentage of time requests (either exempt only, non-exempt
    * only, or all requests as specified by 'throttleTypeOpt') spent on 'threadType' threads.
    * @param threadType      either network or request handler thread type
    * @param metricTags      metrics tags, use 'networkThreadUsageMetricTags()' or
    *                        'ioThreadUsageMetricTags()' to get metrics tags for a specific thread type
    * @param throttleTypeOpt None means total thread usage of all requests (exempt and
    *                        non-exempt), otherwise exempt or non-exempt request type
    */
  def threadUsageMetricName(metrics: Metrics,
                            threadType: ThreadType,
                            metricTags: Map[String, String],
                            throttleTypeOpt: Option[RequestThrottleType]): MetricName = {
    val throttleTypeMetricName = throttleTypeOpt.map(throttleType => s"-${throttleType.name}").getOrElse("")
    metrics.metricName(
      s"request$throttleTypeMetricName-${threadType.name}-time", MetricGroup,
      s"Tracking ${throttleTypeOpt.getOrElse("total")} request utilization percentage of ${threadType.name} threads",
      metricTags.asJava)
  }

  def networkThreadUsageMetricTags(listeners: Seq[String]) : Seq[Map[String, String]] =
    listeners.map(listener => Map(ThreadUsageMetrics.ListenerMetricTag -> listener))

  def listenerNetworkThreadUsageMetricTags(listener: String) : Map[String, String] =
    Map(ThreadUsageMetrics.ListenerMetricTag -> listener)

  def ioThreadUsageMetricTags : Map[String, String] = Map.empty

  /**
    * Returns combined capacity of all network threads responsible for serving endpoints that
    * receive requests from tenants. This method models set of network thread pools (threadpool
    * per endpoint) as one combined threadpool (usage and capacity is additive).
    *
    * @param listeners listener names of endpoints that accept requests from tenants
    */
  def networkThreadsCapacity(metrics: Metrics, listeners: Seq[String]): Double =
    listeners.foldLeft(0.0) { case (capacity, listener) =>
      capacity + threadPoolCapacity(metrics, NetworkThread, listenerNetworkThreadUsageMetricTags(listener))}

  def ioThreadsCapacity(metrics: Metrics): Double =
    threadPoolCapacity(metrics, IoThread, ioThreadUsageMetricTags)

  def ioThreadPoolCapacityMetricName(metrics: Metrics): MetricName =
    threadPoolCapacityMetricName(metrics, IoThread, ioThreadUsageMetricTags,
                                 "(current number of io threads)*100%")

  def networkThreadPoolCapacityMetricName(metrics: Metrics, listener: String): MetricName =
    threadPoolCapacityMetricName(metrics, NetworkThread, listenerNetworkThreadUsageMetricTags(listener),
                                 s"(current number of network threads for $listener)*100%")

  def threadPoolCapacityMetricName(metrics: Metrics, threadType: ThreadType, metricTags: Map[String, String], metricDescription: String): MetricName =
    metrics.metricName(s"total-${threadType.name}-time", MetricGroup, metricDescription, metricTags.asJava)

  private def threadPoolCapacity(metrics: Metrics, threadType: ThreadType, metricTags: Map[String, String]): Double = {
    val metricName = threadPoolCapacityMetricName(metrics, threadType, metricTags, "")
    Option(metrics.metric(metricName)) match {
      case Some(metric) => metric.metricValue().asInstanceOf[Double]
      case _ => 0.0   // this should not happen, but logging here would be too noisy
    }
  }
}


class ThreadUsageSensors(private val metrics: Metrics,
                         private val inactiveSensorExpirationTimeSeconds: Long) {
  private val lock = new ReentrantReadWriteLock()
  private val sensorAccessor = new SensorAccess(lock, metrics)

  /**
    * Records thread usage metric, using the same measure unit as client request quota. Thread
    * usage metrics is broken down by thread type and type of usage (ExemptRequest or
    * NonExemptRequest). Network thread usage metrics are additionally tagged with listener name,
    * since there is a threadpool of network threads per broker endpoint.
    *
    * This method always records metrics, independent of any broker config. The caller is
    * responsible to record metrics under the right conditions (for example, nonExempt usage is
    * recorded only when quotas are enabled).
    *
    * @param value            value to record
    * @param timeMs           current time in milliseconds
    * @param threadType       type of thread (NetworkThread or IoThread)
    * @param metricTags       metric tags, use ThreadUsageMetrics#networkThreadUsageMetricTags or
    *                         ThreadUsageMetrics#ioThreadUsageMetricTags to get metric tags for a
    *                         particular thread type
    * @param throttleTypeOpt  ExemptRequest, NonExemptRequest or None for all request types
    */
  def recordThreadUsage(value: Double,
                        timeMs: Long,
                        threadType: ThreadType,
                        metricTags: Map[String, String],
                        throttleTypeOpt: Option[RequestThrottleType] = None) {
    val sensorName = threadUsageSensorName(threadType, metricTags, throttleTypeOpt)
    val metricName = ThreadUsageMetrics.threadUsageMetricName(metrics, threadType, metricTags, throttleTypeOpt)
    val sensor = getOrCreateSensor(sensorName, metricName)
    sensor.record(value, timeMs)
  }

  def recordIoThreadUsage(value: Double, timeMs: Long,
                          throttleTypeOpt: Option[RequestThrottleType] = None): Unit = {
    recordThreadUsage(value, timeMs, IoThread, ThreadUsageMetrics.ioThreadUsageMetricTags, throttleTypeOpt)
  }

  def recordNetworkThreadUsage(value: Double, timeMs: Long, listenerName: String,
                               throttleTypeOpt: Option[RequestThrottleType] = None): Unit = {
    recordThreadUsage(value, timeMs, NetworkThread,
                      ThreadUsageMetrics.listenerNetworkThreadUsageMetricTags(listenerName), throttleTypeOpt)
  }

  private def threadUsageSensorName(threadType: ThreadType,
                                    metricTags: Map[String, String],
                                    throttleTypeOpt: Option[RequestThrottleType]): String = {
    val suffix = if (metricTags.isEmpty)
      ""
    else
      s"-${metricTags.values.mkString(":")}"

    val prefix = throttleTypeOpt.map(throttleType => s"${throttleType.name}-").getOrElse("")
    s"$prefix-${threadType.name}-thread-usage-$suffix"
  }

  private def getOrCreateSensor(sensorName: String, metricName: MetricName): Sensor = {
    sensorAccessor.getOrCreate(
      sensorName,
      inactiveSensorExpirationTimeSeconds,
      metricName,
      None,
      new Rate
    )
  }

}
