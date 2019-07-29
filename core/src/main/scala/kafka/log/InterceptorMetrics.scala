package kafka.log

import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool

/**
  * This class contains metrics for interceptor failures
  */
class InterceptorMetrics(topic: String, interceptorClassName: String) extends KafkaMetricsGroup {

  val tags: scala.collection.Map[String, String] = Map("topic" -> topic, "interceptorClassName" -> interceptorClassName)

  private val metricTypeMap = new Pool[String, Meter]

  def totalRejectedRecordsPerSec = metricTypeMap.getAndMaybePut(InterceptorStats.TotalRejectedRecordsPerSec,
    newMeter(InterceptorStats.TotalRejectedRecordsPerSec, "requests", TimeUnit.SECONDS, tags))

  // this method helps with metricTypeMap first before deleting a topic
  def removeMetricHelper(metricType: String, tags: scala.collection.Map[String, String]): Unit = {
    val metric: Meter = metricTypeMap.remove(metricType)
    if (metric != null)
      removeMetric(metricType, tags)
  }

  def close(): Unit = {
    removeMetricHelper(InterceptorStats.TotalRejectedRecordsPerSec, tags)
  }

}

object InterceptorStats {
  val TotalRejectedRecordsPerSec = "TotalRejectedRecordsPerSec"
}

class InterceptorStats {

  private val stats = new Pool[(String, String), InterceptorMetrics]

  private def retrieveMetrics(topic: String, interceptorClassName: String): InterceptorMetrics =
    stats.getAndMaybePut((topic, interceptorClassName), new InterceptorMetrics(topic, interceptorClassName))

  def logRejectedRecords(topic: String, interceptorClassName: String): Unit =
    retrieveMetrics(topic, interceptorClassName).totalRejectedRecordsPerSec.mark()

  def removeMetrics(topic: String, interceptorClassName: String): Unit = {
    val metrics = stats.remove((topic, interceptorClassName))
    if (metrics != null)
      metrics.close()
  }

  def close(): Unit = {
    stats.values.foreach(_.close())
  }

}