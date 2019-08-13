/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer

import kafka.utils.Logging
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.Value
import org.apache.kafka.common.utils.Time

import scala.collection.mutable

case class TenantInfo(timestamp: Long, metricTags: Map[String, String])

/**
  * Records active tenants, where a tenant is a quota-abiding entity, over a sliding window of size
  * $timeWindowMs. This assumes single-level hierarchy of quota entities (similar to CCloud), so this is only
  * enabled in CCloud.
  *
  * A tenant becomes active when a broker receives a request (like fetch, produce, or metadata request) from
  * that tenant. A tenant becomes inactive when the broker does not receive anything from that tenant within
  * a $timeWindowMs timeframe.
  *
  * @param metrics @Metrics Metrics instance
  * @param time @Time object to use
  * @param timeWindowMs @Long length of active window in milliseconds
  */

class ActiveTenantsManager(private val metrics: Metrics,
                           private val time: Time,
                           private val timeWindowMs: Long) {

  private val activeWindow = new ConcurrentHashMap[String, TenantInfo]()
  private var lastRecordTimeMs = time.milliseconds()

  private val activeTenantsSensor = metrics.sensor("ActiveTenants")
  activeTenantsSensor.add(metrics.metricName("active-tenants-count",
    "multi-tenant-metrics",
    s"The number of active tenants over a $timeWindowMs window"), new Value())

  def trackActiveTenant(metricTags: Map[String, String], timeMs: Long): Unit = {
    activeWindow.put(metricTags.toString(), TenantInfo(timeMs, metricTags))
    if (lastRecordTimeMs + timeWindowMs < timeMs) {
      lastRecordTimeMs = timeMs
      recordNumActiveTenants(timeMs)
    }
  }

  def getActiveTenants(): mutable.Set[Map[String, String]] = {
    pruneInactiveTenants(time.milliseconds())
    val s = mutable.Set[Map[String, String]]()
    activeWindow.forEach(toBiConsumer((_: String, tenantInfo: TenantInfo) => {
      s += tenantInfo.metricTags
    }))
    s
  }

  private def recordNumActiveTenants(timeMs: Long): Unit = {
    pruneInactiveTenants(timeMs)
    activeTenantsSensor.record(activeWindow.size, timeMs)
  }

  private def pruneInactiveTenants(timeMs: Long): Unit = {
    activeWindow.forEach(toBiConsumer((metricTag: String, tenantInfo: TenantInfo) => {
      // If trackActiveTenant() updates a tenant's timestamp in between a pruning iteration, remove() will not
      // remove the tenant with the new timestamp. It will only remove that which matches both key and value.
      if (tenantInfo.timestamp + timeWindowMs < timeMs) {
        activeWindow.remove(metricTag, tenantInfo)
      }
    }))
  }

  implicit def toBiConsumer[T, U](op: (T, U) => Unit): BiConsumer[T, U] = {
    new BiConsumer[T, U] {
      override def accept(t: T, u: U): Unit = op.apply(t, u)
    }
  }
}
