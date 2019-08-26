/*
 Copyright 2019 Confluent Inc.
 */

package kafka.server

import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer

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
  * @param activeTimeWindowMs @Long length of active window in milliseconds
  */

class ActiveTenantsManager(private val metrics: Metrics,
                           private val time: Time,
                           val activeTimeWindowMs: Long) {

  private val activeWindow = new ConcurrentHashMap[String, TenantInfo]()
  private var lastRecordTimeMs = time.milliseconds()

  private val activeTenantsSensor = metrics.sensor("ActiveTenants")
  activeTenantsSensor.add(metrics.metricName("active-tenants-count",
    "multi-tenant-metrics",
    s"The number of active tenants over a $activeTimeWindowMs window"), new Value())

  def trackActiveTenant(metricTags: Map[String, String], timeMs: Long): Unit = {
    trackActiveTenant(metricTags, timeMs, _ => Unit)
  }

  def trackActiveTenant(metricTags: Map[String, String], timeMs: Long, resetQuotaCallback: Map[String, String] => Unit): Unit = {
    activeWindow.put(metricTags.toString(), TenantInfo(timeMs, metricTags))
    maybeRecordNumActiveTenants(timeMs, resetQuotaCallback)
  }

  private def maybeRecordNumActiveTenants(timeMs: Long, resetQuotaCallback: Map[String, String] => Unit): Unit = {
    if (lastRecordTimeMs + activeTimeWindowMs < timeMs) {
      recordNumActiveTenants(timeMs, resetQuotaCallback)
      lastRecordTimeMs = timeMs
    }
  }

  private def recordNumActiveTenants(timeMs: Long, resetQuotaCallback: Map[String, String] => Unit): Unit = {
    pruneInactiveTenants(timeMs, resetQuotaCallback)
    activeTenantsSensor.record(activeWindow.size, timeMs)
  }

  def getActiveTenants(): mutable.Set[Map[String, String]] = getActiveTenants(_ => Unit)

  def getActiveTenants(resetQuotaCallback: Map[String, String] => Unit): mutable.Set[Map[String, String]] = {
    pruneInactiveTenants(time.milliseconds(), resetQuotaCallback)
    val s = mutable.Set[Map[String, String]]()
    activeWindow.forEach(toBiConsumer((_: String, tenantInfo: TenantInfo) => {
      s += tenantInfo.metricTags
    }))
    s
  }

  private def pruneInactiveTenants(timeMs: Long, resetQuotaCallback: Map[String, String] => Unit): Unit = {
    activeWindow.forEach(toBiConsumer((metricTags: String, tenantInfo: TenantInfo) => {
      // If trackActiveTenant() updates a tenant's timestamp in between a pruning iteration, remove() will not
      // remove the tenant with the new timestamp. It will only remove that which matches both key and value.
      if (tenantInfo.timestamp + activeTimeWindowMs < timeMs) {
        resetQuotaCallback(tenantInfo.metricTags)
        activeWindow.remove(metricTags, tenantInfo)
      }
    }))
  }

  private implicit def toBiConsumer[T, U](op: (T, U) => Unit): BiConsumer[T, U] = {
    new BiConsumer[T, U] {
      override def accept(t: T, u: U): Unit = op.apply(t, u)
    }
  }
}
