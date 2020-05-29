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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.{Seq, Set}
import kafka.server.Constants._
import kafka.server.ReplicationQuotaManagerConfig._
import kafka.utils.CoreUtils._
import kafka.utils.Logging
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigDef.ValidString.in
import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.metrics.stats.SimpleRate
import org.apache.kafka.common.utils.Time

/**
  * Configuration settings for quota management
  *
  * @param quotaBytesPerSecond The statically-configured bytes per second quota allocated to internal replication
  * @param numQuotaSamples            The number of samples to retain in memory
  * @param quotaWindowSizeSeconds     The time span of each sample
  * @param allReplicasThrottled       true - all replicas on this broker for the specific quota type are throttled
  *                                   false - replica throttling relies on the topic-level dynamic config
  */
case class ReplicationQuotaManagerConfig(quotaBytesPerSecond: Long = Defaults.QuotaBytesPerSecond,
                                         numQuotaSamples: Int = Defaults.DefaultNumQuotaSamples,
                                         quotaWindowSizeSeconds: Int = Defaults.DefaultQuotaWindowSizeSeconds,
                                         allReplicasThrottled: Boolean = false)

object ReplicationQuotaManagerConfig {
  /**
   * The possible values for the
   * `KafkaConfig.FollowerReplicationThrottledReplicasProp` and `KafkaConfig.LeaderReplicationThrottledReplicasProp` configs
   */
  val NoThrottledReplicasValue = "none"
  val AllThrottledReplicasValue = "*"
  // Purge sensors after 1 hour of inactivity
  val InactiveSensorExpirationTimeSeconds = 3600

  val LeaderReplicationThrottledRateProp = "leader.replication.throttled.rate"
  val FollowerReplicationThrottledRateProp = "follower.replication.throttled.rate"
  val LeaderReplicationThrottledReplicasProp = "leader.replication.throttled.replicas"
  val FollowerReplicationThrottledReplicasProp = "follower.replication.throttled.replicas"

  val ReconfigurableConfigs: Set[String] = Set(
    LeaderReplicationThrottledReplicasProp,
    LeaderReplicationThrottledRateProp,
    FollowerReplicationThrottledReplicasProp,
    FollowerReplicationThrottledRateProp
  )

  def allReplicasThrottled(throttledReplicas: String): Boolean =
    AllThrottledReplicasValue.equals(throttledReplicas)

  /**
   * The config validator for the broker-level throttled replicas conifg
   */
  def throttledReplicasValidator: Validator =
    in(ReplicationQuotaManagerConfig.NoThrottledReplicasValue, ReplicationQuotaManagerConfig.AllThrottledReplicasValue)
}

trait ReplicaQuota {
  def record(value: Long): Unit
  def isThrottled(topicPartition: TopicPartition): Boolean
  def isQuotaExceeded: Boolean
}

object Constants {
  val AllReplicas = Seq[Int](-1)
  val NoReplicas = Seq[Int](-2)
}

/**
 * Tracks replication metrics and comparing them to any quotas for throttled partitions.
 *
 * @param config    The quota configs
 * @param metrics   The Metrics instance
 * @param quotaType The name / key for this quota manager, typically leader or follower
 * @param time      Time object to use
 */
class ReplicationQuotaManager(val config: ReplicationQuotaManagerConfig,
                              private val metrics: Metrics,
                              protected[server] val quotaType: QuotaType,
                              private val time: Time) extends Logging with ReplicaQuota with DiskUsageBasedThrottleListener {
  private val lock = new ReentrantReadWriteLock()
  private val throttledPartitions = new ConcurrentHashMap[String, Seq[Int]]()
  private var quota: Quota = _
  private val allReplicasThrottled = new AtomicBoolean(config.allReplicasThrottled)
  private val sensorAccess = new SensorAccess(lock, metrics)
  private val rateMetricName = metrics.metricName("byte-rate", quotaType.toString,
    s"Tracking byte-rate for ${quotaType}")

  updateQuota(Quota.upperBound(config.quotaBytesPerSecond.toDouble))

  /**
    * Update the quota
    *
    * @param quota
    */
  def updateQuota(quota: Quota): Unit = {
    debug(s"updateQuota requested for $quotaType from ${this.quota} to $quota")
    if (quotaType == QuotaType.FollowerReplication && lastSignalledQuotaOptRef.get.isDefined) {
      // this means that disk based throttling is currently active, hence, we will just log and avoid updating the quota
      info("Can't update replication throttle since disk throttling is currently active!")
      return
    }
    doUpdateQuota(quota)
  }

  private def doUpdateQuota(quota: Quota): Unit = {
    inWriteLock(lock) {
      this.quota = quota
      //The metric could be expired by another thread, so use a local variable and null check.
      val metric = metrics.metrics.get(rateMetricName)
      if (metric != null) {
        metric.config(getQuotaMetricConfig(quota))
      }
    }
  }

  /**
    * Check if the quota is currently exceeded
    *
    * @return
    */
  override def isQuotaExceeded: Boolean = {
    try {
      sensor().checkQuotas()
    } catch {
      case qve: QuotaViolationException =>
        trace(s"$quotaType: Quota violated for sensor (${sensor().name}), metric: (${qve.metric.metricName}), " +
          s"metric-value: (${qve.value}), bound: (${qve.bound})")
        return true
    }
    false
  }

  /**
    * Is the passed partition throttled by this ReplicationQuotaManager
    *
    * @param topicPartition the partition to check
    * @return
    */
  override def isThrottled(topicPartition: TopicPartition): Boolean = {
    // check topic-specific replica throttles first
    val partitions = throttledPartitions.get(topicPartition.topic)
    if (partitions == null || partitions.isEmpty)
      allReplicasThrottled.get()
    else if ((partitions eq AllReplicas) || partitions.contains(topicPartition.partition))
      true
    else // some replicas are throttled but this one is not or this topic's is exempt from throttling
      false
  }

  /**
    * Add the passed value to the throttled rate. This method ignores the quota with
    * the value being added to the rate even if the quota is exceeded
    *
    * @param value
    */
  def record(value: Long): Unit = {
    sensor().record(value.toDouble, time.milliseconds(), false)
  }

  /**
    * Update the set of throttled partitions for this QuotaManager. The partitions passed, for
    * any single topic, will replace any previous
    *
    * @param topic
    * @param partitions the set of throttled partitions
    * @return
    */
  def markThrottled(topic: String, partitions: Seq[Int]): Unit = {
    throttledPartitions.put(topic, partitions)
  }

  /**
   * Mark all replication replicas on this broker as throttled
   */
  def markBrokerThrottled(): Unit = {
    allReplicasThrottled.set(true)
  }

  /**
    * Remove list of throttled replicas for a certain topic
    *
    * @param topic
    * @return
    */
  def removeThrottle(topic: String): Unit = {
    throttledPartitions.remove(topic)
  }

  /**
   * Disables broker-level replication throttling
   * @param resetThrottle - whether to reset the throttle to the statically-set value
   */
  def removeBrokerThrottle(resetThrottle: Boolean): Unit = {
    val throttleEnabled = if (resetThrottle)
      config.allReplicasThrottled
    else
      false
    allReplicasThrottled.set(throttleEnabled)
  }

  /**
    * Returns the bound of the configured quota
    *
    * @return
    */
  def upperBound(): Long = {
    inReadLock(lock) {
      quota.bound().toLong
    }
  }

  private def getQuotaMetricConfig(quota: Quota): MetricConfig = {
    new MetricConfig()
      .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS)
      .samples(config.numQuotaSamples)
      .quota(quota)
  }

  private def sensor(): Sensor = {
    sensorAccess.getOrCreate(
      quotaType.toString,
      InactiveSensorExpirationTimeSeconds,
      rateMetricName,
      Some(getQuotaMetricConfig(quota)),
      new SimpleRate
    )
  }

  override def handleDiskSpaceLow(cappedQuotaInBytesPerSec: Long): Unit = {
    if (quotaType == QuotaType.FollowerReplication) {
      logger.info("Updating Follower quota (due to low disk) to: {}", cappedQuotaInBytesPerSec)
      doUpdateQuota(Quota.upperBound(cappedQuotaInBytesPerSec.toDouble))
      markBrokerThrottled()
    }
  }

  override def handleDiskSpaceRecovered(): Unit = {
    if (quotaType == QuotaType.FollowerReplication) {
      val resetQuota = config.quotaBytesPerSecond
      logger.info("Resetting Follower quota (due to low disk) to: {}", resetQuota)
      doUpdateQuota(Quota.upperBound(resetQuota.toDouble))
      removeBrokerThrottle(true)
    }
  }
}
