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

import kafka.server.QuotaType._
import kafka.utils.CoreUtils.parseCsvList
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.quota.ClientQuotaCallback
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.config.internals.ConfluentConfigs

object QuotaType  {
  case object Fetch extends QuotaType
  case object Produce extends QuotaType
  case object Request extends QuotaType
  case object LeaderReplication extends QuotaType
  case object FollowerReplication extends QuotaType
  case object AlterLogDirsReplication extends QuotaType
}
sealed trait QuotaType

object QuotaFactory extends Logging {

  object UnboundedQuota extends ReplicaQuota {
    override def isThrottled(topicPartition: TopicPartition): Boolean = false
    override def isQuotaExceeded: Boolean = false
    def record(value: Long): Unit = ()
  }

  case class QuotaManagers(fetch: ClientQuotaManager,
                           produce: ClientQuotaManager,
                           request: ClientRequestQuotaManager,
                           leader: ReplicationQuotaManager,
                           follower: ReplicationQuotaManager,
                           alterLogDirs: ReplicationQuotaManager,
                           clientQuotaCallback: Option[ClientQuotaCallback]) {
    def shutdown(): Unit = {
      fetch.shutdown
      produce.shutdown
      request.shutdown
      clientQuotaCallback.foreach(_.close())
    }
  }

  def instantiate(cfg: KafkaConfig, metrics: Metrics, time: Time, threadNamePrefix: String): QuotaManagers = {

    val clientQuotaCallback = Option(cfg.getConfiguredInstance(KafkaConfig.ClientQuotaCallbackClassProp,
      classOf[ClientQuotaCallback]))
    val activeTenantsManager = if (isMultiTenant(cfg))
        Option(new ActiveTenantsManager(metrics, time, BrokerBackpressureConfig.DefaultActiveWindowMs))
      else
        None
    QuotaManagers(
      new ClientQuotaManager(clientFetchConfig(cfg), metrics, Fetch, time, threadNamePrefix, clientQuotaCallback, activeTenantsManager),
      new ClientQuotaManager(clientProduceConfig(cfg), metrics, Produce, time, threadNamePrefix, clientQuotaCallback, activeTenantsManager),
      new ClientRequestQuotaManager(clientRequestConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallback, activeTenantsManager),
      new ReplicationQuotaManager(replicationConfig(cfg, LeaderReplication), metrics, LeaderReplication, time),
      new ReplicationQuotaManager(replicationConfig(cfg, FollowerReplication), metrics, FollowerReplication, time),
      new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, AlterLogDirsReplication, time),
      clientQuotaCallback
    )
  }

  def clientProduceConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    if (cfg.producerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds,
      backpressureConfig = brokerBackpressureConfig(cfg, Produce)
    )
  }

  def clientFetchConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    if (cfg.consumerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds,
      backpressureConfig = brokerBackpressureConfig(cfg, Fetch)
    )
  }

  def clientRequestConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    ClientQuotaManagerConfig(
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds,
      backpressureConfig = brokerBackpressureConfig(cfg, Request)
    )
  }

  def replicationConfig(cfg: KafkaConfig, quotaType: QuotaType): ReplicationQuotaManagerConfig = {
    val throttleRate = quotaType match {
      case QuotaType.LeaderReplication => cfg.ReplicationLeaderThrottleRate
      case QuotaType.FollowerReplication => cfg.ReplicationFollowerThrottleRate
      case _ => ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault
    }
    val replicasAreThrottled = quotaType match {
      case QuotaType.LeaderReplication => cfg.ReplicationLeaderReplicasAreThrottled
      case QuotaType.FollowerReplication => cfg.ReplicationFollowerReplicasAreThrottled
      case _ => false
    }
    ReplicationQuotaManagerConfig(
      quotaBytesPerSecond = throttleRate,
      numQuotaSamples = cfg.numReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds,
      allReplicasThrottled = replicasAreThrottled
    )
  }

  def alterLogDirsReplicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numAlterLogDirsReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.alterLogDirsReplicationQuotaWindowSizeSeconds
    )
  }

  /**
   * Returns true if broker is configured for tenant-level quotas, in which case broker tracks
   * active tenants and can be configured to apply back-pressure mechanisms.
   */
  def isMultiTenant(cfg: KafkaConfig): Boolean = {
    val quotaCallbackStr: String = Option(cfg.originalsStrings.get(KafkaConfig.ClientQuotaCallbackClassProp))
      .map(_.toString).getOrElse("")
    quotaCallbackStr.contains(ConfluentConfigs.TENANT_QUOTA_CALLBACK_CLASS)
  }

  /**
    * Returns true if broker back-pressure is enabled in the broker config for the given quota type,
    * which requires tenant-level quotas to be configured as well
    *
    * Backpressure for any replication-type quotas is not supported even though this method could
    * return true for a replication-type quota (e.g., FollowerReplication) since it could be
    * listed in the config.
    */
  def backpressureEnabledInConfig(cfg: KafkaConfig, quotaType: QuotaType): Boolean = {
    isMultiTenant(cfg) && Option(cfg.getString(ConfluentConfigs.BACKPRESSURE_TYPES_CONFIG)).exists { backpressureTypeProp =>
      val backressureTypesList = CoreUtils.parseCsvList(backpressureTypeProp)
      backressureTypesList.exists(backpressureType => backpressureType == quotaType.toString.toLowerCase)}
  }

  /**
    * Returns broker backpressure config for the given broker config and quota type.
    *
    * Broker back-pressure requires tenant-level quotas to be enabled in the broker config,
    * and 'multitenant.listener.names' to be configured as well. Backpressure for any
    * replication-type quotas is not supported.
    */
  def brokerBackpressureConfig(cfg: KafkaConfig, quotaType: QuotaType): BrokerBackpressureConfig = {
    val backpressureEnabled = backpressureEnabledInConfig(cfg, quotaType)
    val tenantListenerNames = quotaType match {
      case QuotaType.Request =>
        val listeners = Option(cfg.getString(ConfluentConfigs.MULTITENANT_LISTENER_NAMES_CONFIG))
          .map { listenerNameList => parseCsvList(listenerNameList) }
          .getOrElse(Seq())
          .filter(listenerName => cfg.advertisedListeners.exists(endpoint => endpoint.listenerName.value() == listenerName))
        if (listeners.isEmpty && backpressureEnabled) {
          warn(s"Invalid multitenant listener names provided in config. Request backpressure will be disabled")
        }
        listeners
      case _ => Seq()
    }

    BrokerBackpressureConfig(
      backpressureEnabledInConfig = backpressureEnabled,
      tenantEndpointListenerNames = tenantListenerNames,
      maxQueueSize = cfg.queuedMaxRequests.toDouble
    )
  }

}
