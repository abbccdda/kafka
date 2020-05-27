/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import java.util
import java.util.Collections

import kafka.server.Defaults
import kafka.server.KafkaConfig._
import kafka.server.link.ClusterLinkConfig._
import kafka.server.link.ClusterLinkConfigDefaults._
import org.apache.kafka.clients.CommonClientConfigs._
import org.apache.kafka.clients.{ClientDnsLookup, CommonClientConfigs}
import org.apache.kafka.common.config.ConfigDef.{ConfigKey, ValidString}
import org.apache.kafka.common.config.ConfigDef.Importance.{HIGH, LOW, MEDIUM}
import org.apache.kafka.common.config.ConfigDef.Range.atLeast
import org.apache.kafka.common.config.ConfigDef.Type._
import org.apache.kafka.common.config.ConfigDef.ValidString.in
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, SaslConfigs}
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._

object ClusterLinkConfigDefaults {
  val NumClusterLinkFetchers = 1
  val RetryBackoffMs = 100L
  val MetadataMaxAgeMs = 5 * 60 * 1000
  val OffsetSyncMsDefault = 30000
  val RetryTimeoutMs = 5 * 60 * 1000
  val AclSyncMsDefault = 5000
  val TopicConfigSyncMsDefault = 5000
}

class ClusterLinkConfig(props: util.Map[_, _]) extends AbstractConfig(configDef, props, false) {

  val log: Logger = LoggerFactory.getLogger(getClass())


  validate(props)

  val numClusterLinkFetchers = getInt(NumClusterLinkFetchersProp)
  val consumerOffsetSyncEnable = getBoolean(ClusterLinkConfig.ConsumerOffsetSyncEnableProp)
  val consumerOffsetSyncMs = getInt(ClusterLinkConfig.ConsumerOffsetSyncMsProp)
  val consumerGroupFilters: Option[GroupFiltersJson] = GroupFilterJson.parse(getString(ConsumerOffsetGroupFiltersProp))
  val aclSyncEnable = getBoolean(AclSyncEnableProp)
  val aclFilters: Option[AclFiltersJson] = AclJson.parse(getString(AclFiltersProp))
  val aclSyncMs = getInt(AclSyncMsProp)
  val topicConfigSyncMs = getInt(TopicConfigSyncMsProp)
  val bootstrapServers = getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
  val dnsLookup = ClientDnsLookup.forConfig(getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG))
  val securityProtocol = SecurityProtocol.forName(getString(SECURITY_PROTOCOL_CONFIG))
  val saslMechanism = getString(SaslConfigs.SASL_MECHANISM)
  val requestTimeoutMs = getInt(RequestTimeoutMsProp)
  val connectionsMaxIdleMs = getLong(ConnectionsMaxIdleMsProp)
  val replicaSocketTimeoutMs = getInt(ReplicaSocketTimeoutMsProp)
  val replicaSocketReceiveBufferBytes = getInt(ReplicaSocketReceiveBufferBytesProp)
  val replicaFetchMaxBytes = getInt(ReplicaFetchMaxBytesProp)
  val replicaFetchWaitMaxMs = getInt(ReplicaFetchWaitMaxMsProp)
  val replicaFetchMinBytes = getInt(ReplicaFetchMinBytesProp)
  val replicaFetchResponseMaxBytes = getInt(ReplicaFetchResponseMaxBytesProp)
  val replicaFetchBackoffMs = getInt(ReplicaFetchBackoffMsProp)

  val metadataRefreshBackoffMs = getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG)
  val metadataMaxAgeMs = getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG)

  val retryTimeoutMs: Int = getInt(RetryTimeoutMsProp)
}

object ClusterLinkConfig {

  val NumClusterLinkFetchersProp = "num.cluster.link.fetchers"
  val NumClusterLinkFetchersDoc = "Number of fetcher threads used to replicate messages from source brokers in cluster links."

  val ConsumerOffsetSyncEnableProp = "consumer.offset.sync.enable"
  val ConsumerOffsetSyncEnableDoc = "Whether or not to migrate consumer offsets from the source cluster."

  val ConsumerOffsetSyncMsProp = "consumer.offset.sync.ms"
  val ConsumerOffsetSyncMsDoc = "How often to sync consumer offsets."

  val ConsumerOffsetGroupFiltersProp = "consumer.offset.group.filters"
  val ConsumerOffsetGroupFiltersDoc = "JSON to denote the list of consumer groups to be migrated."

  val RetryTimeoutMsProp = "cluster.link.retry.timeout.ms"
  val RetryTimeoutMsDoc = "The number of milliseconds after which failures are no longer retried and" +
    " partitions are marked as failed. If the source topic is deleted and recreated within this timeout," +
    " the link may contain records from the old as well as the new topic."

  val AclSyncEnableProp = "acl.sync.enable"
  val AclSyncEnableDoc = "Whether or not to migrate ACLs"

  val AclFiltersProp = "acl.filters"
  val AclFiltersDoc = "JSON to denote the list of ACLs to be migrated."

  val AclSyncMsProp = "acl.sync.ms"
  val AclSyncMsDoc = "How often to refresh the ACLs."

  val TopicConfigSyncMsProp = "topic.config.sync.ms"
  val TopicConfigSyncMsDoc = "How often to refresh the topic configs."

  val TenantPrefixProp = "confluent.tenant.prefix"
  val TenantPrefixPropDoc = "Internal config for tenant prefix for cluster links in Confluent Cloud." +
    " Cannot be explicitly configured since prefix is automatically persisted based on request context."

  def main(args: Array[String]): Unit = {
    println(configDef.toHtml)
  }

  private val configDef = new ConfigDef()
    .define(NumClusterLinkFetchersProp, INT, NumClusterLinkFetchers, LOW, NumClusterLinkFetchersDoc)
    .define(ConsumerOffsetSyncEnableProp, BOOLEAN, false, LOW, ConsumerOffsetSyncEnableDoc)
    .define(ConsumerOffsetSyncMsProp, INT, OffsetSyncMsDefault, LOW, ConsumerOffsetSyncMsDoc)
    .define(ConsumerOffsetGroupFiltersProp, STRING, "", GroupFilterJson.VALIDATOR, LOW, ConsumerOffsetGroupFiltersDoc)
    .define(RetryTimeoutMsProp, INT, RetryTimeoutMs, MEDIUM, RetryTimeoutMsDoc)
    .define(AclSyncEnableProp, BOOLEAN, false, LOW, AclSyncEnableDoc)
    .define(AclFiltersProp, STRING, "", AclJson.VALIDATOR, LOW, AclFiltersDoc)
    .define(AclSyncMsProp, INT, AclSyncMsDefault, LOW, AclSyncMsDoc)
    .define(TopicConfigSyncMsProp, INT, TopicConfigSyncMsDefault, LOW, TopicConfigSyncMsDoc)
    .define(BOOTSTRAP_SERVERS_CONFIG, LIST, Collections.emptyList, new ConfigDef.NonNullValidator, HIGH, BOOTSTRAP_SERVERS_DOC)
    .define(CLIENT_DNS_LOOKUP_CONFIG, STRING, ClientDnsLookup.USE_ALL_DNS_IPS.toString,
      in(ClientDnsLookup.DEFAULT.toString, ClientDnsLookup.USE_ALL_DNS_IPS.toString, ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString),
      MEDIUM, CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
    .define(SECURITY_PROTOCOL_CONFIG, STRING, DEFAULT_SECURITY_PROTOCOL, MEDIUM, SECURITY_PROTOCOL_DOC)
    .define(ReplicaSocketTimeoutMsProp, INT, Defaults.ReplicaSocketTimeoutMs, LOW, ReplicaSocketTimeoutMsDoc)
    .define(RequestTimeoutMsProp, INT, Defaults.RequestTimeoutMs, LOW, RequestTimeoutMsDoc)
    .define(ConnectionsMaxIdleMsProp, LONG, Defaults.ConnectionsMaxIdleMs, LOW, ConnectionsMaxIdleMsDoc)
    .define(ReplicaSocketReceiveBufferBytesProp, INT, Defaults.ReplicaSocketReceiveBufferBytes, LOW, ReplicaSocketReceiveBufferBytesDoc)
    .define(ReplicaFetchMaxBytesProp, INT, Defaults.ReplicaFetchMaxBytes, atLeast(0), LOW, ReplicaFetchMaxBytesDoc)
    .define(ReplicaFetchWaitMaxMsProp, INT, Defaults.ReplicaFetchWaitMaxMs, LOW, ReplicaFetchWaitMaxMsDoc)
    .define(ReplicaFetchBackoffMsProp, INT, Defaults.ReplicaFetchBackoffMs, atLeast(0), LOW, ReplicaFetchBackoffMsDoc)
    .define(ReplicaFetchMinBytesProp, INT, Defaults.ReplicaFetchMinBytes, LOW, ReplicaFetchMinBytesDoc)
    .define(ReplicaFetchResponseMaxBytesProp, INT, Defaults.ReplicaFetchResponseMaxBytes, atLeast(0), LOW, ReplicaFetchResponseMaxBytesDoc)
    .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, LONG, RetryBackoffMs, atLeast(0), LOW, CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
    .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, LONG, MetadataMaxAgeMs, atLeast(0), LOW, CommonClientConfigs.METADATA_MAX_AGE_DOC)

    .withClientSslSupport()
    .withClientSaslSupport()
    .defineInternal(TenantPrefixProp, STRING, null, ValidString.in(null), LOW, TenantPrefixPropDoc)

  def configNames: Seq[String] = configDef.names.asScala.toSeq.sorted

  def configKeys: Map[String, ConfigKey] = configDef.configKeys.asScala.toMap

  def typeOf(name: String): Option[ConfigDef.Type] = Option(configDef.configKeys().get(name)).map(_.`type`)

  private[link] def validate(props: util.Map[_, _]): Unit = {
    val parsedProps = configDef.parse(props)
    val aclSyncEnable = parsedProps.get(AclSyncEnableProp).asInstanceOf[Boolean]
    val aclFilters: String = props.get(AclFiltersProp).asInstanceOf[String]
    if (aclSyncEnable && AclJson.parse(aclFilters).isEmpty) {
      throw new InvalidConfigurationException("ACL migration is enabled but acl.filters is not set."
        +   " Please set acl.filters to proceed with ACL migration.")
    }
  }
}

case class ClusterLinkProps(config: ClusterLinkConfig, tenantPrefix: Option[String])
