/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.io.{File, IOException}
import java.net.{InetAddress, SocketTimeoutException}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.{Collections, Optional}
import java.util.function.Supplier

import com.typesafe.scalalogging.Logger
import io.confluent.http.server.annotations.InterBrokerListener
import kafka.api.{KAFKA_0_9_0, KAFKA_2_2_IV0, KAFKA_2_4_IV1}
import kafka.cluster.Broker
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException, InconsistentBrokerMetadataException, InconsistentClusterIdException}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.TierLogComponents
import kafka.log.{LogConfig, LogManager}
import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.network.SocketServer
import io.confluent.http.server.{KafkaHttpServer, KafkaHttpServerBinder, KafkaHttpServerLoader}
import io.confluent.rest.{BeginShutdownBrokerHandle, InternalRestServer}
import kafka.security.CredentialProvider
import kafka.server.link.ClusterLinkFactory
import kafka.tier.fetcher.{TierFetcher, TierFetcherConfig}
import kafka.tier.state.TierPartitionStateFactory
import kafka.tier.store.{GcsTierObjectStore, GcsTierObjectStoreConfig, MockInMemoryTierObjectStore, S3TierObjectStore, S3TierObjectStoreConfig, TierObjectStore, TierObjectStoreConfig}
import kafka.tier.{TierDeletedPartitionsCoordinator, TierReplicaManager}
import kafka.tier.fetcher.TierStateFetcher
import kafka.tier.tasks.{TierTasks, TierTasksConfig}
import kafka.tier.topic.{TierTopicConsumer, TierTopicManager, TierTopicManagerConfig}
import kafka.utils._
import kafka.zk.{AdminZkClient, BrokerInfo, KafkaZkClient}
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ManualMetadataUpdater, NetworkClient, NetworkClientUtils, CommonClientConfigs}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.message.ControlledShutdownRequestData
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter, _}
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, KafkaThread, LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Endpoint, Node}
import org.apache.kafka.common.config.internals.ConfluentConfigs
import org.apache.kafka.common.security.fips.FipsValidator
import org.apache.kafka.common.errors.StaleBrokerEpochException
import org.apache.kafka.server.audit.{AuditLogProvider, AuditLogProviderFactory}
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.http.{MetadataServer, MetadataServerFactory}
import org.apache.kafka.server.license.LicenseValidator
import org.apache.kafka.server.multitenant.MultiTenantMetadata
import org.apache.zookeeper.client.ZKClientConfig
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._
import scala.collection.{Map, Seq, mutable}

object KafkaServer {
  // Copy the subset of properties that are relevant to Logs
  // I'm listing out individual properties here since the names are slightly different in each Config class...
  private[kafka] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
    val logProps = new util.HashMap[String, Object]()
    logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(LogConfig.MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(LogConfig.MaxCompactionLagMsProp, kafkaConfig.logCleanerMaxCompactionLagMs)
    logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(LogConfig.UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(LogConfig.PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(LogConfig.MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(LogConfig.MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps.put(LogConfig.MessageDownConversionEnableProp, kafkaConfig.logMessageDownConversionEnable: java.lang.Boolean)
    logProps.put(LogConfig.TierEnableProp, kafkaConfig.tierEnable: java.lang.Boolean)
    logProps.put(LogConfig.TierLocalHotsetBytesProp, kafkaConfig.tierLocalHotsetBytes: java.lang.Long)
    logProps.put(LogConfig.TierLocalHotsetMsProp, kafkaConfig.tierLocalHotsetMs: java.lang.Long)
    logProps.put(LogConfig.TierSegmentHotsetRollMinBytesProp, kafkaConfig.tierSegmentHotsetRollMinBytes: java.lang.Integer)
    logProps.put(LogConfig.PreferTierFetchMsProp, kafkaConfig.preferTierFetchMs: java.lang.Long)
    logProps.put(LogConfig.SegmentSpeculativePrefetchEnableProp, kafkaConfig.segmentSpeculativePrefetchEnable: java.lang.Boolean)

    // confluent configs needed for topic-level overrides below:

    // we should not pass in the rendered interceptor classes but the original list of string class names
    // this is because the LogConfig itself is expected to construct the interceptor class;
    // it also means that we will have different instance of the interceptor class on broker and topic-level,
    // which should be fine since we do not guarantee any sharing of interceptors anyways
    logProps.put(LogConfig.AppendRecordInterceptorClassesProp, kafkaConfig.getList(KafkaConfig.AppendRecordInterceptorClassesProp))

    // confluent configs needed for schema validation
    // only confluent.schema.registy.url is needed to enable the feature. The rest are security related
    logProps.computeIfAbsent(ConfluentConfigs.SCHEMA_REGISTRY_URL_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SCHEMA_REGISTRY_URL_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.USER_INFO_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.USER_INFO_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.BEARER_AUTH_CREDENTIALS_SOURCE_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.BEARER_AUTH_CREDENTIALS_SOURCE_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.BEARER_AUTH_TOKEN_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.BEARER_AUTH_TOKEN_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.SSL_PROTOCOL_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SSL_PROTOCOL_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.SSL_KEYSTORE_TYPE_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SSL_KEYSTORE_TYPE_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.SSL_KEYSTORE_LOCATION_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SSL_KEYSTORE_PASSWORD_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.SSL_KEY_PASSWORD_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SSL_KEY_PASSWORD_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SSL_TRUSTSTORE_TYPE_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
    logProps.computeIfAbsent(ConfluentConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, (_: String) => kafkaConfig.getString(ConfluentConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG))

    logProps
  }

  private [kafka] def augmentWithKafkaConfig(logProps: util.Map[String, Object], kafkaConfig: KafkaConfig): Unit = {
    // we augment the props with broker-level kafka configs for configuring the interceptor
    if (kafkaConfig != null) {
      logProps.put(ConfluentConfigs.MAX_CACHE_SIZE_CONFIG, kafkaConfig.getInt(ConfluentConfigs.MAX_CACHE_SIZE_CONFIG))
      logProps.put(ConfluentConfigs.MAX_RETRIES_CONFIG, kafkaConfig.getInt(ConfluentConfigs.MAX_RETRIES_CONFIG))
      logProps.put(ConfluentConfigs.RETRIES_WAIT_MS_CONFIG, kafkaConfig.getInt(ConfluentConfigs.RETRIES_WAIT_MS_CONFIG))
      logProps.put(ConfluentConfigs.MISSING_ID_QUERY_RANGE_CONFIG, kafkaConfig.getInt(ConfluentConfigs.MISSING_ID_QUERY_RANGE_CONFIG))
      logProps.put(ConfluentConfigs.MISSING_ID_CACHE_TTL_CONFIG, kafkaConfig.getLong(ConfluentConfigs.MISSING_ID_CACHE_TTL_CONFIG))
    }
  }

  private[server] def metricConfig(kafkaConfig: KafkaConfig): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples)
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

  def zkClientConfigFromKafkaConfig(config: KafkaConfig, forceZkSslClientEnable: Boolean = false) =
    if (!config.zkSslClientEnable && !forceZkSslClientEnable)
      None
    else {
      val clientConfig = new ZKClientConfig()
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslClientEnableProp, "true")
      config.zkClientCnxnSocketClassName.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkClientCnxnSocketProp, _))
      config.zkSslKeyStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreLocationProp, _))
      config.zkSslKeyStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStorePasswordProp, x.value))
      config.zkSslKeyStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslKeyStoreTypeProp, _))
      config.zkSslTrustStoreLocation.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreLocationProp, _))
      config.zkSslTrustStorePassword.foreach(x => KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStorePasswordProp, x.value))
      config.zkSslTrustStoreType.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslTrustStoreTypeProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslProtocolProp, config.ZkSslProtocol)
      config.ZkSslEnabledProtocols.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEnabledProtocolsProp, _))
      config.ZkSslCipherSuites.foreach(KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCipherSuitesProp, _))
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp, config.ZkSslEndpointIdentificationAlgorithm)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslCrlEnableProp, config.ZkSslCrlEnable.toString)
      KafkaConfig.setZooKeeperClientProperty(clientConfig, KafkaConfig.ZkSslOcspEnableProp, config.ZkSslOcspEnable.toString)
      Some(clientConfig)
    }

  /**
   * Initiates a shutdown of the Java Virtual Machine by
   * creating a thread that calls #{@code Exit#exit()}
   *
   * Note that the thread invoking exit blocks until the JVM terminates.
   *
   * @return the #{@code Thread} which is calling exit
   */
  def initiateShutdown(): Thread = {
    val previousInitiations = externalShutdownInitiations.getAndAdd(1)
    val th = KafkaThread.daemon(s"external-shutdown-$previousInitiations", () => {
      logger.warn("Initiating externally-requested shutdown by calling Exit " +
        "(there were {} externally-initiated shutdowns previously)", previousInitiations)
      Exit.exit(0)
      logger.warn("Externally-requested shutdown finished successfully")
    })

    th.start()
    th
  }

  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
  val externalShutdownInitiations: AtomicLong = new AtomicLong(0)
  val logger = Logger(LoggerFactory.getLogger("KafkaServer"))
}

/**
 * Handle which supports gathering broker metadata and triggering a beginShutdown
 * command for the broker via the Rest server.
 */
case class BeginShutdownBrokerHandleAdapter(delegate: KafkaServer) extends BeginShutdownBrokerHandle {
  override def brokerId(): Long = delegate.config.brokerId
  override def brokerEpoch(): Long = delegate.kafkaController.brokerEpoch
  override def controllerId(): Integer = delegate.metadataCache.getControllerId.get
  override def underReplicatedPartitions(): Long = delegate.replicaManager.underReplicatedPartitionCount
  override def beginShutdown(brokerEpoch: Long): Unit = delegate.beginShutdown(brokerEpoch)
}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = Time.SYSTEM, threadNamePrefix: Option[String] = None,
                  kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()) extends Logging with KafkaMetricsGroup {
  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)
  private var controlledShutdownLatch = new CountDownLatch(1)

  private var shutdownLatch = new CountDownLatch(1)

  //properties for MetricsContext
  private val metricsPrefix: String = "kafka.server"
  private val KAFKA_CLUSTER_ID: String = "kafka.cluster.id"
  private val KAFKA_BROKER_ID: String = "kafka.broker.id"

  private var logContext: LogContext = null

  var kafkaYammerMetrics: KafkaYammerMetrics = null
  var metrics: Metrics = null

  val brokerState: BrokerState = new BrokerState

  var dataPlaneRequestProcessor: KafkaApis = null
  var controlPlaneRequestProcessor: KafkaApis = null

  var metadataServer: MetadataServer = null
  var auditLogProvider: AuditLogProvider = null

  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = null
  var controlPlaneRequestHandlerPool: KafkaRequestHandlerPool = null

  var logDirFailureChannel: LogDirFailureChannel = null
  var logManager: LogManager = null

  var replicaManager: ReplicaManager = null
  var adminManager: AdminManager = null
  var clusterLinkManager: ClusterLinkFactory.LinkManager = null
  var tokenManager: DelegationTokenManager = null

  var dynamicConfigHandlers: Map[String, ConfigHandler] = null
  var dynamicConfigManager: DynamicConfigManager = null
  var credentialProvider: CredentialProvider = null
  var tokenCache: DelegationTokenCache = null

  var groupCoordinator: GroupCoordinator = null

  var transactionCoordinator: TransactionCoordinator = null

  var tierReplicaManagerOpt: Option[TierReplicaManager] = None
  var tierTopicConsumerOpt: Option[TierTopicConsumer] = None
  var tierTopicManagerOpt: Option[TierTopicManager] = None
  var tierFetcherOpt: Option[TierFetcher] = None
  var tierStateFetcherOpt: Option[TierStateFetcher] = None
  var tierObjectStoreOpt: Option[TierObjectStore] = None
  var tierDeletedPartitionsCoordinatorOpt: Option[TierDeletedPartitionsCoordinator] = None
  var tierTasksOpt: Option[TierTasks] = None

  var kafkaController: KafkaController = null

  var kafkaScheduler: KafkaScheduler = null

  var metadataCache: MetadataCache = null
  var quotaManagers: QuotaFactory.QuotaManagers = null

  val zkClientConfig: ZKClientConfig = KafkaServer.zkClientConfigFromKafkaConfig(config).getOrElse(new ZKClientConfig())
  private var _zkClient: KafkaZkClient = null
  val correlationId: AtomicInteger = new AtomicInteger(0)
  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))).toMap

  private var _clusterId: String = null
  private var _brokerTopicStats: BrokerTopicStats = null

  var multitenantMetadata: MultiTenantMetadata = null

  private var licenseValidator: LicenseValidator = null

  var httpServer: Option[KafkaHttpServer] = None
  private var httpServerBinder: KafkaHttpServerBinder = null

  private var fipsValidator: FipsValidator = null

  private var internalRestServer: InternalRestServer = null

  def clusterId: String = _clusterId

  // Visible for testing
  private[kafka] def zkClient = _zkClient

  private[kafka] def brokerTopicStats = _brokerTopicStats

  newGauge("ExternalShutdownInitiations", () => KafkaServer.externalShutdownInitiations.get())
  newGauge("BrokerState", () => brokerState.currentState)
  newGauge("ClusterId", () => clusterId)
  newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size)

  val linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time, logger.underlying)

  if (linuxIoMetricsCollector.usable()) {
    newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes())
    newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes())
  }

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup(): Unit = {
    try {
      info("starting")

      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")

      if (startupComplete.get)
        return

      val canStartup = isStartingUp.compareAndSet(false, true)
      if (canStartup) {
        brokerState.newState(Starting)

        /* check FIPS */
        if (config.fipsEnabled) {
          fipsValidator = ConfluentConfigs.buildFipsValidator()
          checkFips1402(config)
        }
        info(s"FIPS mode enabled: ${config.fipsEnabled}")

        /* setup zookeeper */
        initZkClient(time)

        /* Get or create cluster_id */
        _clusterId = getOrGenerateClusterId(zkClient)
        info(s"Cluster ID = $clusterId")

        /* load metadata */
        val (preloadedBrokerMetadataCheckpoint, initialOfflineDirs) = getBrokerMetadataAndOfflineDirs

        /* check cluster id */
        if (preloadedBrokerMetadataCheckpoint.clusterId.isDefined && preloadedBrokerMetadataCheckpoint.clusterId.get != clusterId)
          throw new InconsistentClusterIdException(
            s"The Cluster ID ${clusterId} doesn't match stored clusterId ${preloadedBrokerMetadataCheckpoint.clusterId} in meta.properties. " +
            s"The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.")

        httpServerBinder = new KafkaHttpServerBinder()
        httpServerBinder.bindInstance(classOf[ClusterResource], new ClusterResource(clusterId))

        /* generate brokerId */
        config.brokerId = getOrGenerateBrokerId(preloadedBrokerMetadataCheckpoint)
        logContext = new LogContext(s"[KafkaServer id=${config.brokerId}] ")
        this.logIdent = logContext.logPrefix

        // initialize dynamic broker configs from ZooKeeper. Any updates made after this will be
        // applied after DynamicConfigManager starts.
        config.dynamicConfig.initialize(zkClient)

        /* start scheduler */
        kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
        kafkaScheduler.startup()

        /* create and configure metrics */
        kafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
        kafkaYammerMetrics.configure(config.originals)

        val jmxReporter = new JmxReporter()
        jmxReporter.configure(config.originals)

        val reporters = new util.ArrayList[MetricsReporter]
        reporters.add(jmxReporter)

        val metricConfig = KafkaServer.metricConfig(config)
        val metricsContext = createKafkaMetricsContext()
        metrics = new Metrics(metricConfig, reporters, time, true, metricsContext)

        /* register broker metrics */
        _brokerTopicStats = new BrokerTopicStats

        quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
        notifyClusterListeners(kafkaMetricsReporters ++ metrics.reporters.asScala)

        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

        val tierTopicManagerConfig = new TierTopicManagerConfig(config, tieredStorageInterBrokerClientConfigsSupplier, _clusterId)
        if (config.tierFeature) {
          tierObjectStoreOpt = config.tierBackend match {
            case "S3" => Some(new S3TierObjectStore(new S3TierObjectStoreConfig(Optional.of(clusterId), config)))
            case "GCS" => Some(new GcsTierObjectStore(new GcsTierObjectStoreConfig(Optional.of(clusterId), config)))
            case "mock" => Some(new MockInMemoryTierObjectStore(new TierObjectStoreConfig(Optional.of(clusterId), config)))
            case v => throw new IllegalStateException(s"Unknown TierObjectStore type: %s".format(v))
          }
          tierFetcherOpt = Some(new TierFetcher(time, new TierFetcherConfig(config), tierObjectStoreOpt.get, kafkaScheduler, metrics, logContext))
          tierStateFetcherOpt = Some(new TierStateFetcher(config.tierObjectFetcherThreads, tierObjectStoreOpt.get))
          tierTopicConsumerOpt = Some(new TierTopicConsumer(tierTopicManagerConfig, logDirFailureChannel, tierStateFetcherOpt.get, metrics, time))
        }

        tierReplicaManagerOpt = Some(new TierReplicaManager())

        /* start log manager */
        val tierLogComponents = TierLogComponents(tierTopicConsumerOpt, tierObjectStoreOpt, new TierPartitionStateFactory(config.tierFeature))
        logManager = LogManager(config, initialOfflineDirs, zkClient, brokerState, kafkaScheduler, time, brokerTopicStats, logDirFailureChannel, tierLogComponents)
        logManager.startup()

        metadataCache = new MetadataCache(config.brokerId)


        // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
        // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
        tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames)
        credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache)

        // multi-tenant metadata watcher should be initialized after dynamic config manager is
        // initialized and before socket server processors
        multitenantMetadata = ConfluentConfigs.buildMultitenantMetadata(config.values)

        //auditLogProvider.start() will called as part of authorizer.start()
        auditLogProvider = AuditLogProviderFactory.create(config.originals, clusterId)
        auditLogProvider.setMetrics(metrics)

        // Create and start the socket server acceptor threads so that the bound port is known.
        // Delay starting processors until the end of the initialization sequence to ensure
        // that credentials have been loaded before processing authentications.
        socketServer = new SocketServer(config, metrics, time, credentialProvider, auditLogProvider)
        socketServer.startup(startProcessingRequests = false)

        clusterLinkManager = ClusterLinkFactory.createLinkManager(config, clusterId, quotaManagers.clusterLink,
          zkClient, metrics, time, threadNamePrefix = None, tierStateFetcherOpt)
        adminManager = new AdminManager(config, metrics, metadataCache, zkClient, clusterLinkManager)

        /* start replica manager */
        replicaManager = createReplicaManager(isShuttingDown, tierLogComponents)
        replicaManager.startup()

        // This broker registration delay is intended to be temporary and will be removed on completion of
        // this Jira.
        // https://confluentinc.atlassian.net/browse/WFLOW-124
        // The registration delay will prevent a broker from establishing leadership after starting up.
        // This helps to avoid client unavailability when external load balancers in GCP take a long time
        // to start routing traffic.
        if (config.brokerStartupRegistrationDelay > 0) {
          info(s"Delaying broker startup by ${config.brokerStartupRegistrationDelay}ms")
          Thread.sleep(config.brokerStartupRegistrationDelay.longValue())
        }

        val brokerInfo = createBrokerInfo
        val brokerEpoch = zkClient.registerBroker(brokerInfo)

        // Now that the broker is successfully registered, checkpoint its metadata
        checkpointBrokerMetadata(BrokerMetadata(config.brokerId, Some(clusterId)))

        /* start token manager */
        tokenManager = new DelegationTokenManager(config, tokenCache, time , zkClient)
        tokenManager.startup()

        /* tiered storage components */
        if (config.tierFeature) {
          tierTopicManagerOpt = Some(new TierTopicManager(tierTopicManagerConfig, tierTopicConsumerOpt.get, adminZkClientSupplier))
          tierTopicManagerOpt.get.startup()

          tierDeletedPartitionsCoordinatorOpt = Some(new TierDeletedPartitionsCoordinator(kafkaScheduler, replicaManager,
            tierTopicConsumerOpt.get, config.tierTopicDeleteCheckIntervalMs, config.tierMetadataNamespace, time))
          tierDeletedPartitionsCoordinatorOpt.get.startup()

          tierTasksOpt = Some(new TierTasks(TierTasksConfig(config), replicaManager, tierReplicaManagerOpt.get, tierDeletedPartitionsCoordinatorOpt.get, tierTopicManagerOpt.get, tierObjectStoreOpt.get, time))
          tierTasksOpt.get.start()
        }

        /* start kafka controller */
        kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, brokerEpoch, tokenManager, tierTopicManagerOpt, threadNamePrefix, clusterLinkManager)
        kafkaController.startup()

        /* start group coordinator */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        groupCoordinator = GroupCoordinator(config, zkClient, replicaManager, Time.SYSTEM, metrics)
        groupCoordinator.startup()

        /* start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"), zkClient, metrics, metadataCache, Time.SYSTEM)
        transactionCoordinator.startup()

        metadataServer = MetadataServerFactory.create(clusterId, config.originals)

        val authorizerServerInfo =
          brokerInfo.broker.toServerInfo(
            clusterId, config, metadataServer, httpServerBinder, auditLogProvider, metrics)
        httpServerBinder.bindInstance(
          classOf[Endpoint],
          classOf[InterBrokerListener],
          authorizerServerInfo.interBrokerEndpoint())

        /* Get the authorizer and initialize it if one is specified.*/
        authorizer = config.authorizer
        authorizer.foreach(_.configure(config.originals))
        val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = authorizer match {
          case Some(authZ) =>
            authZ.start(authorizerServerInfo).asScala.map { case (ep, cs) =>
              ep -> cs.toCompletableFuture
            }
          case None =>
            brokerInfo.broker.endPoints.map { ep =>
              ep.toJava -> CompletableFuture.completedFuture[Void](null)
            }.toMap
        }

        /* Startup the clusterLinkManager once we have the authorizer and kafkaController */
        clusterLinkManager.startup(brokerInfo.broker.endPoint(config.interBrokerListenerName).toJava,
          replicaManager, adminManager, kafkaController, authorizer)

        val fetchManager = new FetchManager(Time.SYSTEM,
          new FetchSessionCache(config.maxIncrementalFetchSessionCacheSlots,
            KafkaServer.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))

        /* start processing requests */
        dataPlaneRequestProcessor = new KafkaApis(socketServer.dataPlaneRequestChannel, replicaManager, adminManager,
          clusterLinkManager.admin, groupCoordinator, transactionCoordinator,
          kafkaController, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
          fetchManager, brokerTopicStats, clusterId, time, tokenManager, tierDeletedPartitionsCoordinatorOpt)


        dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config, config.brokerId, socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
          config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent", SocketServer.DataPlaneThreadPrefix, metrics)

        socketServer.controlPlaneRequestChannelOpt.foreach { controlPlaneRequestChannel =>
          controlPlaneRequestProcessor = new KafkaApis(controlPlaneRequestChannel, replicaManager, adminManager,
            clusterLinkManager.admin, groupCoordinator, transactionCoordinator,
            kafkaController, zkClient, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
            fetchManager, brokerTopicStats, clusterId, time, tokenManager, tierDeletedPartitionsCoordinatorOpt)

          controlPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config,
            config.brokerId,
            socketServer.controlPlaneRequestChannelOpt.get,
            controlPlaneRequestProcessor,
            time,
            1,
            s"${SocketServer.ControlPlaneMetricPrefix}RequestHandlerAvgIdlePercent",
            SocketServer.ControlPlaneThreadPrefix,
            metrics)
        }

        Mx4jLoader.maybeLoad()

        /* Add all reconfigurable instances for config change notification before starting config handlers */
        config.dynamicConfig.addReconfigurables(this)

        /* start dynamic config manager */
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.Topic -> new TopicConfigHandler(replicaManager, config, quotaManagers, kafkaController),
                                                           ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
                                                           ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
                                                           ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers),
                                                           ConfigType.ClusterLink -> new ClusterLinkConfigHandler(clusterLinkManager))

        // Create the config manager. start listening to notifications
        dynamicConfigManager = new DynamicConfigManager(zkClient, dynamicConfigHandlers)
        dynamicConfigManager.startup()

        socketServer.startProcessingRequests(authorizerFutures)

        authorizerFutures.values.foreach(_.join())
        metadataServer.start()
        httpServer = KafkaHttpServerLoader.load(config.originals, httpServerBinder.createInjector()).asScala

        if (config.internalRestServerBindPort != null) {
          internalRestServer = new InternalRestServer(config.internalRestServerBindPort, BeginShutdownBrokerHandleAdapter(this))
          internalRestServer.start()
        }

        brokerState.newState(RunningAsBroker)
        controlledShutdownLatch = new CountDownLatch(1)
        shutdownLatch = new CountDownLatch(1)

        if (multitenantMetadata != null) {
          val endpoint = brokerInfo.broker.brokerEndPoint(config.interBrokerListenerName).connectionString()
          multitenantMetadata.handleSocketServerInitialized(endpoint)
        }

        httpServer.foreach(_.start())
        if (!httpServer.forall(_.awaitStarted(config.httpServerStartTimeout))) {
          throw new IllegalStateException("Kafka HTTP server failed to start up.")
        }
        httpServer.flatMap(_.getError.asScala).foreach { t => throw t }

        startupComplete.set(true)

        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(metricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
        info("started")

        // confluent-server` disables license only for Cloud. Start license manager if
        // not using the multi-tenant authorizer.
        if (!authorizer.map(_.getClass.getName).contains(ConfluentConfigs.MULTITENANT_AUTHORIZER_CLASS_NAME)) {
          val interBrokerEndpoint = brokerInfo.broker.endPoint(config.interBrokerListenerName).toJava
          licenseValidator = ConfluentConfigs.buildLicenseValidator(config, interBrokerEndpoint)
          licenseValidator.start(config.brokerId.toString)
        }
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  private def adminZkClientSupplier: Supplier[AdminZkClient] = {
    () => new AdminZkClient(_zkClient)
  }

  /* public for usage in tests */
  def tieredStorageInterBrokerClientConfigsSupplier: Supplier[util.Map[String, Object]] = {
    () => {
      if (config.tierMetadataBootstrapServers != null) {
        Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.tierMetadataBootstrapServers)
      } else {
        val interBrokerClientConfigsOpt = metadataCache.getAliveBrokers
          .find(_.id == config.brokerId)
          .map { broker =>
            val interBrokerEndPoint = broker.endPoint(config.interBrokerListenerName).toJava
            ConfluentConfigs.interBrokerClientConfigs(config, interBrokerEndPoint)
          }
        interBrokerClientConfigsOpt.getOrElse(Collections.emptyMap[String, Object]())
      }
    }
  }

  private[server] def notifyClusterListeners(clusterListeners: Seq[AnyRef]): Unit = {
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  private[server] def notifyMetricsReporters(metricsReporters: Seq[AnyRef]): Unit = {
    val metricsContext = createKafkaMetricsContext()
    metricsReporters.foreach {
      case x: MetricsReporter => x.contextChange(metricsContext)
      case _ => //do nothing
    }
  }

  private[server] def createKafkaMetricsContext() : KafkaMetricsContext = {
    val contextLabels = new util.HashMap[String, Object]
    contextLabels.put(KAFKA_CLUSTER_ID, clusterId)
    contextLabels.put(KAFKA_BROKER_ID, config.brokerId.toString)
    contextLabels.putAll(config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX))
    // add Confluent metrics context labels
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "kafka")
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion)
    contextLabels.put(ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId)
    val metricsContext = new KafkaMetricsContext(metricsPrefix, contextLabels)
    metricsContext
  }

  protected def createReplicaManager(isShuttingDown: AtomicBoolean, tierLogComponents: TierLogComponents): ReplicaManager = {
    val tierReplicaComponents = TierReplicaComponents(tierReplicaManagerOpt, tierFetcherOpt, tierStateFetcherOpt, tierLogComponents)
    new ReplicaManager(config, metrics, time, zkClient, kafkaScheduler, logManager, isShuttingDown, quotaManagers,
      brokerTopicStats, metadataCache, logDirFailureChannel, tierReplicaComponents, Some(clusterLinkManager))
  }

  private def initZkClient(time: Time): Unit = {
    info(s"Connecting to zookeeper on ${config.zkConnect}")

    def createZkClient(zkConnect: String, isSecure: Boolean) = {
      KafkaZkClient(zkConnect, isSecure, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
        config.zkMaxInFlightRequests, time, name = Some("Kafka server"), zkClientConfig = Some(zkClientConfig))
    }

    val chrootIndex = config.zkConnect.indexOf("/")
    val chrootOption = {
      if (chrootIndex > 0) Some(config.zkConnect.substring(chrootIndex))
      else None
    }

    val secureAclsEnabled = config.zkEnableSecureAcls
    val isZkSecurityEnabled = JaasUtils.isZkSaslEnabled() || KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig)

    if (secureAclsEnabled && !isZkSecurityEnabled)
      throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but ZooKeeper client TLS configuration identifying at least $KafkaConfig.ZkSslClientEnableProp, $KafkaConfig.ZkClientCnxnSocketProp, and $KafkaConfig.ZkSslKeyStoreLocationProp was not present and the " +
        s"verification of the JAAS login file failed ${JaasUtils.zkSecuritySysConfigString}")

    // make sure chroot path exists
    chrootOption.foreach { chroot =>
      val zkConnForChrootCreation = config.zkConnect.substring(0, chrootIndex)
      val zkClient = createZkClient(zkConnForChrootCreation, secureAclsEnabled)
      zkClient.makeSurePersistentPathExists(chroot)
      info(s"Created zookeeper path $chroot")
      zkClient.close()
    }

    _zkClient = createZkClient(config.zkConnect, secureAclsEnabled)
    _zkClient.createTopLevelPaths()
  }

  private def getOrGenerateClusterId(zkClient: KafkaZkClient): String = {
    zkClient.getClusterId.getOrElse(zkClient.createOrGetClusterId(CoreUtils.generateUuidAsBase64))
  }

  private[server] def createBrokerInfo: BrokerInfo = {
    val endPoints = config.advertisedListeners.map(e => s"${e.host}:${e.port}")
    zkClient.getAllBrokersInCluster.filter(_.id != config.brokerId).foreach { broker =>
      val commonEndPoints = broker.endPoints.map(e => s"${e.host}:${e.port}").intersect(endPoints)
      require(commonEndPoints.isEmpty, s"Configured end points ${commonEndPoints.mkString(",")} in" +
        s" advertised listeners are already registered by broker ${broker.id}")
    }

    val listeners = config.advertisedListeners.map { endpoint =>
      if (endpoint.port == 0)
        endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
      else
        endpoint
    }

    val updatedEndpoints = listeners.map(endpoint =>
      if (endpoint.host == null || endpoint.host.trim.isEmpty)
        endpoint.copy(host = InetAddress.getLocalHost.getCanonicalHostName)
      else
        endpoint
    )

    val jmxPort = System.getProperty("com.sun.management.jmxremote.port", "-1").toInt
    BrokerInfo(Broker(config.brokerId, updatedEndpoints, config.rack), config.interBrokerProtocolVersion, jmxPort)
  }

  /**
   * Performs controlled shutdown
   */
  private def controlledShutdown(): Unit = {

    def node(broker: Broker): Node = broker.node(config.interBrokerListenerName)

    val socketTimeoutMs = config.controllerSocketTimeoutMs

    def doControlledShutdown(retries: Int): Boolean = {
      val metadataUpdater = new ManualMetadataUpdater()
      val networkClient = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
          config.interBrokerSecurityProtocol,
          JaasContext.Type.SERVER,
          config,
          config.interBrokerListenerName,
          config.saslMechanismInterBrokerProtocol,
          time,
          config.saslInterBrokerHandshakeRequestEnable,
          logContext)
        val selector = new Selector(
          NetworkReceive.UNLIMITED,
          config.connectionsMaxIdleMs,
          metrics,
          time,
          "kafka-server-controlled-shutdown",
          Map.empty[String, String].asJava,
          false,
          channelBuilder,
          logContext
        )
        new NetworkClient(
          selector,
          metadataUpdater,
          config.brokerId.toString,
          1,
          0,
          0,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          config.requestTimeoutMs,
          ClientDnsLookup.DEFAULT,
          time,
          false,
          new ApiVersions,
          logContext)
      }

      var shutdownSucceeded: Boolean = false

      try {

        var remainingRetries = retries
        var prevController: Broker = null
        var ioException = false

        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request.
          // If the controller id or the broker registration are missing, we sleep and retry (if there are remaining retries)
          zkClient.getControllerId match {
            case Some(controllerId) =>
              zkClient.getBroker(controllerId) match {
                case Some(broker) =>
                  // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
                  // attempt, connect to the most recent controller
                  if (ioException || broker != prevController) {

                    ioException = false

                    if (prevController != null)
                      networkClient.close(node(prevController).idString)

                    prevController = broker
                    metadataUpdater.setNodes(Seq(node(prevController)).asJava)
                  }
                case None =>
                  info(s"Broker registration for controller $controllerId is not available (i.e. the Controller's ZK session expired)")
              }
            case None =>
              info("No controller registered in ZooKeeper")
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!NetworkClientUtils.awaitReady(networkClient, node(prevController), time, socketTimeoutMs))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val controlledShutdownApiVersion: Short =
                if (config.interBrokerProtocolVersion < KAFKA_0_9_0) 0
                else if (config.interBrokerProtocolVersion < KAFKA_2_2_IV0) 1
                else if (config.interBrokerProtocolVersion < KAFKA_2_4_IV1) 2
                else 3

              val controlledShutdownRequest = new ControlledShutdownRequest.Builder(
                  new ControlledShutdownRequestData()
                    .setBrokerId(config.brokerId)
                    .setBrokerEpoch(kafkaController.brokerEpoch),
                    controlledShutdownApiVersion)
              val request = networkClient.newClientRequest(node(prevController).idString, controlledShutdownRequest,
                time.milliseconds(), true)
              val clientResponse = NetworkClientUtils.sendAndReceive(networkClient, request, time)

              val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]
              if (shutdownResponse.error == Errors.NONE && shutdownResponse.data.remainingPartitions.isEmpty) {
                shutdownSucceeded = true
                info("Controlled shutdown succeeded")
              }
              else {
                info(s"Remaining partitions to move: ${shutdownResponse.data.remainingPartitions}")
                info(s"Error from controller: ${shutdownResponse.error}")
              }
            }
            catch {
              case ioe: IOException =>
                ioException = true
                warn("Error during controlled shutdown, possibly because leader movement took longer than the " +
                  s"configured controller.socket.timeout.ms and/or request.timeout.ms: ${ioe.getMessage}")
                // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally
        networkClient.close()

      shutdownSucceeded
    }

    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      info("Starting controlled shutdown")

      brokerState.newState(PendingControlledShutdown)

      val shutdownSucceeded = doControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
    }
  }

  /**
   * beginShutdown will start the controlled shutdown process for the broker if the
   * expectedBrokerEpoch matches the actual broker epoch.
   *
   * If the expectedBrokerEpoch does not match the actual epoch, a StaleBrokerEpochException
   * will be thrown.
   *
   * It is safe to call this method from multiple threads, controlled shutdown will only be
   * executed once.
   *
   * Callers of beginShutdown are responsible for ensuring the broker has successfully started up.
   */
  def beginShutdown(expectedBrokerEpoch: Long): Unit = {
    // In order to use beginShutdown, it's expected that startup completed, so
    // it's safe to assume kafkaController is set.
    val currentBrokerEpoch = kafkaController.brokerEpoch
    if (expectedBrokerEpoch == currentBrokerEpoch) {
      info("Beginning controlled shutdown")
      ensureControlledShutdownOnce()
    } else {
      throw new StaleBrokerEpochException(s"Expected epoch $expectedBrokerEpoch does not match current epoch $currentBrokerEpoch")
    }
  }

  /**
   * Serialize controlled shutdown requests to ensure only one is running at a time.
   *
   * If multiple controlled shutdown requests are issued, all will block on the first
   * caller which aquired the lock succeeding or failing.
   */
  def ensureControlledShutdownOnce(): Unit = synchronized {
    if (controlledShutdownLatch.getCount > 0) {
      CoreUtils.swallow(controlledShutdown(), this)
      brokerState.newState(BrokerShuttingDown)
      controlledShutdownLatch.countDown()
    }
    controlledShutdownLatch.await()
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown(): Unit = {
    try {
      info("shutting down")

      if (isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
      // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
      // `true` at the end of this method.
      if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
        ensureControlledShutdownOnce()

        if (licenseValidator != null)
          CoreUtils.swallow(licenseValidator.close(), this)

        httpServer match {
          case Some(server) => {
            CoreUtils.swallow(server.stop(), this)
            if (!server.awaitStopped(config.httpServerStopTimeout)) {
              warn("Timed out while waiting for Kafka HTTP server to shutdown. Continuing Kafka server shutdown.")
            }
          }
          case None =>
        }

        if (dynamicConfigManager != null)
          CoreUtils.swallow(dynamicConfigManager.shutdown(), this)

        // Stop socket server to stop accepting any more connections and requests.
        // Socket server will be shutdown towards the end of the sequence.
        if (socketServer != null)
          CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
        if (dataPlaneRequestHandlerPool != null)
          CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
        if (controlPlaneRequestHandlerPool != null)
          CoreUtils.swallow(controlPlaneRequestHandlerPool.shutdown(), this)
        if (kafkaScheduler != null)
          CoreUtils.swallow(kafkaScheduler.shutdown(), this)

        if (dataPlaneRequestProcessor != null)
          CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
        if (controlPlaneRequestProcessor != null)
          CoreUtils.swallow(controlPlaneRequestProcessor.close(), this)

        if (internalRestServer != null)
          CoreUtils.swallow(internalRestServer.stop(), this)

        if (metadataServer != null)
          CoreUtils.swallow(metadataServer.close(), this)

        if (auditLogProvider != null)
          CoreUtils.swallow(auditLogProvider.close(), this)

        CoreUtils.swallow(authorizer.foreach(_.close()), this)
        if (adminManager != null)
          CoreUtils.swallow(adminManager.shutdown(), this)

        if (transactionCoordinator != null)
          CoreUtils.swallow(transactionCoordinator.shutdown(), this)
        if (groupCoordinator != null)
          CoreUtils.swallow(groupCoordinator.shutdown(), this)

        if (tokenManager != null)
          CoreUtils.swallow(tokenManager.shutdown(), this)

        CoreUtils.swallow(tierDeletedPartitionsCoordinatorOpt.foreach(_.shutdown()), this)

        CoreUtils.swallow(tierTasksOpt.foreach(_.shutdown()), this)

        CoreUtils.swallow(tierTopicManagerOpt.foreach(_.shutdown()), this)

        if (replicaManager != null)
          CoreUtils.swallow(replicaManager.shutdown(), this)
        if (clusterLinkManager != null)
          CoreUtils.swallow(clusterLinkManager.shutdown(), this)
        if (logManager != null)
          CoreUtils.swallow(logManager.shutdown(), this)

        if (tierStateFetcherOpt.isDefined)
          CoreUtils.swallow(tierStateFetcherOpt.get.close(), this)

        if (tierFetcherOpt.isDefined)
          CoreUtils.swallow(tierFetcherOpt.get.close(), this)

        if (tierObjectStoreOpt.isDefined)
          CoreUtils.swallow(tierObjectStoreOpt.get.close(), this)

        if (kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown(), this)

        if (zkClient != null)
          CoreUtils.swallow(zkClient.close(), this)

        if (quotaManagers != null)
          CoreUtils.swallow(quotaManagers.shutdown(), this)

        // Even though socket server is stopped much earlier, controller can generate
        // response for controlled shutdown request. Shutdown server at the end to
        // avoid any failures (e.g. when metrics are recorded)
        if (socketServer != null)
          CoreUtils.swallow(socketServer.shutdown(), this)
        if (metrics != null)
          CoreUtils.swallow(metrics.close(), this)
        if (brokerTopicStats != null)
          CoreUtils.swallow(brokerTopicStats.close(), this)

        if (multitenantMetadata != null) {
          multitenantMetadata.close(config.brokerSessionUuid)
        }

        // Clear all reconfigurable instances stored in DynamicBrokerConfig
        config.dynamicConfig.clear()

        brokerState.newState(NotRunning)

        startupComplete.set(false)
        isShuttingDown.set(false)
        CoreUtils.swallow(AppInfoParser.unregisterAppInfo(metricsPrefix, config.brokerId.toString, metrics), this)
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager

  def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  /**
   * Reads the BrokerMetadata. If the BrokerMetadata doesn't match in all the log.dirs, InconsistentBrokerMetadataException is
   * thrown.
   *
   * The log directories whose meta.properties can not be accessed due to IOException will be returned to the caller
   *
   * @return A 2-tuple containing the brokerMetadata and a sequence of offline log directories.
   */
  private def getBrokerMetadataAndOfflineDirs: (BrokerMetadata, Seq[String]) = {
    val brokerMetadataMap = mutable.HashMap[String, BrokerMetadata]()
    val brokerMetadataSet = mutable.HashSet[BrokerMetadata]()
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    for (logDir <- config.logDirs) {
      try {
        val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
        brokerMetadataOpt.foreach { brokerMetadata =>
          brokerMetadataMap += (logDir -> brokerMetadata)
          brokerMetadataSet += brokerMetadata
        }
      } catch {
        case e: IOException =>
          offlineDirs += logDir
          error(s"Fail to read $brokerMetaPropsFile under log directory $logDir", e)
      }
    }

    if (brokerMetadataSet.size > 1) {
      val builder = new StringBuilder

      for ((logDir, brokerMetadata) <- brokerMetadataMap)
        builder ++= s"- $logDir -> $brokerMetadata\n"

      throw new InconsistentBrokerMetadataException(
        s"BrokerMetadata is not consistent across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
        s"or partial data was manually copied from another broker. Found:\n${builder.toString()}"
      )
    } else if (brokerMetadataSet.size == 1)
      (brokerMetadataSet.last, offlineDirs)
    else
      (BrokerMetadata(-1, None), offlineDirs)
  }


  /**
   * Checkpoint the BrokerMetadata to all the online log.dirs
   *
   * @param brokerMetadata
   */
  private def checkpointBrokerMetadata(brokerMetadata: BrokerMetadata) = {
    for (logDir <- config.logDirs if logManager.isLogDirOnline(new File(logDir).getAbsolutePath)) {
      val checkpoint = brokerMetadataCheckpoints(logDir)
      checkpoint.write(brokerMetadata)
    }
  }

  /**
   * Generates new brokerId if enabled or reads from meta.properties based on following conditions
   * <ol>
   * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
   * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
   * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
   * <ol>
   *
   * @return The brokerId.
   */
  private def getOrGenerateBrokerId(brokerMetadata: BrokerMetadata): Int = {
    val brokerId = config.brokerId

    if (brokerId >= 0 && brokerMetadata.brokerId >= 0 && brokerMetadata.brokerId != brokerId)
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerMetadata.brokerId} in meta.properties. " +
        s"If you moved your data, make sure your configured broker.id matches. " +
        s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    else if (brokerMetadata.brokerId < 0 && brokerId < 0 && config.brokerIdGenerationEnable) // generate a new brokerId from Zookeeper
      generateBrokerId
    else if (brokerMetadata.brokerId >= 0) // pick broker.id from meta.properties
      brokerMetadata.brokerId
    else
      brokerId
  }

  /**
    * Return a sequence id generated by updating the broker sequence id path in ZK.
    * Users can provide brokerId in the config. To avoid conflicts between ZK generated
    * sequence id and configured brokerId, we increment the generated sequence id by KafkaConfig.MaxReservedBrokerId.
    */
  private def generateBrokerId: Int = {
    try {
      zkClient.generateBrokerSequenceId() + config.maxReservedBrokerId
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }

  /**
    * Check whether the TLS Cipher algorithms, TLS protocol versions and broker security protocol
    * meet the FIPS requirement.
    */
  private def checkFips1402(brokerConfiguration: KafkaConfig): Unit = {
    brokerConfiguration.listenerSecurityProtocolMap.foreach { case (listenerName, _) =>
      fipsValidator.validateFipsTls(brokerConfiguration.valuesWithPrefixOverride(listenerName.configPrefix))
    }

    val listenerSecurityProtocolMap = brokerConfiguration.listenerSecurityProtocolMap
      .map { case (listener, protocol) => listener.value() -> protocol }.asJava

    fipsValidator.validateFipsBrokerProtocol(listenerSecurityProtocolMap)
  }
}
