// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.coordinator;

import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.clients.Writer;
import io.confluent.security.store.kafka.coordinator.MetadataServiceAssignment.AssignmentError;
import java.net.InetSocketAddress;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

/**
 * Node manager for Metadata Service. A Kafka group coordinator is used to manage active nodes
 * and elect a single master writer.
 */
public class MetadataNodeManager extends Thread implements MetadataServiceRebalanceListener {

  private static final String COORDINATOR_METRICS_PREFIX = "confluent.metadata.service";
  private static final String JMX_PREFIX = "confluent.metadata.service";

  private final Logger log;
  private final Time time;
  private final NodeMetadata nodeMetadata;
  private final Writer writer;
  private final Metrics metrics;
  private final String clientId;
  private final ConsumerNetworkClient coordinatorNetworkClient;
  private final MetadataServiceCoordinator coordinator;
  private final AtomicBoolean isAlive;

  private volatile NodeMetadata masterWriterNode;
  private volatile int masterWriterGenerationId;
  private volatile Collection<NodeMetadata> activeNodes;

  public MetadataNodeManager(Collection<URL> nodeUrls,
                             KafkaStoreConfig config,
                             Writer metadataWriter,
                             Time time) {
    this.nodeMetadata = new NodeMetadata(nodeUrls);
    this.writer = metadataWriter;
    this.time = time;

    ConsumerConfig coordinatorConfig = new ConsumerConfig(config.coordinatorConfigs());
    long rebalanceTimeoutMs = coordinatorConfig.getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
    if (rebalanceTimeoutMs < config.refreshTimeout.toMillis()) {
      throw new ConfigException(String.format(
          "Metadata service coordinator rebalance timeout %d should be higher than refresh timeout %d",
          rebalanceTimeoutMs, config.refreshTimeout.toMillis()));
    }

    this.clientId = coordinatorConfig.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
    this.metrics = createMetrics(clientId, coordinatorConfig, time);

    String logPrefix = String.format("[%s clientId=%s, groupId=%s]",
        MetadataNodeManager.class.getName(),
        clientId,
        coordinatorConfig.getString(ConsumerConfig.GROUP_ID_CONFIG));
    LogContext logContext = new LogContext(logPrefix);
    this.log = logContext.logger(MetadataNodeManager.class);

    Metadata metadata = new Metadata(
            coordinatorConfig.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG),
            coordinatorConfig.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
            logContext,
            new ClusterResourceListeners());
    List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
        coordinatorConfig.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
        coordinatorConfig.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG));
    metadata.bootstrap(addresses, time.milliseconds());

    KafkaClient networkClient = createKafkaClient(coordinatorConfig, metadata, time, logContext);
    coordinatorNetworkClient = new ConsumerNetworkClient(
        logContext,
        networkClient,
        metadata,
        time,
        coordinatorConfig.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG),
        coordinatorConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
        Integer.MAX_VALUE
    );
    coordinator = new MetadataServiceCoordinator(
        logContext,
        coordinatorNetworkClient,
        nodeMetadata,
        coordinatorConfig,
        metrics,
        COORDINATOR_METRICS_PREFIX,
        time,
        this
    );

    setName("metadata-service-coordinator");
    this.isAlive = new AtomicBoolean(true);
    this.activeNodes = Collections.emptySet();
  }

  @Override
  public void run() {
    try {
      log.debug("Starting metadata node coordinator");
      while (isAlive.get()) {
        coordinator.poll(Duration.ofMillis(Long.MAX_VALUE));
      }
    } catch (Throwable e) {
      if (isAlive.get())
        log.error("Metadata service node manager thread failed", e);
    }
  }

  public synchronized  boolean isMasterWriter() {
    if (!isAlive.get())
      return false;
    return this.nodeMetadata.equals(masterWriterNode);
  }

  public synchronized  URL masterWriterUrl(String protocol) {
    if (!isAlive.get())
      return null;
    return masterWriterNode == null ? null : masterWriterNode.url(protocol);
  }

  public synchronized Collection<URL> activeNodeUrls(String protocol) {
    if (!isAlive.get())
      return Collections.emptySet();
    else
      return activeNodes.stream()
          .map(n -> n.url(protocol))
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
  }

  @Override
  public synchronized void onAssigned(MetadataServiceAssignment assignment, int generationId) {
    log.debug("onAssigned generation {} assignment {}", assignment, generationId);

    this.activeNodes = assignment.nodes().values();

    stopWriter(null);
    NodeMetadata newWriter = assignment.writerNodeMetadata();
    if (assignment.error() == AssignmentError.NONE.errorCode && newWriter != null) {
      this.masterWriterNode = newWriter;
      this.masterWriterGenerationId = generationId;
      if (nodeMetadata.equals(newWriter))
        this.writer.startWriter(generationId);
    }
  }

  @Override
  public synchronized void onRevoked(int generationId) {
    log.debug("onRevoked generation {}", generationId);
    stopWriter(generationId);
  }

  @Override
  public synchronized void onWriterResigned(int generationId) {
    log.debug("onWriterResigned {}", generationId);
    if (this.nodeMetadata.equals(masterWriterNode) && masterWriterGenerationId == generationId) {
      stopWriter(generationId);
      onWriterResigned();
    }
  }

  // Visibility for testing
  protected void onWriterResigned() {
    coordinator.onWriterResigned();
  }

  public void close(Duration closeTimeout) {
    log.debug("Closing Metadata Service node manager");
    synchronized (this) {
      isAlive.set(false);
      this.masterWriterNode = null;
    }
    coordinatorNetworkClient.wakeup();
    AtomicReference<Throwable> firstException = new AtomicReference<>();
    try {
      coordinator.close(time.timer(closeTimeout.toMillis()));
    } catch (Throwable e) {
      firstException.set(e);
    }

    Utils.closeQuietly(coordinatorNetworkClient, "coordinatorNetworkClient", firstException);
    Utils.closeQuietly(metrics, "metrics", firstException);
    AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

    Throwable exception = firstException.getAndSet(null);
    if (exception != null)
      throw new KafkaException("Failed to close Metadata Service node manager", exception);
  }

  // Visibility to override int tests
  protected KafkaClient createKafkaClient(ConsumerConfig coordinatorConfig,
                                              Metadata metadata,
                                              Time time,
                                              LogContext logContext) {
    Selector selector = new Selector(
        coordinatorConfig.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG),
        metrics,
        time,
        COORDINATOR_METRICS_PREFIX,
        ClientUtils.createChannelBuilder(coordinatorConfig, time),
        logContext);

   return new NetworkClient(
        selector,
        metadata,
        clientId,
        100, // same as KafkaConsumer
        coordinatorConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
        coordinatorConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
        coordinatorConfig.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
        coordinatorConfig.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
        coordinatorConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
        ClientDnsLookup.DEFAULT,
        time,
        true,
        new ApiVersions(),
        logContext);
  }

  private Metrics createMetrics(String clientId, ConsumerConfig coordinatorConfig, Time time) {
    Map<String, String> metricsTags = new LinkedHashMap<>();
    metricsTags.put("client-id", clientId);
    long sampleWindowMs = coordinatorConfig.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG);
    MetricConfig metricConfig = new MetricConfig()
        .samples(coordinatorConfig.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
        .timeWindow(sampleWindowMs, TimeUnit.MILLISECONDS)
        .tags(metricsTags);
    List<MetricsReporter>
        reporters = coordinatorConfig.getConfiguredInstances(
        CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
        MetricsReporter.class
    );
    reporters.add(new JmxReporter(JMX_PREFIX));
    Metrics metrics = new Metrics(metricConfig, reporters, time);
    AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics);
    return metrics;
  }

  private void stopWriter(Integer stoppingGenerationId) {
    if (nodeMetadata.equals(this.masterWriterNode))
      this.writer.stopWriter(stoppingGenerationId);
    this.masterWriterNode = null;
    this.masterWriterGenerationId = -1;
  }
}
