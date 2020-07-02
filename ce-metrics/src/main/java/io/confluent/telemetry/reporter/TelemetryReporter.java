package io.confluent.telemetry.reporter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.confluent.telemetry.BrokerConfigUtils;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsCollectorTask;
import io.confluent.telemetry.collector.KafkaMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.MetricsCollectorProvider;
import io.confluent.telemetry.events.EventLogger;
import io.confluent.telemetry.events.EventLoggerConfig;
import io.confluent.telemetry.events.exporter.http.EventHttpExporter;
import io.confluent.telemetry.events.v1.ConfigEvent;
import io.confluent.telemetry.exporter.Exporter;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.http.HttpExporter;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.provider.KafkaServerProvider;
import io.confluent.telemetry.provider.Provider;
import io.confluent.telemetry.provider.ProviderRegistry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.confluent.telemetry.ConfluentTelemetryConfig.CONFIG_EVENTS_ENABLE_CONFIG;
import static io.confluent.telemetry.provider.Utils.configEvent;

public class TelemetryReporter implements MetricsReporter, ClusterResourceListener {

  private static final Logger log = LoggerFactory.getLogger(TelemetryReporter.class);

  /**
   * Note that rawOriginalConfig may be different than originalConfig.originals()
   * if we're on the broker (since we inject local exporter configs before creating
   * the ConfluentTelemetryConfig object).
   */
  private Map<String, Object> rawOriginalConfig;
  private ConfluentTelemetryConfig originalConfig;
  private ConfluentTelemetryConfig config;
  private volatile Context ctx;

  private MetricsCollectorTask collectorTask;
  private final Map<String, Exporter> exporters = new ConcurrentHashMap<>();
  private final Map<String, MetricsCollector> exporterCollectors = new ConcurrentHashMap<>();
  private final List<MetricsCollector> collectors = new CopyOnWriteArrayList<>();
  private volatile Predicate<MetricKey> unionPredicate;
  private KafkaMetricsCollector.StateLedger kafkaMetricsStateLedger = new KafkaMetricsCollector.StateLedger();

  private Provider activeProvider;

  private EventLogger<ConfigEvent> configEventLogger = new EventLogger<ConfigEvent>();

  /**
   * Note: we are assuming that these methods are invoked in the following order:
   *  1. configure() [must be called first & only once]
   *  2. contextChange() [must be called second]
   *  3. contextChange() / reconfigurableConfigs() / reconfigure() [each may be called multiple times]
   */
  @SuppressWarnings("unchecked")
  @Override
  public synchronized void configure(Map<String, ?> configs) {
    this.rawOriginalConfig = (Map<String, Object>) configs;
    this.kafkaMetricsStateLedger.configure(configs);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    createConfiguration(configs, false);
  }

  /* Implementing Reconfigurable interface to make this reporter dynamically reconfigurable. */
  @Override
  public synchronized void reconfigure(Map<String, ?> configs) {
    if (this.config == null) {
      throw new IllegalStateException("contextChange() was not called before reconfigure()");
    }

    ConfluentTelemetryConfig newConfig = createConfiguration(configs, true);
    ConfluentTelemetryConfig oldConfig = this.config;
    this.config = newConfig;

    ExporterConfig httpConfig = newConfig.allExporters().get(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME);
    configEventLogger.reconfigure(httpConfig.originals());

    if (this.activeProvider != null && this.config.getBoolean(CONFIG_EVENTS_ENABLE_CONFIG)) {
      this.configEventLogger.log(configEvent(config.originals(),
              activeProvider.configInclude(),
              activeProvider.resource(),
              activeProvider.domain() + "/config/dynamic"));
    }

    this.unionPredicate = createUnionPredicate(this.config);
    reconfigureCollectors();
    reconfigureExporters(oldConfig, newConfig);
  }

  private ConfluentTelemetryConfig createConfiguration(Map<String, ?> configs, boolean doLog) {
    configs = ConfluentTelemetryConfig.reconcileConfigs(configs);

    // the original config may contain local exporter overrides so add those first
    Map<String, Object> validateConfig = Maps.newHashMap(this.originalConfig.originals());

    // put all filtered configs (avoid applying configs that are not dynamic)
    // TODO: remove once this is fixed https://confluentinc.atlassian.net/browse/CPKAFKA-4828
    validateConfig.putAll(onlyReconfigurables(configs));

    // validation should be handled by ConfigDef Validators
    return new ConfluentTelemetryConfig(validateConfig, doLog);
  }

  private void initExporters() {
    initExporters(
      this.config.enabledExporters()
    );
  }

  private void initExporters(
        Map<String, ExporterConfig> toInit
  ) {
    for (Map.Entry<String, ExporterConfig> entry : toInit.entrySet()) {
      log.info("Creating {} exporter named '{}'", entry.getValue().getType().name(), entry.getKey());
      ExporterConfig exporterConfig = entry.getValue();
      Exporter newExporter = null;

      if (exporterConfig instanceof KafkaExporterConfig) {
        newExporter = KafkaExporter.newBuilder((KafkaExporterConfig) exporterConfig).build();
      } else if (exporterConfig instanceof HttpExporterConfig) {
        newExporter = new HttpExporter((HttpExporterConfig) exporterConfig);
      }

      // init exporter collectors
      if (newExporter instanceof MetricsCollectorProvider) {
        MetricsCollector collector = ((MetricsCollectorProvider) newExporter).collector(this.unionPredicate, this.ctx);
        collectors.add(collector);
        exporterCollectors.put(entry.getKey(), collector);
      }

      this.exporters.put(entry.getKey(), newExporter);
    }
  }

  private void updateExporters(
          Map<String, ExporterConfig> toReconfigure
  ) {
    // reconfigure exporters
    for (Map.Entry<String, ExporterConfig> entry : toReconfigure.entrySet()) {
      Exporter exporter = this.exporters.get(entry.getKey());
      ExporterConfig exporterConfig = entry.getValue();
      if (exporter instanceof HttpExporter) {
        ((HttpExporter) exporter).reconfigure((HttpExporterConfig) exporterConfig);
      } else if (exporter instanceof KafkaExporter) {
        ((KafkaExporter) exporter).reconfigure((KafkaExporterConfig) exporterConfig);
      }
    }
  }

  private void closeExporters(
          Map<String, ExporterConfig> toClose
  ) {
    // shutdown exporters
    for (Map.Entry<String, ExporterConfig> entry : toClose.entrySet()) {
      log.info("Closing {} exporter named '{}'", entry.getValue().getType().name(), entry.getKey());
      Exporter exporter = this.exporters.remove(entry.getKey());

      // TODO: we should find a better way to expose metrics from exporters
      // remove exporter associated collector(s)
      if (exporter instanceof MetricsCollectorProvider) {
        this.collectors.remove(
                this.exporterCollectors.remove(entry.getKey())
        );
      }

      try {
        exporter.close();
      } catch (Exception e) {
        log.warn("exception closing {} exporter named '{}'",
                entry.getValue().getType(), entry.getKey(), e
        );
      }
    }
  }

  private void reconfigureExporters(ConfluentTelemetryConfig oldConfig, ConfluentTelemetryConfig newConfig) {
    Set<String> oldEnabled = oldConfig.enabledExporters().keySet();
    Set<String> newEnabled = newConfig.enabledExporters().keySet();
    closeExporters(
      newConfig.allExportersWithNames(
        Sets.difference(oldEnabled, newEnabled)
      )
    );
    updateExporters(
      newConfig.allExportersWithNames(
        Sets.intersection(oldEnabled, newEnabled)
      )
    );
    initExporters(
      newConfig.allExportersWithNames(
        Sets.difference(newEnabled, oldEnabled)
      )
    );
  }

  private void reconfigureCollectors() {
    Stream.concat(collectors.stream(), Stream.of(this.collectorTask))
        .forEach(collector -> collector.reconfigurePredicate(this.unionPredicate));
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    if (this.config == null) {
      throw new IllegalStateException("contextChange() was not called before reconfigurableConfigs()");
    }
    Set<String> reconfigurables = new HashSet<String>(ConfluentTelemetryConfig.RECONFIGURABLES);

    // handle generic exporter configs
    for (String name : this.config.allExporters().keySet()) {
      reconfigurables.addAll(
        ExporterConfig.RECONFIGURABLES.stream()
          .map(c -> ConfluentTelemetryConfig.exporterPrefixForName(name) + c)
          .collect(Collectors.toSet())
      );
    }

    // HttpExporterConfig related reconfigurable configs.
    for (String name : this.config.allHttpExporters().keySet()) {
      reconfigurables.addAll(
        HttpExporterConfig.RECONFIGURABLE_CONFIGS.stream()
          .map(c -> ConfluentTelemetryConfig.exporterPrefixForName(name) + c)
          .collect(Collectors.toSet())
        );
    }

    return reconfigurables;
  }

  @Override
  public void contextChange(MetricsContext metricsContext) {
    /**
     * Select the provider on {@link MetricsReporter#contextChange(MetricsContext)}.
     *
     * 1. Lookup the provider from the {@link ProviderRegistry} using _namespace tag in the
     * MetricsContext metadata.
     * 2. If a provider is found, validated all required labels are available
     * 3. If validation succeeds: initialize the provider, start the metric collection task, set metrics labels for services/libraries that expose metrics
     */
    if (this.rawOriginalConfig == null) {
      throw new IllegalStateException("configure() was not called before contextChange()");
    }

    log.debug("metricsContext {}", metricsContext.contextLabels());
    if (!metricsContext.contextLabels().containsKey(MetricsContext.NAMESPACE)) {
      log.error("_namespace not found in metrics context. Metrics collection is disabled");
      return;
    }

    this.activeProvider = ProviderRegistry.getProvider(metricsContext.contextLabels().get(MetricsContext.NAMESPACE));

    if (this.activeProvider == null) {
      log.error("No provider was detected for context {}. Available providers {}.",
          metricsContext.contextLabels(),
          ProviderRegistry.providers.keySet());
      return;
    }

    log.debug("provider {} is selected.", this.activeProvider.getClass().getCanonicalName());

    if (!this.activeProvider.validate(metricsContext, this.rawOriginalConfig)) {
      log.info("Validation failed for {} context {} config {}", this.activeProvider.getClass(), metricsContext.contextLabels(), this.rawOriginalConfig);
     return;
    }

    if (this.collectorTask == null) {
      // Initialize the provider only once. contextChange(..) can be called more than once,
      //but once it's been initialized and all necessary labels are present then we don't re-initialize again.
      this.activeProvider.configure(this.rawOriginalConfig);
    }

    this.activeProvider.contextChange(metricsContext);

    if (this.collectorTask == null) {

      // we need to wait for contextChange call to initialize configs (due to 'isBroker' check)
      initConfig();

      startMetricCollectorTask();
    }

    if (config.getBoolean(CONFIG_EVENTS_ENABLE_CONFIG)) {
      this.configEventLogger.log(configEvent(config.originals(), activeProvider.configInclude(), activeProvider.resource(), activeProvider.domain() + "/config/static"));
    }
  }

  private void initConfig() {
    this.originalConfig = new ConfluentTelemetryConfig(
        maybeInjectLocalExporter(this.activeProvider, this.rawOriginalConfig));
    maybeConfigureEventLogger(originalConfig);
    this.config = originalConfig;
    this.unionPredicate = createUnionPredicate(this.config);
  }

  private void maybeConfigureEventLogger(ConfluentTelemetryConfig configs) {
    // Inherit http exporter configs from the _confluent http exporter.
    ExporterConfig defaultHttpExporterConfig = configs.allExporters().get(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME);
    if (defaultHttpExporterConfig == null) {
      log.error("_confluent exporter config is empty.");
    }

    Map<String, Object> eventConfig = defaultHttpExporterConfig.originals();
    // Add required configs for event logger.
    eventConfig.put(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG, EventHttpExporter.class.getCanonicalName());
    this.configEventLogger.configure(eventConfig);
  }

  private void startMetricCollectorTask() {
    ctx = new Context(this.activeProvider.resource(),
      this.activeProvider.domain(),
      config.getBoolean(ConfluentTelemetryConfig.DEBUG_ENABLED),
      true);

    initExporters();
    initCollectors();

    this.collectorTask = new MetricsCollectorTask(
      ctx,
      () -> this.exporters.values(),
      collectors,
      config.getLong(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG),
      this.unionPredicate
    );

    this.collectorTask.start();
  }

  private void initCollectors() {
    collectors.add(
      KafkaMetricsCollector.newBuilder()
        .setContext(ctx)
        .setMetricsPredicate(unionPredicate)
        .setLedger(kafkaMetricsStateLedger)
        .build()
    );

    collectors.addAll(this.activeProvider.extraCollectors(ctx, unionPredicate));
  }

  @VisibleForTesting
  Map<String, Exporter> getExporters() {
    return this.exporters;
  }

  @VisibleForTesting
  public List<MetricsCollector> getCollectors() {
    return collectors;
  }

  /**
   * Called when the metrics repository is closed.
   */
  @Override
  public void close() {
    log.info("Stopping TelemetryReporter collectorTask");
    if (collectorTask != null) {
        collectorTask.close();
    }

    if (exporters != null) {
        for (Exporter exporter : exporters.values()) {
            try {
                exporter.close();
            } catch (Exception e) {
                log.error("Error while closing {}", exporter, e);
            }
        }
    }

    this.kafkaMetricsStateLedger.close();

  }

  @Override
  public synchronized void onUpdate(ClusterResource clusterResource) {
    // NOOP. The cluster id is part of metrics context.
  }

  @Override
  public void init(List<KafkaMetric> metrics) {
    this.kafkaMetricsStateLedger.init(metrics);
  }

  /**
   * This is called whenever a metric is updated or added
   */
  @Override
  public void metricChange(KafkaMetric metric) {
    this.kafkaMetricsStateLedger.metricChange(metric);
  }

  /**
   * This is called whenever a metric is removed
   */
  @Override
  public void metricRemoval(KafkaMetric metric) {
    this.kafkaMetricsStateLedger.metricRemoval(metric);
  }

  private Map<String, ?> onlyReconfigurables(Map<String, ?> originals) {
    return reconfigurableConfigs().stream()
      .filter(c -> originals.containsKey(c))
      .collect(Collectors.toMap(c -> c, c -> originals.get(c)));
  }

  private static Predicate<MetricKey> createUnionPredicate(ConfluentTelemetryConfig config) {
    List<Predicate<MetricKey>> enabledPredicates =
        config.enabledExporters()
            .values().stream()
            .map(ExporterConfig::buildMetricsPredicate)
            .collect(Collectors.toList());

    // combine all the predicates with ORs
    return
        enabledPredicates
            .stream()
            .reduce(Predicate::or)
            // if there are no exporters, then never match metrics
            .orElse(metricKey -> false);
  }

  private static Map<String, Object> prefixedExporterConfigs(String prefix, Map<String, Object> configs) {
    String exporterPrefix = ConfluentTelemetryConfig.exporterPrefixForName(prefix);
    return configs.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toMap(
            e -> exporterPrefix + e.getKey(),
            e -> e.getValue()));
  }

  private static Map<String, Object> maybeInjectLocalExporter(Provider provider, Map<String, Object> originals) {
    Map<String, Object> configs = new HashMap<>();
    // this check is how we determine if we're inside the broker
    if (provider instanceof KafkaServerProvider) {
      // first add the local exporter default values
      configs.putAll(
          prefixedExporterConfigs(
              ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME,
              ConfluentTelemetryConfig.EXPORTER_LOCAL_DEFAULTS)
      );

      // then apply derived broker config
      configs.putAll(
          prefixedExporterConfigs(
              ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME,
              BrokerConfigUtils.deriveLocalProducerConfigs(originals))
      );

      // use 'confluent.balancer.topic.replication.factor' if set, otherwise use our default
      final String balanceReplicationFactor = BrokerConfigUtils.getBalanceReplicationFactor(originals);
      if (balanceReplicationFactor != null) {
        configs.putAll(
            prefixedExporterConfigs(
                ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME,
                ImmutableMap.of(
                    KafkaExporterConfig.TOPIC_REPLICAS_CONFIG,
                    balanceReplicationFactor
                )
            )
        );
      }
    }

    // finally apply the originals
    configs.putAll(originals);

    return configs;
  }
}
