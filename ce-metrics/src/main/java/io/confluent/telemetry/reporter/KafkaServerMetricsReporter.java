package io.confluent.telemetry.reporter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsCollectorTask;
import io.confluent.telemetry.collector.CPUMetricsCollector;
import io.confluent.telemetry.collector.KafkaMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.MetricsCollectorProvider;
import io.confluent.telemetry.collector.VolumeMetricsCollector;
import io.confluent.telemetry.collector.YammerMetricsCollector;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.Exporter;
import io.confluent.telemetry.exporter.http.HttpExporter;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;

import io.opencensus.proto.resource.v1.Resource;

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

import kafka.metrics.KafkaYammerMetrics;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.AppInfoParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaServerMetricsReporter implements MetricsReporter, ClusterResourceListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaServerMetricsReporter.class);

    private static final String DOMAIN = "io.confluent.kafka.server";

    @VisibleForTesting
    public static final String LABEL_CLUSTER_ID = "cluster.id";

    @VisibleForTesting
    public static final String LABEL_BROKER_ID = "broker.id";

    @VisibleForTesting
    public static final String LABEL_BROKER_RACK = "broker.rack";

    @VisibleForTesting
    public static final String RESOURCE_TYPE_KAFKA = "kafka";

    private ConfluentTelemetryConfig originalConfig;
    private ConfluentTelemetryConfig config;
    private volatile Context ctx;

    private MetricsCollectorTask collectorTask;
    private KafkaMetricsCollector.StateLedger kafkaMetricsStateLedger = new KafkaMetricsCollector.StateLedger();
    private final Map<String, Exporter> exporters = new ConcurrentHashMap<>();
    private final Map<String, MetricsCollector> exporterCollectors = new ConcurrentHashMap<>();
    private final List<MetricsCollector> collectors = new CopyOnWriteArrayList<>();
    private volatile Predicate<MetricKey> whitelistPredicate;
    private boolean started = false;

    @Override
    public synchronized void onUpdate(ClusterResource clusterResource) {
        if (started) {
            return;
        }

        // prevent this reporter from starting up on clients
        //   note: this is not a completely fail-safe check, it is still possible
        //   for a degenerate client configs to contain broker id configs
        if (config.getBrokerId() == null || clusterResource.clusterId() == null) {
            log.warn("{} only supports Kafka brokers, metrics collection will not be started", KafkaServerMetricsReporter.class);

            // set started to prevent warning messages on every client metadata update
            started = true;
            return;
        }

        ResourceBuilderFacade resourceBuilderFacade = new ResourceBuilderFacade(RESOURCE_TYPE_KAFKA)
            .withVersion(AppInfoParser.getVersion())
            .withId(clusterResource.clusterId())
            .withNamespacedLabel(LABEL_CLUSTER_ID, clusterResource.clusterId())
            .withNamespacedLabel(LABEL_BROKER_ID, config.getBrokerId())
            .withLabels(config.getLabels());

        // Do not add kafka.broker.rack if data is unavailable.
        config.getBrokerRack().ifPresent(value -> resourceBuilderFacade.withNamespacedLabel(LABEL_BROKER_RACK, value));

        Resource resource = resourceBuilderFacade.build();
        this.ctx = new Context(resource, DOMAIN, config.getBoolean(ConfluentTelemetryConfig.DEBUG_ENABLED), true);

        initExporters();
        initCollectors();

        this.collectorTask = new MetricsCollectorTask(
            ctx,
            () -> this.exporters.values(),
            collectors,
            config.getLong(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG),
            whitelistPredicate);

        this.collectorTask.start();
        started = true;
    }

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    public synchronized void configure(Map<String, ?> configs) {
        this.originalConfig = new ConfluentTelemetryConfig(configs);
        this.config = originalConfig;
        this.kafkaMetricsStateLedger.configure(configs);
        this.whitelistPredicate = this.config.buildMetricWhitelistFilter();
    }

    /* Implementing Reconfigurable interface to make this reporter dynamically reconfigurable. */
    @Override
    public synchronized void reconfigure(Map<String, ?> configs) {

        // start with original configs from properties file
        Map<String, Object> newOriginals = new HashMap<>(this.originalConfig.originals());

        // put all filtered configs (avoid applying configs that are not dynamic)
        // TODO: remove once this is fixed https://confluentinc.atlassian.net/browse/CPKAFKA-4828
        newOriginals.putAll(onlyReconfigurables(configs));

        ConfluentTelemetryConfig newConfig = new ConfluentTelemetryConfig(newOriginals);
        ConfluentTelemetryConfig oldConfig = this.config;
        this.config = newConfig;

        reconfigureWhitelist(newConfig);
        reconfigureExporters(oldConfig, newConfig);
    }

    private void reconfigureWhitelist(ConfluentTelemetryConfig newConfig) {
        this.whitelistPredicate = newConfig.buildMetricWhitelistFilter();
        Stream.concat(collectors.stream(), Stream.of(this.collectorTask))
            .forEach(collector -> collector.reconfigureWhitelist(this.whitelistPredicate));
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
            ExporterConfig exporterConfig = entry.getValue();
            Exporter newExporter = null;

            if (exporterConfig instanceof KafkaExporterConfig) {
                newExporter = KafkaExporter.newBuilder((KafkaExporterConfig) exporterConfig).build();
            } else if (exporterConfig instanceof HttpExporterConfig) {
                newExporter = new HttpExporter((HttpExporterConfig) exporterConfig);
            }

            // init exporter collectors
            if (newExporter instanceof MetricsCollectorProvider) {
                MetricsCollector collector = ((MetricsCollectorProvider) newExporter).collector(this.whitelistPredicate, this.ctx);
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
            }
        }
    }

    private void closeExporters(
        Map<String, ExporterConfig> toClose
    ) {
        // shutdown exporters
        for (Map.Entry<String, ExporterConfig> entry : toClose.entrySet()) {
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

    @Override
    public Set<String> reconfigurableConfigs() {
        if (this.config == null) {
            throw new IllegalStateException("configure() was not called before reconfigurableConfigs()");
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
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        ConfluentTelemetryConfig.validateReconfiguration(configs);
    }

    private void initCollectors() {
        collectors.add(
            KafkaMetricsCollector.newBuilder()
                .setContext(ctx)
                .setLedger(kafkaMetricsStateLedger)
                .setMetricWhitelistFilter(whitelistPredicate)
                .build()
        );

        collectors.add(
            CPUMetricsCollector.newBuilder()
                .setContext(ctx)
                .setMetricWhitelistFilter(whitelistPredicate)
                .build()
        );

        collectors.add(
            VolumeMetricsCollector.newBuilder(config)
                .setContext(ctx)
                .setMetricWhitelistFilter(whitelistPredicate)
                .build()
        );

        collectors.add(
            YammerMetricsCollector.newBuilder()
                .setContext(ctx)
                .setMetricsRegistry(KafkaYammerMetrics.defaultRegistry())
                .setMetricWhitelistFilter(whitelistPredicate)
                .build()
        );
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
        log.info("Stopping KafkaServerMetricsReporter collectorTask");
        this.kafkaMetricsStateLedger.close();
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
    }

    /**
     * This is called when the collectorTask is first registered to initially register all existing
     * metrics
     *
     * @param metrics All currently existing metrics
     */
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
}
