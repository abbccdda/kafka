package io.confluent.telemetry.reporter;

import static io.confluent.telemetry.TelemetryResourceType.KAFKA;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
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
import io.confluent.telemetry.exporter.Exporter;
import io.confluent.telemetry.exporter.http.HttpExporter;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.opencensus.proto.resource.v1.Resource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    private ConfluentTelemetryConfig originalConfig;
    private ConfluentTelemetryConfig config;

    private MetricsCollectorTask collectorTask;
    private KafkaMetricsCollector.StateLedger kafkaMetricsStateLedger = new KafkaMetricsCollector.StateLedger();
    private Set<Exporter> exporters;

    private List<MetricsCollector> collectors;
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

        ResourceBuilderFacade resourceBuilderFacade = new ResourceBuilderFacade(KAFKA)
            .withVersion(AppInfoParser.getVersion())
            .withId(clusterResource.clusterId())
            .withNamespacedLabel(LABEL_CLUSTER_ID, clusterResource.clusterId())
            .withNamespacedLabel(LABEL_BROKER_ID, config.getBrokerId())
            .withLabels(config.getLabels())

            // Included for backwards compatibility with existing tags.
            // Can be removed once https://confluentinc.atlassian.net/browse/METRICS-516 is completed
            .withLabelAliases(ImmutableMap.of(
                KAFKA.prefixLabel(LABEL_CLUSTER_ID), "cluster_id",
                KAFKA.prefixLabel(LABEL_BROKER_ID), "broker_id"
            ));

        // Do not add kafka.broker.rack if data is unavailable.
        config.getBrokerRack().ifPresent(value -> resourceBuilderFacade.withNamespacedLabel(LABEL_BROKER_RACK, value));

        Resource resource = resourceBuilderFacade.build();

        Predicate<MetricKey> whitelistPredicate = this.config.buildMetricWhitelistFilter();
        Context ctx = new Context(resource, config.getBoolean(ConfluentTelemetryConfig.DEBUG_ENABLED), true);


        this.collectors = this.initCollectors(ctx, whitelistPredicate);

        this.collectorTask = new MetricsCollectorTask(
            ctx,
            exporters,
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

        this.exporters = initExporters();
    }

    /* Implementing Reconfigurable interface to make this reporter dynamically reconfigurable. */
    @Override
    public synchronized void reconfigure(Map<String, ?> configs) {

        // start with original configs from properties file
        Map<String, Object> newConfig = new HashMap<>(this.originalConfig.originals());

        // put all filtered configs (avoid applying configs that are not dynamic)
        // TODO: remove once this is fixed https://confluentinc.atlassian.net/browse/CPKAFKA-4828
        newConfig.putAll(onlyReconfigurables(configs));

        this.config = new ConfluentTelemetryConfig(newConfig);

        Predicate<MetricKey> whitelistPredicate = this.config.buildMetricWhitelistFilter();
        for (MetricsCollector collector : collectors) {
            collector.reconfigureWhitelist(whitelistPredicate);
        }
        this.collectorTask.reconfigureWhitelist(whitelistPredicate);

        // HttpExporter related reconfigurations.
        for (Exporter exporter : this.exporters) {
            if (exporter instanceof HttpExporter) {
                ((HttpExporter) exporter).reconfigure(new HttpExporterConfig(configs));
            }
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        Set<String> reconfigurables = new HashSet<String>(ConfluentTelemetryConfig.RECONFIGURABLES);

        // HttpExporterConfig related reconfigurable configs.
        reconfigurables.addAll(HttpExporterConfig.RECONFIGURABLE_CONFIGS);

        return reconfigurables;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        ConfluentTelemetryConfig.validateReconfiguration(configs);
        // Validating HttpExporterConfig related reconfigurable configs.
        HttpExporterConfig.validateReconfiguration(configs);
    }

    private List<MetricsCollector> initCollectors(Context ctx, Predicate<MetricKey> whitelistPredicate) {
        ImmutableList.Builder<MetricsCollector> collectors = ImmutableList.builder();

        collectors.add(
            KafkaMetricsCollector.newBuilder()
                .setContext(ctx)
                .setDomain(DOMAIN)
                .setLedger(kafkaMetricsStateLedger)
                .setMetricWhitelistFilter(whitelistPredicate)
                .build()
        );

        collectors.add(
            CPUMetricsCollector.newBuilder()
                .setDomain(DOMAIN)
                .setContext(ctx)
                .setMetricWhitelistFilter(whitelistPredicate)
                .build()
        );

        collectors.add(
            VolumeMetricsCollector.newBuilder(config)
                .setContext(ctx)
                .setDomain(DOMAIN)
                .setMetricWhitelistFilter(whitelistPredicate)
                .build()
        );

        collectors.add(
            YammerMetricsCollector.newBuilder()
                .setContext(ctx)
                .setDomain(DOMAIN)
                .setMetricsRegistry(KafkaYammerMetrics.defaultRegistry())
                .setMetricWhitelistFilter(whitelistPredicate)
                .build()
        );

        for (Exporter exporter : this.exporters) {
            if (exporter instanceof MetricsCollectorProvider) {
                collectors.add(((MetricsCollectorProvider) exporter).collector(whitelistPredicate, ctx, DOMAIN));
            }
        }

        return collectors.build();
    }

    private Set<Exporter> initExporters() {
        Builder<Exporter> builder = ImmutableSet.builder();
        config.createKafkaExporterConfig().ifPresent(cfg -> {
            builder.add(KafkaExporter.newBuilder(cfg).build());
        });
        config.createHttpExporterConfig().ifPresent(cfg -> {
            builder.add(new HttpExporter(cfg));
        });
        return builder.build();
    }

    @VisibleForTesting
    Set<Exporter> getExporters() {
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
            for (Exporter exporter : exporters) {
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
