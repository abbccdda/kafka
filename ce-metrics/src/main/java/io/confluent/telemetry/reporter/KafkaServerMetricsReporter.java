package io.confluent.telemetry.reporter;

import static io.confluent.observability.telemetry.TelemetryResourceType.KAFKA;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.yammer.metrics.Metrics;
import io.confluent.observability.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricsCollectorTask;
import io.confluent.telemetry.collector.CPUMetricsCollector;
import io.confluent.telemetry.collector.KafkaMetricsCollector;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.MetricsCollectorProvider;
import io.confluent.telemetry.collector.VolumeMetricsCollector;
import io.confluent.telemetry.collector.YammerMetricsCollector;
import io.confluent.telemetry.exporter.Exporter;
import io.confluent.telemetry.exporter.file.FileExporter;
import io.confluent.telemetry.exporter.http.HttpExporter;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.opencensus.proto.resource.v1.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
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

    private ConfluentTelemetryConfig config;
    private MetricsCollectorTask collectorTask;
    private KafkaMetricsCollector.StateLedger kafkaMetricsStateLedger = new KafkaMetricsCollector.StateLedger();
    private Set<Exporter> exporters;

    private List<MetricsCollector> collectors;

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        if (this.collectorTask != null) {
            log.warn("onUpdate called multiple times for {}", KafkaServerMetricsReporter.class);
            // Exit early so we don't start a second collector task
            return;
        }

        Resource resource = new ResourceBuilderFacade(KAFKA)
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
            ))
            .build();

        Context ctx = new Context(resource, config.getBoolean(ConfluentTelemetryConfig.DEBUG_ENABLED), true);


        this.collectors = this.initCollectors(ctx);

        this.collectorTask = new MetricsCollectorTask(
            ctx,
            exporters,
            collectors,
            config.getLong(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG));

        this.collectorTask.start();
    }

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new ConfluentTelemetryConfig(configs);
        this.kafkaMetricsStateLedger.configure(configs);

        this.exporters = initExporters();
    }

    private List<MetricsCollector> initCollectors(Context ctx) {
        ImmutableList.Builder<MetricsCollector> collectors = ImmutableList.builder();
        collectors.add(
            KafkaMetricsCollector.newBuilder(config)
                .setContext(ctx)
                .setDomain(DOMAIN)
                .setLedger(kafkaMetricsStateLedger)
                .build()
        );

        collectors.add(
            CPUMetricsCollector.newBuilder(config)
                .setDomain(DOMAIN)
                .setContext(ctx)
                .build()
        );

        collectors.add(
            VolumeMetricsCollector.newBuilder(config)
                .setContext(ctx)
                .setDomain(DOMAIN)
                .build()
        );

        collectors.add(
            YammerMetricsCollector.newBuilder(config)
                .setContext(ctx)
                .setDomain(DOMAIN)
                .setMetricsRegistry(Metrics.defaultRegistry())
                .build()
        );

        for (Exporter exporter : this.exporters) {
            if (exporter instanceof MetricsCollectorProvider) {
                collectors.add(((MetricsCollectorProvider) exporter).collector(config, ctx, DOMAIN));
            }
        }

        return collectors.build();
    }

    private Set<Exporter> initExporters() {
        Builder<Exporter> builder = ImmutableSet.builder();
        config.createKafkaExporterConfig().ifPresent(cfg -> {
            builder.add(KafkaExporter.newBuilder(cfg).build());
        });
        config.createFileExporterConfig().ifPresent(cfg -> {
            builder.add(FileExporter.newBuilder(cfg).build());
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

}
