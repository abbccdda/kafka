package io.confluent.telemetry.reporter;

import com.google.common.annotations.VisibleForTesting;
import com.yammer.metrics.Metrics;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricsCollectorTask;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.TelemetryResourceType;
import io.confluent.telemetry.collector.CPUMetricsCollector;
import io.confluent.telemetry.collector.KafkaMetricsCollector;
import io.confluent.telemetry.collector.VolumeMetricsCollector;
import io.confluent.telemetry.collector.YammerMetricsCollector;
import io.confluent.telemetry.exporter.Exporter;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.opencensus.proto.resource.v1.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    /**
     * Included for compatibility with existing tags.
     * <code>kafka_id</code> is the canonical resource identifier
     * (following the <code>${resource-type}_id</code> format)
     */
    @VisibleForTesting
    public static final String LABEL_CLUSTER_ID = "cluster_id";

    @VisibleForTesting
    public static final String LABEL_BROKER_ID = "broker_id";

    private Map<String, ?> configs;
    private MetricsCollectorTask collectorTask;
    private Exporter exporter;
    private KafkaMetricsCollector.StateLedger kafkaMetricsStateLedger = new KafkaMetricsCollector.StateLedger();

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        if (this.collectorTask != null) {
            log.warn("onUpdate called multiple times for {}", KafkaServerMetricsReporter.class);
            // Exit early so we don't start a second collector task
            return;
        }

        ConfluentTelemetryConfig cfg = new ConfluentTelemetryConfig(this.configs);

        Resource resource = new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
            .withVersion(AppInfoParser.getVersion())
            .withId(clusterResource.clusterId())
            .withLabel(LABEL_CLUSTER_ID, clusterResource.clusterId())
            .withLabel(LABEL_BROKER_ID, cfg.getBrokerId())
            .withLabels(cfg.getLabels())
            .build();

        Context ctx = new Context(resource, cfg.getBoolean(ConfluentTelemetryConfig.DEBUG_ENABLED), true);

        KafkaMetricsCollector kafkaMetricsCollector =
            KafkaMetricsCollector.newBuilder(cfg)
                .setContext(ctx)
                .setDomain(DOMAIN)
                .setLedger(kafkaMetricsStateLedger)
                .build();

        CPUMetricsCollector cpuMetrics = CPUMetricsCollector.newBuilder(cfg)
                .setDomain(DOMAIN)
                .setContext(ctx)
                .build();

        VolumeMetricsCollector volumeMetrics = VolumeMetricsCollector.newBuilder(cfg)
                .setContext(ctx)
                .setDomain(DOMAIN)
                .build();

        YammerMetricsCollector yammerMetrics = YammerMetricsCollector.newBuilder(cfg)
                .setContext(ctx)
                .setDomain(DOMAIN)
                .setMetricsRegistry(Metrics.defaultRegistry())
                .build();

        this.exporter = KafkaExporter.newBuilder(cfg)
                .build();

        this.collectorTask = new MetricsCollectorTask(
            ctx,
            this.exporter,
            Arrays.asList(kafkaMetricsCollector, cpuMetrics, volumeMetrics, yammerMetrics),
            cfg.getLong(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG));

        this.collectorTask.start();
    }

    /**
     * Configure this class with the given key-value pairs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
        this.kafkaMetricsStateLedger.configure(configs);
    }

    /**
     * Called when the metrics repository is closed.
     */
    @Override
    public void close() {
        log.info("Stopping KafkaServerMetricsReporter collectorTask");
        this.kafkaMetricsStateLedger.close();
        collectorTask.close();
        try {
            exporter.close();
        } catch (Exception e) {
            log.error("Error while closing {}", exporter, e);
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
