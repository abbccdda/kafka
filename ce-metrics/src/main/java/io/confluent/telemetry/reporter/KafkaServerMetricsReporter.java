package io.confluent.telemetry.reporter;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.Metrics;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricsCollectorTask;
import io.confluent.telemetry.collector.CPUMetricsCollector;
import io.confluent.telemetry.collector.KafkaMetricsCollector;
import io.confluent.telemetry.collector.VolumeMetricsCollector;
import io.confluent.telemetry.collector.YammerMetricsCollector;
import io.confluent.telemetry.exporter.Exporter;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaServerMetricsReporter implements MetricsReporter, ClusterResourceListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaServerMetricsReporter.class);

    private static final String DOMAIN = "io.confluent.kafka.server";
    public static final String CLUSTER_ID_LABEL = "cluster_id";

    // Server is generic and is applicable for all CP services. KafkaServerMetricsReporter id is ambiguous as some replica fetcher metrics
    // have a broker_id tag with the target replica as value.
    public static final String SERVER_ID_LABEL = "server_id";

    private volatile String clusterId;
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


        this.clusterId = clusterResource.clusterId();

        ConfluentTelemetryConfig cfg = new ConfluentTelemetryConfig(this.configs);

        Map<String, String> labels = cfg.getLabels();
        labels.put(CLUSTER_ID_LABEL, this.clusterId);
        labels.put(SERVER_ID_LABEL, cfg.getBrokerId());

        Context ctx = new Context(ImmutableMap.copyOf(labels), cfg.getBoolean(ConfluentTelemetryConfig.DEBUG_ENABLED));

        // Set context with labels
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
