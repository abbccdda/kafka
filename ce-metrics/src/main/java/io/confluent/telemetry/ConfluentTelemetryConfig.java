// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.telemetry;

import com.google.common.base.Joiner;
import io.confluent.monitoring.common.TimeBucket;
import io.confluent.telemetry.collector.VolumeMetricsCollector.VolumeMetricsCollectorConfig;
import io.confluent.telemetry.exporter.file.FileExporterConfig;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfluentTelemetryConfig extends AbstractConfig {

    public static final String PREFIX = "confluent.telemetry.";
    public static final String PREFIX_LABELS = PREFIX + "labels.";
    public static final String PREFIX_EXPORTER = PREFIX + "exporter.";
    public static final String PREFIX_METRICS_COLLECTOR = PREFIX + "metrics.collector.";

    public static final String COLLECT_INTERVAL_CONFIG = PREFIX_METRICS_COLLECTOR + "interval.ms";
    public static final Long DEFAULT_COLLECT_INTERVAL = TimeBucket.SIZE;
    public static final String COLLECT_INTERVAL_DOC = "The metrics reporter will collect new metrics "
            + "from the system in intervals defined by this setting. This means that control "
            + "center system health data lags by this duration, or that rebalancer may compute a plan "
            + "based on broker data that is stale by this duration. The default is a reasonable value "
            + "for production environments and it typically does not need to be changed.";

    public static final String WHITELIST_CONFIG = PREFIX_METRICS_COLLECTOR + "whitelist";
    public static final String WHITELIST_DOC =
        "Regex matching the converted (snake_case) metric name to be published to the "
        + "metrics topic.\n\nBy default this includes all the metrics required by Confluent "
        + "Control Center and Confluent Auto Data Balancer. This should typically never be "
        + "modified unless requested by Confluent.";
    public static final String DEFAULT_WHITELIST;

    public static final List<String> DEFAULT_BROKER_MONITORING_METRICS = Collections.unmodifiableList(
            Arrays.asList(
                "active_controller_count",
                "bytes_in_per_sec",
                "bytes_out_per_sec",
                "cpu_usage",
                "disk_total_bytes",
                "disk_usable_bytes",
                "failed_fetch_requests_per_sec",
                "failed_produce_requests_per_sec",
                "in_sync_replicas_count",
                "leader_count",
                "leader_election_rate_and_time_ms",
                "local_time_ms",
                "log_end_offset",
                "log_start_offset",
                "network_processor_avg_idle_percent",
                "max_lag",
                "num_log_segments",
                "offline_partitions_count",
                "partition_count",
                "remote_time_ms",
                "replicas_count",
                "request_handler_avg_idle_percent",
                "request_queue_size",
                "request_queue_time_ms",
                "requests_per_sec",
                "response_queue_size",
                "response_queue_time_ms",
                "response_send_time_ms",
                "size",
                "total_fetch_requests_per_sec",
                "total_produce_requests_per_sec",
                "total_time_ms",
                "unclean_leader_elections_per_sec",
                "under_replicated",
                "under_replicated_partitions",
                "zookeeper_disconnects_per_sec",
                "zookeeper_expires_per_sec"
            )
    );

    static {
        StringBuilder builder = new StringBuilder(".*");
        Joiner.on(".*|.*").appendTo(builder, DEFAULT_BROKER_MONITORING_METRICS);
        builder.append(".*");
        DEFAULT_WHITELIST = builder.toString();
    }

    public static final String DEBUG_ENABLED = PREFIX + "debug.enabled";
    public static final String DEBUG_ENABLED_DOC = "Enable debug metadata for metrics collection";
    public static final boolean DEFAULT_DEBUG_ENABLED = false;

    public static final String EXPORTER_KAFKA_ENABLED_CONFIG = KafkaExporterConfig.PREFIX + "enabled";
    public static final String EXPORTER_KAFKA_ENABLED_DOC = "True if the KafkaExporter is enabled.";
    public static final boolean EXPORTER_KAFKA_ENABLED_DEFAULT = true;

    public static final String EXPORTER_FILE_ENABLED_CONFIG = FileExporterConfig.PREFIX + "enabled";
    public static final String EXPORTER_FILE_ENABLED_DOC = "True if the FileExporter is enabled.";
    public static final boolean EXPORTER_FILE_ENABLED_DEFAULT = false;


    public static final String EXPORTER_HTTP_ENABLED_CONFIG = HttpExporterConfig.PREFIX + "enabled";
    public static final String EXPORTER_HTTP_ENABLED_DOC = "True if the HttpExporter is enabled.";
    public static final boolean EXPORTER_HTTP_ENABLED_DEFAULT = false;

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(
                COLLECT_INTERVAL_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_COLLECT_INTERVAL,
                ConfigDef.Importance.LOW,
                COLLECT_INTERVAL_DOC
        ).define(
                WHITELIST_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_WHITELIST,
                ConfigDef.Importance.LOW,
                WHITELIST_DOC
        ).define(
                DEBUG_ENABLED,
                ConfigDef.Type.BOOLEAN,
                DEFAULT_DEBUG_ENABLED,
                ConfigDef.Importance.LOW,
                DEBUG_ENABLED_DOC
        ).define(
                EXPORTER_KAFKA_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                EXPORTER_KAFKA_ENABLED_DEFAULT,
                ConfigDef.Importance.LOW,
                EXPORTER_KAFKA_ENABLED_DOC
        ).define(
                EXPORTER_FILE_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                EXPORTER_FILE_ENABLED_DEFAULT,
                ConfigDef.Importance.LOW,
                EXPORTER_FILE_ENABLED_DOC
        ).define(
                EXPORTER_HTTP_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                EXPORTER_HTTP_ENABLED_DEFAULT,
                ConfigDef.Importance.LOW,
                EXPORTER_HTTP_ENABLED_DOC
        );

    public static final Predicate<MetricKey> ALWAYS_TRUE = metricKey -> true;

    public static final String LEGACY_PREFIX = "confluent.telemetry.metrics.reporter.";

    private static final ConfigPropertyTranslater DEPRECATION_TRANSLATER =
        new ConfigPropertyTranslater.Builder()
            .withPrefixTranslation(LEGACY_PREFIX + "labels.", PREFIX_LABELS)
            .withTranslation(LEGACY_PREFIX + "whitelist", WHITELIST_CONFIG)
            .withTranslation(LEGACY_PREFIX + "publish.ms", COLLECT_INTERVAL_CONFIG)
            .build();

    private final VolumeMetricsCollectorConfig volumeMetricsCollectorConfig;

    public ConfluentTelemetryConfig(Map<String, ?> originals) {
        super(CONFIG, DEPRECATION_TRANSLATER.translate(originals));
        this.volumeMetricsCollectorConfig = new VolumeMetricsCollectorConfig(originals);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toRst());
    }

    public Map<String, String> getLabels() {
        Map<String, String> labels = new HashMap<>();
        for (Map.Entry<String, ?> entry : super.originals().entrySet()) {
            if (entry.getKey().startsWith(PREFIX_LABELS)) {
                labels.put(entry.getKey().substring(PREFIX_LABELS.length()), (String) entry.getValue());
            }
        }
        return labels;
    }

    public String getBrokerId() {
        return (String) originals().get(KafkaConfig.BrokerIdProp());
    }

    /**
     * Get a predicate that filters metrics based on the whitelist configuration.
     */
    public Predicate<MetricKey> getMetricFilter() {
        // Configure the PatternPredicate.
        String regexString = getString(ConfluentTelemetryConfig.WHITELIST_CONFIG).trim();

        if (regexString.isEmpty()) {
            return ALWAYS_TRUE;
        }
        Predicate<String> patternPredicate = Pattern.compile(regexString).asPredicate();

        // TODO We eventually plan to also support configuration of blacklist via label values.
        // Presumably we need a Map<String, Map<String, String>> of:
        // metric name -> label key -> label value

        return metricNameAndLabels -> patternPredicate.test(metricNameAndLabels.getName());
    }

    /**
     * Create a {@link KafkaExporterConfig} from these properties if enabled
     */
    public Optional<KafkaExporterConfig> createKafkaExporterConfig() {
        return getBoolean(EXPORTER_KAFKA_ENABLED_CONFIG)
            ? Optional.of(new KafkaExporterConfig(this.originals()))
            : Optional.empty();
    }

    /**
     * Create a {@link FileExporterConfig} from these properties if enabled
     */
    public Optional<FileExporterConfig> createFileExporterConfig() {
        return getBoolean(EXPORTER_FILE_ENABLED_CONFIG)
            ? Optional.of(new FileExporterConfig(this.originals()))
            : Optional.empty();
    }

    /**
     * Create a {@link HttpExporterConfig} from these properties if enabled
     */
    public Optional<HttpExporterConfig> createHttpExporterConfig() {
        return getBoolean(EXPORTER_HTTP_ENABLED_CONFIG)
            ? Optional.of(new HttpExporterConfig(this.originals()))
            : Optional.empty();
    }

    public VolumeMetricsCollectorConfig getVolumeMetricsCollectorConfig() {
        return volumeMetricsCollectorConfig;
    }
}
