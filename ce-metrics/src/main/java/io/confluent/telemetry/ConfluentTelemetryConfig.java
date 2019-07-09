// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.telemetry;

import com.google.common.base.Joiner;
import io.confluent.monitoring.common.MonitoringProducerDefaults;
import io.confluent.monitoring.common.TimeBucket;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConfluentTelemetryConfig extends AbstractConfig {

    public static final String METRICS_REPORTER_PREFIX = "confluent.telemetry.metrics.reporter.";
    public static final String METRICS_REPORTER_TAGS_PREFIX = "confluent.telemetry.metrics.reporter.labels.";
    public static final String METRICS_REPORTER_TOPIC_PREFIX = "confluent.telemetry.metrics.reporter.topic.";
    public static final String BOOTSTRAP_SERVERS_CONFIG =
            METRICS_REPORTER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    public static final String BOOTSTRAP_SERVERS_DOC = "Bootstrap servers for the KafkaExporter cluster "
            + "metrics will be published to. The metrics cluster may be different from the cluster(s) "
            + "whose metrics are being collected. Several production KafkaExporter clusters can publish to a "
            + "single metrics cluster, for example.";
    // metrics topic default settings should be consistent with
    // control center default metrics topic settings
    // to ensure topics have the same settings regardless of which one starts first
    public static final String TOPIC_CONFIG = METRICS_REPORTER_PREFIX + "topic";
    public static final String DEFAULT_TOPIC_CONFIG = "_confluent-telemetry-metrics";
    public static final String TOPIC_DOC = "Topic on which metrics data will be written.";
    public static final String TOPIC_CREATE_CONFIG = METRICS_REPORTER_PREFIX + "topic.create";
    public static final boolean DEFAULT_TOPIC_CREATE_CONFIG = true;
    public static final String TOPIC_CREATE_DOC = "Create the metrics topic if it does not exist.";
    public static final String TOPIC_PARTITIONS_CONFIG = METRICS_REPORTER_PREFIX + "topic.partitions";
    public static final int DEFAULT_TOPIC_PARTITIONS_CONFIG = 12;
    public static final String TOPIC_PARTITIONS_DOC = "Number of partitions in the metrics topic.";
    public static final String TOPIC_REPLICAS_CONFIG = METRICS_REPORTER_PREFIX + "topic.replicas";
    public static final int DEFAULT_TOPIC_REPLICAS_CONFIG = 3;
    public static final String TOPIC_REPLICAS_DOC =
            "Number of replicas in the metric topic. It must not be higher than the number "
                    + "of brokers in the KafkaExporter cluster.";
    public static final String TOPIC_RETENTION_MS_CONFIG =
            METRICS_REPORTER_PREFIX + "topic.retention.ms";
    public static final long DEFAULT_TOPIC_RETENTION_MS_CONFIG = TimeUnit.DAYS.toMillis(3);
    public static final String TOPIC_RETENTION_MS_DOC = "Retention time for the metrics topic.";
    public static final String TOPIC_RETENTION_BYTES_CONFIG =
            METRICS_REPORTER_PREFIX + "topic.retention.bytes";
    public static final long DEFAULT_TOPIC_RETENTION_BYTES_CONFIG = -1L;
    public static final String TOPIC_RETENTION_BYTES_DOC = "Retention bytes for the metrics topic.";
    public static final String TOPIC_ROLL_MS_CONFIG = METRICS_REPORTER_PREFIX + "topic.roll.ms";
    public static final long DEFAULT_TOPIC_ROLL_MS_CONFIG = TimeUnit.HOURS.toMillis(4);
    public static final String TOPIC_ROLL_MS_DOC = "Log rolling time for the metrics topic.";
    public static final String TOPIC_MAX_MESSAGE_BYTES_CONFIG =
            METRICS_REPORTER_PREFIX + "topic." + TopicConfig.MAX_MESSAGE_BYTES_CONFIG;
    public static final int DEFAULT_TOPIC_MAX_MESSAGE_BYTES_CONFIG =
            MonitoringProducerDefaults.MAX_REQUEST_SIZE;
    public static final String
            TOPIC_MAX_MESSAGE_BYTES_DOC = "Maximum message size for the metrics topic.";
    public static final String PUBLISH_PERIOD_CONFIG = METRICS_REPORTER_PREFIX + "publish.ms";
    public static final Long DEFAULT_PUBLISH_PERIOD = TimeBucket.SIZE;
    public static final String PUBLISH_PERIOD_DOC = "The metrics reporter will publish new metrics "
            + "to the metrics topic in intervals defined by this setting. This means that control "
            + "center system health data lags by this duration, or that rebalancer may compute a plan "
            + "based on broker data that is stale by this duration. The default is a reasonable value "
            + "for production environments and it typically does not need to be changed.";
    public static final String WHITELIST_CONFIG = METRICS_REPORTER_PREFIX + "whitelist";
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
    public static final String DEFAULT_WHITELIST;
    public static final String WHITELIST_DOC =
            "Regex matching the converted (snake_case) metric name to be published to the "
                    + "metrics topic.\n\nBy default this includes all the metrics required by Confluent "
                    + "Control Center and Confluent Auto Data Balancer. This should typically never be "
                    + "modified unless requested by Confluent.";
    public static final String
            VOLUME_METRICS_REFRESH_PERIOD_MS =
            METRICS_REPORTER_PREFIX + "volume.metrics.refresh.ms";
    public static final long DEFAULT_VOLUME_METRICS_REFRESH_PERIOD = 15000L;
    public static final String VOLUME_METRICS_REFRESH_PERIOD_DOC =
            "The minimum interval at which to fetch new volume metrics.";

    public static final String DEBUG_ENABLED = METRICS_REPORTER_PREFIX + "debug.enabled";
    public static final boolean DEFAULT_DEBUG_ENABLED = false;
    public static final String DEBUG_ENABLED_DOC = "Enable debug metadata for metrics collection";

    private static final ConfigDef CONFIG;

    static {
        Joiner defaultWhitelistBuilder = Joiner.on(".*|.*");
        StringBuilder builder = new StringBuilder(".*");
        defaultWhitelistBuilder.appendTo(builder, DEFAULT_BROKER_MONITORING_METRICS);
        builder.append(".*");
        DEFAULT_WHITELIST = builder.toString();
    }

    static {
        CONFIG = new ConfigDef()
                .define(
                        BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        BOOTSTRAP_SERVERS_DOC
                ).define(
                        TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_TOPIC_CONFIG,
                        ConfigDef.Importance.LOW,
                        TOPIC_DOC
                ).define(
                        TOPIC_CREATE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_TOPIC_CREATE_CONFIG,
                        ConfigDef.Importance.LOW,
                        TOPIC_CREATE_DOC
                ).define(
                        TOPIC_PARTITIONS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_TOPIC_PARTITIONS_CONFIG,
                        ConfigDef.Importance.LOW,
                        TOPIC_PARTITIONS_DOC
                ).define(
                        TOPIC_REPLICAS_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_TOPIC_REPLICAS_CONFIG,
                        ConfigDef.Importance.LOW,
                        TOPIC_REPLICAS_DOC
                ).define(
                        TOPIC_RETENTION_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_TOPIC_RETENTION_MS_CONFIG,
                        ConfigDef.Importance.LOW,
                        TOPIC_RETENTION_MS_DOC
                ).define(
                        TOPIC_RETENTION_BYTES_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_TOPIC_RETENTION_BYTES_CONFIG,
                        ConfigDef.Importance.LOW,
                        TOPIC_RETENTION_BYTES_DOC
                ).define(
                        TOPIC_ROLL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_TOPIC_ROLL_MS_CONFIG,
                        ConfigDef.Importance.LOW,
                        TOPIC_ROLL_MS_DOC
                ).define(
                        TOPIC_MAX_MESSAGE_BYTES_CONFIG,
                        ConfigDef.Type.INT,
                        DEFAULT_TOPIC_MAX_MESSAGE_BYTES_CONFIG,
                        ConfigDef.Range.atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        TOPIC_MAX_MESSAGE_BYTES_DOC
                ).define(
                        PUBLISH_PERIOD_CONFIG,
                        ConfigDef.Type.LONG,
                        DEFAULT_PUBLISH_PERIOD,
                        ConfigDef.Importance.LOW,
                        PUBLISH_PERIOD_DOC
                ).define(
                        WHITELIST_CONFIG,
                        ConfigDef.Type.STRING,
                        DEFAULT_WHITELIST,
                        ConfigDef.Importance.LOW,
                        WHITELIST_DOC
                ).define(
                        VOLUME_METRICS_REFRESH_PERIOD_MS,
                        ConfigDef.Type.LONG,
                        DEFAULT_VOLUME_METRICS_REFRESH_PERIOD,
                        ConfigDef.Importance.LOW,
                        VOLUME_METRICS_REFRESH_PERIOD_DOC
                ).define(
                        DEBUG_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        DEFAULT_DEBUG_ENABLED,
                        ConfigDef.Importance.LOW,
                        DEBUG_ENABLED_DOC
                );
    }
    public static final Predicate<MetricKey> ALWAYS_TRUE = metricKey -> true;


    public ConfluentTelemetryConfig(Properties props) {
        super(CONFIG, props);
    }

    public ConfluentTelemetryConfig(Map<String, ?> clientConfigs) {
        super(CONFIG, clientConfigs);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toRst());
    }

    private Map<String, Object> producerConfigDefaults() {
        Map<String, Object> defaults = new HashMap<>();
        defaults.putAll(MonitoringProducerDefaults.PRODUCER_CONFIG_DEFAULTS);
        defaults.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer"
        );
        defaults.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.telemetry.serde.OpencensusMetricsProto"
        );
        defaults.put(ProducerConfig.CLIENT_ID_CONFIG, "confluent-telemetry-metrics-reporter");
        return defaults;
    }

    public Properties getProducerProperties() {
        Properties props = new Properties();
        props.putAll(producerConfigDefaults());
        props.putAll(getClientProperties());
        return props;
    }

    public Properties getClientProperties() {
        Properties props = new Properties();
        for (Map.Entry<String, ?> entry : super.originals().entrySet()) {
            if (entry.getKey().startsWith(METRICS_REPORTER_PREFIX)) {
                props.put(entry.getKey().substring(METRICS_REPORTER_PREFIX.length()), entry.getValue());
            }
        }

        // we require bootstrap servers
        Object bootstrap = props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrap == null) {
            throw new ConfigException(
                    "Missing required property "
                            + METRICS_REPORTER_PREFIX
                            + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
            );
        }
        return props;
    }

    public Map<String, String> tags() {
        Map<String, String> labels = new HashMap<>();
        for (Map.Entry<String, ?> entry : super.originals().entrySet()) {
            if (entry.getKey().startsWith(METRICS_REPORTER_TAGS_PREFIX)) {
                labels.put(entry.getKey().substring(METRICS_REPORTER_TAGS_PREFIX.length()), (String) entry.getValue());
            }
        }
        return labels;
    }

    public Map<String, String> topicConfig() {
        int topicReplicas = getInt(ConfluentTelemetryConfig.TOPIC_REPLICAS_CONFIG);
        // set minIsr to be consistent with
        // control center {@link io.confluent.controlcenter.util.TopicInfo.Builder.setReplication}
        Integer minIsr = Math.min(3, topicReplicas < 3 ? 1 : topicReplicas - 1);

        final Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr.toString());
        topicConfig.put(TopicConfig.RETENTION_MS_CONFIG,
                getLong(ConfluentTelemetryConfig.TOPIC_RETENTION_MS_CONFIG).toString());
        topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG,
                getLong(ConfluentTelemetryConfig.TOPIC_RETENTION_BYTES_CONFIG).toString());
        topicConfig.put(TopicConfig.SEGMENT_MS_CONFIG,
                getLong(ConfluentTelemetryConfig.TOPIC_ROLL_MS_CONFIG).toString());
        topicConfig.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
                getInt(ConfluentTelemetryConfig.TOPIC_MAX_MESSAGE_BYTES_CONFIG).toString());
        topicConfig.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name);

        return topicConfig;
    }


    public String[] getBrokerLogVolumes() {
        String logDirsString = null;
        if (originals().containsKey("log.dirs")) {
            logDirsString = (String) originals().get("log.dirs");
        }
        if (logDirsString == null) {
            if (originals().containsKey("log.dir")) {
                logDirsString = (String) originals().get("log.dir");
            }
        }
        String[] volumeMetricsLogDirs = null;
        if (logDirsString != null) {
            volumeMetricsLogDirs = logDirsString.split("\\s*,\\s*");
        }

        return volumeMetricsLogDirs;
    }


    public String getBrokerId() {
        return (String) originals().get("broker.id");
    }

    /**
     * Get a predicate that filters metrics based on the whitelist configuration.
     */
    public Predicate<MetricKey> metricFilter() {
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
}
