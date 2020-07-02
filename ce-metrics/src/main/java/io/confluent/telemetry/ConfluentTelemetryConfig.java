// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.telemetry;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.telemetry.common.config.ConfigUtils;
import io.confluent.telemetry.collector.VolumeMetricsCollector.VolumeMetricsCollectorConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.ExporterConfig.ExporterType;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConfluentTelemetryConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(ConfluentTelemetryConfig.class);

    public static final String PREFIX = "confluent.telemetry.";
    public static final String PREFIX_LABELS = PREFIX + "labels.";
    public static final String PREFIX_EXPORTER = PREFIX + "exporter.";
    public static final String PREFIX_METRICS_COLLECTOR = PREFIX + "metrics.collector.";

    public static final String COLLECT_INTERVAL_CONFIG = PREFIX_METRICS_COLLECTOR + "interval.ms";
    public static final Long DEFAULT_COLLECT_INTERVAL = TimeUnit.MINUTES.toMillis(1);
    public static final String COLLECT_INTERVAL_DOC = "The metrics reporter will collect new metrics "
            + "from the system in intervals defined by this setting. This means that control "
            + "center system health data lags by this duration, or that rebalancer may compute a plan "
            + "based on broker data that is stale by this duration. The default is a reasonable value "
            + "for production environments and it typically does not need to be changed.";

    public static final String METRICS_INCLUDE_CONFIG = PREFIX_METRICS_COLLECTOR + "include";
    public static final String METRICS_INCLUDE_CONFIG_ALIAS = PREFIX_METRICS_COLLECTOR + "whitelist";

    public static final String METRICS_INCLUDE_DOC =
        "Regex matching the converted (snake_case) metric name to be published to the "
        + "metrics topic.\n\nBy default this includes all the metrics required by "
        + "Proactive Support and Confluent Auto Data Balancer. This should typically never "
        + "be modified unless requested by Confluent.";
    public static final String DEFAULT_METRICS_INCLUDE;

    public static final List<String> DEFAULT_BROKER_MONITORING_METRICS = Collections.unmodifiableList(
            Arrays.asList(
                "active_controller_count",
                "bytes_in",
                "bytes_out",
                "process_cpu_load",
                "disk_total_bytes",
                "disk_usable_bytes",
                "failed_fetch_requests",
                "failed_produce_requests",
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
                "requests",
                "response_queue_size",
                "response_queue_time_ms",
                "response_send_time_ms",
                "size",
                "total_fetch_requests",
                "total_produce_requests",
                "total_time_ms",
                "unclean_leader_elections",
                "under_replicated",
                "under_replicated_partitions",
                "zookeeper_disconnects",
                "zookeeper_expires",
                // System level metrics for all services
                "io.confluent.system/.*("
                    + "|system_cpu_load"
                    + "|system_load_average"
                    + "|free_physical_memory_size"
                    + "|total_physical_memory_size"
                    + "|committed"
                    + "|used)",
                // Control Center metrics
                "io.confluent.controlcenter/.*("
                    + "metrics_input_topic_progress"
                    + "|monitoring_input_topic_progress"
                    + "|misconfigured_topics"
                    + "|missing_topic_configurations"
                    + "|broker_log_persistent_dir"
                    + "|cluster_offline"
                    + "|streams_status"
                    + "|total_lag"
                    + "|request_latency"
                    + "|response_size"
                    + "|response_rate)"
            )
    );

    public static final String CONFIG_EVENTS_ENABLE_CONFIG = PREFIX + "events.enable";
    public static final String CONFIG_EVENTS_ENABLE_DOC = "Enable config events collection." +
            "Disabled by default for v6.0.";
    public static final Boolean CONFIG_EVENTS_ENABLE_DEFAULT = false;

    public static final String CONFIG_EVENTS_INCLUDE_CONFIG = PREFIX + "events.collector.include";
    public static final String CONFIG_EVENTS_INCLUDE_DOC =
            "Regex matching the config names to be published to telemetry collector."
                    + "This should typically never be modified unless requested by Confluent.";
    public static final String CONFIG_EVENTS_INCLUDE_DEFAULT;

    //https://confluentinc.atlassian.net/wiki/spaces/PM/pages/906395695/1-pager+Telemetry+of+Configuration+and+Control+Event+Data#id-1-pager:TelemetryofConfigurationandControlEventData-Staticserviceconfiguration
    // TODO: Add include list for non-kafka components.
    public static final List<String> CONFIG_EVENTS_INCLUDE = Collections.unmodifiableList(
            Arrays.asList("auto.create.topics.enable",
                    "broker.id",
                    "compression.type",
                    "confluent.tier.local.hotset.bytes",
                    "confluent.tier.local.hotset.ms",
                    "confluent.tier.metadata.replication.factor",
                    "confluent.tier.s3.region",
                    "delete.topic.enable",
                    "leader.imbalance.check.interval.seconds",
                    "leader.imbalance.per.broker.percentage",
                    "log.dir",
                    "log.dirs",
                    "log.flush.interval.messages",
                    "log.flush.interval.ms",
                    "log.flush.offset.checkpoint.interval.ms",
                    "log.flush.scheduler.interval.ms",
                    "log.flush.start.offset.checkpoint.interval.ms",
                    "log.retention.bytes",
                    "log.retention.hours",
                    "log.retention.minutes",
                    "log.retention.ms",
                    "log.roll.hours",
                    "log.roll.ms",
                    "log.segment.bytes",
                    "log.segment.delete.delay.ms",
                    "message.max.bytes",
                    "min.insync.replicas",
                    "num.io.threads",
                    "num.network.threads",
                    "num.recovery.threads.per.data.dir",
                    "num.replica.alter.log.dirs.threads",
                    "num.replica.fetchers",
                    "offset.metadata.max.bytes",
                    "offsets.commit.required.acks",
                    "offsets.commit.timeout.ms",
                    "offsets.load.buffer.size",
                    "offsets.retention.check.interval.ms",
                    "offsets.retention.minutes",
                    "offsets.topic.compression.codec",
                    "offsets.topic.num.partitions",
                    "offsets.topic.replication.factor",
                    "queued.max.requests",
                    "replica.fetch.min.bytes",
                    "replica.fetch.wait.max.ms",
                    "replica.high.watermark.checkpoint.interval.ms",
                    "replica.lag.time.max.ms",
                    "replica.socket.receive.buffer.bytes",
                    "replica.socket.timeout.ms",
                    "request.timeout.ms",
                    "socket.receive.buffer.bytes",
                    "socket.request.max.bytes",
                    "socket.send.buffer.bytes",
                    "transacation.*",
                    "unclean.leader.election.enable",
                    "zookeeper.connection.timeout.ms",
                    "zookeeper.max.in.flight.requests",
                    "zookeeper.session.timeout.ms",
                    "zookeeper.set.acl",
                    "broker.id.generation.enable",
                    "broker.rack",
                    "confluent.tier.archiver.num.threads",
                    "confluent.tier.backend",
                    "confluent.tier.enable",
                    "confluent.tier.feature",
                    "confluent.tier.fetcher.num.threads",
                    "connections.max.idle.ms",
                    "connections.max.reauth.ms",
                    "controlled.*",
                    "controller.socket.timeout.ms",
                    "default.replication.factor",
                    "delegation.token.expiry.time.ms",
                    "delegation.token.max.lifetime.ms",
                    "delete.records.purgatory.purge.interval.requests",
                    "fetch.*",
                    "group.*",
                    "log.cleaner.*",
                    "log.index.*",
                    "log.message.*",
                    "log.preallocate",
                    "log.retention.check.interval.ms",
                    "max.*",
                    "num.partitions",
                    "principal.builder.class",
                    "producer.purgatory.purge.interval.requests",
                    "queued.max.request.bytes",
                    "replica.*",
                    "reserved.broker.max.id",
                    "sasl.client.callback.handler.class",
                    "sasl.enabled.mechanisms",
                    "sasl.login.class",
                    "sasl.mechanism.inter.broker.protocol",
                    "sasl.server.callback.handler.class",
                    "security.inter.broker.protocol",
                    "ssl.client.auth",
                    "ssl.enabled.protocols",
                    "ssl.engine.builder.class",
                    "ssl.protocol",
                    "ssl.provider",
                    "zookeeper.clientCnxnSocket",
                    "zookeeper.ssl.client.enable",
                    "alter.*",
                    "authorizer.class.name",
                    "client.quota.callback.class",
                    "confluent.log.placement.constraints",
                    "confluent.tier.topic.delete.check.interval.ms",
                    "connection.failed.authentication.delay.ms",
                    "create.topic.policy.class.name",
                    "kafka.metrics.polling.interval.secs",
                    "kafka.metrics.reporters",
                    "listener.security.protocol.map",
                    "log.message.downconversion.enable",
                    "metric.reporters",
                    "metrics.*",
                    "password.encoder.cipher.algorithm",
                    "password.encoder.iterations",
                    "password.encoder.key.length",
                    "password.encoder.keyfactory.algorithm",
                    "quota.*",
                    "replication.*",
                    "security.providers",
                    "ssl.endpoint.identification.algorithm",
                    "enable.fips"));


    static {
        StringBuilder builder = new StringBuilder(".*");
        Joiner.on(".*|.*").appendTo(builder, DEFAULT_BROKER_MONITORING_METRICS);
        builder.append(".*");
        DEFAULT_METRICS_INCLUDE = builder.toString();

        StringBuilder eventBuilder = new StringBuilder(".*");
        Joiner.on(".*|.*").appendTo(eventBuilder, CONFIG_EVENTS_INCLUDE);
        eventBuilder.append(".*");
        CONFIG_EVENTS_INCLUDE_DEFAULT = eventBuilder.toString();
    }

    public static final String DEBUG_ENABLED = PREFIX + "debug.enabled";
    public static final String DEBUG_ENABLED_DOC = "Enable debug metadata for metrics collection";
    public static final boolean DEFAULT_DEBUG_ENABLED = false;

    // Default local Kafka Exporter config for Self Balancing Kafka.
    // This actually gets used inside the reporter itself
    public static final String EXPORTER_LOCAL_NAME = "_local";
    // The following are the set of default metrics that are exported to local
    // _confluent-telemetry-metrics topic onprem for Self Balancing Kafka.
    // Reference for the same cloud specific config:
    // https://github.com/confluentinc/cc-spec-kafka/blob/v0.205.x/plugins/kafka/templates/serverConfig.tmpl#L14
    public static final String EXPORTER_LOCAL_METRICS_INCLUDE = ".*bytes_in.*|" +
        ".*bytes_out.*|.*process_cpu_load.*|.*local_time_ms.*|.*log_flush_rate_and_time_ms.*|" +
        ".*messages_in.*|.*request_handler_avg_idle_percent.*|.*requests.*|" +
        ".*request_queue_size.*|.*request_queue_time_ms.*|.*response_queue_size.*|" +
        ".*size.*|.*total_fetch_requests.*|.*total_produce_requests.*|" +
        ".*total_time_ms.*|.*replication_bytes_in.*|.*replication_bytes_out.*";

    public static final Map<String, Object> EXPORTER_LOCAL_DEFAULTS =
        ImmutableMap.of(
            ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name(),
            ExporterConfig.ENABLED_CONFIG, true,
            ExporterConfig.METRICS_INCLUDE_CONFIG, EXPORTER_LOCAL_METRICS_INCLUDE,

            // This will get overriden by getDynamicDefaults() however not defaulting
            // this makes us have to provide this config even when this reporter is explicitly disabled. 
            KafkaExporterConfig.PREFIX_PRODUCER + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",

            KafkaExporterConfig.PREFIX_PRODUCER + CommonClientConfigs.CLIENT_ID_CONFIG, "confluent-telemetry-reporter-local-producer"

            // Note: the rest of the client configs get handled in parseLocalClientConfigs()
        );

    // Default HTTP Exporter config to send telemetry data to Confluent Cloud.
    public static final String EXPORTER_CONFLUENT_NAME = "_confluent";
    public static final Map<String, Object> EXPORTER_CONFLUENT_DEFAULTS =
        ImmutableMap.of(
            ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.http.name(),
            ExporterConfig.ENABLED_CONFIG, false,
            HttpExporterConfig.CLIENT_BASE_URL, "https://collector.telemetry.confluent.cloud"
        );

    public static final Map<String, Map<String, Object>> EXPORTER_DEFAULT_CONFIGS =
        ImmutableMap.of(
            // omit the local exporter since defaults are applied inside the reporter
            EXPORTER_CONFLUENT_NAME, EXPORTER_CONFLUENT_DEFAULTS
        );

    public static String exporterPrefixForName(String name) {
        return PREFIX_EXPORTER + name + ".";
    }

    /**
     * High level configs tied only to the default Confluent Cloud Http Exporter for simplified usage.
     */
    public static final String TELEMETRY_API_KEY = PREFIX + "api.key";
    public static final String TELEMETRY_API_KEY_DOC = "The API key used to authenticate HTTP requests with the Confluent telemetry server";

    public static final String TELEMETRY_API_SECRET = PREFIX + "api.secret";
    public static final String TELEMETRY_API_SECRET_DOC = "The API secret used to authenticate HTTP requests with the Confluent Telemetry server";

    public static final String TELEMETRY_ENABLED_CONFIG = PREFIX + "enabled";
    public static final String TELEMETRY_ENABLED_DOC = "True if telemetry data can to be reported to Confluent Cloud";
    public static final boolean TELEMETRY_ENABLED_DEFAULT = false;

    public static final String TELEMETRY_PROXY_URL = PREFIX + HttpExporterConfig.PREFIX_PROXY + "url";
    public static final String TELEMETRY_PROXY_URL_DOC = "The URL for an explicit (i.e. not transparent) forward HTTP proxy to send Telemetry data to Confluent Cloud";

    public static final String TELEMETRY_PROXY_USERNAME = PREFIX + HttpExporterConfig.PREFIX_PROXY + "username";
    public static final String TELEMETRY_PROXY_USERNAME_DOC = "The username credential for the forward HTTP proxy to send Telemetry data to Confluent Cloud";

    public static final String TELEMETRY_PROXY_PASSWORD = PREFIX + HttpExporterConfig.PREFIX_PROXY + "password";
    public static final String TELEMETRY_PROXY_PASSWORD_DOC = "The password credential for the forward HTTP proxy to send Telemetry data to Confluent Cloud";


    public static final Set<String> RECONFIGURABLES =
        ImmutableSet.of(
            // not including METRICS_INCLUDE_CONFIG_ALIAS in dynamic configs,
            // since we have never set those configs dynamically anywhere
            METRICS_INCLUDE_CONFIG,
            TELEMETRY_ENABLED_CONFIG,
            TELEMETRY_API_KEY,
            TELEMETRY_API_SECRET,
            TELEMETRY_PROXY_URL,
            TELEMETRY_PROXY_USERNAME,
            TELEMETRY_PROXY_PASSWORD
        );

    // Internal map used to reconcile config parameters for default _confluent http exporter using the
    // high level end-user friendly http.telemetry flags and http.telemetry.exporter._confluent flags.
    // Sample Map contents:
    // Map(
    //   confluent.telemetry.enabled => confluent.telemetry.exporter._confluent.enabled,
    //   confluent.telemetry.api.key => confluent.telemetry.exporter._confluent.api.key,
    //   confluent.telemetry.api.secret => confluent.telemetry.exporter._confluent.api.secret,
    //   confluent.telemetry.proxy.url => confluent.telemetry.exporter._confluent.proxy.url
    //   confluent.telemetry.proxy.username => confluent.telemetry.exporter._confluent.proxy.username
    //   confluent.telemetry.proxy.password => confluent.telemetry.exporter._confluent.proxy.password
    //)
    private static final ImmutableMap<String, String> RECONCILABLE_CONFIG_MAP =
        ImmutableMap.<String, String>builder()
            .put(TELEMETRY_ENABLED_CONFIG, exporterPrefixForName(EXPORTER_CONFLUENT_NAME) + ExporterConfig.ENABLED_CONFIG)
            .put(TELEMETRY_API_KEY, exporterPrefixForName(EXPORTER_CONFLUENT_NAME) + HttpExporterConfig.API_KEY)
            .put(TELEMETRY_API_SECRET, exporterPrefixForName(EXPORTER_CONFLUENT_NAME) + HttpExporterConfig.API_SECRET)
            .put(TELEMETRY_PROXY_URL, exporterPrefixForName(EXPORTER_CONFLUENT_NAME) + HttpExporterConfig.PROXY_URL)
            .put(TELEMETRY_PROXY_USERNAME, exporterPrefixForName(EXPORTER_CONFLUENT_NAME) + HttpExporterConfig.PROXY_USERNAME)
            .put(TELEMETRY_PROXY_PASSWORD, exporterPrefixForName(EXPORTER_CONFLUENT_NAME) + HttpExporterConfig.PROXY_PASSWORD)
            .build();

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(
                COLLECT_INTERVAL_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_COLLECT_INTERVAL,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.LOW,
                COLLECT_INTERVAL_DOC
        ).define(
                METRICS_INCLUDE_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_METRICS_INCLUDE,
                new RegexConfigDefValidator("Metrics filter for configuration"),
                ConfigDef.Importance.LOW,
                METRICS_INCLUDE_DOC
        ).define(
                CONFIG_EVENTS_INCLUDE_CONFIG,
                ConfigDef.Type.STRING,
                CONFIG_EVENTS_INCLUDE_DEFAULT,
                new RegexConfigDefValidator("Events filter for configuration"),
                ConfigDef.Importance.LOW,
                CONFIG_EVENTS_INCLUDE_DOC
        ).define(
                CONFIG_EVENTS_ENABLE_CONFIG,
                ConfigDef.Type.BOOLEAN,
                CONFIG_EVENTS_ENABLE_DEFAULT,
                ConfigDef.Importance.LOW,
                CONFIG_EVENTS_ENABLE_DOC
        ).define(
                DEBUG_ENABLED,
                ConfigDef.Type.BOOLEAN,
                DEFAULT_DEBUG_ENABLED,
                ConfigDef.Importance.LOW,
                DEBUG_ENABLED_DOC
        ).define(
            TELEMETRY_API_KEY,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            TELEMETRY_API_KEY_DOC
        ).define(
            TELEMETRY_API_SECRET,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.HIGH,
            TELEMETRY_API_SECRET_DOC
        ).define(
            TELEMETRY_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            TELEMETRY_ENABLED_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TELEMETRY_ENABLED_DOC
        ).define(
            TELEMETRY_PROXY_URL,
            ConfigDef.Type.STRING,
            null,
            new HttpExporterConfig.URIValidator(),
            ConfigDef.Importance.LOW,
            TELEMETRY_PROXY_URL_DOC
        ).define(
            TELEMETRY_PROXY_USERNAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            TELEMETRY_PROXY_USERNAME_DOC
        ).define(
            TELEMETRY_PROXY_PASSWORD,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.LOW,
            TELEMETRY_PROXY_PASSWORD_DOC
        );

    public static final Predicate<MetricKey> ALWAYS_TRUE = metricKey -> true;

    private final VolumeMetricsCollectorConfig volumeMetricsCollectorConfig;

    private final Map<String, ExporterConfig> exporterConfigMap;

    public ConfluentTelemetryConfig(Map<String, ?> originals) {
        this(originals, true);
    }

    public ConfluentTelemetryConfig(Map<String, ?> originals, boolean doLog) {
        super(CONFIG, reconcileConfigs(ConfigUtils.translateDeprecated(originals, new String[][]{
            {METRICS_INCLUDE_CONFIG, METRICS_INCLUDE_CONFIG_ALIAS}})), doLog);
        this.volumeMetricsCollectorConfig = new VolumeMetricsCollectorConfig(originals, doLog);
        this.exporterConfigMap = createExporterMap(doLog);
        if (this.enabledExporters().isEmpty()) {
            log.warn("no telemetry exporters are enabled");
        }
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

    public Predicate<MetricKey> buildMetricsPredicate() {
        return buildMetricsPredicate(getString(METRICS_INCLUDE_CONFIG));
    }

    public static Predicate<MetricKey> buildMetricsPredicate(String regexString) {
        // Configure the PatternPredicate.
        regexString = regexString.trim();

        if (regexString.isEmpty()) {
            return ALWAYS_TRUE;
        }
        // sourceCompatibility and targetCompatibility is set to Java 8 in build.gradle hence avoid
        // using `asMatchPredicate` method of `Predicate` class.
        Pattern pattern = Pattern.compile(regexString);

        // TODO We eventually plan to also support configuration of exclude via label values.
        // Presumably we need a Map<String, Map<String, String>> of:
        // metric name -> label key -> label value

        return metricNameAndLabels -> pattern.matcher(metricNameAndLabels.getName()).matches();
    }

    public VolumeMetricsCollectorConfig getVolumeMetricsCollectorConfig() {
        return volumeMetricsCollectorConfig;
    }

    // We have 2 sets of config flags for _confluent http exporter.
    // 1) confluent.telemetry.<flag>
    // 2) confluent.telemetry.exporter._confluent.<flag>
    // The confluent.telemetry.exporter._confluent.<flag> takes higher precedence over confluent.telemetry.<flag>
    // when the end-user passes both. If the user passes only one of them, then that flag's value
    // will be used to determine the behavior of the _confluent exporter.
    @SuppressWarnings("unchecked")
    public static Map<String, ?> reconcileConfigs(Map<String, ?> config) {
        // For each high-level key, if corresponding low-level key is not passed,
        // then the high-level key takes precedence.
        Map<String, Object> tempConfig = (Map<String, Object>) config;
        for (String reconcilableHighLevelKey: RECONCILABLE_CONFIG_MAP.keySet()) {
            String lowLevelKey = RECONCILABLE_CONFIG_MAP.get(reconcilableHighLevelKey);
            if (tempConfig.containsKey(reconcilableHighLevelKey) && !tempConfig.containsKey(lowLevelKey)) {
                log.info("Applying value of {} flag for default _confluent http exporter as {}" +
                    "isn't passed", reconcilableHighLevelKey, lowLevelKey);
                tempConfig.put(lowLevelKey, tempConfig.get(reconcilableHighLevelKey));
            }
        }
        return tempConfig;
    }

    private Map<String, ExporterConfig> createExporterMap(boolean doLog) {
        String configPrefix = PREFIX_EXPORTER;
        Map<String, Map<String, Object>> exporters = Maps.newHashMap();

        // put our default exporter configs
        for (Map.Entry<String, Map<String, Object>> entry : EXPORTER_DEFAULT_CONFIGS.entrySet()) {
            Map<String, Object> defaults =  Maps.newHashMap(entry.getValue());
            exporters.put(entry.getKey(), defaults);
        }

        // parse/add user-specified configs
        final Map<String, Object> exporterConfigs = Maps.newHashMap();
        exporterConfigs.putAll(this.originalsWithPrefix(configPrefix));
        for (Map.Entry<String, Object> entry : exporterConfigs.entrySet()) {
            final String key = entry.getKey();
            int nameLength = key.indexOf(".");
            if (nameLength > 0) {
                String name = key.substring(0, nameLength);
                String property = key.substring(nameLength + 1);

                exporters
                    .computeIfAbsent(name, k -> Maps.newHashMap())
                    .put(property, entry.getValue());
            }
        }

        // create config objects
        Map<String, ExporterConfig> exporterConfigMap = Maps.newHashMap();
        Map<String, Object> originals = originals();
        for (Map.Entry<String,  Map<String, Object>> entry : exporters.entrySet()) {

            // use global metrics include config if exporter-level config is not provided
            if (!entry.getValue().containsKey(ExporterConfig.METRICS_INCLUDE_CONFIG) && originals.containsKey(
                METRICS_INCLUDE_CONFIG)) {
                entry.getValue().put(ExporterConfig.METRICS_INCLUDE_CONFIG, originals.get(METRICS_INCLUDE_CONFIG));
            }

            // we need to know the type in order to initialize the exporter
            ExporterType exporterType = ExporterConfig.parseType(entry.getValue().get(ExporterConfig.TYPE_CONFIG));

            ExporterConfig config = null;
            if (exporterType.equals(ExporterConfig.ExporterType.kafka)) {
                config = new KafkaExporterConfig(entry.getValue(), doLog);
            } else if (exporterType.equals(ExporterConfig.ExporterType.http)) {
                config = new HttpExporterConfig(entry.getValue(), doLog);
            } else {
                // we should never hit this since we've already validated the type above
                throw new RuntimeException("unexpected ExporterType value");
            }

            exporterConfigMap.put(
                entry.getKey(),
                config
            );
        }

        return exporterConfigMap;
    }

    public Map<String, ExporterConfig> allExporters() {
        return filterConfigMap(this.exporterConfigMap, null, null, null, ExporterConfig.class);
    }

    public Map<String, ExporterConfig> enabledExporters() {
        return filterConfigMap(this.exporterConfigMap, null, true, null, ExporterConfig.class);
    }

    public Map<String, ExporterConfig> allExportersWithNames(Set<String> names) {
        return filterConfigMap(this.exporterConfigMap, null, null, names, ExporterConfig.class);
    }

    public Map<String, HttpExporterConfig> allHttpExporters() {
        return filterConfigMap(this.exporterConfigMap, ExporterType.http, null, null, HttpExporterConfig.class);
    }

    public Map<String, KafkaExporterConfig> allKafkaExporters() {
        return filterConfigMap(this.exporterConfigMap, ExporterType.http, null, null, KafkaExporterConfig.class);
    }

    private static <T> Map<String, T> filterConfigMap(
        Map<String, ExporterConfig> configNameMap,
        ExporterType type, Boolean isEnabled,
        Set<String> names, Class<T> castTo
    ) {
        return configNameMap.entrySet().stream()
            .filter(e -> {
                return type == null || e.getValue().getType().equals(type);
            })
            .filter(e -> {
                return isEnabled == null || isEnabled.booleanValue() == e.getValue().isEnabled();
            })
            .filter(e -> {
                return names == null || names.contains(e.getKey());
            })
            .collect(
                Collectors.toMap(
                    e -> e.getKey(),
                    e -> castTo.cast(e.getValue())
                )
            );
    }
}
