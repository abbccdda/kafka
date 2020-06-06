// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.telemetry;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.telemetry.collector.VolumeMetricsCollector.VolumeMetricsCollectorConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.ExporterConfig.ExporterType;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;

import kafka.server.KafkaConfig;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs.ClientType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
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

    // TODO: remove the local exporter for non-broker components
    // Default local Kafka Exporter config for Self Balancing Kafka.
    public static final String EXPORTER_LOCAL_NAME = "_local";
    public static final Map<String, Object> EXPORTER_LOCAL_DEFAULTS =
        ImmutableMap.of(
            ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name(),
            ExporterConfig.ENABLED_CONFIG, true,

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
            EXPORTER_LOCAL_NAME, EXPORTER_LOCAL_DEFAULTS,
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
            WHITELIST_CONFIG,
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
                WHITELIST_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_WHITELIST,
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(String name, Object value) {
                        String regexString = value.toString();
                        try {
                            Pattern.compile(regexString);
                        } catch (PatternSyntaxException e) {
                            throw new ConfigException(
                                "Metrics filter for configuration "
                                + name
                                + " is not a valid regular expression"
                            );
                        }
                    }
                },
                ConfigDef.Importance.LOW,
                WHITELIST_DOC
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
            ConfigDef.Type.STRING,
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
        super(CONFIG, reconcileConfigs(originals), doLog);
        this.volumeMetricsCollectorConfig = new VolumeMetricsCollectorConfig(originals);
        this.exporterConfigMap = createExporterMap();
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

    public String getBrokerId() {
        // TODO remove dependency on KafkaConfig for non-broker components
        return (String) originals().get(KafkaConfig.BrokerIdProp());
    }

    /**
     * Get kafka broker rack information.
     */
    public Optional<String> getBrokerRack() {
        // TODO remove dependency on KafkaConfig for non-broker components
        return Optional.ofNullable((String) originals().get(KafkaConfig.RackProp()));
    }

    public String getMetricsWhitelistRegex() {
        return getString(WHITELIST_CONFIG);
    }

    public Predicate<MetricKey> buildMetricWhitelistFilter() {
        return buildMetricWhitelistFilter(getMetricsWhitelistRegex());
    }

    private static Predicate<MetricKey> buildMetricWhitelistFilter(String regexString) {
        // Configure the PatternPredicate.
        regexString = regexString.trim();

        if (regexString.isEmpty()) {
            return ALWAYS_TRUE;
        }
        // sourceCompatibility and targetCompatibility is set to Java 8 in build.gradle hence avoid
        // using `asMatchPredicate` method of `Predicate` class.
        Pattern pattern = Pattern.compile(regexString);

        // TODO We eventually plan to also support configuration of blacklist via label values.
        // Presumably we need a Map<String, Map<String, String>> of:
        // metric name -> label key -> label value

        return metricNameAndLabels -> pattern.matcher(metricNameAndLabels.getName()).matches();
    }

    public VolumeMetricsCollectorConfig getVolumeMetricsCollectorConfig() {
        return volumeMetricsCollectorConfig;
    }

    public static void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        // validation should be handled by ConfigDef Validators
        new ConfluentTelemetryConfig(configs, false);
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

    private Map<String, Map<String, Object>> parseLocalClientConfigs() {
        // TODO remove dependency on ConfluentConfigs for non-broker components
        return ImmutableMap.of(
            EXPORTER_LOCAL_NAME,
            ConfluentConfigs.clientConfigs(this, ConfluentConfigs.INTERBROKER_REPORTER_CLIENT_CONFIG_PREFIX,
                ClientType.PRODUCER, "", "confluent-telemetry-reporter").entrySet().stream()
                // don't start a metrics reporter inside the local producer
                .filter(e -> !e.getKey().equals(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG))
                // don't use ConfluentConfigs.clientConfigs client-id
                .filter(e -> !e.getKey().equals(CommonClientConfigs.CLIENT_ID_CONFIG))
                // we don't need to pass through null values
                .filter(e -> e.getValue() != null)
                .collect(
                    Collectors.toMap(
                        e -> KafkaExporterConfig.PREFIX_PRODUCER + e.getKey(),
                        e -> e.getValue()
                    )
                )
        );
    }

    private Map<String, ExporterConfig> createExporterMap() {
        String configPrefix = PREFIX_EXPORTER;
        Map<String, Map<String, Object>> exporters = Maps.newHashMap();

        // put our default exporter configs
        Map<String, Map<String, Object>> dynamicDefaults = parseLocalClientConfigs();
        for (Map.Entry<String, Map<String, Object>> entry : EXPORTER_DEFAULT_CONFIGS.entrySet()) {
            // apply dynamic defaults as well
            Map<String, Object> defaults =  Maps.newHashMap(entry.getValue());
            if (dynamicDefaults.containsKey(entry.getKey())) {
                defaults.putAll(dynamicDefaults.get(entry.getKey()));
            }
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
        for (Map.Entry<String,  Map<String, Object>> entry : exporters.entrySet()) {

            // we need to know the type in order to initialize the exporter
            ExporterType exporterType = ExporterConfig.parseType(entry.getValue().get(ExporterConfig.TYPE_CONFIG));

            ExporterConfig config = null;
            if (exporterType.equals(ExporterConfig.ExporterType.kafka)) {
                config = new KafkaExporterConfig(entry.getValue());
            } else if (exporterType.equals(ExporterConfig.ExporterType.http)) {
                config = new HttpExporterConfig(entry.getValue());
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
