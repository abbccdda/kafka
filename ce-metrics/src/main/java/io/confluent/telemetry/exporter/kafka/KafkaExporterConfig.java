package io.confluent.telemetry.exporter.kafka;

import com.google.common.base.Verify;
import io.confluent.monitoring.common.MonitoringProducerDefaults;
import io.confluent.telemetry.ConfigPropertyTranslater;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.serde.OpencensusMetricsProto;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static io.confluent.metrics.reporter.ConfluentMetricsReporterConfig.DEFAULT_MIN_ISR;

public class KafkaExporterConfig extends AbstractConfig {

    public static final String PREFIX = ConfluentTelemetryConfig.PREFIX_EXPORTER + "kafka.";
    public static final String PREFIX_PRODUCER = PREFIX + "producer.";
    public static final String PREFIX_TOPIC = PREFIX + "topic.";

    public static final String BOOTSTRAP_SERVERS_CONFIG = PREFIX_PRODUCER + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
    public static final String BOOTSTRAP_SERVERS_DOC = "Bootstrap servers for the Kafka cluster "
        + "telemetry will be published to. The target cluster may be different from the cluster(s) "
        + "whose telemetry is being collected. Several production Kafka clusters can publish to a "
        + "single target Kafka cluster, for example.";

    // metrics topic default settings should be consistent with
    // control center default metrics topic settings
    // to ensure topics have the same settings regardless of which one starts first
    public static final String TOPIC_NAME_CONFIG = PREFIX_TOPIC + "name";
    public static final String TOPIC_NAME_DOC = "Topic to which metrics data will be written.";
    public static final String DEFAULT_TOPIC_NAME = "_confluent-telemetry-metrics";

    public static final String TOPIC_CREATE_CONFIG = PREFIX_TOPIC + "create";
    public static final String TOPIC_CREATE_DOC = "Create the metrics topic if it does not exist.";
    public static final boolean DEFAULT_TOPIC_CREATE = true;

    public static final String TOPIC_PARTITIONS_CONFIG = PREFIX_TOPIC + "partitions";
    public static final String TOPIC_PARTITIONS_DOC = "Number of partitions in the metrics topic.";
    public static final int DEFAULT_TOPIC_PARTITIONS = 12;

    public static final String TOPIC_REPLICAS_CONFIG = PREFIX_TOPIC + "replicas";
    public static final String TOPIC_REPLICAS_DOC =
        "Number of replicas in the metric topic. It must not be higher than the number "
        + "of brokers in the destination Kafka cluster.";
    public static final int DEFAULT_TOPIC_REPLICAS = 3;

    public static final String TOPIC_RETENTION_MS_CONFIG = PREFIX_TOPIC + "retention.ms";
    public static final String TOPIC_RETENTION_MS_DOC = "Retention time for the metrics topic.";
    public static final long DEFAULT_TOPIC_RETENTION_MS = TimeUnit.DAYS.toMillis(3);

    public static final String TOPIC_RETENTION_BYTES_CONFIG = PREFIX_TOPIC + "retention.bytes";
    public static final String TOPIC_RETENTION_BYTES_DOC = "Retention bytes for the metrics topic.";
    public static final long DEFAULT_TOPIC_RETENTION_BYTES = -1L;

    public static final String TOPIC_ROLL_MS_CONFIG = PREFIX_TOPIC + "roll.ms";
    public static final String TOPIC_ROLL_MS_DOC = "Log rolling time for the metrics topic.";
    public static final long DEFAULT_TOPIC_ROLL_MS = TimeUnit.HOURS.toMillis(4);

    public static final String TOPIC_MAX_MESSAGE_BYTES_CONFIG = PREFIX_TOPIC + TopicConfig.MAX_MESSAGE_BYTES_CONFIG;
    public static final String TOPIC_MAX_MESSAGE_BYTES_DOC = "Maximum message size for the metrics topic.";
    public static final int DEFAULT_TOPIC_MAX_MESSAGE_BYTES = MonitoringProducerDefaults.MAX_REQUEST_SIZE;

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(
            BOOTSTRAP_SERVERS_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            BOOTSTRAP_SERVERS_DOC
        ).define(
            TOPIC_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_TOPIC_NAME,
            ConfigDef.Importance.LOW,
            TOPIC_NAME_DOC
        ).define(
            TOPIC_CREATE_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_TOPIC_CREATE,
            ConfigDef.Importance.LOW,
            TOPIC_CREATE_DOC
        ).define(
            TOPIC_PARTITIONS_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_TOPIC_PARTITIONS,
            ConfigDef.Importance.LOW,
            TOPIC_PARTITIONS_DOC
        ).define(
            TOPIC_REPLICAS_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_TOPIC_REPLICAS,
            ConfigDef.Importance.LOW,
            TOPIC_REPLICAS_DOC
        ).define(
            TOPIC_RETENTION_MS_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_RETENTION_MS,
            ConfigDef.Importance.LOW,
            TOPIC_RETENTION_MS_DOC
        ).define(
            TOPIC_RETENTION_BYTES_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_RETENTION_BYTES,
            ConfigDef.Importance.LOW,
            TOPIC_RETENTION_BYTES_DOC
        ).define(
            TOPIC_ROLL_MS_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_ROLL_MS,
            ConfigDef.Importance.LOW,
            TOPIC_ROLL_MS_DOC
        ).define(
            TOPIC_MAX_MESSAGE_BYTES_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_TOPIC_MAX_MESSAGE_BYTES,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.MEDIUM,
            TOPIC_MAX_MESSAGE_BYTES_DOC
        );


    private static final String LEGACY_PREFIX = "confluent.telemetry.metrics.reporter.";

    private static final ConfigPropertyTranslater DEPRECATION_TRANSLATER =
        new ConfigPropertyTranslater.Builder()
            .withPrefixTranslation(ConfluentTelemetryConfig.LEGACY_PREFIX + "topic.", PREFIX_TOPIC)
            .withPrefixTranslation(ConfluentTelemetryConfig.LEGACY_PREFIX, PREFIX_PRODUCER)
            .build();

    public KafkaExporterConfig(Map<String, ?> originals) {
        super(CONFIG, DEPRECATION_TRANSLATER.translate(originals));
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toRst());
    }

    private Map<String, Object> producerConfigDefaults() {
        Map<String, Object> defaults = new HashMap<>(MonitoringProducerDefaults.PRODUCER_CONFIG_DEFAULTS);
        defaults.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ByteArraySerializer.class.getName()
        );
        defaults.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            OpencensusMetricsProto.class.getName()
        );
        defaults.put(ProducerConfig.CLIENT_ID_CONFIG, "confluent-telemetry-metrics-reporter");
        return defaults;
    }

    Properties getProducerProperties() {
        Properties props = new Properties();
        props.putAll(producerConfigDefaults());

        props.putAll(originalsWithPrefix(PREFIX_PRODUCER));

        if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
          throw new ConfigException(
              "Missing required property "
                  + PREFIX_PRODUCER
                  + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
          );
        }

        return props;
    }

    Map<String, String> getTopicConfig() {

        final Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(DEFAULT_MIN_ISR));
        topicConfig.put(TopicConfig.RETENTION_MS_CONFIG,
            getLong(TOPIC_RETENTION_MS_CONFIG).toString());
        topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG,
            getLong(TOPIC_RETENTION_BYTES_CONFIG).toString());
        topicConfig.put(TopicConfig.SEGMENT_MS_CONFIG,
            getLong(TOPIC_ROLL_MS_CONFIG).toString());
        topicConfig.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
            getInt(TOPIC_MAX_MESSAGE_BYTES_CONFIG).toString());
        topicConfig.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name);

        return topicConfig;
    }

    boolean isCreateTopic() {
        return getBoolean(TOPIC_CREATE_CONFIG);
    }

    int getTopicReplicas() {
        Integer topicReplicas = getInt(TOPIC_REPLICAS_CONFIG);
        Verify.verify(topicReplicas > 0, "topic needs at least 1 replica");
        return topicReplicas;
    }

    int getTopicPartitions() {
        Integer topicPartitions = getInt(TOPIC_PARTITIONS_CONFIG);
        Verify.verify(topicPartitions > 0, "topic needs at least 1 partition");
        return topicPartitions;
    }

    String getTopicName() {
        return getString(TOPIC_NAME_CONFIG);
    }
}
