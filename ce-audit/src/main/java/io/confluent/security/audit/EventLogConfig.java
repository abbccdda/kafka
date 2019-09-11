package io.confluent.security.audit;

import io.confluent.security.audit.appender.EventProducerDefaults;
import io.confluent.security.audit.appender.LogEventAppender;
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

public class EventLogConfig extends AbstractConfig {

  public static final String EVENT_LOGGER_PREFIX = "confluent.security.event.logger.";

  public static final String EVENT_LOGGER_CLASS_CONFIG = EVENT_LOGGER_PREFIX + "class";
  public static final String DEFAULT_EVENT_LOGGER_CLASS_CONFIG =
      LogEventAppender.class.getCanonicalName();
  public static final String EVENT_LOGGER_CLASS_DOC = "Class to use for delivering event logs.";

  // Logger name
  public static final String EVENT_LOG_NAME_CONFIG = EVENT_LOGGER_PREFIX + "name";
  public static final String DEFAULT_EVENT_LOG_NAME_CONFIG = "event";
  public static final String EVENT_LOG_NAME_DOC = "Name for the logger, used in getLogger()";

  // Kafka configuration for Kafka logger
  public static final String BOOTSTRAP_SERVERS_CONFIG =
      EVENT_LOGGER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = "Bootstrap servers for the KafkaEventLogger "
      + "event logs will be published to. The event logs cluster may be different from the cluster(s) "
      + "whose event logs are being collected. Several production KafkaEventLogger clusters can publish to a "
      + "single event logs cluster, for example.";

  public static final String EVENT_LOG_PRINCIPAL_CONFIG = EVENT_LOGGER_PREFIX + "principal";
  public static final String DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG = "_confluent-security-event-logger";
  public static final String EVENT_LOG_PRINCIPAL_DOC = "The principal that is used to produce log events";

  public static final String TOPIC_CONFIG = EVENT_LOGGER_PREFIX + "topic";
  public static final String DEFAULT_TOPIC_CONFIG = "_confluent-security-event";
  public static final String TOPIC_DOC = "Topic on which event logs will be written.";
  public static final String TOPIC_CREATE_CONFIG = EVENT_LOGGER_PREFIX + "topic.create";
  public static final boolean DEFAULT_TOPIC_CREATE_CONFIG = true;
  public static final String TOPIC_CREATE_DOC = "Create the event log topic if it does not exist.";
  public static final String TOPIC_PARTITIONS_CONFIG = EVENT_LOGGER_PREFIX + "topic.partitions";
  public static final int DEFAULT_TOPIC_PARTITIONS_CONFIG = 12;
  public static final String TOPIC_PARTITIONS_DOC = "Number of partitions in the event log topic.";
  public static final String TOPIC_REPLICAS_CONFIG = EVENT_LOGGER_PREFIX + "topic.replicas";
  public static final int DEFAULT_TOPIC_REPLICAS_CONFIG = 3;
  public static final String TOPIC_REPLICAS_DOC =
      "Number of replicas in the event log topic. It must not be higher than the number "
          + "of brokers in the KafkaExporter cluster.";
  public static final String TOPIC_RETENTION_MS_CONFIG =
      EVENT_LOGGER_PREFIX + "topic.retention.ms";
  public static final long DEFAULT_TOPIC_RETENTION_MS_CONFIG = TimeUnit.DAYS.toMillis(30);
  public static final String TOPIC_RETENTION_MS_DOC = "Retention time for the event log topic.";
  public static final String TOPIC_RETENTION_BYTES_CONFIG =
      EVENT_LOGGER_PREFIX + "topic.retention.bytes";
  public static final long DEFAULT_TOPIC_RETENTION_BYTES_CONFIG = -1L;
  public static final String TOPIC_RETENTION_BYTES_DOC = "Retention bytes for the event log topic.";
  public static final String TOPIC_ROLL_MS_CONFIG = EVENT_LOGGER_PREFIX + "topic.roll.ms";
  public static final long DEFAULT_TOPIC_ROLL_MS_CONFIG = TimeUnit.HOURS.toMillis(4);
  public static final String TOPIC_ROLL_MS_DOC = "Log rolling time for the event log topic.";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(
            EVENT_LOGGER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            DEFAULT_EVENT_LOGGER_CLASS_CONFIG,
            ConfigDef.Importance.HIGH,
            EVENT_LOGGER_CLASS_DOC
        ).define(
            EVENT_LOG_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_EVENT_LOG_NAME_CONFIG,
            ConfigDef.Importance.LOW,
            EVENT_LOG_NAME_DOC
        ).define(
            BOOTSTRAP_SERVERS_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            BOOTSTRAP_SERVERS_DOC
        ).define(
            EVENT_LOG_PRINCIPAL_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG,
            ConfigDef.Importance.LOW,
            EVENT_LOG_PRINCIPAL_DOC
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
        );
  }

  public EventLogConfig(Map<String, ?> configs) {
    super(CONFIG, configs);
  }


  private Map<String, Object> producerConfigDefaults() {
    Map<String, Object> defaults = new HashMap<>();
    defaults.putAll(EventProducerDefaults.PRODUCER_CONFIG_DEFAULTS);
    defaults.put(ProducerConfig.CLIENT_ID_CONFIG, "confluent-event-logger");
    return defaults;
  }

  public Properties producerProperties() {
    Properties props = new Properties();
    props.putAll(producerConfigDefaults());
    props.putAll(clientProperties());
    return props;
  }

  public Properties clientProperties() {
    Properties props = new Properties();
    for (Map.Entry<String, ?> entry : super.originals().entrySet()) {
      if (entry.getKey().startsWith(EVENT_LOGGER_PREFIX)) {
        props.put(entry.getKey().substring(EVENT_LOGGER_PREFIX.length()), entry.getValue());
      }
    }

    // we require bootstrap servers
    Object bootstrap = props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    if (bootstrap == null) {
      throw new ConfigException(
          "Missing required property "
              + EVENT_LOGGER_PREFIX
              + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
      );
    }
    return props;
  }

  public Map<String, String> topicConfig() {
    int topicReplicas = getInt(TOPIC_REPLICAS_CONFIG);
    // set minIsr to be consistent with
    // control center {@link io.confluent.controlcenter.util.TopicInfo.Builder.setReplication}
    Integer minIsr = Math.min(3, topicReplicas < 3 ? 1 : topicReplicas - 1);

    final Map<String, String> topicConfig = new HashMap<>();
    topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsr.toString());
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG,
        getLong(TOPIC_RETENTION_MS_CONFIG).toString());
    topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG,
        getLong(TOPIC_RETENTION_BYTES_CONFIG).toString());
    topicConfig.put(TopicConfig.SEGMENT_MS_CONFIG,
        getLong(TOPIC_ROLL_MS_CONFIG).toString());
    topicConfig.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name);

    return topicConfig;
  }

}
