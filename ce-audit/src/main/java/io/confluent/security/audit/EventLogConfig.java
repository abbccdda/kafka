package io.confluent.security.audit;

import io.confluent.security.audit.appender.EventProducerDefaults;
import io.confluent.security.audit.appender.LogEventAppender;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

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
      EVENT_LOGGER_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = "Bootstrap servers for the KafkaEventLogger "
      + "event logs will be published to. The event logs cluster may be different from the cluster(s) "
      + "whose event logs are being collected. Several production KafkaEventLogger clusters can publish to a "
      + "single event logs cluster, for example.";

  public static final String EVENT_LOG_PRINCIPAL_CONFIG = EVENT_LOGGER_PREFIX + "principal";
  public static final String DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG = "User:_confluent-security-event-logger";
  public static final String EVENT_LOG_PRINCIPAL_DOC = "The principal that is used to produce log events";

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

  // If the router is not configured, it will default to sending both allowed and denied messages
  // to this topic
  public static final String DEFAULT_TOPIC = "_confluent-audit-log";
  // All event topics must begin with this prefix
  public static final String EVENT_TOPIC_PREFIX = EventLogConfig.DEFAULT_TOPIC;
  public static final String ROUTER_CONFIG = EVENT_LOGGER_PREFIX + "router.config";
  public static final String DEFAULT_ROUTER = "{\"default_topics\":{\"allowed\":\"" +
      DEFAULT_TOPIC + "\",\"denied\":\"" + DEFAULT_TOPIC + "\"}}";
  public static final String ROUTER_DOC = "JSON configuration for routing events to topics";

  public static final String ROUTER_CACHE_ENTRIES_CONFIG =
      EVENT_LOGGER_PREFIX + "router.cache.entries";
  public static final int DEFAULT_ROUTER_CACHE_ENTRIES = 10000;
  public static final String ROUTER_CACHE_ENTRIES_DOC = "Number of Resource entries that the router cache should support";

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
            ROUTER_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_ROUTER,
            ConfigDef.Importance.LOW,
            ROUTER_DOC
        ).define(
            ROUTER_CACHE_ENTRIES_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_ROUTER_CACHE_ENTRIES,
            ConfigDef.Importance.LOW,
            ROUTER_CACHE_ENTRIES_DOC
        );
  }

  public EventLogConfig(Map<String, ?> configs) {
    super(CONFIG, configs);
  }


  private Map<String, Object> producerConfigDefaults() {
    Map<String, Object> defaults = new HashMap<>();
    defaults.putAll(EventProducerDefaults.PRODUCER_CONFIG_DEFAULTS);
    defaults.put(CommonClientConfigs.CLIENT_ID_CONFIG, "confluent-event-logger");
    return defaults;
  }

  public KafkaPrincipal eventLogPrincipal() {
    return SecurityUtils.parseKafkaPrincipal(getString(EVENT_LOG_PRINCIPAL_CONFIG));
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
    Object bootstrap = props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    if (bootstrap == null) {
      throw new ConfigException(
          "Missing required property "
              + BOOTSTRAP_SERVERS_CONFIG
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

  public AuditLogRouterJsonConfig routerJsonConfig() throws ConfigException {
    try {
      return AuditLogRouterJsonConfig.load(
          getString(ROUTER_CONFIG));
    } catch (IllegalArgumentException | IOException e) {
      throw new ConfigException("Invalid router config", e);
    }
  }

}
