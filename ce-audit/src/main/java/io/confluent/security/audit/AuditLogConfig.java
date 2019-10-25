/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit;

import static io.confluent.events.EventLoggerConfig.CLOUD_EVENT_ENCODING_DOC;
import static io.confluent.events.EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING;
import static io.confluent.events.EventLoggerConfig.EVENT_LOGGER_PREFIX;
import static io.confluent.events.EventLoggerConfig.KAFKA_EXPORTER_PREFIX;
import static io.confluent.events.EventLoggerConfig.TOPIC_CONFIG;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.events.EventLoggerConfig;
import io.confluent.events.exporter.kafka.KafkaExporter;
import io.confluent.events.exporter.kafka.TopicSpec;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditLogConfig extends AbstractConfig {

  protected static final Logger log = LoggerFactory.getLogger(AuditLogConfig.class);

  public static final String AUDIT_PREFIX = "confluent.security.";
  public static final String AUDIT_EVENT_LOGGER_PREFIX = AUDIT_PREFIX + EVENT_LOGGER_PREFIX;
  public static final String AUDIT_EVENT_ROUTER_PREFIX = AUDIT_PREFIX + "event.router.";

  public static final String AUDIT_LOGGER_ENABLED_CONFIG = AUDIT_EVENT_LOGGER_PREFIX + "enabled";
  public static final String DEFAULT_AUDIT_LOGGER_ENABLED_CONFIG = "true";
  public static final String AUDIT_LOGGER_ENABLED_DOC = "Should the event logger be enabled.";

  public static final String AUDIT_CLOUD_EVENT_ENCODING_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.CLOUD_EVENT_ENCODING_CONFIG;
  public static final String DEFAULT_AUDIT_CLOUD_EVENT_ENCODING_CONFIG = CLOUD_EVENT_STRUCTURED_ENCODING;
  public static final String AUDIT_CLOUD_EVENT_ENCODING_DOC = CLOUD_EVENT_ENCODING_DOC;


  public static final String EVENT_EXPORTER_CLASS_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG;
  public static final String DEFAULT_EVENT_EXPORTER_CLASS_CONFIG = KafkaExporter.class
      .getCanonicalName();

  public static final String BOOTSTRAP_SERVERS_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG;

  public static final String TOPIC_CREATE_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.TOPIC_CREATE_CONFIG;
  public static final boolean DEFAULT_TOPIC_CREATE_CONFIG = true;

  public static final String TOPIC_PARTITIONS_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.TOPIC_PARTITIONS_CONFIG;
  public static final int DEFAULT_TOPIC_PARTITIONS_CONFIG = 12;

  public static final String TOPIC_REPLICAS_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.TOPIC_REPLICAS_CONFIG;
  public static final int DEFAULT_TOPIC_REPLICAS_CONFIG = 1;

  public static final String TOPIC_RETENTION_MS_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.TOPIC_RETENTION_MS_CONFIG;
  public static final long DEFAULT_TOPIC_RETENTION_MS_CONFIG = TimeUnit.DAYS.toMillis(30);

  public static final String TOPIC_RETENTION_BYTES_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.TOPIC_RETENTION_BYTES_CONFIG;
  public static final long DEFAULT_TOPIC_RETENTION_BYTES_CONFIG = -1L;

  public static final String TOPIC_ROLL_MS_CONFIG =
      AUDIT_PREFIX + EventLoggerConfig.TOPIC_ROLL_MS_CONFIG;
  public static final long DEFAULT_TOPIC_ROLL_MS_CONFIG = TimeUnit.HOURS.toMillis(4);

  // Producer defaults
  // ensure that our production is replicated
  private static final String DEFAULT_PRODUCER_ACKS_CONFIG = "all";
  private static final String DEFAULT_PRODUCER_COMPRESSION_TYPE_CONFIG = "lz4";
  private static final String DEFAULT_PRODUCER_INTERCEPTOR_CLASSES_CONFIG = "";
  private static final String DEFAULT_PRODUCER_KEY_SERIALIZER_CLASS_CONFIG = ByteArraySerializer.class
      .getName();
  private static final String DEFAULT_PRODUCER_LINGER_MS_CONFIG = "500";
  // default is 0, we would like to keep trying if possible
  private static final int DEFAULT_PRODUCER_RETRIES_CONFIG = 10;
  private static final long DEFAULT_PRODUCER_RETRY_BACKOFF_MS_CONFIG = 500;
  private static final String DEFAULT_PRODUCER_VALUE_SERIALIZER_CLASS_CONFIG = ByteArraySerializer.class
      .getName();
  private static final int DEFAULT_PRODUCER_MAX_BLOCK_MS_CONFIG = 0;

  public static final String AUDIT_LOG_PRINCIPAL_CONFIG = AUDIT_EVENT_ROUTER_PREFIX + "principal";
  public static final String DEFAULT_AUDIT_LOG_PRINCIPAL_CONFIG = "User:_confluent-security-event-logger";
  public static final String AUDIT_LOG_PRINCIPAL_DOC = "The principal that is used to produce log events";

  // Configuration for the EventTopicRouter
  public static final String ROUTER_CONFIG = AUDIT_EVENT_ROUTER_PREFIX + "config";
  public static final String DEFAULT_ROUTER = "";
  public static final String ROUTER_DOC = "JSON configuration for routing events to topics";

  public static final String ROUTER_CACHE_ENTRIES_CONFIG =
      AUDIT_EVENT_ROUTER_PREFIX + "cache.entries";
  public static final int DEFAULT_ROUTER_CACHE_ENTRIES = 10000;
  public static final String ROUTER_CACHE_ENTRIES_DOC = "Number of Resource entries that the router cache should support";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(
            AUDIT_LOGGER_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_AUDIT_LOGGER_ENABLED_CONFIG,
            ConfigDef.Importance.HIGH,
            AUDIT_LOGGER_ENABLED_DOC
        ).define(
            EVENT_EXPORTER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            DEFAULT_EVENT_EXPORTER_CLASS_CONFIG,
            ConfigDef.Importance.HIGH,
            EventLoggerConfig.EVENT_EXPORTER_CLASS_DOC
        ).define(
            AUDIT_LOG_PRINCIPAL_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_AUDIT_LOG_PRINCIPAL_CONFIG,
            ConfigDef.Importance.LOW,
            AUDIT_LOG_PRINCIPAL_DOC
        ).define(
            TOPIC_CREATE_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_TOPIC_CREATE_CONFIG,
            ConfigDef.Importance.LOW,
            EventLoggerConfig.TOPIC_CREATE_DOC
        ).define(
            TOPIC_PARTITIONS_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_TOPIC_PARTITIONS_CONFIG,
            ConfigDef.Importance.LOW,
            EventLoggerConfig.TOPIC_PARTITIONS_DOC
        ).define(
            TOPIC_REPLICAS_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_TOPIC_REPLICAS_CONFIG,
            ConfigDef.Importance.LOW,
            EventLoggerConfig.TOPIC_REPLICAS_DOC
        ).define(
            TOPIC_RETENTION_MS_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_RETENTION_MS_CONFIG,
            ConfigDef.Importance.LOW,
            EventLoggerConfig.TOPIC_RETENTION_MS_DOC
        ).define(
            TOPIC_RETENTION_BYTES_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_RETENTION_BYTES_CONFIG,
            ConfigDef.Importance.LOW,
            EventLoggerConfig.TOPIC_RETENTION_BYTES_DOC
        ).define(
            TOPIC_ROLL_MS_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_ROLL_MS_CONFIG,
            ConfigDef.Importance.LOW,
            EventLoggerConfig.TOPIC_ROLL_MS_DOC
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
        ).define(
            AUDIT_CLOUD_EVENT_ENCODING_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_AUDIT_CLOUD_EVENT_ENCODING_CONFIG,
            ConfigDef.Importance.LOW,
            AUDIT_CLOUD_EVENT_ENCODING_DOC
        );
  }

  public AuditLogConfig(Map<String, ?> configs) {
    super(CONFIG, configs);
  }

  public KafkaPrincipal eventLogPrincipal() {
    return SecurityUtils.parseKafkaPrincipal(getString(AUDIT_LOG_PRINCIPAL_CONFIG));
  }

  public static Properties kafkaProducerOverrides() {
    Properties props = new Properties();

    // ensure that our production is replicated
    props.put(ProducerConfig.ACKS_CONFIG, DEFAULT_PRODUCER_ACKS_CONFIG);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, DEFAULT_PRODUCER_COMPRESSION_TYPE_CONFIG);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        DEFAULT_PRODUCER_KEY_SERIALIZER_CLASS_CONFIG);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        DEFAULT_PRODUCER_VALUE_SERIALIZER_CLASS_CONFIG);
    props.put(ProducerConfig.LINGER_MS_CONFIG, DEFAULT_PRODUCER_LINGER_MS_CONFIG);
    props.put(ProducerConfig.RETRIES_CONFIG, DEFAULT_PRODUCER_RETRIES_CONFIG);
    props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
        DEFAULT_PRODUCER_INTERCEPTOR_CLASSES_CONFIG);
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, DEFAULT_PRODUCER_RETRY_BACKOFF_MS_CONFIG);
    // The producer should not block on metadata refreshes.
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, DEFAULT_PRODUCER_MAX_BLOCK_MS_CONFIG);

    return props;
  }

  public static Properties kafkaInterBrokerClientConfig(Map<String, ?> originals) {
    Properties props = new Properties();

    if (originals.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) != null) {
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
          originals.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    // inherit the inter-broker client / SASL / SSL  properties from the base level
    ConfigDef cfg = new ConfigDef();
    SaslConfigs.addClientSaslSupport(cfg);
    SslConfigs.addClientSslSupport(cfg);
    originals.entrySet().stream()
        // Filter out the audit log props as they are parsed later.
        .filter(e -> !e.getKey().startsWith(AuditLogConfig.AUDIT_PREFIX))
        // Choose only client props (Admin client has most CommonClientConfigs we care about)
        .filter(e -> cfg.names().contains(e.getKey()) || AdminClientConfig.configNames()
            .contains(e.getKey()))
        .filter(e -> e.getValue() instanceof String)
        .forEach(e -> props.setProperty(e.getKey(), String.valueOf(e.getValue())));
    return props;
  }

  public AuditLogRouterJsonConfig routerJsonConfig() throws ConfigException {
    try {
      String routerConfig = getString(ROUTER_CONFIG);
      if (routerConfig.isEmpty()) {
        return AuditLogRouterJsonConfig.defaultConfig();
      }
      return AuditLogRouterJsonConfig.load(getString(ROUTER_CONFIG));
    } catch (IllegalArgumentException | IOException e) {
      throw new ConfigException("Invalid router config", e);
    }
  }

  public static Map<String, Object> toEventLoggerConfig(Map<String, ?> originals)
      throws ConfigException {

    // Layer on config in the following order of precedence.
    // 1. Kafka Client overrides (interbroker client SASL / SSL props)
    // 2. Kafka Producer overrides
    // 3. AuditLogConfig defaults
    // 4. Any properties for event logger under audit namespace
    // 5. Bootstrap servers from Router JSON
    // 6. Topics JSON from Router JSON
    // 7. Set the blocking behavior of Event logger log() method to false.
    AuditLogConfig alc = new AuditLogConfig(originals);
    AuditLogRouterJsonConfig aljc = alc.routerJsonConfig();

    Map<String, Object> eventLoggerConfig = new HashMap<>();

    // 1. Kafka Client overrides (interbroker client SASL / SSL props)
    kafkaInterBrokerClientConfig(originals)
        .forEach((k, v) -> eventLoggerConfig.put(KAFKA_EXPORTER_PREFIX + k, v));

    // 2. Kafka Producer overrides
    kafkaProducerOverrides()
        .forEach((k, v) -> eventLoggerConfig.put(KAFKA_EXPORTER_PREFIX + k, v));

    // 3. AuditLogConfig defaults
    alc.values().entrySet().stream()
        .filter(e -> e.getKey().startsWith(AuditLogConfig.AUDIT_EVENT_LOGGER_PREFIX))
        .forEach(entry -> {
          eventLoggerConfig.put(entry.getKey().substring(AuditLogConfig.AUDIT_PREFIX.length()),
              entry.getValue());
        });

    // 4. Any properties for event logger under audit namespace
    // This is same method that is used to handle embedded producer/consumer properties.
    // Unwrap the properties and add it to the map that is passed on to the event logger.
    originals.entrySet().stream()
        .filter(e -> e.getKey().startsWith(AuditLogConfig.AUDIT_EVENT_LOGGER_PREFIX))
        .forEach(entry -> {
          eventLoggerConfig.put(entry.getKey().substring(AuditLogConfig.AUDIT_PREFIX.length()),
              entry.getValue());
        });

    // 5. Bootstrap servers from Router JSON
    if (aljc.bootstrapServers() != null && !aljc.bootstrapServers().isEmpty()) {
      eventLoggerConfig.put(EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG, aljc.bootstrapServers());
    }

    if (!eventLoggerConfig.containsKey(EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      // we must have bootstrap servers from one of these sources. If this fails, keep logging to file.
      throw new ConfigException(
          "Missing required property " + BOOTSTRAP_SERVERS_CONFIG + ". Either specify " +
              "bootstrap brokers in either '" + ROUTER_CONFIG + "' or '" + BOOTSTRAP_SERVERS_CONFIG
              + "' or '" +
              CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + "'");
    }

    // 6. Topics JSON from Router JSON
    Map<String, TopicSpec> allTopics = new HashMap<>();
    try {
      ObjectMapper mapper = new ObjectMapper();
      TopicSpec.Topics topics = new TopicSpec.Topics();

      // This property can be specified independently for the Kafka exporter. So, if it is set, merge this with
      // the router config.
      String topicConfig = (String) originals.get(AUDIT_PREFIX + TOPIC_CONFIG);
      if (topicConfig != null) {
        topics = mapper.readValue(topicConfig, TopicSpec.Topics.class);
        topics.topics.forEach(e -> allTopics.put(e.name(), e));
      }

      // Add (override if present) topics from the router config. This is to make sure that the topic settings
      // form the router config take precedence.
      aljc.destinations.topics.entrySet().stream().forEach(e -> {

        TopicSpec spec = TopicSpec.builder()
            .setName(e.getKey())
            .setTopicConfig(TopicConfig.RETENTION_MS_CONFIG, "" + e.getValue().retentionMs)
            .build();
        if (allTopics.containsKey(spec.name())) {
          log.warn("Overriding spec {} from {} with {} from {}",
              allTopics.get(spec.name()),
              AUDIT_PREFIX + TOPIC_CONFIG,
              spec,
              ROUTER_CONFIG);
        }
        allTopics.put(spec.name(), spec);
      });

      topics.setTopics(new ArrayList<>(allTopics.values()));
      eventLoggerConfig.put(TOPIC_CONFIG, mapper.writeValueAsString(topics));

      // 7.  Always set the event logger blocking behavior to false.
      eventLoggerConfig.put(EventLoggerConfig.EVENT_LOGGER_LOG_BLOCKING_CONFIG, false);

    } catch (IllegalArgumentException | IOException e) {
      throw new ConfigException("Invalid router config", e);
    }
    return eventLoggerConfig;
  }

}
