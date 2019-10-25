/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.confluent.events.exporter.LogExporter;
import io.confluent.events.exporter.kafka.TopicSpec;
import java.io.IOException;
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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class EventLoggerConfig extends AbstractConfig {


  public static final String EVENT_LOGGER_PREFIX = "event.logger.";

  // Configuration for event exporters.
  public static final String EVENT_EXPORTER_CLASS_CONFIG = EVENT_LOGGER_PREFIX + "exporter.class";
  public static final String DEFAULT_EVENT_EXPORTER_CLASS_CONFIG = LogExporter.class
      .getCanonicalName();
  public static final String EVENT_EXPORTER_CLASS_DOC = "Class to use for delivering event logs.";

  public static final String EVENT_LOGGER_LOG_BLOCKING_CONFIG = EVENT_LOGGER_PREFIX + "blocking";
  public static final Boolean DEFAULT_EVENT_LOGGER_LOG_BLOCKING_CONFIG = true;
  public static final String EVENT_LOGGER_LOG_BLOCKING_CONFIG_DOC = "Block for topic creation in the log(...) method.";

  // Configuration for LogExporter
  public static final String LOG_EVENT_EXPORTER_NAME_CONFIG =
      EVENT_LOGGER_PREFIX + "exporter.log.name";
  public static final String DEFAULT_LOG_EVENT_EXPORTER_NAME_CONFIG = "event";
  public static final String LOG_EVENT_EXPORTER_NAME_DOC = "Name for the logger, used in getLogger()";

  // Configuration for Kafka exporter
  public static final String KAFKA_EXPORTER_PREFIX = EVENT_LOGGER_PREFIX + "exporter.kafka.";

  public static final String BOOTSTRAP_SERVERS_CONFIG =
      KAFKA_EXPORTER_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = "Bootstrap servers for the KafkaExporter "
      + "event logs will be published to. The event logs cluster may be different from the cluster(s) "
      + "whose event logs are being collected. Several production KafkaExporter clusters can publish to a "
      + "single event logs cluster, for example.";

  public static final String REQUEST_TIMEOUT_MS_CONFIG =
      KAFKA_EXPORTER_PREFIX + AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG;
  // Same as the Adminclient timeout.
  public static final int DEFAULT_REQUEST_TIMEOUT_MS = 12000;

  public static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

  // Allow the Kafka exporter to create topics ?
  public static final String TOPIC_CREATE_CONFIG = KAFKA_EXPORTER_PREFIX + "topic.create";
  public static final boolean DEFAULT_TOPIC_CREATE_CONFIG = true;
  public static final String TOPIC_CREATE_DOC = "Create the event log topic if it does not exist.";

  // Default topic configuration. Used if the properties for the topics is not specified.
  public static final String TOPIC_PARTITIONS_CONFIG = KAFKA_EXPORTER_PREFIX + "topic.partitions";
  public static final int DEFAULT_TOPIC_PARTITIONS_CONFIG = 12;
  public static final String TOPIC_PARTITIONS_DOC = "Number of partitions in the event log topic.";

  public static final String TOPIC_REPLICAS_CONFIG = KAFKA_EXPORTER_PREFIX + "topic.replicas";
  public static final int DEFAULT_TOPIC_REPLICAS_CONFIG = 3;
  public static final String TOPIC_REPLICAS_DOC =
      "Number of replicas in the event log topic. It must not be higher than the number "
          + "of brokers in the KafkaExporter cluster.";

  public static final String TOPIC_RETENTION_MS_CONFIG =
      KAFKA_EXPORTER_PREFIX + "topic.retention.ms";
  public static final long DEFAULT_TOPIC_RETENTION_MS_CONFIG = TimeUnit.DAYS.toMillis(30);
  public static final String TOPIC_RETENTION_MS_DOC = "Retention time for the event log topic.";

  public static final String TOPIC_RETENTION_BYTES_CONFIG =
      KAFKA_EXPORTER_PREFIX + "topic.retention.bytes";
  public static final long DEFAULT_TOPIC_RETENTION_BYTES_CONFIG = -1L;
  public static final String TOPIC_RETENTION_BYTES_DOC = "Retention bytes for the event log topic.";

  public static final String TOPIC_ROLL_MS_CONFIG = KAFKA_EXPORTER_PREFIX + "topic.roll.ms";
  public static final long DEFAULT_TOPIC_ROLL_MS_CONFIG = TimeUnit.HOURS.toMillis(4);
  public static final String TOPIC_ROLL_MS_DOC = "Log rolling time for the event log topic.";

  // This is default "catch all" topic for all events who dont have any routes configured.
  public static final String DEFAULT_TOPIC = "_confluent-events";
  public static final String TOPIC_CONFIG = KAFKA_EXPORTER_PREFIX + "topic.config";
  public static final String DEFAULT_TOPIC_CONFIG =
      "{\"topics\":[{\"name\":\"" + DEFAULT_TOPIC + "\"}]}";
  public static final String TOPIC_CONFIG_DOC = "JSON configuration for managing topics for the Kafka exporter.";

  // Producer defaults
  public static final String CLOUD_EVENT_STRUCTURED_ENCODING = "structured";
  public static final String CLOUD_EVENT_BINARY_ENCODING = "binary";
  public static final String CLOUD_EVENT_ENCODING_CONFIG = EVENT_LOGGER_PREFIX + "cloudevent.codec";
  public static final String DEFAULT_CLOUD_EVENT_ENCODING_CONFIG = CLOUD_EVENT_STRUCTURED_ENCODING;
  public static final String CLOUD_EVENT_ENCODING_DOC =
      "Which cloudevent encoding to use. By default, the Kafka exporter uses" +
          "structured encoding.";
  // ensure that our production is replicated
  private static final String DEFAULT_PRODUCER_ACKS_CONFIG = "all";
  private static final String DEFAULT_PRODUCER_COMPRESSION_TYPE_CONFIG = "lz4";
  private static final String DEFAULT_PRODUCER_KEY_SERIALIZER_CLASS_CONFIG = ByteArraySerializer.class
      .getName();
  private static final String DEFAULT_PRODUCER_VALUE_SERIALIZER_CLASS_CONFIG = ByteArraySerializer.class
      .getName();
  private static final String DEFAULT_PRODUCER_LINGER_MS_CONFIG = "500";
  // ensure that our requests are accepted in order
  private static final int DEFAULT_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 1;
  private static final long DEFAULT_PRODUCER_RETRY_BACKOFF_MS_CONFIG = 500;
  private static final ConfigDef CONFIG;
  private static final String DEFAULT_MIN_ISR = "1";

  static {
    CONFIG = new ConfigDef()
        .define(
            EVENT_LOGGER_LOG_BLOCKING_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_EVENT_LOGGER_LOG_BLOCKING_CONFIG,
            ConfigDef.Importance.HIGH,
            EVENT_LOGGER_LOG_BLOCKING_CONFIG_DOC
        ).define(
            EVENT_EXPORTER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            DEFAULT_EVENT_EXPORTER_CLASS_CONFIG,
            ConfigDef.Importance.HIGH,
            EVENT_EXPORTER_CLASS_DOC
        ).define(
            LOG_EVENT_EXPORTER_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_LOG_EVENT_EXPORTER_NAME_CONFIG,
            ConfigDef.Importance.LOW,
            LOG_EVENT_EXPORTER_NAME_DOC
        ).define(
            BOOTSTRAP_SERVERS_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.LOW,
            BOOTSTRAP_SERVERS_DOC
        ).define(
            REQUEST_TIMEOUT_MS_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_REQUEST_TIMEOUT_MS,
            ConfigDef.Importance.LOW,
            REQUEST_TIMEOUT_MS_DOC
        )
        .define(
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
            TOPIC_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_TOPIC_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_CONFIG_DOC
        ).define(
            TOPIC_ROLL_MS_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_ROLL_MS_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_ROLL_MS_DOC
        ).define(
            CLOUD_EVENT_ENCODING_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_CLOUD_EVENT_ENCODING_CONFIG,
            ConfigDef.Importance.LOW,
            CLOUD_EVENT_ENCODING_DOC
        );
  }

  public EventLoggerConfig(Map<String, ?> configs) {
    super(CONFIG, configs);
  }

  private Map<String, Object> producerConfigDefaults() {
    Map<String, Object> defaults = ImmutableMap.<String, Object>builder()
        .put(ProducerConfig.ACKS_CONFIG, DEFAULT_PRODUCER_ACKS_CONFIG)
        .put(ProducerConfig.COMPRESSION_TYPE_CONFIG, DEFAULT_PRODUCER_COMPRESSION_TYPE_CONFIG)
        .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            DEFAULT_PRODUCER_KEY_SERIALIZER_CLASS_CONFIG)
        .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            DEFAULT_PRODUCER_VALUE_SERIALIZER_CLASS_CONFIG)
        .put(ProducerConfig.LINGER_MS_CONFIG, DEFAULT_PRODUCER_LINGER_MS_CONFIG)
        .put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, DEFAULT_PRODUCER_RETRY_BACKOFF_MS_CONFIG)
        .put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
            DEFAULT_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)
        .put(CommonClientConfigs.CLIENT_ID_CONFIG, "confluent-event-logger")
        .build();
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
      if (entry.getKey().startsWith(KAFKA_EXPORTER_PREFIX)) {
        props.put(entry.getKey().substring(KAFKA_EXPORTER_PREFIX.length()), entry.getValue());
      }
    }

    // we require bootstrap servers
    if (!props.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new ConfigException("Missing required property " + BOOTSTRAP_SERVERS_CONFIG);
    }
    return props;
  }

  public Map<String, String> defaultTopicConfig() {
    final Map<String, String> topicConfig = new HashMap<>();

    // Set default min.isr to 1. https://github.com/confluentinc/ce-kafka/pull/772
    topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, DEFAULT_MIN_ISR);

    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, getLong(TOPIC_RETENTION_MS_CONFIG).toString());
    topicConfig
        .put(TopicConfig.RETENTION_BYTES_CONFIG, getLong(TOPIC_RETENTION_BYTES_CONFIG).toString());
    topicConfig.put(TopicConfig.SEGMENT_MS_CONFIG, getLong(TOPIC_ROLL_MS_CONFIG).toString());
    topicConfig.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name);

    return topicConfig;
  }

  public Map<String, TopicSpec> getTopicSpecs() throws ConfigException {

    Map<String, TopicSpec> allTopics = new HashMap<>();
    try {
      ObjectMapper mapper = new ObjectMapper();

      TopicSpec.Topics topics = mapper.readValue(getString(TOPIC_CONFIG), TopicSpec.Topics.class);
      topics.topics().stream().forEach(t -> allTopics.put(t.name(), t));

    } catch (IllegalArgumentException | IOException e) {
      throw new ConfigException(TOPIC_CONFIG, get(TOPIC_CONFIG),
          "Invalid router config: " + e.getMessage());
    }
    return allTopics;
  }


}
