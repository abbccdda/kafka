// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.security.store.kafka;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;

public class KafkaStoreConfig extends AbstractConfig {

  public static final String PREFIX = "confluent.metadata.";

  // The number of partitions in the security metadata topic _confluent-metadata-auth
  // is currently not configurable. This is to ensure that we can reliably wait for partition
  // metadata even if controller sends metadata updates with a subset of partitions in some
  // edge cases.
  public static final int NUM_PARTITIONS = 6;

  public static final String REPLICATION_FACTOR_PROP = "confluent.metadata.topic.replication.factor";
  private static final short REPLICATION_FACTOR_DEFAULT = 3;
  private static final String REPLICATION_FACTOR_DOC = "Replication factor of the metadata topic."
      + " This is used for creation of the topic if it doesn't exist. Replication factor cannot be"
      + " altered after the topic is created.";

  public static final String TOPIC_CREATE_TIMEOUT_PROP = "confluent.metadata.topic.create.timeout.ms";
  private static final int TOPIC_CREATE_TIMEOUT_DEFAULT = 300000;
  private static final String TOPIC_CREATE_TIMEOUT_DOC = "The number of milliseconds to wait for"
      + " metadata topic to be created during start up.";

  public static final String REFRESH_TIMEOUT_PROP = "confluent.metadata.refresh.timeout.ms";
  private static final int REFRESH_TIMEOUT_DEFAULT = 60000;
  private static final String REFRESH_TIMEOUT_DOC = "The number of milliseconds to wait for cache"
      + " to be refreshed after a write completes successfully.";

  public static final String RETRY_TIMEOUT_PROP = "confluent.metadata.retry.timeout.ms";
  private static final int RETRY_TIMEOUT_DEFAULT = 24 * 60 * 60 * 1000;
  private static final String RETRY_TIMEOUT_DOC = "Timeout for metadata store retries after which"
      + " the authorizer is marked as failed. All requests are denied access if all metadata reader or"
      + " writer operations fail for this duration.";

  public static final String BOOTSTRAP_SERVERS_PROP = PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

  public static final String TOPIC_PREFIX = "_confluent-metadata";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(REPLICATION_FACTOR_PROP, Type.SHORT, REPLICATION_FACTOR_DEFAULT,
            atLeast(1), Importance.LOW, REPLICATION_FACTOR_DOC)
        .define(TOPIC_CREATE_TIMEOUT_PROP, Type.INT, TOPIC_CREATE_TIMEOUT_DEFAULT,
            atLeast(1), Importance.LOW, TOPIC_CREATE_TIMEOUT_DOC)
        .define(REFRESH_TIMEOUT_PROP, Type.INT, REFRESH_TIMEOUT_DEFAULT,
            atLeast(1), Importance.LOW, REFRESH_TIMEOUT_DOC)
        .define(RETRY_TIMEOUT_PROP, Type.INT, RETRY_TIMEOUT_DEFAULT,
            atLeast(1), Importance.LOW, RETRY_TIMEOUT_DOC);
  }

  public final Duration topicCreateTimeout;
  public final Duration refreshTimeout;
  public final Duration retryTimeout;
  private final String brokerId;

  public KafkaStoreConfig(Map<?, ?> props) {
    super(CONFIG, props);

    topicCreateTimeout = Duration.ofMillis(getInt(TOPIC_CREATE_TIMEOUT_PROP));
    refreshTimeout = Duration.ofMillis(getInt(REFRESH_TIMEOUT_PROP));
    retryTimeout = Duration.ofMillis(getInt(RETRY_TIMEOUT_PROP));
    Object brokerId = props.get("broker.id");
    this.brokerId = brokerId == null ? "unknown" : String.valueOf(brokerId);
  }

  public Map<String, Object> consumerConfigs(String topic) {
    Map<String, Object> configs = baseConfigs();
    configs.putAll(originalsWithPrefix(PREFIX + "consumer."));

    configs.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("%s-consumer-%s", topic, brokerId));

    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    configs.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
    return configs;
  }

  public Map<String, Object> producerConfigs(String topic) {
    Map<String, Object> configs = baseConfigs();
    configs.putAll(originalsWithPrefix(PREFIX + "producer."));
    configs.put(ConsumerConfig.CLIENT_ID_CONFIG,  String.format("%s-producer-%s", topic, brokerId));

    configs.put(ProducerConfig.ACKS_CONFIG, "all");
    configs.put(ProducerConfig.RETRIES_CONFIG, "0");
    configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    return configs;
  }

  public Map<String, Object> coordinatorConfigs() {
    Map<String, Object> configs = baseConfigs();
    configs.putAll(originalsWithPrefix(PREFIX + "coordinator."));
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, TOPIC_PREFIX + "-coordinator-group");
    configs.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("%s-coordinator-%s", TOPIC_PREFIX, brokerId));

    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    configs.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

    // Consumer will be created with deserializer overrides
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return configs;
  }

  public Map<String, Object> adminClientConfigs() {
    Map<String, Object> configs = baseConfigs();
    configs.putAll(originalsWithPrefix(PREFIX + "admin."));
    configs.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format("%s-admin-%s", TOPIC_PREFIX, brokerId));
    return configs;
  }

  // Allow inheritance of security configs for reader/writer/coordinator
  private Map<String, Object> baseConfigs() {
    Map<String, Object> configs = originals();
    configs.putAll(originalsWithPrefix(PREFIX));
    configs.keySet().removeAll(originalsWithPrefix(PREFIX, false).keySet());
    CONFIG.names().stream().filter(name -> name.startsWith(PREFIX))
        .map(name -> name.substring(PREFIX.length()))
        .forEach(configs::remove);
    return configs;
  }

  public NewTopic metadataTopicCreateConfig(String topic, int numPartitions) {
    short replicationFactor = getShort(REPLICATION_FACTOR_PROP);
    Map<String, String> configs = new HashMap<>();
    configs.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer");
    String configPrefix = PREFIX + "topic.";
    originalsWithPrefix(configPrefix, true).forEach((k, v) -> {
      if (!CONFIG.names().contains(configPrefix + k))
        configs.put(k, String.valueOf(v));
    });
    configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    configs.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
    configs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(Math.min(replicationFactor, 2)));
    return  new NewTopic(topic, numPartitions, replicationFactor).configs(configs);
  }

  @Override
  public String toString() {
    return String.format("%s: %n\t%s", getClass().getName(), Utils.mkString(values(), "", "", "=", "%n\t"));
  }

  public static void main(String[] args) throws Exception {
    try (PrintStream out = args.length == 0 ? System.out
        : new PrintStream(new FileOutputStream(args[0]), false, StandardCharsets.UTF_8.name())) {
      out.println(CONFIG.toHtmlTable());
      if (out != System.out) {
        out.close();
      }
    }
  }
}
