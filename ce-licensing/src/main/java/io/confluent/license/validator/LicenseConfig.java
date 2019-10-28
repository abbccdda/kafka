// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.license.validator;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import io.confluent.license.LicenseStore;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.Utils;

public class LicenseConfig extends AbstractConfig {

  public static final String PREFIX = "confluent.license.";
  public static final String PRODUCER_PREFIX = PREFIX + "producer.";
  public static final String CONSUMER_PREFIX = PREFIX + "consumer.";

  public static final String LICENSE_PROP = "confluent.license";
  private static final String LICENSE_DEFAULT = "";
  private static final String LICENSE_DOC = "License for Confluent plugins.";

  public static final String TOPIC_PROP = "confluent.license.topic";
  private static final String TOPIC_DEFAULT = "_confluent-license";
  private static final String TOPIC_DOC = "Topic used for storing Confluent license";

  public static final String REPLICATION_FACTOR_PROP = "confluent.license.topic.replication.factor";
  private static final short REPLICATION_FACTOR_DEFAULT = 3;
  private static final String REPLICATION_FACTOR_DOC = "Replication factor of the license topic."
      + " This is used for creation of the topic if it doesn't exist. Replication factor cannot be"
      + " altered after the topic is created.";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(LICENSE_PROP, Type.STRING, LICENSE_DEFAULT, Importance.HIGH, LICENSE_DOC)
        .define(TOPIC_PROP, Type.STRING, TOPIC_DEFAULT, Importance.LOW, TOPIC_DOC)
        .define(REPLICATION_FACTOR_PROP, Type.SHORT, REPLICATION_FACTOR_DEFAULT,
            atLeast(1), Importance.LOW, REPLICATION_FACTOR_DOC);
  }

  public final String license;
  public final String topic;
  private final int replicationFactor;
  private final String componentId;

  public LicenseConfig(String componentId, Map<?, ?> props) {
    super(CONFIG, props);
    this.componentId = componentId;
    license = getString(LICENSE_PROP);
    topic = getString(TOPIC_PROP);
    replicationFactor = getShort(REPLICATION_FACTOR_PROP);
  }

  public Map<String, Object> producerConfigs() {
    Map<String, Object> configs = baseConfigs();
    configs.putAll(originalsWithPrefix(PRODUCER_PREFIX));
    configs.putIfAbsent(CommonClientConfigs.CLIENT_ID_CONFIG,
          String.format("%s-producer-%s", TOPIC_DEFAULT, componentId));
    return configs;
  }

  public Map<String, Object> consumerConfigs() {
    Map<String, Object> configs = baseConfigs();
    configs.putAll(originalsWithPrefix(CONSUMER_PREFIX));
    configs.putIfAbsent(CommonClientConfigs.CLIENT_ID_CONFIG,
        String.format("%s-consumer-%s", TOPIC_DEFAULT, componentId));
    return configs;
  }

  public Map<String, Object> topicConfigs() {
    Map<String, Object> configs = baseConfigs();
    configs.put(LicenseStore.REPLICATION_FACTOR_CONFIG, String.valueOf(replicationFactor));
    configs.putIfAbsent(CommonClientConfigs.CLIENT_ID_CONFIG,
        String.format("%s-admin-%s", TOPIC_DEFAULT, componentId));
    return configs;
  }

  private Map<String, Object> baseConfigs() {
    Map<String, Object> configs = originals();
    configs.putAll(originalsWithPrefix(PREFIX));
    configs.keySet().removeAll(originalsWithPrefix(PREFIX, false).keySet());
    return configs;
  }

  @Override
  public String toString() {
    return String.format("%s: %n\t%s", getClass().getName(),
        Utils.mkString(values(), "", "", "=", "%n\t"));
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
