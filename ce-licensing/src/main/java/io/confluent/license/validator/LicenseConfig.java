// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.license.validator;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import io.confluent.license.LicenseStore;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs.ClientType;
import org.apache.kafka.common.utils.Utils;

public class LicenseConfig extends AbstractConfig {

  public static final String PREFIX = "confluent.license.";
  public static final String PRODUCER_PREFIX = PREFIX + "producer.";
  public static final String CONSUMER_PREFIX = PREFIX + "consumer.";
  public static final String ADMIN_PREFIX = PREFIX + "admin.";

  public static final String LICENSE_PROP = "confluent.license";
  private static final String LICENSE_DEFAULT = "";
  private static final String LICENSE_DOC = "License for Confluent plugins.";

  public static final String TOPIC_PROP = "confluent.license.topic";
  public static final String TOPIC_DEFAULT = "_confluent-license";
  private static final String TOPIC_DOC = "Topic used for storing Confluent license";

  public static final String REPLICATION_FACTOR_PROP = "confluent.license.topic.replication.factor";
  private static final short REPLICATION_FACTOR_DEFAULT = 3;
  private static final String REPLICATION_FACTOR_DOC = "Replication factor of the license topic."
      + " This is used for creation of the topic if it doesn't exist. Replication factor cannot be"
      + " altered after the topic is created.";

  public static final String TOPIC_CREATE_TIMEOUT_PROP = "confluent.metadata.topic.create.timeout.ms";
  private static final int TOPIC_CREATE_TIMEOUT_DEFAULT = 600000;
  private static final String TOPIC_CREATE_TIMEOUT_DOC = "The number of milliseconds to wait for"
      + " license topic to be created during start up.";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(LICENSE_PROP, Type.STRING, LICENSE_DEFAULT, Importance.HIGH, LICENSE_DOC)
        .define(TOPIC_PROP, Type.STRING, TOPIC_DEFAULT, Importance.LOW, TOPIC_DOC)
        .define(REPLICATION_FACTOR_PROP, Type.SHORT, REPLICATION_FACTOR_DEFAULT,
            atLeast(1), Importance.LOW, REPLICATION_FACTOR_DOC)
        .define(TOPIC_CREATE_TIMEOUT_PROP, Type.INT, TOPIC_CREATE_TIMEOUT_DEFAULT,
            atLeast(1), Importance.LOW, TOPIC_CREATE_TIMEOUT_DOC);
  }

  public final String license;
  public final String topic;
  final Duration topicCreateTimeout;
  final int replicationFactor;
  private final String componentId;

  public LicenseConfig(String componentId, Map<?, ?> props) {
    super(CONFIG, props);
    this.componentId = componentId;
    license = getString(LICENSE_PROP);
    topic = getString(TOPIC_PROP);
    replicationFactor = getShort(REPLICATION_FACTOR_PROP);
    topicCreateTimeout = Duration.ofMillis(getInt(TOPIC_CREATE_TIMEOUT_PROP));
  }

  public Map<String, Object> producerConfigs() {
    return ConfluentConfigs.clientConfigs(this, PREFIX, ClientType.PRODUCER, TOPIC_DEFAULT, componentId);
  }

  public Map<String, Object> consumerConfigs() {
    return ConfluentConfigs.clientConfigs(this, PREFIX, ClientType.CONSUMER, TOPIC_DEFAULT, componentId);
  }

  public Map<String, Object> topicAndAdminClientConfigs() {
    Map<String, Object> configs =
        ConfluentConfigs.clientConfigs(this, PREFIX, ClientType.ADMIN, TOPIC_DEFAULT, componentId);
    configs.put(LicenseStore.REPLICATION_FACTOR_CONFIG, String.valueOf(replicationFactor));
    configs.put(LicenseStore.MIN_INSYNC_REPLICAS_CONFIG, String.valueOf(Math.min(replicationFactor, 2)));
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
