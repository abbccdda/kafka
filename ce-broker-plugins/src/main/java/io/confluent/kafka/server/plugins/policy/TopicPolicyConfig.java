// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.multitenant.MultiTenantConfigRestrictions;

public class TopicPolicyConfig extends AbstractConfig {

  public static final String BASE_PREFIX = "confluent.plugins.";
  public static final String TOPIC_PREFIX = TopicPolicyConfig.BASE_PREFIX + "topic.policy.";

  public static final String SEGMENT_BYTES_MIN_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "segment.bytes.min";
  public static final int DEFAULT_SEGMENT_BYTES_MIN = 50 * 1024 * 1024;
  protected static final String SEGMENT_BYTES_MIN_CONFIG_DOC =
      "The minimum allowed value for the segment.bytes topic config property.";

  public static final String SEGMENT_BYTES_MAX_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "segment.bytes.max";
  public static final int DEFAULT_SEGMENT_BYTES_MAX = 1024 * 1024 * 1024;
  protected static final String SEGMENT_BYTES_MAX_CONFIG_DOC =
      "The maximum allowed value for the segment.bytes topic config property.";

  public static final String SEGMENT_MS_MIN_CONFIG =
          TopicPolicyConfig.TOPIC_PREFIX + "segment.ms.min";
  public static final int DEFAULT_SEGMENT_MS_MIN = 4 * 60 * 60 * 1000;
  protected static final String SEGMENT_MS_MIN_CONFIG_DOC =
      "The minimum allowed value for the segment.ms topic config property.";

  public static final String RETENTION_MS_MAX_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "retention.ms.max";

  // allow max value until CCloud Pro can be special cased
  public static final long DEFAULT_RETENTION_MS_MAX = Long.MAX_VALUE;
  protected static final String RETENTION_MS_MAX_CONFIG_DOC =
      "The maximum allowed value for the retention.ms topic config property.";

  public static final String DELETE_RETENTION_MS_MAX_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "delete.retention.ms.max";
  // Note the value below is set high because it was used in some versions of c3
  public static final long DEFAULT_DELETE_RETENTION_MS_MAX = 60566400000L; // 2 years
  protected static final String DELETE_RETENTION_MS_MAX_CONFIG_DOC =
      "The maximum allowed value for the delete.retention.ms topic config property.";

  public static final String MAX_MESSAGE_BYTES_MAX_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "max.message.bytes.max";
  public static final int DEFAULT_MAX_MESSAGE_BYTES_MAX = 8 * 1024 * 1024;
  protected static final String MAX_MESSAGE_BYTES_MAX_CONFIG_DOC =
      "The maximum allowed value for the max.message.bytes topic config property.";

  public static final String REPLICATION_FACTOR_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "replication.factor";
  protected static final String REPLICATION_FACTOR_DOC =
      "The required replication factor for all topics if set.";

  public static final String MIN_IN_SYNC_REPLICAS_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "min.insync.replicas";
  protected static final String MIN_IN_SYNC_REPLICAS_CONFIG_DOC =
      "The required min.insync.replicas value if set.";

  public static final String MAX_PARTITIONS_PER_TENANT_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "max.partitions.per.tenant";
  public static final int DEFAULT_MAX_PARTITIONS_PER_TENANT = 512;
  protected static final String MAX_PARTITIONS_PER_TENANT_CONFIG_DOC =
      "The maximum partitions per tenant.";

  public static final String INTERNAL_LISTENER_CONFIG =
      TopicPolicyConfig.TOPIC_PREFIX + "internal.listener";
  public static final String DEFAULT_INTERNAL_LISTENER = "INTERNAL";
  protected static final String INTERNAL_LISTENER_CONFIG_DOC =
      "Internal listener to get bootstrap broker for AdminClient.";

  private static final ConfigDef CONFIG;

  static {
    CONFIG = new ConfigDef()
        .define(REPLICATION_FACTOR_CONFIG,
            ConfigDef.Type.SHORT,
            ConfigDef.Importance.HIGH,
            REPLICATION_FACTOR_DOC
        ).define(MIN_IN_SYNC_REPLICAS_CONFIG,
            ConfigDef.Type.SHORT,
            ConfigDef.Importance.HIGH,
            MIN_IN_SYNC_REPLICAS_CONFIG_DOC
        ).define(MAX_MESSAGE_BYTES_MAX_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_MAX_MESSAGE_BYTES_MAX,
            ConfigDef.Importance.HIGH,
            MAX_MESSAGE_BYTES_MAX_CONFIG_DOC
        ).define(SEGMENT_BYTES_MIN_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_SEGMENT_BYTES_MIN,
            ConfigDef.Importance.MEDIUM,
            SEGMENT_BYTES_MIN_CONFIG_DOC
        ).define(SEGMENT_BYTES_MAX_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_SEGMENT_BYTES_MAX,
            ConfigDef.Importance.MEDIUM,
            SEGMENT_BYTES_MAX_CONFIG_DOC
        ).define(SEGMENT_MS_MIN_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_SEGMENT_MS_MIN,
            ConfigDef.Importance.MEDIUM,
            SEGMENT_MS_MIN_CONFIG_DOC
        ).define(DELETE_RETENTION_MS_MAX_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_DELETE_RETENTION_MS_MAX,
            ConfigDef.Importance.MEDIUM,
            DELETE_RETENTION_MS_MAX_CONFIG_DOC
        ).define(RETENTION_MS_MAX_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_RETENTION_MS_MAX,
            ConfigDef.Importance.MEDIUM,
            RETENTION_MS_MAX_CONFIG_DOC
        ).define(MAX_PARTITIONS_PER_TENANT_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_MAX_PARTITIONS_PER_TENANT,
            ConfigDef.Importance.HIGH,
            MAX_PARTITIONS_PER_TENANT_CONFIG_DOC
        ).define(INTERNAL_LISTENER_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_INTERNAL_LISTENER,
            ConfigDef.Importance.HIGH,
            INTERNAL_LISTENER_CONFIG_DOC
        );
  }

  public TopicPolicyConfig(Map<String, ?> clientConfigs) {
    super(CONFIG, clientConfigs);
  }

  public TopicPolicyConfig(Properties props) {
    super(CONFIG, props);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toRst());
  }

  void validateConfigsAreUpdatable(Map<String, String> configs) {
    short requiredMinIsrs = getShort(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG);

    for (Map.Entry<String, String> entry : configs.entrySet()) {
      String configName = entry.getKey();
      if (!MultiTenantConfigRestrictions.UPDATABLE_TOPIC_CONFIGS.contains(configName)) {
        // preserve previous behavior for min.insync.replicas.
        if (configName.equals(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)) {
          if (Integer.parseInt(entry.getValue()) == requiredMinIsrs) {
            continue;
          }
          throw new PolicyViolationException(
              String.format("Config property '%s' must be set to %d or left empty.",
                            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, requiredMinIsrs)
          );
        }
        throw new PolicyViolationException(
            String.format("Altering config property '%s' is disallowed.", configName));
      }
    }
  }

  private void checkPolicyMax(Map<String, String> configs,
                              String maxPolicyConfigProperty,
                              String configProperty) {
    if (configs.containsKey(configProperty)) {
      long configuredValue = Long.parseLong(configs.get(configProperty));
      long policyMax = ((Number) get(maxPolicyConfigProperty)).longValue();
      if (configuredValue > policyMax) {
        throw new PolicyViolationException(
            String.format("Config property '%s' with value '%d' exceeded max limit of %d.",
                configProperty, configuredValue, policyMax));
      }
    }
  }

  private void checkPolicyMin(Map<String, String> configs,
                              String minPolicyConfigProperty,
                              String configProperty) {
    if (configs.containsKey(configProperty)) {
      long configuredValue = Long.parseLong(configs.get(configProperty));
      long policyMin = ((Number) get(minPolicyConfigProperty)).longValue();
      if (configuredValue < policyMin) {
        throw new PolicyViolationException(
            String.format("Config property '%s' with value '%d' exceeded min limit of %d.",
                configProperty, configuredValue, policyMin));
      }
    }
  }

  void validateConfigsAreInRange(Map<String, String> configs) {
    checkPolicyMax(configs, TopicPolicyConfig.MAX_MESSAGE_BYTES_MAX_CONFIG,
        TopicConfig.MAX_MESSAGE_BYTES_CONFIG);
    checkPolicyMax(configs, TopicPolicyConfig.DELETE_RETENTION_MS_MAX_CONFIG,
        TopicConfig.DELETE_RETENTION_MS_CONFIG);
    checkPolicyMax(configs, TopicPolicyConfig.RETENTION_MS_MAX_CONFIG,
        TopicConfig.RETENTION_MS_CONFIG);
    checkPolicyMax(configs, TopicPolicyConfig.SEGMENT_BYTES_MAX_CONFIG,
        TopicConfig.SEGMENT_BYTES_CONFIG);

    checkPolicyMin(configs, TopicPolicyConfig.SEGMENT_BYTES_MIN_CONFIG,
        TopicConfig.SEGMENT_BYTES_CONFIG);
    checkPolicyMin(configs, TopicPolicyConfig.SEGMENT_MS_MIN_CONFIG,
        TopicConfig.SEGMENT_MS_CONFIG);
  }

  public void validateTopicConfigs(Map<String, String> configs) {
    if (configs == null) {
      return;
    }
    validateConfigsAreUpdatable(configs);
    validateConfigsAreInRange(configs);
  }
}
