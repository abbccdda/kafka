// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import io.confluent.kafka.multitenant.MultiTenantConfigRestrictions;
import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlterConfigPolicy implements org.apache.kafka.server.policy.AlterConfigPolicy {
  private static final Logger log = LoggerFactory.getLogger(AlterConfigPolicy.class);

  public static class ClusterPolicyConfig extends AbstractPolicyConfig {
    public static final String CONFIG_PREFIX = "confluent.alter.cluster.configs.";

    public static final String ALTER_ENABLE_CONFIG = CONFIG_PREFIX + "enable";

    public static final String SSL_CIPHER_SUITES_ALLOWED_CONFIG = CONFIG_PREFIX + "ssl.cipher.suites.allowed";
    public static final List<String> DEFAULT_SSL_CIPHER_SUITES_ALLOWED = Arrays.asList(
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384"
    );
    private static final String SSL_CIPHER_SUITES_ALLOWED_DOC = "List of allowed cipher suites for TLS.";

    public static final String NUM_PARTITIONS_MIN_CONFIG = CONFIG_PREFIX +
        KafkaConfig.NumPartitionsProp() + ".min";
    public static final int DEFAULT_NUM_PARTITIONS_MIN = 1;
    private static final String NUM_PARTITIONS_MIN_DOC = "The minimum value allowed for the default number of partitions.";

    public static final String NUM_PARTITIONS_MAX_CONFIG = CONFIG_PREFIX +
        KafkaConfig.NumPartitionsProp() + ".max";
    public static final int DEFAULT_NUM_PARTITIONS_MAX = 100;
    private static final String NUM_PARTITIONS_MAX_DOC = "The maximum value allowed for the default number of partitions.";

    public static final String RETENTION_MS_MIN_CONFIG = CONFIG_PREFIX +
        KafkaConfig.LogRetentionTimeMillisProp() + ".min";
    // Restrict minimum default retention.ms to 1 hour. Note that we currently allow lower values when
    // setting the topic config for compatibility.
    public static final long DEFAULT_RETENTION_MS_MIN = TimeUnit.HOURS.toMillis(1);
    private static final String RETENTION_MS_MIN_DOC = "The minimum value allowed for retention.ms.";

    public static final String RETENTION_MS_MAX_CONFIG = CONFIG_PREFIX +
        KafkaConfig.LogRetentionTimeMillisProp() + ".max";
    public static final long DEFAULT_RETENTION_MS_MAX = TopicPolicyConfig.DEFAULT_RETENTION_MS_MAX;
    private static final String RETENTION_MS_MAX_DOC = "The maximum value allowed for retention.ms.";

    private static final ConfigDef CONFIG_DEF;

    static final String EXTERNAL_LISTENER_SSL_CIPHER_SUITES_CONFIG =
        MultiTenantConfigRestrictions.EXTERNAL_LISTENER_PREFIX + SslConfigs.SSL_CIPHER_SUITES_CONFIG;

    static {
      CONFIG_DEF = new ConfigDef()
        .define(ALTER_ENABLE_CONFIG,
          Type.BOOLEAN,
          false,
          Importance.MEDIUM,
          "Allow AlterConfigs for Broker configs from all listeners")
        .define(SSL_CIPHER_SUITES_ALLOWED_CONFIG,
          Type.LIST,
          DEFAULT_SSL_CIPHER_SUITES_ALLOWED,
          Importance.MEDIUM,
          SSL_CIPHER_SUITES_ALLOWED_DOC
        ).define(NUM_PARTITIONS_MIN_CONFIG,
          Type.LONG,
          DEFAULT_NUM_PARTITIONS_MIN,
          Importance.MEDIUM,
          NUM_PARTITIONS_MIN_DOC
        ).define(NUM_PARTITIONS_MAX_CONFIG,
          Type.LONG,
          DEFAULT_NUM_PARTITIONS_MAX,
          Importance.MEDIUM,
          NUM_PARTITIONS_MAX_DOC
        ).define(RETENTION_MS_MIN_CONFIG,
          Type.LONG,
          DEFAULT_RETENTION_MS_MIN,
          Importance.MEDIUM,
          RETENTION_MS_MIN_DOC
        ).define(RETENTION_MS_MAX_CONFIG,
          Type.LONG,
          DEFAULT_RETENTION_MS_MAX,
          Importance.MEDIUM,
          RETENTION_MS_MAX_DOC
        );
    }

    private final boolean alterConfigsEnabled;
    private final Set<String> allowedCipherSuites = new HashSet<>();

    ClusterPolicyConfig(Map<String, ?> configMap) {
      super(CONFIG_DEF, configMap);
      this.alterConfigsEnabled = getBoolean(ALTER_ENABLE_CONFIG);
      allowedCipherSuites.addAll(getList(SSL_CIPHER_SUITES_ALLOWED_CONFIG));
    }

    public void validateBrokerConfigs(RequestMetadata reqMetadata) {
      if (!alterConfigsEnabled)
        throw AlterConfigPolicy.configUpdateNotAllowed(reqMetadata);
      if (!reqMetadata.resource().name().isEmpty()) {
        log.debug("Not allowing update of BROKER configs for broker {}, we only support config updates for all brokers.",
            reqMetadata.resource().name());
        throw new PolicyViolationException("`ConfigResource.name` must be empty when updating broker configs " +
            "in order to update the configuration of all brokers consistently.");
      }

      Map<String, String> configs = reqMetadata.configs();
      PolicyUtils.validateConfigsAreUpdatable(configs, configName ->
          MultiTenantConfigRestrictions.UPDATABLE_BROKER_CONFIGS.contains(configName));

      checkPolicyMin(configs, RETENTION_MS_MIN_CONFIG, KafkaConfig.LogRetentionTimeMillisProp());
      checkPolicyMax(configs, RETENTION_MS_MAX_CONFIG, KafkaConfig.LogRetentionTimeMillisProp());

      checkPolicyMin(configs, NUM_PARTITIONS_MIN_CONFIG, KafkaConfig.NumPartitionsProp());
      checkPolicyMax(configs, NUM_PARTITIONS_MAX_CONFIG, KafkaConfig.NumPartitionsProp());

      checkSslCiphers(configs);
    }

    private void checkSslCiphers(Map<String, String> configs) {
      List<String> ciphers = parseList(configs, EXTERNAL_LISTENER_SSL_CIPHER_SUITES_CONFIG);
      if (ciphers == null)
        return;

      boolean foundInvalid = ciphers.stream().anyMatch(c ->
          !allowedCipherSuites.contains(c.toUpperCase(Locale.ROOT)));
      if (foundInvalid)
        throw new PolicyViolationException(invalidCipherSuiteMessage(allowedCipherSuites,
          configs.get(EXTERNAL_LISTENER_SSL_CIPHER_SUITES_CONFIG)));
    }

    // Visible for testing
    public static String invalidCipherSuiteMessage(Collection<String> allowedCipherSuites, String cipherSuites) {
      return EXTERNAL_LISTENER_SSL_CIPHER_SUITES_CONFIG + "=" + cipherSuites
           + " contains one or more invalid cipher suites. Allowed cipher suites: " + allowedCipherSuites;
    }

  }

  private ClusterPolicyConfig clusterPolicyConfig;
  private TopicPolicyConfig topicPolicyConfig;

  @Override
  public void configure(Map<String, ?> configMap) {
    this.clusterPolicyConfig = new ClusterPolicyConfig(configMap);
    this.topicPolicyConfig = new TopicPolicyConfig(configMap);
  }

  /**
   * We don't allow any config updates using the external listener where all principals are MultiTenantPrincipals.
   * On internal listeners with regular non-tenant principals, config updates are permitted, for example to update
   * SSL keystores without broker restart.
   */
  @Override
  public void validate(RequestMetadata reqMetadata) throws PolicyViolationException {
    KafkaPrincipal principal = reqMetadata.principal();

    if (principal == null)
      throw new IllegalArgumentException("Request principal not provided to validate alter policy");

    if (principal instanceof MultiTenantPrincipal) {
      switch (reqMetadata.resource().type()) {
        case TOPIC:
          log.trace("Validating request to update configs using principal {}", principal);
          topicPolicyConfig.validateTopicConfigs(reqMetadata.configs());
          break;
        case BROKER:
          clusterPolicyConfig.validateBrokerConfigs(reqMetadata);
          break;
        default:
          throw configUpdateNotAllowed(reqMetadata);
      }
    } else {
      log.info("Allowing update of configs using principal {}", principal);
    }
  }

  private static PolicyViolationException configUpdateNotAllowed(RequestMetadata reqMetadata) {
    log.debug("Not allowing update of {} configs using principal {}", reqMetadata.resource().type(),
        reqMetadata.principal());
    return new PolicyViolationException("Altering resources of type " + reqMetadata.resource().type() + " is not permitted");
  }

  @Override
  public void close() {}

}
