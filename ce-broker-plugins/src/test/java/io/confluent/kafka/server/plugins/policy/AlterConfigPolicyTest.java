// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.policy;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.server.plugins.policy.AlterConfigPolicy.ClusterPolicyConfig;
import java.util.Collections;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertThrows;

public class AlterConfigPolicyTest {
  private AlterConfigPolicy policy;
  private final short minIsr = 1;
  private final short replicationFactor = 3;

  @Before
  public void setUp() {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, Short.toString(replicationFactor));
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put(TopicPolicyConfig.MAX_MESSAGE_BYTES_MAX_CONFIG, "3145728");

    policy = new AlterConfigPolicy();
    policy.configure(config);
  }

  private RequestMetadata requestMetadataWithTopicConfigs(Map<String, String> topicConfigs) {
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.TOPIC, "dummy");
    return new RequestMetadata(cfgResource, topicConfigs, new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1")));
  }

  private RequestMetadata requestMetadataWithTopicConfigs() {
    Map<String, String> topicConfigs = new HashMap<>();
    topicConfigs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Short.toString(minIsr));
    topicConfigs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "4242");
    return requestMetadataWithTopicConfigs(topicConfigs);
  }

  @Test
  public void validateParamsSetOk() {
    policy.validate(requestMetadataWithTopicConfigs());
  }

  @Test
  public void validateNoParamsGivenOk() {
    policy.validate(requestMetadataWithTopicConfigs(Collections.emptyMap()));
  }

  @Test
  public void rejectDeleteRetentionMsTooHigh() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.DELETE_RETENTION_MS_CONFIG,
        "60566400001");
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void rejectSegmentBytesTooLow() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.SEGMENT_BYTES_CONFIG,
        Integer.toString(50 * 1024 * 1024 - 1));
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void rejectSegmentBytesTooHigh() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.SEGMENT_BYTES_CONFIG,
        "1073741825");
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void rejectSegmentMsTooLow() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.SEGMENT_MS_CONFIG,
        Long.toString(500 * 1000L));
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void validateAllAllowedProperties() {
    Map<String, String> topicConfigs = new HashMap<>();
    topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete");
    topicConfigs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100");
    topicConfigs.put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, "100");
    topicConfigs.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime");
    topicConfigs.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100");
    topicConfigs.put(TopicConfig.RETENTION_BYTES_CONFIG, "100");
    topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, "135217728");
    topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "600000");
    policy.validate(requestMetadataWithTopicConfigs(topicConfigs));
  }

  @Test
  public void rejectsSmallMinIsrs() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
        Integer.toString(minIsr - 1));
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void rejectsLargeMinIsrs() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
        Integer.toString(replicationFactor));
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void acceptsValidMinIsr() {
    // minIsr at the minimum allowed value
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
        Integer.toString(minIsr));
    policy.validate(requestMetadataWithTopicConfigs(topicConfigs));

    // minIsr at the maximum allowed value
    Map<String, String> topicConfigs2 = Collections.singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
        Integer.toString(replicationFactor - 1));
    policy.validate(requestMetadataWithTopicConfigs(topicConfigs2));
  }

  @Test
  public void rejectDisallowedConfigProperty1() {
    Map<String, String> topicConfigs = new HashMap<>();
    topicConfigs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100"); // allowed
    topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "100"); // disallowed
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void rejectDisallowedConfigProperty2() {
    Map<String, String> topicConfigs = Collections.singletonMap(ConfluentTopicConfig.TIER_ENABLE_CONFIG,
        "true");
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void allowAllTopicConfigChangesThroughInternalListener() {
    Map<String, String> topicConfigs = new HashMap<>();
    topicConfigs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100");
    topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "100");
    topicConfigs.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true");
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.TOPIC, "dummy");
    RequestMetadata requestMetadata = new RequestMetadata(cfgResource, topicConfigs,
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS"));
    policy.validate(requestMetadata);
  }

  @Test
  public void rejectMaxMessageBytesOutOfRange() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
        "4123123"); // above max configured limit.
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadataWithTopicConfigs(topicConfigs)));
  }

  @Test
  public void acceptMaxMessageBytesAtLimit() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
        "3145728"); // equal to max limit.
    policy.validate(requestMetadataWithTopicConfigs(topicConfigs));
  }

  @Test
  public void acceptMaxMessageBytesInRange() {
    Map<String, String> topicConfigs = Collections.singletonMap(TopicConfig.MAX_MESSAGE_BYTES_CONFIG,
        "10000");
    policy.validate(requestMetadataWithTopicConfigs(topicConfigs));
  }

  @Test
  public void allowBrokerConfigUpdatesFromInternalUser() {
    RequestMetadata brokerRequestMetadata = createBrokerRequestMetadata(
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS"));
    policy.validate(brokerRequestMetadata);
  }

  @Test
  public void allowClusterUpdatesForWhitelistedConfigsIfConfigEnabled() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = new HashMap<>();
    brokerConfigs.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    brokerConfigs.put(KafkaConfig.AutoCreateTopicsEnableProp(), "true");
    brokerConfigs.put(KafkaConfig.NumPartitionsProp(), "50");
    brokerConfigs.put(KafkaConfig.LogRetentionTimeMillisProp(), "7200000");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    policy.validate(requestMetadata);
  }

  @Test
  public void rejectClusterUpdatesIfAnyConfigIsNotWhitelisted() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = new HashMap<>();
    brokerConfigs.put(KafkaConfig.AuthorizerClassNameProp(), "SomeAuthorizer"); // not whitelisted
    brokerConfigs.put(KafkaConfig.AutoCreateTopicsEnableProp(), "true"); // whitelisted
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadata));
  }

  @Test
  public void rejectSpecificBrokerConfigUpdatesFromTenant() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadata));
  }

  @Test
  public void rejectBrokerConfigUpdatesFromTenantByDefault() {
    RequestMetadata brokerRequestMetadata = createBrokerRequestMetadata(
        new MultiTenantPrincipal("tenantUserA", new TenantMetadata("cluster1", "cluster1")));

    assertThrows(PolicyViolationException.class, () -> policy.validate(brokerRequestMetadata));
  }

  @Test
  public void allowClusterUpdatesWithValidSslCiphers() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, " +
            "tls_ecdhe_rsa_with_chacha20_poly1305_sha256," + // validation should be case insensitive
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    policy.validate(requestMetadata);
  }

  @Test
  public void rejectClusterUpdatesWithInvalidSslCiphers() {
    enableAlterClusterConfigs();

    // One valid cipher and one invalid cipher
    Map<String, String> brokerConfigs = Collections.singletonMap(SslConfigs.SSL_CIPHER_SUITES_CONFIG,
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadata));
  }

  @Test
  public void allowClusterUpdatesWithValidNumPartitionsMin() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.NumPartitionsProp(), "1");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    policy.validate(requestMetadata);
  }

  @Test
  public void allowClusterUpdatesWithValidNumPartitionsMax() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.NumPartitionsProp(), "100");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    policy.validate(requestMetadata);
  }

  @Test
  public void rejectClusterUpdatesWithInvalidNumPartitionsMin() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.NumPartitionsProp(), "0");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadata));
  }

  @Test
  public void rejectClusterUpdatesWithInvalidNumPartitionsMax() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.NumPartitionsProp(), "101");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadata));
  }

  @Test
  public void allowClusterUpdatesWithValidRetentionMsMin() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.LogRetentionTimeMillisProp(),
        "3600000");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    policy.validate(requestMetadata);
  }

  @Test
  public void allowClusterUpdatesWithValidRetentionMsMax() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.LogRetentionTimeMillisProp(),
        String.valueOf(Long.MAX_VALUE));
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    policy.validate(requestMetadata);
  }

  @Test
  public void rejectClusterUpdatesWithInvalidRetentionMsMin() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.LogRetentionTimeMillisProp(),
        "3599999");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadata));
  }

  @Test
  public void rejectClusterUpdatesWithInvalidRetentionMsNegative() {
    enableAlterClusterConfigs();

    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.LogRetentionTimeMillisProp(),
        "-1");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, "");
    RequestMetadata requestMetadata = new RequestMetadata(configResource, brokerConfigs, principal);

    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadata));
  }

  @Test
  public void rejectsBrokerLoggerUpdatesFromTenant() {
    RequestMetadata brokerRequestMetadata = createBrokerLoggerRequestMetadata(
        new MultiTenantPrincipal("tenantUserA", new TenantMetadata("cluster1", "cluster1")));

    assertThrows(PolicyViolationException.class, () -> policy.validate(brokerRequestMetadata));
  }

  @Test
  public void allowsBrokerLoggerUpdatesFromInternalUser() {
    RequestMetadata brokerRequestMetadata = createBrokerLoggerRequestMetadata(
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS"));
    policy.validate(brokerRequestMetadata);
  }

  private void enableAlterClusterConfigs() {
    Map<String, String> policyConfigs = new HashMap<>();
    policyConfigs.put(ClusterPolicyConfig.ALTER_ENABLE_CONFIG, "true");
    policyConfigs.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, Short.toString(replicationFactor));
    policy.configure(policyConfigs);
  }

  private RequestMetadata createBrokerRequestMetadata(KafkaPrincipal principal) {
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.BROKER, "dummy");
    Map<String, String> brokerConfigs = Collections.singletonMap(KafkaConfig.MessageMaxBytesProp(),
        "4242");
    return new RequestMetadata(cfgResource, brokerConfigs, principal);
  }

  private RequestMetadata createBrokerLoggerRequestMetadata(KafkaPrincipal principal) {
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "dummy");
    Map<String, String> loggerConfigs = Collections.singletonMap("kafka.tier.archiver.TierArchiver",
        "INFO");
    return new RequestMetadata(cfgResource, loggerConfigs, principal);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsUnknownTypeConfigs() {
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.UNKNOWN, "dummy");
    MultiTenantPrincipal principal = new MultiTenantPrincipal("tenantUserA",
        new TenantMetadata("cluster1", "cluster1"));
    RequestMetadata brokerRequestMetadata = new RequestMetadata(cfgResource, Collections.emptyMap(),
        principal);
    policy.validate(brokerRequestMetadata);
  }
}
