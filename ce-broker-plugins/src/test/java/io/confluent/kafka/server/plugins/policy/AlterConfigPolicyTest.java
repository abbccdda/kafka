// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.policy;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AlterConfigPolicyTest {
  private AlterConfigPolicy policy;
  private RequestMetadata requestMetadata;
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
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Short.toString(minIsr))
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "4242")
        .build();

    requestMetadata = mock(RequestMetadata.class);
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.TOPIC, "dummy");
    when(requestMetadata.resource()).thenReturn(cfgResource);
    when(requestMetadata.principal()).thenReturn(new MultiTenantPrincipal("tenantUserA", new TenantMetadata("cluster1", "cluster1")));
  }

  @Test
  public void validateParamsSetOk() {
    policy.validate(requestMetadata);
  }

  @Test
  public void validateNoParamsGivenOk() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectDeleteRetentionMsTooHigh() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "60566400001")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentBytesTooLow() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.SEGMENT_BYTES_CONFIG, "" + (50 * 1024 * 1024 - 1))
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentBytesTooHigh() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741825")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentMsTooLow() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
            .put(TopicConfig.SEGMENT_MS_CONFIG, "" + (500 * 1000L))
            .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void validateAllAllowedProperties() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100")
        .put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, "100")
        .put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime")
        .put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100")
        .put(TopicConfig.RETENTION_BYTES_CONFIG, "100")
        .put(TopicConfig.RETENTION_MS_CONFIG, "135217728")
        .put(TopicConfig.SEGMENT_MS_CONFIG, "600000")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsSmallMinIsrs() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minIsr - 1))
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsLargeMinIsrs() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(replicationFactor))
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void acceptsValidMinIsr() {
    // minIsr at the minimum allowed value
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minIsr))
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);

    // minIsr at the maximum allowed value
    Map<String, String> topicConfigs2 = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(replicationFactor - 1))
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs2);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectDisallowedConfigProperty1() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100") // allowed
        .put(TopicConfig.SEGMENT_MS_CONFIG, "100") // disallowed
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void rejectDisallowedConfigProperty2() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
            .put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
            .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    assertThrows(PolicyViolationException.class, () -> policy.validate(requestMetadata));
  }

  @Test
  public void allowAllConfigChangesThroughInternalListener() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
            .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100")
            .put(TopicConfig.SEGMENT_MS_CONFIG, "100")
            .put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
            .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    when(requestMetadata.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS"));
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectMaxMessageBytesOutOfRange() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "4123123") // above max configured limit.
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void acceptMaxMessageBytesAtLimit() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "3145728") // equal to max limit.
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void acceptMaxMessageBytesInRange() {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "10000")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsBrokerConfigUpdatesFromTenant() {
    RequestMetadata brokerRequestMetadata = createBrokerRequestMetadata(
        new MultiTenantPrincipal("tenantUserA", new TenantMetadata("cluster1", "cluster1")));
    policy.validate(brokerRequestMetadata);
  }

  @Test
  public void allowsBrokerConfigUpdatesFromInternalUser() {
    RequestMetadata brokerRequestMetadata = createBrokerRequestMetadata(
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS"));
    policy.validate(brokerRequestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsBrokerLoggerUpdatesFromTenant() throws Exception {
    RequestMetadata brokerRequestMetadata = createBrokerLoggerRequestMetadata(
        new MultiTenantPrincipal("tenantUserA", new TenantMetadata("cluster1", "cluster1")));
    policy.validate(brokerRequestMetadata);
  }

  @Test
  public void allowsBrokerLoggerUpdatesFromInternalUser() throws Exception {
    RequestMetadata brokerRequestMetadata = createBrokerLoggerRequestMetadata(
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS"));
    policy.validate(brokerRequestMetadata);
  }

  private RequestMetadata createBrokerRequestMetadata(KafkaPrincipal principal) {
    RequestMetadata brokerRequestMetadata = mock(RequestMetadata.class);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.BROKER, "dummy");
    when(brokerRequestMetadata.resource()).thenReturn(cfgResource);
    Map<String, String> brokerConfigs = ImmutableMap.<String, String>builder()
        .put(KafkaConfig$.MODULE$.MessageMaxBytesProp(), "4242")
        .build();
    when(brokerRequestMetadata.configs()).thenReturn(brokerConfigs);
    when(brokerRequestMetadata.principal()).thenReturn(principal);
    return brokerRequestMetadata;
  }

  private RequestMetadata createBrokerLoggerRequestMetadata(KafkaPrincipal principal) {
    RequestMetadata reqMetadata = mock(RequestMetadata.class);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "dummy");
    when(reqMetadata.resource()).thenReturn(cfgResource);
    Map<String, String> loggerConfigs = ImmutableMap.<String, String>builder()
        .put("kafka.tier.archiver.TierArchiver", "INFO")
        .build();
    when(reqMetadata.configs()).thenReturn(loggerConfigs);
    when(reqMetadata.principal()).thenReturn(principal);
    return reqMetadata;
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsUnknownTypeConfigs() {
    RequestMetadata brokerRequestMetadata = mock(RequestMetadata.class);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.UNKNOWN, "dummy");
    when(brokerRequestMetadata.resource()).thenReturn(cfgResource);
    when(brokerRequestMetadata.principal()).thenReturn(new MultiTenantPrincipal("tenantUserA", new TenantMetadata("cluster1", "cluster1")));
    policy.validate(brokerRequestMetadata);
  }
}
