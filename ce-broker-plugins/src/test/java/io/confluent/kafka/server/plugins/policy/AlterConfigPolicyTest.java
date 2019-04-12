// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.policy;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AlterConfigPolicyTest {
  private AlterConfigPolicy policy;
  private RequestMetadata requestMetadata;

  @Before
  public void setUp() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, "5");
    config.put(TopicPolicyConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4");
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, "21");
    config.put(TopicPolicyConfig.MAX_MESSAGE_BYTES_MAX_CONFIG, "3145728");

    policy = new AlterConfigPolicy();
    policy.configure(config);
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "4")
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "4242")
        .build();

    requestMetadata = mock(RequestMetadata.class);
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.TOPIC, "dummy");
    when(requestMetadata.resource()).thenReturn(cfgResource);
  }

  @Test
  public void validateParamsSetOk() throws Exception {
    policy.validate(requestMetadata);
  }

  @Test
  public void validateNoParamsGivenOk() throws Exception {
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
  public void validateAllAllowedProperties() throws Exception {
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
  public void rejectsBadMinIsrs() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectDissallowedConfigProperty2() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100") // allowed
        .put(TopicConfig.SEGMENT_MS_CONFIG, "100") // disallowed
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectMaxMessageBytesOutOfRange() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "4123123") // above max configured limit.
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void acceptMaxMessageBytesAtLimit() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "3145728") // equal to max limit.
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void acceptMaxMessageBytesInRange() throws Exception {
    Map<String, String> topicConfigs = ImmutableMap.<String, String>builder()
        .put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "10000")
        .build();
    when(requestMetadata.configs()).thenReturn(topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsBrokerConfigUpdatesFromTenant() throws Exception {
    RequestMetadata brokerRequestMetadata = createBrokerRequestMetadata(
        new MultiTenantPrincipal("tenantUserA", new TenantMetadata("cluster1", "cluster1")));
    policy.validate(brokerRequestMetadata);
  }

  @Test
  public void allowsBrokerConfigUpdatesFromInternalUser() throws Exception {
    RequestMetadata brokerRequestMetadata = createBrokerRequestMetadata(
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

  @Test(expected = PolicyViolationException.class)
  public void rejectsUnknownTypeConfigs() throws Exception {
    RequestMetadata brokerRequestMetadata = mock(RequestMetadata.class);
    ConfigResource cfgResource = new ConfigResource(ConfigResource.Type.UNKNOWN, "dummy");
    when(brokerRequestMetadata.resource()).thenReturn(cfgResource);
    policy.validate(brokerRequestMetadata);
  }
}
