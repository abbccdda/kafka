// (Copyright) [2017 - 2017] Confluent, Inc.
package io.confluent.kafka.server.plugins.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import java.util.LinkedHashMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTopicPolicyTest {
  private static final String TENANT = "xx";
  private static final String TOPIC = "xx_test-topic";
  private static final short REPLICATION_FACTOR = 3;
  private static final short MIN_IN_SYNC_REPLICAS = 1;
  private static final int MAX_PARTITIONS = 21;
  private static final int MAX_MESSAGE_BYTES = 4242;

  private CreateTopicPolicy policy;
  private Map<String, String> topicConfigs;

  @Before
  public void setUp() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG, String.valueOf(REPLICATION_FACTOR));
    config.put(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG, String.valueOf(MAX_PARTITIONS));
    policy = new CreateTopicPolicy();
    policy.configure(config);

    topicConfigs = new HashMap<>();
    topicConfigs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(MAX_MESSAGE_BYTES));
    topicConfigs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(MIN_IN_SYNC_REPLICAS));
    topicConfigs = Collections.unmodifiableMap(topicConfigs);
  }

  @Test
  public void testValidateOk() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    final int requestedPartitions = MAX_PARTITIONS - currentPartitions - 1;
    updateTopicMetadata(Collections.singletonMap(TOPIC, currentPartitions));
    policy.ensureValidPartitionCount(TENANT, requestedPartitions);
  }

  @Test
  public void acceptsExactlyMaxPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    final int requestedPartitions = MAX_PARTITIONS - currentPartitions;
    updateTopicMetadata(Collections.singletonMap(TOPIC, currentPartitions));
    policy.ensureValidPartitionCount(TENANT, requestedPartitions);
  }

  @Test
  public void testValidateDoesNotCountOtherTopicPartitions() {
    updateTopicMetadata(Collections.singletonMap(TOPIC, MAX_PARTITIONS));
    policy.ensureValidPartitionCount("badprefix_", MAX_PARTITIONS);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsRequestOverMaxNumberOfPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    final int requestedPartitions = MAX_PARTITIONS - currentPartitions + 1;
    updateTopicMetadata(Collections.singletonMap(TOPIC, currentPartitions));
    policy.ensureValidPartitionCount(TENANT, requestedPartitions);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsCurrentExceedMaxNumberOfPartitions() {
    final int currentPartitions = MAX_PARTITIONS / 2;
    updateTopicMetadata(Collections.singletonMap(TOPIC, currentPartitions));
    policy.ensureValidPartitionCount(TENANT, MAX_PARTITIONS + 1);
  }

  @Test(expected = NotControllerException.class)
  public void rejectsWhenTopicMetadataNotSet() {
    policy.ensureValidPartitionCount(TENANT, 1);
  }

  @Test
  public void returnsZeroWhenTopicMetadataSetButTenantMissing() {
    updateTopicMetadata(Collections.singletonMap(TOPIC, 5));
    String nonExistingTenant = "axx_";
    policy.ensureValidPartitionCount(nonExistingTenant, 0);
  }

  @Test
  public void validateParamsSetOk() {
    updateTopicMetadata(Collections.emptyMap());
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void validateNoReplicationNoTopicConfigGivenOk() {
    updateTopicMetadata(Collections.singletonMap(TOPIC, 5));
    Map<String, String> topicConfigs = Collections.emptyMap();
    RequestMetadata requestMetadata = new RequestMetadata(TOPIC, 10, null, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void validateValidTopicConfigsOk() {
    updateTopicMetadata(Collections.singletonMap(TOPIC, 5));
    Map<String, String> topicConfigs = new HashMap<>();
    topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete");
    topicConfigs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "100");
    topicConfigs.put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, "100");
    topicConfigs.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime");
    topicConfigs.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100");
    topicConfigs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(REPLICATION_FACTOR - 1));
    topicConfigs.put(TopicConfig.RETENTION_BYTES_CONFIG, "100");
    topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, "135217728");
    topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, "600000");
    topicConfigs = Collections.unmodifiableMap(topicConfigs);
    RequestMetadata requestMetadata = new RequestMetadata(TOPIC, 10, null, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void validateValidPartitionAssignmentOk() {
    updateTopicMetadata(Collections.singletonMap(TOPIC, 5));
    List<Integer> part0Assignment = Arrays.asList(0, 1, 2);
    List<Integer> part1Assignment = Arrays.asList(3, 4, 5);

    HashMap<Integer, List<Integer>> replicaAssignments = new HashMap<>();
    replicaAssignments.put(0, part0Assignment);
    replicaAssignments.put(1, part1Assignment);

    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, null, null,
        replicaAssignments,
        topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void validatePartitionAssignmentWithInvalidNumberOfReplicasNotOk() {
    List<Integer> part0Assignment = Arrays.asList(0, 1, 2, 3, 4, 5);
    List<Integer> part1Assignment = Arrays.asList(1, 2, 3, 4, 5, 6);

    HashMap<Integer, List<Integer>> replicaAssignments = new HashMap<>();
    replicaAssignments.put(0, part0Assignment);
    replicaAssignments.put(1, part1Assignment);

    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, null, null,
        replicaAssignments,
        topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void validatePartitionAssignmentWithNoReplicasNotOk() throws Exception {
    List<Integer> emptyAssignment = Collections.emptyList();

    HashMap<Integer, List<Integer>> replicaAssignments = new HashMap<>();
    replicaAssignments.put(0, emptyAssignment);
    replicaAssignments.put(1, emptyAssignment);

    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, null, null,
        replicaAssignments,
        topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void validateInvalidTopicConfigsNotOk() throws Exception {
    HashMap<String, String> topicConfigs = new HashMap<>();
    topicConfigs.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100");
    topicConfigs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(REPLICATION_FACTOR)); // disallowed
    topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, "135217728"); // allowed
    RequestMetadata requestMetadata = new RequestMetadata(TOPIC, 10, null, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsNoPartitionCountGiven() throws Exception {
    Map<String, String> topicConfigs = Collections.emptyMap();
    RequestMetadata requestMetadata = new RequestMetadata(TOPIC, null, null, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsBadRepFactor() throws Exception {
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, MAX_PARTITIONS, (short) 6, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsSmallMinIsrs() throws Exception {
    Map<String, String> topicConfigs = Collections.
            singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(MIN_IN_SYNC_REPLICAS - 1));
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectsLargeMinIsrs() throws Exception {
    Map<String, String> topicConfigs = Collections.
            singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(REPLICATION_FACTOR));
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void acceptsValidMinIsr() throws Exception {
    Map<String, Integer> topicPartitions = new HashMap<>();
    topicPartitions.put("xyz_foo", 3);
    updateTopicMetadata(topicPartitions);

    // minIsr at the minimum allowed value
    Map<String, String> topicConfigs = Collections.
            singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(MIN_IN_SYNC_REPLICAS));
    RequestMetadata requestMetadata = new RequestMetadata(
            TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);

    // minIsr at the maximum allowed value
    Map<String, String> topicConfigs2 = Collections.
            singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(REPLICATION_FACTOR - 1));
    RequestMetadata requestMetadata2 = new RequestMetadata(
            TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs2);
    policy.validate(requestMetadata2);
  }

  @Test(expected = RuntimeException.class)
  public void rejectsBadNumPartitions() throws Exception {
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, 22, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectDeleteRetentionMsTooHigh() {
    Map<String, String> topicConfigs = Collections.
            singletonMap(TopicConfig.DELETE_RETENTION_MS_CONFIG, "60566400001");
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentBytesTooLow() {
    Map<String, String> topicConfigs = Collections.
            singletonMap(TopicConfig.SEGMENT_BYTES_CONFIG, "" + (50 * 1024 * 1024 - 1));
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentBytesTooHigh() {
    Map<String, String> topicConfigs = Collections.
            singletonMap(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741825");
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test(expected = PolicyViolationException.class)
  public void rejectSegmentMsTooLow() {
    Map<String, String> topicConfigs = Collections.
            singletonMap(TopicConfig.SEGMENT_MS_CONFIG, "" + (500 * 1000));
    RequestMetadata requestMetadata = new RequestMetadata(
        TOPIC, MAX_PARTITIONS, REPLICATION_FACTOR, null, topicConfigs);
    policy.validate(requestMetadata);
  }

  @Test
  public void testNumPartitions() {
    Map<String, Integer> topicPartitions0 = new HashMap<>();
    topicPartitions0.put("xyz_foo", 3);
    topicPartitions0.put("xyz_bar", 3);
    topicPartitions0.put(Topic.GROUP_METADATA_TOPIC_NAME, 3);
    topicPartitions0.put("_confluent_metrics", 3);
    updateTopicMetadata(topicPartitions0);
    assertEquals(6, policy.numPartitions("xyz"));

    // Using tenant prefix does not work, tenant must be used
    assertEquals(0, policy.numPartitions("xyz_"));

    // Internal topics are ignored
    assertEquals(0, policy.numPartitions("_"));
    assertEquals(0, policy.numPartitions(""));

    Map<String, Integer> topicPartitions1 = new HashMap<>();
    topicPartitions1.put("xyz_foo", 3);
    topicPartitions1.put(Topic.GROUP_METADATA_TOPIC_NAME, 3);
    updateTopicMetadata(topicPartitions1);
    assertEquals(3, policy.numPartitions("xyz"));
  }

  @Test
  public void testNumPartitionMetrics() {
    Metrics metrics = new Metrics();
    policy.registerMetrics(metrics);

    Map<String, Integer> topicPartitions0 = new HashMap<>();
    topicPartitions0.put("abc_foo", 2);
    topicPartitions0.put("abc_bar", 2);
    topicPartitions0.put("xyz_foo", 3);
    topicPartitions0.put("xyz_bar", 3);
    updateTopicMetadata(topicPartitions0);
    assertEquals(4, policy.numPartitions("abc"));
    assertEquals(6, policy.numPartitions("xyz"));
    assertEquals(Integer.valueOf(4), metricValue(metrics, "abc"));
    assertEquals(Integer.valueOf(6), metricValue(metrics, "xyz"));

    Map<String, Integer> topicPartitions1 = new HashMap<>();
    topicPartitions1.put("xyz_foo", 3);
    updateTopicMetadata(topicPartitions1);
    assertEquals(0, policy.numPartitions("abc"));
    assertEquals(3, policy.numPartitions("xyz"));
    assertEquals(Integer.valueOf(0), metricValue(metrics, "abc"));
    assertEquals(Integer.valueOf(3), metricValue(metrics, "xyz"));

    Map<String, Integer> topicPartitions2 = new HashMap<>();
    topicPartitions2.put("xyz_foo", 1);
    updateTopicMetadata(topicPartitions2);
    assertEquals(0, policy.numPartitions("abc"));
    assertEquals(1, policy.numPartitions("xyz"));
    assertNull(metricValue(metrics, "abc"));
    assertEquals(Integer.valueOf(1), metricValue(metrics, "xyz"));

    metrics.close();
  }

  private Integer metricValue(Metrics metrics, String tenant) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put(TenantMetrics.TENANT_TAG, tenant);
    MetricName name = metrics.metricName("partitions", TenantMetrics.GROUP, "", tags);
    KafkaMetric metric = metrics.metric(name);
    if (metric != null) {
      return (Integer) metric.metricValue();
    } else {
      return null;
    }
  }

  private void updateTopicMetadata(Map<String, Integer> topicToNumPartitions) {
    policy.topicMetadataUpdated(createTopicPartitions(topicToNumPartitions));
  }

  private static List<TopicPartition> createTopicPartitions(Map<String, Integer> topicToNumPartitions) {
    List<TopicPartition> topicPartitions = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : topicToNumPartitions.entrySet()) {
      String topic = entry.getKey();
      int numPartitions = entry.getValue();
      for (int i = 0; i < numPartitions; i++) {
        topicPartitions.add(new TopicPartition(topic, i));
      }
    }
    return topicPartitions;
  }
}
