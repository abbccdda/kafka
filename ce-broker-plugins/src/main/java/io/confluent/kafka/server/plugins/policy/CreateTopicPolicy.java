// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import io.confluent.kafka.multitenant.schema.TenantContext;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.interceptor.TopicMetadataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CreateTopicPolicy implements org.apache.kafka.server.policy.CreateTopicPolicy,
    TopicMetadataListener {

  private static final Logger log = LoggerFactory.getLogger(CreateTopicPolicy.class);

  private short requiredRepFactor;
  private int maxPartitionsPerTenant;
  private TopicPolicyConfig policyConfig;

  // This is updated from multiple threads with a lock held, but reads are lock-free
  private volatile Map<String, Integer> tenantPrefixToNumPartitions;

  @Override
  public void configure(Map<String, ?> configs) {
    this.policyConfig = new TopicPolicyConfig(configs);
    requiredRepFactor = policyConfig.getShort(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG);
    maxPartitionsPerTenant = policyConfig.getInt(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG);
  }

  @Override
  public void validate(RequestMetadata reqMetadata) throws PolicyViolationException {
    // Only apply policy to tenant topics
    if (!TenantContext.isTenantPrefixed(reqMetadata.topic())) {
      return;
    }

    Integer numPartitionsPassed = reqMetadata.numPartitions();
    if (reqMetadata.replicasAssignments() != null && !reqMetadata.replicasAssignments().isEmpty()) {
      numPartitionsPassed = reqMetadata.replicasAssignments().keySet().size();
    }
    if (numPartitionsPassed == null) {
      throw new PolicyViolationException("Must specify number of partitions.");
    }

    Short repFactorPassed = reqMetadata.replicationFactor();
    if (reqMetadata.replicasAssignments() != null && !reqMetadata.replicasAssignments().isEmpty()) {
      repFactorPassed = (short) reqMetadata.replicasAssignments().values().iterator().next().size();
    }
    if (repFactorPassed != null && repFactorPassed != requiredRepFactor) {
      throw new PolicyViolationException("Topic replication factor must be " + requiredRepFactor);
    }

    this.policyConfig.validateTopicConfigs(reqMetadata.configs());
    ensureValidPartitionCount(TenantContext.extractTenantPrefix(reqMetadata.topic()),
        numPartitionsPassed);
  }

  @Override
  public void close() {}

  int numPartitions(String tenantPrefix) {
    if (tenantPrefixToNumPartitions == null) {
      log.info("Policy has not been initialized with topic metadata, returning NOT_CONTROLLER");
      // The policy should only be invoked by the controller, but since we don't have a
      // ControllerLoadInProgressException (like we do for the Group Coordinator), we rely on
      // NotControlletException. The reasoning is that this broker is only the Controller once its
      // state has been initialized completely.
      throw new NotControllerException("Initialization of topic metadata has not been completed");
    }
    int numTenantPartitions = tenantPrefixToNumPartitions.getOrDefault(tenantPrefix, 0);
    log.debug("Found {} partition(s) for tenantPrefix {}.", numTenantPartitions, tenantPrefix);
    return numTenantPartitions;
  }

  /**
   * Validates requested number of partitions. Total number of partitions (requested plus
   * current) should not exceed maximum allowed number of partitions
   *
   * @param tenantPrefix topic prefix for tenantPrefix
   * @param partitionsCount requested number of partitions or the delta if validating a createPartitions request
   * @throws PolicyViolationException if requested number of partitions cannot be created
   */
  void ensureValidPartitionCount(String tenantPrefix,
                                 int partitionsCount) throws PolicyViolationException {
    if (partitionsCount > maxPartitionsPerTenant) {
      throw new PolicyViolationException(String.format(
          "You may not create more than the maximum number of partitions (%d).",
          maxPartitionsPerTenant));
    } else {
      int totalCurrentPartitions = numPartitions(tenantPrefix);
      if (totalCurrentPartitions + partitionsCount > maxPartitionsPerTenant) {
        throw new PolicyViolationException(String.format(
            "You may not create more than %d new partitions. "
                + "Adding the requested number of partitions will exceed %d total partitions. "
                + "Currently, there are %d total topic partitions",
            maxPartitionsPerTenant - totalCurrentPartitions,
            maxPartitionsPerTenant, totalCurrentPartitions));
      }
      log.debug(
          "Validated adding {} partitions to {} current partitions (total={}, max={}) for {}",
          partitionsCount, totalCurrentPartitions, totalCurrentPartitions + partitionsCount,
          maxPartitionsPerTenant, tenantPrefix);
    }
  }

  // It is safe to call this method from multiple threads.
  @Override
  public void topicMetadataUpdated(Collection<TopicPartition> allPartitions) {
    synchronized (this) {
      Map<String, Integer> tenantPrefixToNumPartitions = new HashMap<>();
      for (TopicPartition tp : allPartitions) {
        String tenantPrefix = TenantContext.extractTenantPrefix(tp.topic());
        int numPartitions = tenantPrefixToNumPartitions.getOrDefault(tenantPrefix, 0);
        tenantPrefixToNumPartitions.put(tenantPrefix, numPartitions + 1);
      }
      this.tenantPrefixToNumPartitions = tenantPrefixToNumPartitions;
    }
  }
}
