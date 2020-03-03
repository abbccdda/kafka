// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import io.confluent.kafka.multitenant.schema.TenantContext;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.IntGaugeSuite;
import org.apache.kafka.server.interceptor.Monitorable;
import org.apache.kafka.server.interceptor.TopicMetadataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CreateTopicPolicy implements org.apache.kafka.server.policy.CreateTopicPolicy,
    TopicMetadataListener, Monitorable {

  private static final Logger log = LoggerFactory.getLogger(CreateTopicPolicy.class);

  private short requiredRepFactor;
  private int maxPartitionsPerTenant;
  private TopicPolicyConfig policyConfig;
  private IntGaugeSuite<String> partitionsByTenantMetrics;

  // This is updated from multiple threads with a lock held, but reads are lock-free
  private volatile Map<String, Integer> tenantToNumPartitions;

  @Override
  public void configure(Map<String, ?> configs) {
    this.policyConfig = new TopicPolicyConfig(configs);
    requiredRepFactor = policyConfig.getShort(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG);
    maxPartitionsPerTenant = policyConfig.getInt(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG);
  }

  @Override
  public void registerMetrics(final Metrics metrics) {
    this.partitionsByTenantMetrics = new IntGaugeSuite<>(log, "partitions", metrics,
        tenant -> {
          return metrics.metricName("partitions", TenantMetrics.GROUP,
              "The total number of partitions for this tenant.",
              Collections.singletonMap(TenantMetrics.TENANT_TAG, tenant));
        });
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
    ensureValidPartitionCount(TenantContext.extractTenant(reqMetadata.topic()), numPartitionsPassed);
  }

  @Override
  public synchronized void close() {
    if (this.partitionsByTenantMetrics != null) {
      this.partitionsByTenantMetrics.close();
      this.partitionsByTenantMetrics = null;
    }
  }

  int numPartitions(String tenant) {
    if (tenantToNumPartitions == null) {
      log.info("Policy has not been initialized with topic metadata, returning NOT_CONTROLLER");
      // The policy should only be invoked by the controller, but since we don't have a
      // ControllerLoadInProgressException (like we do for the Group Coordinator), we rely on
      // NotControlletException. The reasoning is that this broker is only the Controller once its
      // state has been initialized completely.
      throw new NotControllerException("Initialization of topic metadata has not been completed");
    }
    int numTenantPartitions = tenantToNumPartitions.getOrDefault(tenant, 0);
    log.debug("Found {} partition(s) for tenant {}.", numTenantPartitions, tenant);
    return numTenantPartitions;
  }

  /**
   * Validates requested number of partitions. Total number of partitions (requested plus
   * current) should not exceed maximum allowed number of partitions
   *
   * @param tenant topic prefix for tenant
   * @param partitionsCount requested number of partitions or the delta if validating a createPartitions request
   * @throws PolicyViolationException if requested number of partitions cannot be created
   */
  void ensureValidPartitionCount(String tenant,
                                 int partitionsCount) throws PolicyViolationException {
    if (partitionsCount > maxPartitionsPerTenant) {
      throw new PolicyViolationException(String.format(
          "You may not create more than the maximum number of partitions (%d).",
          maxPartitionsPerTenant));
    } else {
      int totalCurrentPartitions = numPartitions(tenant);
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
          maxPartitionsPerTenant, tenant);
    }
  }

  // It is safe to call this method from multiple threads.
  @Override
  public void topicMetadataUpdated(Collection<TopicPartition> allPartitions) {
    synchronized (this) {
      Map<String, Integer> tenantPrefixToNumPartitions = new HashMap<>();
      for (TopicPartition tp : allPartitions) {
        String topic = tp.topic();
        if (TenantContext.isTenantPrefixed(topic)) {
          String tenant = TenantContext.extractTenant(topic);
          int numPartitions = tenantPrefixToNumPartitions.getOrDefault(tenant, 0);
          tenantPrefixToNumPartitions.put(tenant, numPartitions + 1);
        }
      }

      Map<String, Integer> oldTenantPrefixToNumPartitions = this.tenantToNumPartitions;
      this.tenantToNumPartitions = tenantPrefixToNumPartitions;

      if (this.partitionsByTenantMetrics != null) {
        // Update the metrics
        for (Entry<String, Integer> entry : tenantPrefixToNumPartitions.entrySet()) {
          this.partitionsByTenantMetrics.update(entry.getKey(), entry.getValue());
        }

        // Remove tenants
        if (oldTenantPrefixToNumPartitions != null) {
          Set<String> tenantsToBeRemoved = oldTenantPrefixToNumPartitions.keySet();
          tenantsToBeRemoved.removeAll(tenantPrefixToNumPartitions.keySet());
          for (String tenant : tenantsToBeRemoved) {
            this.partitionsByTenantMetrics.remove(tenant);
          }
        }
      }
    }
  }
}
