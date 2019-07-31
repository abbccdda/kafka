// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.policy;

import io.confluent.common.InterClusterConnection;
import io.confluent.kafka.multitenant.schema.TenantContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class CreateTopicPolicy implements org.apache.kafka.server.policy.CreateTopicPolicy {

  private static final Logger log =
          LoggerFactory.getLogger(CreateTopicPolicy.class);


  private static final int TIMEOUT_MS = 60 * 1000;

  private short requiredRepFactor;
  private int maxPartitionsPerTenant;
  private TopicPolicyConfig policyConfig;

  Map<String, String> adminClientProps = new HashMap<>();

  @Override
  public void configure(Map<String, ?> cfgMap) {
    this.policyConfig = new TopicPolicyConfig(cfgMap);

    requiredRepFactor = policyConfig.getShort(TopicPolicyConfig.REPLICATION_FACTOR_CONFIG);
    maxPartitionsPerTenant =
            policyConfig.getInt(TopicPolicyConfig.MAX_PARTITIONS_PER_TENANT_CONFIG);

    String listener = policyConfig.getString(TopicPolicyConfig.INTERNAL_LISTENER_CONFIG);

    // get bootstrap broker from internal listener config
    String bootstrapBroker = InterClusterConnection.getBootstrapBrokerForListener(listener, cfgMap);

    // we currently support internal listener security config = PLAINTEXT;
    // TODO: support other security configs
    String securityProtocol = InterClusterConnection.getListenerSecurityProtocol(listener, cfgMap);
    if (securityProtocol == null || securityProtocol.compareTo("PLAINTEXT") != 0) {
      throw new ConfigException(
          String.format("Expected %s listener security config = PLAINTEXT, got %s",
                        listener, securityProtocol));
    }

    log.debug("Using bootstrap servers {} for retrieving tenant's broker and partitions counts",
                 bootstrapBroker);
    adminClientProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapBroker);
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

    Map<String, Object> adminConfig = new HashMap<>(adminClientProps);
    log.debug("Checking partitions count with config: {}", adminClientProps);
    try (AdminClient adminClient = AdminClient.create(adminConfig)) {
      ensureValidPartitionCount(
              adminClient,
              TenantContext.extractTenantPrefix(reqMetadata.topic()),
              numPartitionsPassed
      );
    }
  }

  @Override
  public void close() throws Exception {}

  private void handleNumPartitionsException(String tenantPrefix, Throwable e, String text) {
    if (e instanceof ExecutionException) {
      e = e.getCause();
    }
    log.error("Unable to find the current number of client partitions for {}: {}",
            tenantPrefix, text, e);
    throw new PolicyViolationException("Failed to determine the current number of partitions.");
  }

  /**
   * Returns current number of topic partitions for this tenant
   */
  int numPartitions(Admin adminClient, String tenantPrefix) {
    // List every topic in the cluster.
    Collection<String> allTopicNames = null;
    try {
      ListTopicsResult result = adminClient.listTopics(new ListTopicsOptions().
              timeoutMs(TIMEOUT_MS));
      allTopicNames = result.names().get();
    } catch (Throwable e) {
      handleNumPartitionsException(tenantPrefix, e, "listTopics had an unexpected error.");
    }

    // Now that we know every topic in the cluster, check which ones belong
    // to this tenant, (that is, which ones start with the tenant's prefix.)
    Set<String> topics = new HashSet<>();
    if (log.isTraceEnabled()) {
      log.trace("Listed topics {}", String.join(", ", allTopicNames));
    }
    for (String topicName : allTopicNames) {
      if (TenantContext.hasTenantPrefix(tenantPrefix, topicName) ||
              topicName.equals(Topic.GROUP_METADATA_TOPIC_NAME)) {
        topics.add(topicName);
      }
    }
    if (log.isTraceEnabled()) {
      log.trace("Found topics that we want to describe for tenant prefix '{}': {}",
              tenantPrefix, String.join(", ", topics));
    } else {
      log.debug("Found {} topic(s) that we want to describe for tenant prefix '{}'.",
              topics.size(), tenantPrefix);
    }

    // Now, we will try to describe all the topics which belong to the tenant.
    // We also attempt to describe __consumer_offsets, if it was listed originally.
    // This topic is described as a a canary.  We know that this topic should always
    // be describable.  If it is not, something is very wrong and we should abort.
    DescribeTopicsResult result = null;
    try {
      result = adminClient.describeTopics(topics,
              new DescribeTopicsOptions().timeoutMs(TIMEOUT_MS));
    } catch (Throwable e) {
      handleNumPartitionsException(tenantPrefix, e,
          "describeTopics had an unexpected error.");
    }

    int numTenantPartitions = 0, numUnknownTopics = 0;
    for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : result.values().entrySet()) {
      String topicName = entry.getKey();
      try {
        TopicDescription description = entry.getValue().get();
        if (!topicName.equals(Topic.GROUP_METADATA_TOPIC_NAME)) {
          numTenantPartitions += description.partitions().size();
        }
      } catch (Throwable e) {
        if ((!topicName.equals(Topic.GROUP_METADATA_TOPIC_NAME)) &&
                (e instanceof ExecutionException)) {
          // Topics may have gone away in between being listed and being described.
          // This may happen for two reasons.  One is that a topic may have been deleted
          // and therefore genuinely no longer exist.  Another is the listTopics and
          // describeTopics calls may have gone to different brokers that have slightly
          // different information in their metadata caches.
          // We ignore the missing topics here.
          if (e.getCause() instanceof UnknownTopicOrPartitionException) {
            numUnknownTopics++;
            continue;
          }
        }
        handleNumPartitionsException(tenantPrefix, e,
            "unexpected error while describing the " + entry.getKey() + " topic.");
      }
    }
    log.debug("Found {} partition(s) for tenant {}.  {} topic(s) could not be described.",
            numTenantPartitions, tenantPrefix, numUnknownTopics);
    return numTenantPartitions;
  }

  /**
   * Validates requested number of partitions. Total number of partitions (requested plus
   * current) should not exceed maximum allowed number of partitions
   *
   * @param adminClient Kafka admin client
   * @param tenantPrefix topic prefix for tenant
   * @param partitionsCount requested number of partitions or the delta if validating a createPartitions request
   * @throws PolicyViolationException if requested number of partitions cannot be created
   */
  void ensureValidPartitionCount(Admin adminClient,
                                 String tenantPrefix,
                                 int partitionsCount) throws PolicyViolationException {
    if (partitionsCount > maxPartitionsPerTenant) {
      throw new PolicyViolationException(String.format(
          "You may not create more than the maximum number of partitions (%d).",
          maxPartitionsPerTenant));
    } else {
      int totalCurrentPartitions = numPartitions(adminClient, tenantPrefix);
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

}
