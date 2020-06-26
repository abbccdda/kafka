/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.integration;

import kafka.common.TopicPlacement;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfluentTopicConfig;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataBalancerIntegrationTestUtils {
    private static final Duration OPERATION_FINISH_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration OPERATION_POLL_INTERVAL = Duration.ofSeconds(2);

    private static final Logger LOG = LoggerFactory.getLogger(DataBalancerIntegrationTestUtils.class);

    public static void verifyReplicasMovedToBroker(ConfluentAdmin adminClient, String topic, int brokerId) throws InterruptedException {
        waitForCondition(() -> {
            // Don't check if we're still reassigning
            if (!adminClient.listPartitionReassignments().reassignments().get().isEmpty()) {
                return false;
            }

            // Look for partitions on the new broker
            Map<String, TopicDescription> topics = adminClient.describeTopics(Collections.singletonList(topic)).all().get();
            boolean newBrokerHasReplicas = topics.values().stream().anyMatch(
                desc -> desc.partitions().stream().anyMatch(
                    tpInfo -> tpInfo.replicas().stream().anyMatch(node -> node.id() == brokerId)
                )
            );
            return newBrokerHasReplicas;
        },
        OPERATION_FINISH_TIMEOUT.toMillis(),
        OPERATION_POLL_INTERVAL.toMillis(),
        () -> "Replicas were not balanced onto the new broker");
    }

    public static List<TopicPartition> partitionsOnBroker(int brokerId, Admin adminClient) throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        List<TopicPartition> topicPartitions = adminClient.describeTopics(topics).all().get().entrySet().stream()
                .flatMap(kv -> kv.getValue()
                        .partitions().stream()
                        .filter(tpInfo -> tpInfo.replicas().stream().map(Node::id).anyMatch(id -> id == brokerId))
                        .map(tpInfo -> new TopicPartition(kv.getKey(), tpInfo.partition())))
                .collect(Collectors.toList());
        LOG.info("Partitions on broker {} are {}", brokerId, topicPartitions);
        return topicPartitions;
    }

    public static void verifyTopicPlacement(Admin adminClient, String topic, String topicPlacementString) throws InterruptedException {
        TopicPlacement expectedTopicPlacement = TopicPlacement.parse(topicPlacementString).get();

        LOG.info("Verifying that replica distribution for {} matches {}", topic, expectedTopicPlacement);
        TestUtils.waitForCondition(() -> {
            TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(topic)).all().get().get(topic);
            assertTrue("Expected partitions to be described for topic", !topicDescription.partitions().isEmpty());
            for (TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
                List<Node> observers = topicPartitionInfo.observers();
                List<Node> syncReplicas = new ArrayList<>(topicPartitionInfo.replicas());
                syncReplicas.removeAll(observers);

                // verify that all sync-replica constraints are satisfied
                for (TopicPlacement.ConstraintCount syncReplicaConstraint : expectedTopicPlacement.replicas()) {
                    LOG.info("Verifying sync replicas {} match constraints {}", syncReplicas, syncReplicaConstraint);
                    List<Node> matchingSyncReplicas = new ArrayList<>(syncReplicas);
                    matchingSyncReplicas.removeIf(node -> !syncReplicaConstraint.matches(attributes(node)));
                    assertEquals("Expected number of sync replicas to match topic placement",
                            syncReplicaConstraint.count(), matchingSyncReplicas.size());
                }

                // verify that all observer constraints are satisfied
                for (TopicPlacement.ConstraintCount observerConstraint : expectedTopicPlacement.observers()) {
                    LOG.info("Verifying observers {} match constraints {}", observers, observerConstraint);
                    List<Node> matchingObservers = new ArrayList<>(observers);
                    matchingObservers.removeIf(node -> !observerConstraint.matches(attributes(node)));
                    assertEquals("Expected number of observers to match topic placement",
                            observerConstraint.count(), matchingObservers.size());
                }
            }
            return true;
        },
        OPERATION_FINISH_TIMEOUT.toMillis(),
        OPERATION_POLL_INTERVAL.toMillis(),
        () -> "Change in TopicPlacement did not trigger and complete cluster self-healing in time.");
    }

    public static void alterTopicPlacementConfig(Admin adminClient, String topic, String topicPlacement) throws ExecutionException, InterruptedException {
        ConfigResource topicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry configEntry = new ConfigEntry(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG, topicPlacement);
        AlterConfigOp addTopicPlacement = new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET);
        LOG.info("Changing topic placement for topic {} to {}", topic, topicPlacement);
        adminClient.incrementalAlterConfigs(Collections.singletonMap(topicConfigResource,
                Collections.singleton(addTopicPlacement))).all().get();
        // wait for TopicPlacement update to be propagated
        TestUtils.waitForCondition(() -> {
            Map<ConfigResource, Config> topicConfig = adminClient.describeConfigs(Collections.singleton(
                    topicConfigResource)).all().get();
            Collection<ConfigEntry> configEntries = topicConfig.get(topicConfigResource).entries();
            Optional<ConfigEntry> topicPlacementEntry = configEntries.stream().filter(entry ->
                    entry.name().equals(ConfluentTopicConfig.TOPIC_PLACEMENT_CONSTRAINTS_CONFIG)).findFirst();
            return topicPlacementEntry.isPresent() && topicPlacementEntry.get().value().equals(topicPlacement);
        }, "TopicPlacement change did not complete in time");
        LOG.info("Topic placement change complete");
    }

    private static Map<String, String> attributes(Node node) {
        return Collections.singletonMap("rack", node.rack());
    }

}
