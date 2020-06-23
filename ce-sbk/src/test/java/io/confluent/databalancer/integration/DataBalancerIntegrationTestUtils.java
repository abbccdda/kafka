/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.waitForCondition;

public class DataBalancerIntegrationTestUtils {
    private static final Duration MOVEMENT_FINISH_TIMEOUT = Duration.ofMinutes(2);
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
        MOVEMENT_FINISH_TIMEOUT.toMillis(), "Replicas were not balanced onto the new broker");
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
}
