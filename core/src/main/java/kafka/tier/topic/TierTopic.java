/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import kafka.tier.exceptions.TierTopicIncorrectPartitionCountException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;

import java.util.Collection;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TierTopic {
    private final String topicName;
    private final Supplier<AdminClient> adminClientSupplier;

    private TierTopicPartitioner partitioner;
    private OptionalInt numPartitions = OptionalInt.empty();

    public TierTopic(String tierNamespace, Supplier<AdminClient> adminClientSupplier) {
        this.topicName = topicName(tierNamespace);
        this.adminClientSupplier = adminClientSupplier;
    }

    public boolean ensureTopic(int numPartitions,
                               short replicationFactor) throws TierTopicIncorrectPartitionCountException, InterruptedException {
        try (AdminClient adminClient = adminClientSupplier.get()) {
            boolean created = TierTopicAdmin.ensureTopicCreated(adminClient, topicName, numPartitions, replicationFactor);
            if (created)
                setupPartitioner(numPartitions);
            return created;
        }
    }

    /**
     * Generate the tier topic partitions containing data for tiered partitions.
     *
     * @param tieredPartitions partitions that have been tiered
     * @return The partitions on the Tier Topic containing data for tieredPartitions
     */
    public Set<TopicPartition> toTierTopicPartitions(Collection<TopicIdPartition> tieredPartitions) {
        return toTierTopicPartitions(tieredPartitions, topicName, partitioner);
    }

    public TopicPartition toTierTopicPartition(TopicIdPartition tieredPartition) {
        return toTierTopicPartition(tieredPartition, topicName, partitioner);
    }

    public String topicName() {
        return topicName;
    }

    public OptionalInt numPartitions() {
        return numPartitions;
    }

    public static String topicName(String tierNamespace) {
        if (tierNamespace != null && !tierNamespace.isEmpty())
            return Topic.TIER_TOPIC_NAME + "-" + tierNamespace;
        else
            return Topic.TIER_TOPIC_NAME;
    }

    private void setupPartitioner(int numPartitions) {
        this.numPartitions = OptionalInt.of(numPartitions);
        this.partitioner = new TierTopicPartitioner(numPartitions);
    }

    private static Set<TopicPartition> toTierTopicPartitions(Collection<TopicIdPartition> tieredPartitions,
                                                             String topicName,
                                                             TierTopicPartitioner partitioner) {
        return tieredPartitions
                .stream()
                .map(tieredPartition -> toTierTopicPartition(tieredPartition, topicName, partitioner))
                .collect(Collectors.toSet());
    }

    private static TopicPartition toTierTopicPartition(TopicIdPartition tieredPartition,
                                                       String topicName,
                                                       TierTopicPartitioner partitioner) {
        return new TopicPartition(topicName, partitioner.partitionId(tieredPartition));
    }
}