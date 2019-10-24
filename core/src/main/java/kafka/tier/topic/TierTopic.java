/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import kafka.zk.AdminZkClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;

import java.util.Collection;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TierTopic implements InitializedTierTopic {
    private final String topicName;
    private final Supplier<AdminZkClient> adminZkClientSupplier;

    private TierTopicPartitioner partitioner;
    private OptionalInt numPartitions = OptionalInt.empty();

    public TierTopic(String tierNamespace, Supplier<AdminZkClient> adminZkClientSupplier) {
        this.topicName = topicName(tierNamespace);
        this.adminZkClientSupplier = adminZkClientSupplier;
    }

    /**
     * Check if tier topic exists. Create a new topic with the given configurations if it does not.
     * @param configuredNumPartitions Configured number of partitions
     * @param configuredReplicationFactor Configured replication factor
     * @return Number of partitions in tier topic. Note that this may differ from the configured value if the topic
     *         already exists.
     * @throws Exception Caller is expected to handle any exceptions from the underlying zk client
     */
    public int ensureTopic(int configuredNumPartitions, short configuredReplicationFactor) {
        int numPartitions = TierTopicAdmin.ensureTopic(adminZkClientSupplier.get(), topicName,
                configuredNumPartitions, configuredReplicationFactor);
        setupPartitioner(numPartitions);
        return numPartitions;
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

    static Set<TopicPartition> toTierTopicPartitions(Collection<TopicIdPartition> tieredPartitions,
                                                     String topicName,
                                                     TierTopicPartitioner partitioner) {
        return tieredPartitions
                .stream()
                .map(tieredPartition -> toTierTopicPartition(tieredPartition, topicName, partitioner))
                .collect(Collectors.toSet());
    }

    static TopicPartition toTierTopicPartition(TopicIdPartition tieredPartition,
                                               String topicName,
                                               TierTopicPartitioner partitioner) {
        return new TopicPartition(topicName, partitioner.partitionId(tieredPartition));
    }
}
