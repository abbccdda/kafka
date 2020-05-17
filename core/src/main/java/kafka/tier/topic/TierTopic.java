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
import java.util.stream.Collectors;

public class TierTopic implements InitializedTierTopic {
    private final String topicName;

    private TierTopicPartitioner partitioner;
    private OptionalInt numPartitions = OptionalInt.empty();

    public TierTopic(String tierNamespace) {
        this.topicName = topicName(tierNamespace);
    }

    /**
     * Initializes the TierTopic by first checking if the tier topic exists. Creates a new topic
     * with the given configurations if it does not and then sets up the TierTopic partitioner
     * @param configuredNumPartitions Configured number of partitions
     * @param configuredReplicationFactor Configured replication factor
     * @return Number of partitions in tier topic. Note that this may differ from the configured value if the topic
     *         already exists.
     * @throws Exception Caller is expected to handle any exceptions from the underlying zk client
     */
    public void initialize(AdminZkClient adminZkClient,
                           int configuredNumPartitions,
                           short configuredReplicationFactor) {
        initialize(TierTopicAdmin.ensureTopic(adminZkClient, topicName, configuredNumPartitions, configuredReplicationFactor));
    }

    public void initialize(int numPartitions) {
        this.numPartitions = OptionalInt.of(numPartitions);
        this.partitioner = new TierTopicPartitioner(numPartitions);
    }

    /**
     * Generate the tier topic partitions containing data for tiered partitions.
     *
     * @param tieredPartitions partitions that have been tiered
     * @return The partitions on the Tier Topic containing data for tieredPartitions
     */
    public Set<TopicPartition> toTierTopicPartitions(Collection<TopicIdPartition> tieredPartitions) {
        if (partitioner == null)
            throw new IllegalStateException("initialize must be called for TierTopic before use.");

        return toTierTopicPartitions(tieredPartitions, topicName, partitioner);
    }

    public TopicPartition toTierTopicPartition(TopicIdPartition tieredPartition) {
        if (partitioner == null)
            throw new IllegalStateException("initialize must be called for TierTopic before use.");

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

    public static Set<TopicPartition> toTierTopicPartitions(
        Collection<TopicIdPartition> tieredPartitions,
        String topicName,
        TierTopicPartitioner partitioner) {
        return tieredPartitions
            .stream()
            .map(tieredPartition -> toTierTopicPartition(tieredPartition, topicName, partitioner))
            .collect(Collectors.toSet());
    }

    public static TopicPartition toTierTopicPartition(TopicIdPartition tieredPartition,
        String topicName,
        TierTopicPartitioner partitioner) {
        return new TopicPartition(topicName, partitioner.partitionId(tieredPartition));
    }
}
