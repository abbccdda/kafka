/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.OptionalInt;
import java.util.Set;

public interface InitializedTierTopic {
    Set<TopicPartition> toTierTopicPartitions(Collection<TopicIdPartition> tieredPartitions);
    TopicPartition toTierTopicPartition(TopicIdPartition tieredPartition);
    String topicName();
    OptionalInt numPartitions();
}
