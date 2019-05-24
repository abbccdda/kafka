/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;

public class TierTopicPartitioner {
    private int numPartitions;

    public TierTopicPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    /**
     * Determine the Tier Topic partition that should contain metadata for a given tiered
     * TopicIdPartition
     * @param topicIdPartition tiered topic id partition
     * @return partition
     */
    public int partitionId(TopicIdPartition topicIdPartition) {
        ByteBuffer buffer = ByteBuffer.allocate(16 + 4); // UUID + Long
        buffer.putLong(topicIdPartition.topicId().getMostSignificantBits());
        buffer.putLong(topicIdPartition.topicId().getLeastSignificantBits());
        buffer.putInt(topicIdPartition.partition());
        return Utils.toPositive(Utils.murmur2(buffer.array())) % numPartitions;
    }
}
