/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;

public class FileTierPartitionStateFactory implements TierPartitionStateFactory {
    @Override
    public TierPartitionState initState(File stateDir, TopicPartition topicPartition, boolean tieringEnabled) throws IOException {
        return new FileTierPartitionState(stateDir, topicPartition, tieringEnabled, false);
    }
}
