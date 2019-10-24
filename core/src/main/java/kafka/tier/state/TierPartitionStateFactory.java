/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.log.LogConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;

import java.io.File;
import java.io.IOException;

public class TierPartitionStateFactory {
    private final boolean tierFeatureEnabled;

    public TierPartitionStateFactory(boolean tierFeatureEnabled) {
        this.tierFeatureEnabled = tierFeatureEnabled;
    }

    public TierPartitionState initState(File stateDir, TopicPartition topicPartition, LogConfig config) throws IOException {
        boolean enableTiering = mayEnableTiering(topicPartition, config);
        return new FileTierPartitionState(stateDir, topicPartition, enableTiering);
    }

    public boolean mayEnableTiering(TopicPartition topicPartition, LogConfig config) {
        return tierFeatureEnabled &&
                config.tierEnable() &&
                !config.compact() &&
                !Topic.isInternal(topicPartition.topic());
    }
}
