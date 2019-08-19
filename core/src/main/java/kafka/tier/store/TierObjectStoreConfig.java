/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;

public class TierObjectStoreConfig {
    public String clusterId;
    public Integer brokerId;

    public TierObjectStoreConfig(String clusterId, KafkaConfig config) {
        this.clusterId = clusterId;
        this.brokerId = config.brokerId();
    }

    // used for testing
    public TierObjectStoreConfig(String clusterId,
                          Integer brokerId) {
        this.clusterId = clusterId;
        this.brokerId = brokerId;
    }
}
