/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;

import java.util.Optional;

public class TierObjectStoreConfig {
    public Optional<String> clusterIdOpt;
    public Optional<Integer> brokerIdOpt;

    public TierObjectStoreConfig(Optional<String> clusterIdOpt, KafkaConfig config) {
        this(clusterIdOpt, Optional.of(config.brokerId()));
    }

    protected TierObjectStoreConfig(Optional<String> clusterIdOpt, Optional<Integer> brokerIdOpt) {
        this.clusterIdOpt = clusterIdOpt;
        this.brokerIdOpt = brokerIdOpt;
    }

    public TierObjectStoreConfig(String clusterId, Integer brokerId) {
        this(Optional.of(clusterId), Optional.of(brokerId));
    }

    public static TierObjectStoreConfig createEmpty() {
        return new TierObjectStoreConfig(Optional.empty(), Optional.empty());
    }
}
