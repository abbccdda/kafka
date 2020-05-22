/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;

import java.util.Optional;
import scala.compat.java8.OptionConverters;

public class GcsTierObjectStoreConfig extends TierObjectStoreConfig {
    public String gcsBucket;
    public String gcsPrefix;
    public String gcsRegion;
    public Integer gcsWriteChunkSize;
    public Optional<String> gcsCredFilePath;

    public GcsTierObjectStoreConfig(Optional<String> clusterIdOpt, KafkaConfig config) {
        super(clusterIdOpt, config);
        validateConfig(config);
        this.gcsBucket = config.tierGcsBucket();
        this.gcsRegion = config.tierGcsRegion();
        this.gcsPrefix = config.tierGcsPrefix();
        this.gcsWriteChunkSize = config.tierGcsWriteChunkSize();
        this.gcsCredFilePath = OptionConverters.toJava(config.tierGcsCredFilePath());
    }

    protected GcsTierObjectStoreConfig(Optional<String> clusterIdOpt,
                                       Optional<Integer> brokerIdOpt,
                                       String bucket,
                                       String prefix,
                                       String region,
                                       Integer writeChunkSize,
                                       String credFilePath) {
        super(clusterIdOpt, brokerIdOpt);
        this.gcsBucket = bucket;
        this.gcsRegion = region;
        this.gcsPrefix = prefix;
        this.gcsWriteChunkSize = writeChunkSize;
        this.gcsCredFilePath = Optional.ofNullable(credFilePath);
    }

    // used for testing
    static GcsTierObjectStoreConfig createWithEmptyClusterIdBrokerId(String bucket,
                                                                     String prefix,
                                                                     String region,
                                                                     Integer writeChunkSize,
                                                                     String credFilePath) {
        return new GcsTierObjectStoreConfig(Optional.empty(), Optional.empty(), bucket, prefix, region, writeChunkSize, credFilePath);
    }

    private void validateConfig(KafkaConfig config) {
        if (config.tierGcsRegion() == null)
            throw new IllegalArgumentException(KafkaConfig.TierGcsRegionProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to GCS.");

        if (config.tierGcsBucket() == null)
            throw new IllegalArgumentException(KafkaConfig.TierGcsBucketProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to GCS.");
    }
}
