/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;

public class GcsTierObjectStoreConfig extends TierObjectStoreConfig {
    public String gcsBucket;
    public String gcsRegion;
    public Integer gcsWriteChunkSize;
    public Integer gcsReadChunkSize;

    public GcsTierObjectStoreConfig(String clusterId, KafkaConfig config) {
        super(clusterId, config);
        validateConfig(config);
        this.gcsBucket = config.tierGcsBucket();
        this.gcsRegion = config.tierGcsRegion();
        this.gcsWriteChunkSize = config.tierGcsWriteChunkSize();
        this.gcsReadChunkSize = config.tierGcsReadChunkSize();
    }

    // used for testing
    GcsTierObjectStoreConfig(String clusterId,
                             Integer brokerId,
                             String bucket,
                             String region,
                             Integer writeChunkSize,
                             Integer readChunkSize) {
        super(clusterId, brokerId);
        this.gcsBucket = bucket;
        this.gcsRegion = region;
        this.gcsWriteChunkSize = writeChunkSize;
        this.gcsReadChunkSize = readChunkSize;
    }

    private void validateConfig(KafkaConfig config) {
        if (config.tierGcsRegion() == null)
            throw new IllegalArgumentException(KafkaConfig.TierGcsRegionProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to GCS.");

        if (config.tierGcsBucket() == null)
            throw new IllegalArgumentException(KafkaConfig.TierGcsBucketProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to GCS.");
    }
}
