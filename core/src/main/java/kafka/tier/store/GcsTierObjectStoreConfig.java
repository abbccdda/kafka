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
    public Integer gcsReadChunkSize;
    public Optional<String> gcsCredFilePath;

    public GcsTierObjectStoreConfig(String clusterId, KafkaConfig config) {
        super(clusterId, config);
        validateConfig(config);
        this.gcsBucket = config.tierGcsBucket();
        this.gcsRegion = config.tierGcsRegion();
        this.gcsPrefix = config.tierGcsPrefix();
        this.gcsWriteChunkSize = config.tierGcsWriteChunkSize();
        this.gcsReadChunkSize = config.tierGcsReadChunkSize();
        this.gcsCredFilePath = OptionConverters.toJava(config.tierGcsCredFilePath());
    }

    // used for testing
    GcsTierObjectStoreConfig(String clusterId,
                             Integer brokerId,
                             String bucket,
                             String prefix,
                             String region,
                             Integer writeChunkSize,
                             Integer readChunkSize,
                             String credFilePath) {
        super(clusterId, brokerId);
        this.gcsBucket = bucket;
        this.gcsRegion = region;
        this.gcsPrefix = prefix;
        this.gcsWriteChunkSize = writeChunkSize;
        this.gcsReadChunkSize = readChunkSize;
        this.gcsCredFilePath = Optional.ofNullable(credFilePath);
    }

    private void validateConfig(KafkaConfig config) {
        if (config.tierGcsRegion() == null)
            throw new IllegalArgumentException(KafkaConfig.TierGcsRegionProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to GCS.");

        if (config.tierGcsBucket() == null)
            throw new IllegalArgumentException(KafkaConfig.TierGcsBucketProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to GCS.");
    }
}
