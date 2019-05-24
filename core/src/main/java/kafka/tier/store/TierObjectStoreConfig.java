/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;

public class TierObjectStoreConfig {
    public String clusterId;
    public Integer brokerId;
    public String s3bucket;
    public String s3Region;
    public String s3AwsSecretAccessKey;
    public String s3AwsAccessKeyId;
    public String s3EndpointOverride;
    public String s3SignerOverride;
    public String s3SseAlgorithm;
    public Integer s3MultipartUploadSize;

    public TierObjectStoreConfig(String clusterId, KafkaConfig config) {
        this.clusterId = clusterId;
        this.brokerId = config.brokerId();
        this.s3bucket = config.tierS3Bucket();
        this.s3Region = config.tierS3Region();
        this.s3AwsSecretAccessKey = config.tierS3AwsSecretAccessKey();
        this.s3AwsAccessKeyId = config.tierS3AwsAccessKeyId();
        this.s3EndpointOverride = config.tierS3EndpointOverride();
        this.s3SignerOverride = config.tierS3SignerOverride();
        this.s3MultipartUploadSize = config.tierS3MultipartUploadSize();
        if (!config.tierS3SseAlgorithm().equals(KafkaConfig.TIER_S3_SSE_ALGORITHM_NONE()))
            this.s3SseAlgorithm = config.tierS3SseAlgorithm();
    }

    // used for testing
    TierObjectStoreConfig(String clusterId,
                          Integer brokerId,
                          String s3bucket,
                          String s3Region,
                          String s3AwsSecretAccessKey,
                          String s3AwsAccessKeyId,
                          String s3EndpointOverride,
                          String s3SignerOverride,
                          String s3SseAlgorithm,
                          Integer s3MultipartUploadSize) {
        this.clusterId = clusterId;
        this.brokerId = brokerId;
        this.s3bucket = s3bucket;
        this.s3Region = s3Region;
        this.s3AwsSecretAccessKey = s3AwsSecretAccessKey;
        this.s3AwsAccessKeyId = s3AwsAccessKeyId;
        this.s3EndpointOverride = s3EndpointOverride;
        this.s3SignerOverride = s3SignerOverride;
        this.s3SseAlgorithm = s3SseAlgorithm;
        this.s3MultipartUploadSize = s3MultipartUploadSize;
    }

    public TierObjectStoreConfig() { }
}
