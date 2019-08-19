/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;

public class S3TierObjectStoreConfig extends TierObjectStoreConfig {
    public String s3bucket;
    public String s3Region;
    public String s3AwsSecretAccessKey;
    public String s3AwsAccessKeyId;
    public String s3EndpointOverride;
    public String s3SignerOverride;
    public String s3SseAlgorithm;
    public Integer s3MultipartUploadSize;

    public S3TierObjectStoreConfig(String clusterId, KafkaConfig config) {
        super(clusterId, config);
        validateConfig(config);
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
    S3TierObjectStoreConfig(String clusterId,
                            Integer brokerId,
                            String bucket,
                            String region,
                            String secretAccessKey,
                            String accessKeyId,
                            String endpointOverride,
                            String signerOverride,
                            String sseAlgorithm,
                            Integer multipartUploadSize) {
        super(clusterId, brokerId);
        this.s3bucket = bucket;
        this.s3Region = region;
        this.s3AwsSecretAccessKey = secretAccessKey;
        this.s3AwsAccessKeyId = accessKeyId;
        this.s3EndpointOverride = endpointOverride;
        this.s3SignerOverride = signerOverride;
        this.s3SseAlgorithm = sseAlgorithm;
        this.s3MultipartUploadSize = multipartUploadSize;
    }

    private void validateConfig(KafkaConfig config) {
        if (config.tierS3Region() == null && config.tierS3EndpointOverride() == null)
            throw new IllegalArgumentException(KafkaConfig.TierS3RegionProp() + " or " + KafkaConfig.TierS3EndpointOverrideProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to S3.");

        if (config.tierS3Bucket() == null)
            throw new IllegalArgumentException(KafkaConfig.TierS3BucketProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to S3.");

        if (config.tierS3EndpointOverride() != null && config.tierS3Region() == null)
            throw new IllegalArgumentException(KafkaConfig.TierS3RegionProp() + " must be set if " + KafkaConfig.TierS3EndpointOverrideProp() + " is set.");

        if (config.tierS3AwsAccessKeyId() == null && config.tierS3AwsSecretAccessKey() != null)
            throw new IllegalArgumentException(KafkaConfig.TierS3AwsAccessKeyIdProp() + " must be set if " + KafkaConfig.TierS3AwsSecretAccessKeyProp() + " is set.");

        if (config.tierS3AwsAccessKeyId() != null && config.tierS3AwsSecretAccessKey() == null)
            throw new IllegalArgumentException(KafkaConfig.TierS3AwsSecretAccessKeyProp() + " must be set if " + KafkaConfig.TierS3AwsAccessKeyIdProp() + " is set.");
    }
}
