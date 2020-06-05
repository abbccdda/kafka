/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.types.Password;

import java.util.Optional;
import scala.compat.java8.OptionConverters;

public class S3TierObjectStoreConfig extends TierObjectStoreConfig {
    public String s3Bucket;
    public String s3Region;
    public String s3Prefix;
    public Optional<String> s3AwsSecretAccessKey;
    public Optional<String> s3AwsAccessKeyId;
    public Optional<String> s3EndpointOverride;
    public Optional<String> s3SignerOverride;
    public String s3SseAlgorithm;
    public String s3SseCustomerEncryptionKey;
    public Integer s3AutoAbortThresholdBytes;
    public Optional<String> assumeRoleArn;

    public S3TierObjectStoreConfig(Optional<String> clusterIdOpt, KafkaConfig config) {
        super(clusterIdOpt, config);
        validateConfig(config);
        this.s3Bucket = config.tierS3Bucket();
        this.s3Region = config.tierS3Region();
        this.s3Prefix = config.tierS3Prefix();
        this.s3AwsSecretAccessKey = OptionConverters.toJava(config.tierS3AwsSecretAccessKey()).map(Password::value);
        this.s3AwsAccessKeyId = OptionConverters.toJava(config.tierS3AwsAccessKeyId()).map(Password::value);
        this.s3EndpointOverride = OptionConverters.toJava(config.tierS3EndpointOverride());
        this.s3SignerOverride = OptionConverters.toJava(config.tierS3SignerOverride());
        if (!config.tierS3SseAlgorithm().equals(KafkaConfig.TIER_S3_SSE_ALGORITHM_NONE()))
            this.s3SseAlgorithm = config.tierS3SseAlgorithm();
        this.s3SseCustomerEncryptionKey = config.s3SseCustomerEncryptionKey();
        this.s3AutoAbortThresholdBytes = config.tierS3AutoAbortThresholdBytes();
        this.assumeRoleArn = OptionConverters.toJava(config.tierS3AssumeRoleArn());
    }

    protected S3TierObjectStoreConfig(Optional<String> clusterIdOpt,
                                      Optional<Integer> brokerIdOpt,
                                      String bucket,
                                      String region,
                                      String secretAccessKey,
                                      String accessKeyId,
                                      String endpointOverride,
                                      String signerOverride,
                                      String sseAlgorithm,
                                      String s3SseCustomerEncryptionKey,
                                      Integer s3AutoAbortThresholdBytes,
                                      String s3Prefix,
                                      String assumeRoleArn) {
        super(clusterIdOpt, brokerIdOpt);
        this.s3Bucket = bucket;
        this.s3Region = region;
        this.s3AwsSecretAccessKey = Optional.ofNullable(secretAccessKey);
        this.s3AwsAccessKeyId = Optional.ofNullable(accessKeyId);
        this.s3EndpointOverride = Optional.ofNullable(endpointOverride);
        this.s3SignerOverride = Optional.ofNullable(signerOverride);
        this.s3SseAlgorithm = sseAlgorithm;
        this.s3SseCustomerEncryptionKey = s3SseCustomerEncryptionKey;
        this.s3AutoAbortThresholdBytes = s3AutoAbortThresholdBytes;
        this.s3Prefix = s3Prefix;
        this.assumeRoleArn = Optional.ofNullable(assumeRoleArn);
    }

    // used for testing
    static S3TierObjectStoreConfig createWithEmptyClusterIdBrokerId(String bucket,
                                                                    String region,
                                                                    String secretAccessKey,
                                                                    String accessKeyId,
                                                                    String endpointOverride,
                                                                    String signerOverride,
                                                                    String sseAlgorithm,
                                                                    String s3SseCustomerEncryptionKey,
                                                                    Integer s3AutoAbortThresholdBytes,
                                                                    String s3Prefix,
                                                                    String assumeRoleArn) {
        return new S3TierObjectStoreConfig(Optional.empty(), Optional.empty(), bucket, region, secretAccessKey, accessKeyId,
                endpointOverride, signerOverride, sseAlgorithm, s3SseCustomerEncryptionKey, s3AutoAbortThresholdBytes, s3Prefix, assumeRoleArn);
    }

    private void validateConfig(KafkaConfig config) {
        if (config.tierS3Region() == null && config.tierS3EndpointOverride().isEmpty())
            throw new IllegalArgumentException(KafkaConfig.TierS3RegionProp() + " or " + KafkaConfig.TierS3EndpointOverrideProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to S3.");

        if (config.tierS3Bucket() == null)
            throw new IllegalArgumentException(KafkaConfig.TierS3BucketProp() + " must be set if " + KafkaConfig.TierBackendProp() + " property is set to S3.");

        if (config.tierS3EndpointOverride().isDefined() && config.tierS3Region() == null)
            throw new IllegalArgumentException(KafkaConfig.TierS3RegionProp() + " must be set if " + KafkaConfig.TierS3EndpointOverrideProp() + " is set.");

        if (config.tierS3AwsAccessKeyId().isEmpty() && config.tierS3AwsSecretAccessKey().isDefined())
            throw new IllegalArgumentException(KafkaConfig.TierS3AwsAccessKeyIdProp() + " must be set if " + KafkaConfig.TierS3AwsSecretAccessKeyProp() + " is set.");

        if (config.tierS3AwsAccessKeyId().isDefined() && config.tierS3AwsSecretAccessKey().isEmpty())
            throw new IllegalArgumentException(KafkaConfig.TierS3AwsSecretAccessKeyProp() + " must be set if " + KafkaConfig.TierS3AwsAccessKeyIdProp() + " is set.");
    }
}
