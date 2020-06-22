/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import kafka.server.Defaults;
import kafka.server.KafkaConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TierObjectStoreUtils {
    /**
     * This method generates the TierStoreConfig for the various backends. This is useful for all the tools
     * that would like the generated config in order to connect to the necessary backend.
     * Since, we are going to use the package-private c'tors of the various backends, hence, this method is placed here.
     *
     * @param backend The backend for which the config will be generated
     * @param props   The Properties object for creating the backend
     * @return The config which can then be used to create the backend through the public constructors
     */
    public static TierObjectStoreConfig generateBackendConfig(TierObjectStore.Backend backend, Properties props) {
        List<String> mandatoryProps;
        switch (backend) {
            case S3:
                // The following are mandatory properties for the S3 backend generation
                mandatoryProps = new ArrayList<String>() {{
                    add(KafkaConfig.TierS3BucketProp());
                    add(KafkaConfig.TierS3RegionProp());
                }};
                verifyMandatoryProps(backend, props, mandatoryProps);
                String s3Bucket = props.getProperty(KafkaConfig.TierS3BucketProp());
                String s3Region = props.getProperty(KafkaConfig.TierS3RegionProp());
                // The remaining configs will be loaded from the environment or the defaults provided in KafkaConfig
                return S3TierObjectStoreConfig.createWithEmptyClusterIdBrokerId(s3Bucket, s3Region,
                        props.getProperty(KafkaConfig.TierS3AwsSecretAccessKeyProp()),
                        props.getProperty(KafkaConfig.TierS3AwsAccessKeyIdProp()),
                        props.getProperty(KafkaConfig.TierS3EndpointOverrideProp()),
                        props.getProperty(KafkaConfig.TierS3SignerOverrideProp()),
                        props.getProperty(KafkaConfig.TierS3SseAlgorithmProp(), Defaults.TierS3SseAlgorithm()),
                        props.getProperty(KafkaConfig.TierS3SseCustomerEncryptionKeyProp(), Defaults.TierS3SseCustomerEncryptionKey()),
                        Integer.parseInt(props.getOrDefault(KafkaConfig.TierS3AutoAbortThresholdBytesProp(),
                                Defaults.TierS3AutoAbortThresholdBytes()).toString()),
                        props.getProperty(KafkaConfig.TierS3PrefixProp(), Defaults.TierS3Prefix()),
                        props.getProperty(KafkaConfig.TierS3AssumeRoleArnProp(), Defaults.TierS3AssumeRoleArn()));
            case GCS:
                // The following are mandatory properties for the GCS backend generation
                mandatoryProps = new ArrayList<String>() {{
                    add(KafkaConfig.TierGcsBucketProp());
                    add(KafkaConfig.TierGcsRegionProp());
                }};
                verifyMandatoryProps(backend, props, mandatoryProps);
                String gcsBucket = props.getProperty(KafkaConfig.TierGcsBucketProp());
                String gcsRegion = props.getProperty(KafkaConfig.TierGcsRegionProp());
                String gcsPrefix = props.getProperty(KafkaConfig.TierGcsPrefixProp());
                // The remaining configs will be loaded from the environment or the defaults provided in KafkaConfig
                return GcsTierObjectStoreConfig.createWithEmptyClusterIdBrokerId(gcsBucket,
                        gcsPrefix,
                        gcsRegion,
                        Integer.parseInt(props.getOrDefault(KafkaConfig.TierGcsWriteChunkSizeProp(),
                                Defaults.TierGcsWriteChunkSize()).toString()),
                        props.getProperty(KafkaConfig.TierGcsCredFilePathProp()));
            case Mock:
                return TierObjectStoreConfig.createEmpty();
            default:
                throw new UnsupportedOperationException("Unsupported backend for config generation: " + backend);
        }
    }

    private static void verifyMandatoryProps(TierObjectStore.Backend backend, Properties props, List<String> mandatoryProps) {
        List<String> absentProps = mandatoryProps.stream().filter(key -> !props.containsKey(key)).collect(Collectors.toList());
        if (absentProps.size() > 0) {
            throw new IllegalArgumentException("Missing mandatory props for backend: " + backend + ": " +
                    absentProps + " mandatoryProps: " + mandatoryProps);
        }
    }
}
