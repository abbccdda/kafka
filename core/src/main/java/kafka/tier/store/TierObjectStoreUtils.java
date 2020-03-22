/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import kafka.log.Log;
import kafka.server.Defaults;
import kafka.server.KafkaConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class TierObjectStoreUtils {
    // LOG_DATA_PREFIX is where segment, offset index, time index, transaction index, leader
    // epoch state checkpoint, and producer state snapshot data are stored.
    private static final String LOG_DATA_PREFIX = "0";

    /**
     * Returns a String of the key path for the given object metadata and file type.
     *
     * @param prefix user supplied prefix for object store keys
     * @param objectMetadata The metadata from which to construct the key path
     * @param fileType The file type for the suffix of the path
     * @return
     */
    public static String keyPath(String prefix, TierObjectStore.ObjectMetadata objectMetadata, TierObjectStore.FileType fileType) {
        return prefix
                + LOG_DATA_PREFIX
                + "/" + objectMetadata.objectIdAsBase64()
                + "/" + objectMetadata.topicIdPartition().topicIdAsBase64()
                + "/" + objectMetadata.topicIdPartition().partition()
                + "/" + Log.filenamePrefixFromOffset(objectMetadata.baseOffset())
                + "_" + objectMetadata.tierEpoch()
                + "_v" + objectMetadata.version()
                + "." + fileType.suffix();
    }

    public static String keyPath(TierObjectStore.ObjectMetadata objectMetadata, TierObjectStore.FileType fileType) {
        return keyPath("", objectMetadata, fileType);
    }

    /**
     * Returns a Map containing the metadata of a segment. This metadata is uploaded to storage with the segment.
     *
     * @param objectMetadata The metadata of the segment
     * @param clusterId The name of the cluster
     * @param brokerId The ID of the broker
     * @return
     */
    public static Map<String, String> createSegmentMetadata(TierObjectStore.ObjectMetadata objectMetadata, String clusterId, int brokerId) {
        Map<String, String> metadata = new HashMap<>();
        // metadata_version refers to the version of the data format in the object
        metadata.put("metadata_version", Integer.toString(objectMetadata.version()));
        metadata.put("topic", objectMetadata.topicIdPartition().topic());
        metadata.put("cluster_id", clusterId);
        metadata.put("broker_id", Integer.toString(brokerId));
        return metadata;
    }

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
                    add("cluster-id");
                    add(KafkaConfig.BrokerIdProp());
                    add(KafkaConfig.TierS3BucketProp());
                    add(KafkaConfig.TierS3RegionProp());
                }};
                verifyMandatoryProps(backend, props, mandatoryProps);
                String clusterId = props.getProperty("cluster-id");
                int brokerId = (int) props.get(KafkaConfig.BrokerIdProp());
                String s3Bucket = props.getProperty(KafkaConfig.TierS3BucketProp());
                String s3Region = props.getProperty(KafkaConfig.TierS3RegionProp());
                // The remaining configs will be loaded from the environment or the defaults provided in KafkaConfig
                return new S3TierObjectStoreConfig(clusterId, brokerId, s3Bucket, s3Region,
                        props.getProperty(KafkaConfig.TierS3AwsSecretAccessKeyProp()),
                        props.getProperty(KafkaConfig.TierS3AwsAccessKeyIdProp()),
                        props.getProperty(KafkaConfig.TierS3EndpointOverrideProp()),
                        props.getProperty(KafkaConfig.TierS3SignerOverrideProp()),
                        props.getProperty(KafkaConfig.TierS3SseAlgorithmProp(), Defaults.TierS3SseAlgorithm()),
                        (Integer) props.getOrDefault(KafkaConfig.TierS3MultipartUploadSizeProp(), Defaults.TierS3MultipartUploadSize()),
                        (Integer) props.getOrDefault(KafkaConfig.TierS3AutoAbortThresholdBytesProp(), Defaults.TierS3AutoAbortThresholdBytes()),
                        props.getProperty(KafkaConfig.TierS3PrefixProp(), Defaults.TierS3Prefix()));
            case Mock:
                mandatoryProps = new ArrayList<String>() {{
                    add("cluster-id");
                    add(KafkaConfig.BrokerIdProp());
                }};
                verifyMandatoryProps(backend, props, mandatoryProps);
                clusterId = props.getProperty("cluster-id");
                brokerId = (int) props.get(KafkaConfig.BrokerIdProp());
                return new TierObjectStoreConfig(clusterId, brokerId);
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
