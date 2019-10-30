/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import kafka.log.Log;

import java.util.HashMap;
import java.util.Map;

public class TierObjectStoreUtils {
    // LOG_DATA_PREFIX is where segment, offset index, time index, transaction index, leader
    // epoch state checkpoint, and producer state snapshot data are stored.
    private static final String LOG_DATA_PREFIX = "0";

    /**
     * Returns a String of the key path for the given object metadata and file type.
     *
     * @param objectMetadata The metadata from which to construct the key path
     * @param fileType The file type for the suffix of the path
     * @return
     */
    public static String keyPath(TierObjectStore.ObjectMetadata objectMetadata, TierObjectStore.FileType fileType) {
        return LOG_DATA_PREFIX
                + "/" + objectMetadata.objectIdAsBase64()
                + "/" + objectMetadata.topicIdPartition().topicIdAsBase64()
                + "/" + objectMetadata.topicIdPartition().partition()
                + "/" + Log.filenamePrefixFromOffset(objectMetadata.baseOffset())
                + "_" + objectMetadata.tierEpoch()
                + "_v" + objectMetadata.version()
                + "." + fileType.suffix();
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
}
