/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.log.Log;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierPartitionForceRestore;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import kafka.utils.CoreUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;

public interface TierObjectStore {
    enum FileType {
        SEGMENT("segment"),
        OFFSET_INDEX("offset-index"),
        TIMESTAMP_INDEX("timestamp-index"),
        TRANSACTION_INDEX("transaction-index"),
        PRODUCER_STATE("producer-state"),
        EPOCH_STATE("epoch-state"),
        TIER_STATE_SNAPSHOT("tier-state-snapshot");

        private final String suffix;

        public String suffix() {
            return suffix;
        }

        FileType(String suffix) {
            this.suffix = suffix;
        }
    }

    enum DataTypePathPrefix {
        // where topic data such as segments and associated indices, epoch state
        // checkpoints and producer state snapshot data are stored.
        TOPIC("0"),
        // where TierPartitionState restoration snapshots are stored
        TIER_STATE_RESTORE_SNAPSHOTS("1");

        private final String prefix;

        DataTypePathPrefix(String prefix) {
            this.prefix = prefix;
        }
    }

    enum Backend {
        AzureBlockBlob("AzureBlockBlob"),
        GCS("GCS"),
        Mock("Mock"),
        S3("S3"),
        Test("Test");

        private final String name;

        public String getName() {
            return name;
        }

        Backend(String name) {
            this.name = name;
        }
    }

    Backend getBackend();

    TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata,
                                      FileType fileType,
                                      Integer byteOffsetStart,
                                      Integer byteOffsetEnd) throws IOException;

    default TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata,
                                              FileType fileType,
                                              Integer byteOffsetStart) throws IOException {
        return getObject(objectMetadata, fileType, byteOffsetStart, null);
    }

    default TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata, FileType fileType) throws IOException {
        return getObject(objectMetadata, fileType, null);
    }

    void putObject(ObjectStoreMetadata objectMetadata, File file, FileType type) throws TierObjectStoreRetriableException, IOException;

    void putSegment(ObjectMetadata objectMetadata,
                    File segmentData,
                    File offsetIndexData,
                    File timestampIndexData,
                    Optional<File> producerStateSnapshotData,
                    Optional<ByteBuffer> transactionIndexData,
                    Optional<ByteBuffer> epochState) throws TierObjectStoreRetriableException,
            IOException;

    void deleteSegment(ObjectMetadata objectMetadata) throws IOException;

    void close();

    interface ObjectStoreMetadata {

        /**
         * Converts the ObjectStoreMetadata to an object store key path, taking into account a
         * given key prefix and file type
         * @param keyPrefix object key prefix
         * @param fileType object file type
         * @return String key path in object storage
         */
        String toPath(String keyPrefix, TierObjectStore.FileType fileType);

        /**
         * Converts an ObjectStoreMetadata to a map of metadata that may be useful to place on
         * objects in object storage, if this functionality is present in the object store
         * implementation of choice
         * @param clusterIdOpt optional kafka cluster id
         * @param brokerIdOpt optional kafka broker id
         * @return map of KV pairs to place in object storage
         */
        Map<String, String> objectMetadata(Optional<String> clusterIdOpt, Optional<Integer> brokerIdOpt);
    }

    class ObjectMetadata implements ObjectStoreMetadata {
        // The current key path version number, used to map segment metadata to its location in object storage.
        // IMPORTANT: do not bump this version without adding a new key_version field to
        // TierSegmentUploadInitiate and supplying it to ObjectMetadata
        private static final int CURRENT_KEY_PATH_VERSION = 0;

        private final TopicIdPartition topicIdPartition;
        private final UUID objectId;
        private final int tierEpoch;
        private final long baseOffset;
        private final boolean hasAbortedTxns;
        private final boolean hasProducerState;
        private final boolean hasEpochState;
        private final int version = CURRENT_KEY_PATH_VERSION;

        public ObjectMetadata(TopicIdPartition topicIdPartition,
                              UUID objectId,
                              int tierEpoch,
                              long baseOffset,
                              boolean hasAbortedTxns,
                              boolean hasProducerState,
                              boolean hasEpochState) {
            this.topicIdPartition = topicIdPartition;
            this.objectId = objectId;
            this.tierEpoch = tierEpoch;
            this.baseOffset = baseOffset;
            this.hasAbortedTxns = hasAbortedTxns;
            this.hasProducerState = hasProducerState;
            this.hasEpochState = hasEpochState;
        }

        public ObjectMetadata(TierObjectMetadata metadata) {
            this.topicIdPartition = metadata.topicIdPartition();
            this.objectId = metadata.objectId();
            this.tierEpoch = metadata.tierEpoch();
            this.baseOffset = metadata.baseOffset();
            this.hasAbortedTxns = metadata.hasAbortedTxns();
            this.hasProducerState = metadata.hasProducerState();
            this.hasEpochState = metadata.hasEpochState();
        }

        public TopicIdPartition topicIdPartition() {
            return topicIdPartition;
        }

        public UUID objectId() {
            return objectId;
        }

        public String objectIdAsBase64() {
            return CoreUtils.uuidToBase64(objectId());
        }

        public int tierEpoch() {
            return tierEpoch;
        }

        public long baseOffset() {
            return baseOffset;
        }

        public boolean hasAbortedTxns() {
            return hasAbortedTxns;
        }

        public boolean hasProducerState() {
            return hasProducerState;
        }

        public boolean hasEpochState() {
            return hasEpochState;
        }

        @Override
        public Map<String, String> objectMetadata(Optional<String> clusterIdOpt, Optional<Integer> brokerIdOpt) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("metadata_version", Integer.toString(version));
            metadata.put("topic", topicIdPartition().topic());
            clusterIdOpt.ifPresent(clusterId -> metadata.put("cluster_id", clusterId));
            brokerIdOpt.ifPresent(brokerId -> metadata.put("broker_id", Integer.toString(brokerId)));
            return metadata;
        }

        @Override
        public String toPath(String keyPrefix,
                             TierObjectStore.FileType fileType) {
            return keyPrefix +
                    DataTypePathPrefix.TOPIC.prefix +
                    "/" + objectIdAsBase64() +
                    "/" + topicIdPartition().topicIdAsBase64() +
                    "/" + topicIdPartition().partition() +
                    "/" + Log.filenamePrefixFromOffset(baseOffset()) +
                    "_" + tierEpoch() +
                    "_v" + version +
                    "." + fileType.suffix();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectMetadata that = (ObjectMetadata) o;
            return tierEpoch == that.tierEpoch &&
                    baseOffset == that.baseOffset &&
                    Objects.equals(topicIdPartition, that.topicIdPartition) &&
                    Objects.equals(objectId, that.objectId) &&
                    hasAbortedTxns == that.hasAbortedTxns &&
                    hasProducerState == that.hasProducerState &&
                    hasEpochState == that.hasEpochState &&
                    version == that.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicIdPartition, objectId, tierEpoch, baseOffset, hasAbortedTxns, hasProducerState, hasEpochState);
        }

        @Override
        public String toString() {
            return "ObjectMetadata(" +
                    "topic=" + topicIdPartition +
                    ", objectIdAsBase64=" + objectIdAsBase64() +
                    ", tierEpoch=" + tierEpoch +
                    ", baseOffset=" + baseOffset +
                    ", hasAbortedTxns=" + hasAbortedTxns +
                    ", hasProducerState=" + hasProducerState +
                    ", hasEpochState=" + hasEpochState +
                    ')';
        }
    }

    class TierStateRestoreSnapshotMetadata implements ObjectStoreMetadata {
        // The current key path version number, used to map restore metadata to its location in
        // object storage.
        // IMPORTANT: do not bump this version without adding a new key_version field to
        // TierPartitionForceRestore and supplying it to RestoreSnapshotMetadata.toPath
        private static final int CURRENT_KEY_PATH_VERSION = 0;

        private final TopicIdPartition topicIdPartition;
        private final long startOffset;
        private final long endOffset;
        private final String contentHash;
        private final int version = CURRENT_KEY_PATH_VERSION;

        public TierStateRestoreSnapshotMetadata(TopicIdPartition topicIdPartition,
                                                long startOffset,
                                                long endOffset,
                                                String contentHash) {
            this.topicIdPartition = topicIdPartition;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.contentHash = contentHash;
        }

        public TierStateRestoreSnapshotMetadata(TierPartitionForceRestore metadata) {
            this(metadata.topicIdPartition(),
                    metadata.startOffset(),
                    metadata.endOffset(),
                    metadata.contentHash());
        }

        public int version() {
            return version;
        }

        public TopicIdPartition topicIdPartition() {
            return topicIdPartition;
        }


        @Override
        public Map<String, String> objectMetadata(Optional<String> clusterIdOpt, Optional<Integer> brokerIdOpt) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("metadata_version", Integer.toString(version));
            metadata.put("topic", topicIdPartition().topic());
            clusterIdOpt.ifPresent(clusterId -> metadata.put("cluster_id", clusterId));
            brokerIdOpt.ifPresent(brokerId -> metadata.put("broker_id", Integer.toString(brokerId)));
            return metadata;
        }

        @Override
        public String toPath(String keyPrefix,
                             TierObjectStore.FileType fileType) {
            return keyPrefix +
                    DataTypePathPrefix.TIER_STATE_RESTORE_SNAPSHOTS.prefix +
                    "/" + topicIdPartition().topicIdAsBase64() +
                    "/" + topicIdPartition().partition() +
                    "/" + Log.filenamePrefixFromOffset(startOffset) +
                    "-" + Log.filenamePrefixFromOffset(endOffset) +
                    "_" + contentHash +
                    "_v" + version +
                    "." + fileType.suffix();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TierStateRestoreSnapshotMetadata that = (TierStateRestoreSnapshotMetadata) o;
            return topicIdPartition.equals(that.topicIdPartition) &&
                    startOffset == that.startOffset &&
                    endOffset == that.endOffset &&
                    contentHash.equals(that.contentHash) &&
                    version == that.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicIdPartition, startOffset, endOffset, contentHash, version);
        }

        @Override
        public String toString() {
            return "TierStateRestoreSnapshotMetadata(" +
                    "version=" + version +
                    ", topic=" + topicIdPartition +
                    ", startOffset=" + startOffset +
                    ", endOffset=" + endOffset +
                    ", contentHash=" + contentHash +
                    ')';
        }
    }
}

