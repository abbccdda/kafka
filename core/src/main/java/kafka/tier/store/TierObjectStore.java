/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import kafka.utils.CoreUtils;

import java.io.File;
import java.io.IOException;
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
        EPOCH_STATE("epoch-state");

        private final String suffix;

        public String suffix() {
            return suffix;
        }

        FileType(String suffix) {
            this.suffix = suffix;
        }
    }

    TierObjectStoreResponse getObject(ObjectMetadata objectMetadata,
                                      FileType fileType,
                                      Integer byteOffsetStart,
                                      Integer byteOffsetEnd) throws IOException;

    default TierObjectStoreResponse getObject(ObjectMetadata objectMetadata,
                                              FileType fileType,
                                              Integer byteOffsetStart) throws IOException {
        return getObject(objectMetadata, fileType, byteOffsetStart, null);
    }

    default TierObjectStoreResponse getObject(ObjectMetadata objectMetadata, FileType fileType) throws IOException {
        return getObject(objectMetadata, fileType, null);
    }

    void putSegment(ObjectMetadata objectMetadata,
                    File segmentData,
                    File offsetIndexData,
                    File timestampIndexData,
                    Optional<File> producerStateSnapshotData,
                    Optional<ByteBuffer> transactionIndexData,
                    Optional<File> epochState) throws TierObjectStoreRetriableException, IOException;

    void deleteSegment(ObjectMetadata objectMetadata) throws IOException;

    void close();

    class ObjectMetadata {
        private static final int CURRENT_VERSION = 0;

        private final int version;
        private final TopicIdPartition topicIdPartition;
        private final UUID objectId;
        private final int tierEpoch;
        private final long baseOffset;
        private final boolean hasAbortedTxns;

        public ObjectMetadata(TopicIdPartition topicIdPartition,
                              UUID objectId,
                              int tierEpoch,
                              long baseOffset,
                              boolean hasAbortedTxns) {
            this.version = CURRENT_VERSION;
            this.topicIdPartition = topicIdPartition;
            this.objectId = objectId;
            this.tierEpoch = tierEpoch;
            this.baseOffset = baseOffset;
            this.hasAbortedTxns = hasAbortedTxns;
        }

        public ObjectMetadata(TierObjectMetadata metadata) {
            this.version = metadata.version();
            this.topicIdPartition = metadata.topicIdPartition();
            this.objectId = metadata.objectId();
            this.tierEpoch = metadata.tierEpoch();
            this.baseOffset = metadata.baseOffset();
            this.hasAbortedTxns = metadata.hasAbortedTxns();
        }

        public int version() {
            return version;
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

        public long baseOffet() {
            return baseOffset;
        }

        public boolean hasAbortedTxns() {
            return hasAbortedTxns;
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
                    hasAbortedTxns == that.hasAbortedTxns;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicIdPartition, objectId, tierEpoch, baseOffset, hasAbortedTxns);
        }

        @Override
        public String toString() {
            return "ObjectMetadata(" +
                    "topic=" + topicIdPartition +
                    ", objectIdAsBase64=" + objectIdAsBase64() +
                    ", tierEpoch=" + tierEpoch +
                    ", startOffset=" + baseOffset +
                    ", hasAbortedTxns=" + hasAbortedTxns +
                    ')';
        }
    }
}

