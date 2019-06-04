/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.tier.domain.TierObjectMetadata;

import java.io.File;
import java.io.IOException;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import java.util.Optional;

public interface TierObjectStore {
    enum TierObjectStoreFileType {

        SEGMENT("segment"),
        OFFSET_INDEX("offset-index"),
        TIMESTAMP_INDEX("timestamp-index"),
        TRANSACTION_INDEX("transaction-index"),
        PRODUCER_STATE("producer-status"),
        EPOCH_STATE("epoch-state");

        private final String suffix;

        public String getSuffix() {
            return suffix;
        }

        TierObjectStoreFileType(String suffix) {
            this.suffix = suffix;
        }
    }

    TierObjectStoreResponse getObject(TierObjectMetadata objectMetadata,
                                      TierObjectStoreFileType objectFileType,
                                      Integer byteOffsetStart, Integer byteOffsetEnd)
            throws IOException;

    default TierObjectStoreResponse getObject(TierObjectMetadata objectMetadata,
                                      TierObjectStoreFileType objectFileType,
                                      Integer byteOffsetStart)
            throws IOException {
        return getObject(objectMetadata, objectFileType, byteOffsetStart, null);
    }

    default TierObjectStoreResponse getObject(TierObjectMetadata objectMetadata,
                                      TierObjectStoreFileType objectFileType)
            throws IOException {
        return getObject(objectMetadata, objectFileType, null);
    }

    TierObjectMetadata putSegment(TierObjectMetadata objectMetadata,
                                  File segmentData,
                                  File offsetIndexData,
                                  File timestampIndexData,
                                  Optional<File> producerStateSnapshotData,
                                  File transactionIndexData,
                                  Optional<File> epochState)
            throws TierObjectStoreRetriableException, IOException;

    void close();
}
