/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

class FetchRequestTestUtils {
    static TierObjectStore ioExceptionThrowingTierObjectStore() {
        return new TierObjectStore() {
            @Override
            public TierObjectStoreResponse getObject(TierObjectMetadata objectMetadata, TierObjectStoreFileType objectFileType, Integer byteOffsetStart, Integer byteOffsetEnd) throws IOException {
                throw new IOException("");
            }

            @Override
            public TierObjectMetadata putSegment(TierObjectMetadata objectMetadata,
                                                 File segmentData, File offsetIndexData,
                                                 File timestampIndexData,
                                                 Optional<File> producerStateSnapshotData,
                                                 File transactionIndexData,
                                                 Optional<File> epochState) throws IOException {
                throw new IOException("");
            }

            @Override
            public void close() {

            }
        };
    }
    static TierObjectStore fileReturningTierObjectStore(File offsetIndexFile, File timestampIndexFile) {
        return new TierObjectStore() {
            @Override
            public TierObjectStoreResponse getObject(TierObjectMetadata objectMetadata,
                                                     TierObjectStoreFileType objectFileType,
                                                     Integer byteOffsetStart,
                                                     Integer byteOffsetEnd) throws IOException {
                FileInputStream inputStream = null;
                long objectSize = 0;
                switch (objectFileType) {
                    case OFFSET_INDEX:
                        inputStream = new FileInputStream(offsetIndexFile);
                        objectSize = offsetIndexFile.length();
                        break;
                    case TIMESTAMP_INDEX:
                        inputStream = new FileInputStream(timestampIndexFile);
                        objectSize = timestampIndexFile.length();
                        break;
                }

                InputStream finalInputStream = inputStream;
                long finalObjectSize = objectSize;
                return new TierObjectStoreResponse() {

                    @Override
                    public InputStream getInputStream() {
                        return finalInputStream;
                    }

                    @Override
                    public Long getObjectSize() {
                        return finalObjectSize;
                    }

                    @Override
                    public void close() {
                        try {
                            finalInputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                };
            }

            @Override
            public TierObjectMetadata putSegment(TierObjectMetadata objectMetadata,
                                                 File segmentData, File offsetIndexData,
                                                 File timestampIndexData,
                                                 Optional<File> producerStateSnapshotData,
                                                 File transactionIndexData,
                                                 Optional<File> epochState) throws IOException {
                throw new IOException("");
            }

            @Override
            public void close() { }
        };
    }
}
