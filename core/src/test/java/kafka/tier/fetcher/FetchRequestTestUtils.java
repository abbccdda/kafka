/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

class FetchRequestTestUtils {
    static TierObjectStore ioExceptionThrowingTierObjectStore() {
        return new TierObjectStore() {
            @Override
            public TierObjectStoreResponse getObject(TierObjectStore.ObjectMetadata objectMetadata, FileType objectFileType, Integer byteOffsetStart, Integer byteOffsetEnd) throws IOException {
                throw new IOException("");
            }

            @Override
            public TierObjectStoreResponse getObject(ObjectMetadata objectMetadata, FileType fileType, Integer byteOffsetStart) throws IOException {
                throw new IOException("");
            }

            @Override
            public TierObjectStoreResponse getObject(ObjectMetadata objectMetadata, FileType fileType) throws IOException {
                throw new IOException("");
            }

            @Override
            public void putSegment(TierObjectStore.ObjectMetadata objectMetadata,
                                   File segmentData, File offsetIndexData,
                                   File timestampIndexData,
                                   Optional<File> producerStateSnapshotData,
                                   Optional<ByteBuffer> transactionIndexData,
                                   Optional<File> epochState) throws IOException {
                throw new IOException("");
            }

            @Override
            public void deleteSegment(ObjectMetadata objectMetadata) throws IOException {
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
            public TierObjectStoreResponse getObject(ObjectMetadata objectMetadata,
                                                     FileType objectFileType,
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
                    public Long getStreamSize() {
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
            public void putSegment(ObjectMetadata objectMetadata,
                                   File segmentData, File offsetIndexData,
                                   File timestampIndexData,
                                   Optional<File> producerStateSnapshotData,
                                   Optional<ByteBuffer> transactionIndexData,
                                   Optional<File> epochState) throws IOException {
                throw new IOException("");
            }

            @Override
            public void deleteSegment(ObjectMetadata objectMetadata) {
            }

            @Override
            public void close() {
            }
        };
    }
}
