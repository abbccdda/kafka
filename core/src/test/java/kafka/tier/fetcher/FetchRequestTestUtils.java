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
            public Backend getBackend() {
                return Backend.Mock;
            }

            @Override
            public TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata, FileType objectFileType, Integer byteOffsetStart, Integer byteOffsetEnd) throws IOException {
                throw new IOException("");
            }

            @Override
            public TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata, FileType fileType, Integer byteOffsetStart) throws IOException {
                throw new IOException("");
            }

            @Override
            public TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata, FileType fileType) throws IOException {
                throw new IOException("");
            }

            @Override
            public void putSegment(ObjectMetadata objectMetadata,
                                   File segmentData, File offsetIndexData,
                                   File timestampIndexData,
                                   Optional<File> producerStateSnapshotData,
                                   Optional<ByteBuffer> transactionIndexData,
                                   Optional<ByteBuffer> epochState) throws IOException {
                throw new IOException("");
            }

            @Override
            public void putObject(ObjectStoreMetadata objectMetadata,
                                  File file,
                                  FileType fileType) throws IOException {
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
            public Backend getBackend() {
                return Backend.Mock;
            }

            @Override
            public TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata,
                                                     FileType objectFileType,
                                                     Integer byteOffsetStart,
                                                     Integer byteOffsetEnd) throws IOException {
                FileInputStream inputStream = null;
                switch (objectFileType) {
                    case OFFSET_INDEX:
                        inputStream = new FileInputStream(offsetIndexFile);
                        break;
                    case TIMESTAMP_INDEX:
                        inputStream = new FileInputStream(timestampIndexFile);
                        break;
                }

                InputStream finalInputStream = inputStream;
                return new TierObjectStoreResponse() {

                    @Override
                    public InputStream getInputStream() {
                        return finalInputStream;
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
            public void putObject(ObjectStoreMetadata objectMetadata,
                                  File file,
                                  FileType fileType) throws IOException {
                throw new IOException("");
            }

            @Override
            public void putSegment(ObjectMetadata objectMetadata,
                                   File segmentData, File offsetIndexData,
                                   File timestampIndexData,
                                   Optional<File> producerStateSnapshotData,
                                   Optional<ByteBuffer> transactionIndexData,
                                   Optional<ByteBuffer> epochState) throws IOException {
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
