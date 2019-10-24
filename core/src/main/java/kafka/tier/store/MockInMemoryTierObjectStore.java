/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.log.Log;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MockInMemoryTierObjectStore implements TierObjectStore, AutoCloseable {
    public volatile boolean throwExceptionOnSegmentFetch = false;
    public volatile boolean throwExceptionOnTransactionFetch = false;

    private final static String LOG_DATA_PREFIX = "0/";
    // KEY_TO_BLOB is static so that a mock object store can be shared across brokers
    // We can remove the shared state once we have more substantial system tests that use S3.
    private final static ConcurrentHashMap<String, byte[]> KEY_TO_BLOB = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TierObjectStore.FileType, Integer> objectCounts = new ConcurrentHashMap<>();
    private final TierObjectStoreConfig config;

    public MockInMemoryTierObjectStore(TierObjectStoreConfig config) {
        this.config = config;
    }

    public ConcurrentHashMap<String, byte[]> getStored() {
         return KEY_TO_BLOB;
    }

    public ConcurrentHashMap<TierObjectStore.FileType, Integer> getObjectCounts() {
        return objectCounts;
    }

    private boolean shouldThrow(FileType objectFileType) {
        return throwExceptionOnSegmentFetch && objectFileType == FileType.SEGMENT ||
                throwExceptionOnTransactionFetch && objectFileType == FileType.TRANSACTION_INDEX;
    }

    @Override
    public TierObjectStoreResponse getObject(ObjectMetadata objectMetadata,
                                             FileType objectFileType,
                                             Integer byteOffset,
                                             Integer byteOffsetEnd) throws IOException {
        if (shouldThrow(objectFileType)) {
            throw new IOException("");
        }

        String key = keyPath(objectMetadata, objectFileType);
        byte[] blob = KEY_TO_BLOB.get(key);
        if (blob == null)
            throw new IOException(String.format("No bytes for key %s", key));
        int start = byteOffset == null ? 0 : byteOffset;
        int end = byteOffsetEnd == null ? blob.length : byteOffsetEnd;
        int byteBufferSize = Math.min(end - start, blob.length);
        ByteBuffer buf = ByteBuffer.allocate(byteBufferSize);
        buf.put(blob, start, byteBufferSize);
        buf.flip();

        return new MockInMemoryTierObjectStoreResponse(new ByteArrayInputStream(blob), byteBufferSize);
    }

    @Override
    public void close() {
    }

    private void incrementObjectCount(TierObjectStore.FileType fileType) {
        objectCounts.compute(fileType, (key, integer) -> integer == null ? 1 : integer++);
    }

    @Override
    public void putSegment(ObjectMetadata objectMetadata,
                           File segmentData,
                           File offsetIndexData,
                           File timestampIndexData,
                           Optional<File> producerStateSnapshotData,
                           Optional<ByteBuffer> transactionIndexData,
                           Optional<File> epochState) {

        writeFileToArray(keyPath(objectMetadata, FileType.SEGMENT), segmentData);
        incrementObjectCount(FileType.SEGMENT);

        writeFileToArray(keyPath(objectMetadata, FileType.OFFSET_INDEX), offsetIndexData);
        incrementObjectCount(FileType.OFFSET_INDEX);

        writeFileToArray(keyPath(objectMetadata, FileType.TIMESTAMP_INDEX), timestampIndexData);
        incrementObjectCount(FileType.TIMESTAMP_INDEX);

        producerStateSnapshotData.ifPresent(data -> {
            writeFileToArray(keyPath(objectMetadata, FileType.PRODUCER_STATE), data);
            incrementObjectCount(FileType.PRODUCER_STATE);
        });

        transactionIndexData.ifPresent(data -> {
            this.writeBufToArray(keyPath(objectMetadata, FileType.TRANSACTION_INDEX), data);
            incrementObjectCount(FileType.TRANSACTION_INDEX);
        });

        if (epochState.isPresent()) {
            writeFileToArray(keyPath(objectMetadata, FileType.EPOCH_STATE), epochState.get());
            incrementObjectCount(FileType.EPOCH_STATE);
        }
    }

    @Override
    public void deleteSegment(ObjectMetadata objectMetadata) {
        for (FileType type : FileType.values())
            KEY_TO_BLOB.remove(keyPath(objectMetadata, type));
    }

    public String keyPath(ObjectMetadata objectMetadata, FileType fileType) {
        return LOG_DATA_PREFIX
                + objectMetadata.objectIdAsBase64()
                + "/" + objectMetadata.topicIdPartition().topicIdAsBase64()
                + "/" + objectMetadata.topicIdPartition().partition()
                + "/" + Log.filenamePrefixFromOffset(objectMetadata.baseOffet())
                + "_" + objectMetadata.tierEpoch()
                + "_v" + objectMetadata.version()
                + "." + fileType.suffix();
    }

    private void writeFileToArray(String filePath, File file) {
        try (FileChannel sourceChan = FileChannel.open(file.toPath())) {
            ByteBuffer buf = ByteBuffer.allocate((int) sourceChan.size());
            sourceChan.read(buf);
            KEY_TO_BLOB.put(filePath, buf.array());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeBufToArray(String filePath, ByteBuffer buf) {
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        KEY_TO_BLOB.put(filePath, arr);
    }

    private static class MockInMemoryTierObjectStoreResponse implements TierObjectStoreResponse {
        private final InputStream inputStream;
        private final long objectSize;

        MockInMemoryTierObjectStoreResponse(InputStream inputStream, long objectSize) {
            this.inputStream = inputStream;
            this.objectSize = objectSize;
        }

        @Override
        public InputStream getInputStream() {
            return this.inputStream;
        }

        @Override
        public Long getObjectSize() {
            return this.objectSize;
        }

        @Override
        public void close() {
            try {
                inputStream.close();
            } catch (IOException ignored) { }
        }
    }

}
