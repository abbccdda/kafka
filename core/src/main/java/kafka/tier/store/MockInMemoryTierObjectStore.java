/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import kafka.tier.domain.TierObjectMetadata;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class MockInMemoryTierObjectStore implements TierObjectStore, AutoCloseable {
    // KEY_TO_BLOB is static so that a mock object store can be shared across brokers
    // We can remove the shared state once we have more substantial system tests that use S3.
    private final static ConcurrentHashMap<String, byte[]> KEY_TO_BLOB = new ConcurrentHashMap<>();
    private final TierObjectStoreConfig config;

    public MockInMemoryTierObjectStore(TierObjectStoreConfig config) {
        this.config = config;
    }

    public ConcurrentHashMap<String, byte[]> getStored() {
         return KEY_TO_BLOB;
    }

    @Override
    public TierObjectStoreResponse getObject(
            TierObjectMetadata objectMetadata, TierObjectStoreFileType objectFileType,
            Integer byteOffset, Integer byteOffsetEnd)
            throws IOException {
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

    @Override
    public TierObjectMetadata putSegment(
            TierObjectMetadata objectMetadata, File segmentData,
            File offsetIndexData, File timestampIndexData,
            File producerStateSnapshotData, File transactionIndexData,
            Optional<File> epochState) throws IOException {
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.SEGMENT),
                segmentData);
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.OFFSET_INDEX),
                offsetIndexData);
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.TIMESTAMP_INDEX),
                timestampIndexData);
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.PRODUCER_STATE),
                producerStateSnapshotData);
        this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.TRANSACTION_INDEX),
                transactionIndexData);
        if (epochState.isPresent())
            this.writeFileToArray(keyPath(objectMetadata, TierObjectStoreFileType.EPOCH_STATE),
                    epochState.get());
        return objectMetadata;
    }

    private String keyPath(TierObjectMetadata objectMetadata, TierObjectStoreFileType fileType) {
        return String.format("%s/%s/%d/%020d_%d.%s",
                objectMetadata.messageId(),
                objectMetadata.topicIdPartition().topicId(),
                objectMetadata.topicIdPartition().partition(),
                objectMetadata.startOffset(),
                objectMetadata.tierEpoch(),
                fileType.getSuffix());
    }

    private void writeFileToArray(String filePath, File file) throws IOException {
        try (FileChannel sourceChan = FileChannel.open(file.toPath())) {
            ByteBuffer buf = ByteBuffer.allocate((int) sourceChan.size());
            sourceChan.read(buf);
            KEY_TO_BLOB.put(filePath, buf.array());
        }
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
