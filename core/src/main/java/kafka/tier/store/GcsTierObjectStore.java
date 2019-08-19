/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.WriteChannel;
import kafka.tier.exceptions.TierObjectStoreFatalException;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/*
 *  Google Cloud Storage manages SSE keys by default.
 *  https://cloud.google.com/storage/docs/encryption/default-keys
 */
public class GcsTierObjectStore implements TierObjectStore {
    private final static Logger log = LoggerFactory.getLogger(GcsTierObjectStore.class);
    private final String clusterId;
    private final int brokerId;
    private final String bucket;
    // If write or read chunkSize is 0, then the respective default value of the GCS implementation is used
    private final int writeChunkSize;
    private final int readChunkSize;
    private final Storage storage;

    public GcsTierObjectStore(GcsTierObjectStoreConfig config) {
        this(storage(), config);
    }

    GcsTierObjectStore(Storage storage, GcsTierObjectStoreConfig config) {
        this.clusterId = config.clusterId;
        this.brokerId = config.brokerId;
        this.storage = storage;
        this.bucket = config.gcsBucket;
        this.writeChunkSize = config.gcsWriteChunkSize;
        this.readChunkSize = config.gcsReadChunkSize;
        expectBucket(bucket, config.gcsRegion);
    }

    @Override
    public TierObjectStoreResponse getObject(ObjectMetadata objectMetadata,
                                             FileType fileType,
                                             Integer byteOffsetStart,
                                             Integer byteOffsetEnd) {
        final String key = TierObjectStoreUtils.keyPath(objectMetadata, fileType);
        final BlobId blobId = BlobId.of(bucket, key);
        if (byteOffsetStart != null && byteOffsetEnd != null && byteOffsetStart > byteOffsetEnd)
            throw new IllegalStateException("Invalid range of byteOffsetStart and byteOffsetEnd");
        if (byteOffsetStart == null && byteOffsetEnd != null)
            throw new IllegalStateException("Cannot specify a byteOffsetEnd without specifying a " + "byteOffsetStart");
        log.debug("Fetching object from gcs://{}/{}, with range of {} to {}", bucket, key, byteOffsetStart, byteOffsetEnd);

        try {
            Blob blob = storage.get(blobId);
            ReadChannel reader = storage.reader(blobId);
            long byteOffsetStartLong = byteOffsetStart == null ? 0 : byteOffsetStart.longValue();
            long byteOffsetEndLong = byteOffsetEnd == null ? blob.getSize() : byteOffsetEnd.longValue();
            return new GcsTierObjectStoreResponse(reader, byteOffsetStartLong, byteOffsetEndLong, readChunkSize);
        } catch (StorageException e) {
            throw new TierObjectStoreRetriableException("Failed to fetch segment " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when fetching segment " + objectMetadata, e);
        }
    }

    @Override
    public void putSegment(ObjectMetadata objectMetadata, File segmentData,
                           File offsetIndexData, File timestampIndexData,
                           Optional<File> producerStateSnapshotData,
                           Optional<ByteBuffer> transactionIndexData,
                           Optional<File> epochState) {
        Map<String, String> metadata = TierObjectStoreUtils.createSegmentMetadata(objectMetadata, clusterId, brokerId);

        try {
            putFile(TierObjectStoreUtils.keyPath(objectMetadata, FileType.SEGMENT), metadata, segmentData);
            putFile(TierObjectStoreUtils.keyPath(objectMetadata, FileType.OFFSET_INDEX), metadata, offsetIndexData);
            putFile(TierObjectStoreUtils.keyPath(objectMetadata, FileType.TIMESTAMP_INDEX), metadata, timestampIndexData);
            if (producerStateSnapshotData.isPresent())
                putFile(TierObjectStoreUtils.keyPath(objectMetadata, FileType.PRODUCER_STATE), metadata, producerStateSnapshotData.get());
            if (transactionIndexData.isPresent())
                putBuf(TierObjectStoreUtils.keyPath(objectMetadata, FileType.TRANSACTION_INDEX), metadata, transactionIndexData.get());
            if (epochState.isPresent())
                putFile(TierObjectStoreUtils.keyPath(objectMetadata, FileType.EPOCH_STATE), metadata, epochState.get());
        } catch (StorageException e) {
            throw new TierObjectStoreRetriableException("Failed to upload segment " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when uploading segment " + objectMetadata, e);
        }
    }

    @Override
    public void deleteSegment(ObjectMetadata objectMetadata) {
        List<BlobId> blobIds = new ArrayList<>();
        for (FileType type : FileType.values())
            blobIds.add(BlobId.of(bucket, TierObjectStoreUtils.keyPath(objectMetadata, type)));
        log.debug("Deleting " + blobIds);

        try {
            storage.delete(blobIds);
        } catch (StorageException e) {
            throw new TierObjectStoreRetriableException("Failed to delete segment " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when deleting segment " + objectMetadata, e);
        }
    }

    @Override
    public void close() {
      // Nothing to do here
    }
    
    private void putFile(String key, Map<String, String> metadata, File file) throws IOException {
        BlobId blobId = BlobId.of(bucket, key);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setMetadata(metadata).build();
        log.debug("Uploading object to gcs://{}/{}", bucket, key);
        try (WriteChannel writer = storage.writer(blobInfo); FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            if (writeChunkSize > 0)
                writer.setChunkSize(writeChunkSize);
            long position = 0;
            long fileLength = file.length();
            while (position < fileLength)
                position += fileChannel.transferTo(position, fileLength, writer);
        }
    }

    private void putBuf(String key, Map<String, String> metadata, ByteBuffer buf) throws IOException {
        BlobId blobId = BlobId.of(bucket, key);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setMetadata(metadata).build();
        log.debug("Uploading object gcs://{}/{}", bucket, key);
        try (WriteChannel writer = storage.writer(blobInfo)) {
            if (writeChunkSize > 0)
                writer.setChunkSize(writeChunkSize);
            while (buf.hasRemaining())
                writer.write(buf);
        }
    }

    private static Storage storage() {
        return StorageOptions.getDefaultInstance().getService();
    }

    private void expectBucket(final String bucket, final String expectedRegion)
            throws TierObjectStoreFatalException {
        Bucket bucketObj;
        try {
            bucketObj = storage.get(bucket, Storage.BucketGetOption.fields(Storage.BucketField.LOCATION));
        } catch (StorageException e) {
            throw new TierObjectStoreFatalException("Unable to access bucket " + bucket, e);
        }
        if (bucketObj == null)
            throw new TierObjectStoreFatalException("Configured bucket " + bucket + " does not exist or could not be found");
        String actualRegion = bucketObj.getLocation();
        if (!expectedRegion.equals(actualRegion)) {
            log.warn("Bucket region {} does not match expected region {}", actualRegion, expectedRegion);
        }
    }

    private static class GcsTierObjectStoreResponse implements TierObjectStoreResponse {
        private final InputStream inputStream;
        private final long objectSize;
        
        GcsTierObjectStoreResponse(ReadChannel channel, long startOffset, long endOffset, int chunkSize)
                throws IOException {
            if (chunkSize > 0)
                channel.setChunkSize(chunkSize);
            channel.seek(startOffset);
            this.inputStream = Channels.newInputStream(channel);
            this.objectSize = endOffset - startOffset;
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
        }

        @Override
        public InputStream getInputStream() {
            return inputStream;
        }

        @Override
        public Long getObjectSize() {
            return objectSize;
        }
    }
}
