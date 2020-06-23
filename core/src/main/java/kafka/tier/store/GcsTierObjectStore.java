/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.WriteChannel;
import com.google.common.collect.Lists;
import kafka.tier.exceptions.TierObjectStoreFatalException;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
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
import java.util.OptionalInt;

/*
 *  Google Cloud Storage manages SSE keys by default.
 *  https://cloud.google.com/storage/docs/encryption/default-keys
 */
public class GcsTierObjectStore implements TierObjectStore {
    private final static Logger log = LoggerFactory.getLogger(GcsTierObjectStore.class);
    private final static int UNKNOWN_END_RANGE_CHUNK_SIZE = 1_000_000;
    private final Optional<String> clusterIdOpt;
    private final Optional<Integer> brokerIdOpt;
    private final String bucket;
    private final String prefix;
    // If write or read chunkSize is 0, then the respective default value of the GCS implementation is used
    private final int writeChunkSize;
    private final Storage storage;

    public GcsTierObjectStore(GcsTierObjectStoreConfig config) {
        this(storage(config), config);
    }

    GcsTierObjectStore(Storage storage, GcsTierObjectStoreConfig config) {
        this.clusterIdOpt = config.clusterIdOpt;
        this.brokerIdOpt = config.brokerIdOpt;
        this.storage = storage;
        this.bucket = config.gcsBucket;
        this.prefix = config.gcsPrefix;
        this.writeChunkSize = config.gcsWriteChunkSize;
        expectBucket(bucket, config.gcsRegion);
    }

    @Override
    public Backend getBackend() {
        return Backend.GCS;
    }

    @Override
    public TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata,
                                             FileType fileType,
                                             Integer byteOffsetStart,
                                             Integer byteOffsetEnd) {
        final String key = keyPath(objectMetadata, fileType);
        final BlobId blobId = BlobId.of(bucket, key);
        if (byteOffsetStart != null && byteOffsetEnd != null && byteOffsetStart > byteOffsetEnd)
            throw new IllegalStateException("Invalid range of byteOffsetStart and byteOffsetEnd");
        if (byteOffsetStart == null && byteOffsetEnd != null)
            throw new IllegalStateException("Cannot specify a byteOffsetEnd without specifying a " + "byteOffsetStart");
        log.debug("Fetching object from gs://{}/{}, with range of {} to {}", bucket, key, byteOffsetStart, byteOffsetEnd);

        try {
            ReadChannel reader = storage.reader(blobId);
            long byteOffsetStartLong = byteOffsetStart == null ? 0 : byteOffsetStart.longValue();
            OptionalInt chunkSize = byteOffsetEnd == null ? OptionalInt.empty() :
                    OptionalInt.of(byteOffsetEnd - byteOffsetStart);
            return new GcsTierObjectStoreResponse(reader, byteOffsetStartLong, chunkSize);
        } catch (StorageException e) {
            throw new TierObjectStoreRetriableException(
                    String.format("Failed to fetch object, blobId: %s metadata: %s type: %s "
                                    + "range %s-%s", blobId, objectMetadata, fileType,
                            byteOffsetStart, byteOffsetEnd), e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException(
                    String.format("Unknown exception when fetching object, blobId: %s metadata: "
                                    + "%s type: %s range %s-%s", blobId, objectMetadata, fileType,
                            byteOffsetStart, byteOffsetEnd), e);
        }
    }

    @Override
    public void putSegment(ObjectMetadata objectMetadata, File segmentData,
                           File offsetIndexData, File timestampIndexData,
                           Optional<File> producerStateSnapshotData,
                           Optional<ByteBuffer> transactionIndexData,
                           Optional<ByteBuffer> epochState) {
        Map<String, String> metadata = objectMetadata.objectMetadata(clusterIdOpt, brokerIdOpt);
        try {
            putFile(keyPath(objectMetadata, FileType.SEGMENT), metadata, segmentData);
            putFile(keyPath(objectMetadata, FileType.OFFSET_INDEX), metadata, offsetIndexData);
            putFile(keyPath(objectMetadata, FileType.TIMESTAMP_INDEX), metadata, timestampIndexData);
            if (producerStateSnapshotData.isPresent())
                putFile(keyPath(objectMetadata, FileType.PRODUCER_STATE), metadata, producerStateSnapshotData.get());
            if (transactionIndexData.isPresent())
                putBuf(keyPath(objectMetadata, FileType.TRANSACTION_INDEX), metadata,
                        transactionIndexData.get());
            if (epochState.isPresent())
                putBuf(keyPath(objectMetadata, FileType.EPOCH_STATE), metadata, epochState.get());
        } catch (StorageException e) {
            throw new TierObjectStoreRetriableException("Failed to upload segment: " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when uploading segment: " + objectMetadata, e);
        }
    }

    @Override
    public void putObject(ObjectStoreMetadata objectMetadata, File file, FileType fileType) {
        Map<String, String> metadata = objectMetadata.objectMetadata(clusterIdOpt, brokerIdOpt);
        try {
            putFile(keyPath(objectMetadata, fileType), metadata, file);
        } catch (StorageException e) {
            throw new TierObjectStoreRetriableException(
                    String.format("Failed to upload object %s, file %s, type %s",
                            objectMetadata, file, fileType), e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException(
                    String.format("Failed to upload object %s, file %s, type %s",
                            objectMetadata, file, fileType), e);
        }
    }

    @Override
    public void deleteSegment(ObjectMetadata objectMetadata) {
        List<BlobId> blobIds = new ArrayList<>();

        for (FileType type : FileType.values()) {
            switch (type) {
                case TRANSACTION_INDEX:
                    if (objectMetadata.hasAbortedTxns()) {
                        blobIds.add(BlobId.of(bucket, keyPath(objectMetadata, type)));
                    }
                    break;
                case PRODUCER_STATE:
                    if (objectMetadata.hasProducerState()) {
                        blobIds.add(BlobId.of(bucket, keyPath(objectMetadata, type)));
                    }
                    break;
                case EPOCH_STATE:
                    if (objectMetadata.hasEpochState()) {
                        blobIds.add(BlobId.of(bucket, keyPath(objectMetadata, type)));
                    }
                    break;
                default:
                    blobIds.add(BlobId.of(bucket, keyPath(objectMetadata, type)));
                    break;
            }
        }
        log.debug("Deleting " + blobIds);

        List<BlobId> foundBlobIds = new ArrayList<>();
        try {
            // success is a list of booleans corresponding to successful deletions of blobIds
            List<Boolean> success = storage.delete(blobIds);
            log.debug("Deletion result " + success);
            // check for failed deletes and verify if those objects are still present
            for (int blobIndex = 0; blobIndex < success.size(); blobIndex++) {
                if (!success.get(blobIndex)) {
                    Blob blob = storage.get(blobIds.get(blobIndex));
                    if (blob != null) {
                        log.warn("Found object " + blob.getBlobId() + " that was expected to be deleted of " + objectMetadata);
                        foundBlobIds.add(blob.getBlobId());
                    }
                }
            }
        } catch (StorageException e) {
            throw new TierObjectStoreRetriableException("Failed to delete segment: " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when deleting segment: " + objectMetadata, e);
        }

        if (!foundBlobIds.isEmpty()) {
            throw new TierObjectStoreRetriableException("Deletion failed for " + objectMetadata
                    + ". Blobs still exist in object storage with blob ids: " + foundBlobIds);
        }
    }

    @Override
    public void close() {
      // Nothing to do here
    }

    private void putFile(String key, Map<String, String> metadata, File file) throws IOException {
        BlobId blobId = BlobId.of(bucket, key);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setMetadata(metadata).build();
        log.debug("Uploading object to gs://{}/{}", bucket, key);
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
        log.debug("Uploading object gs://{}/{}", bucket, key);
        try (WriteChannel writer = storage.writer(blobInfo)) {
            if (writeChunkSize > 0)
                writer.setChunkSize(writeChunkSize);
            while (buf.hasRemaining())
                writer.write(buf);
        }
    }

    private static Storage storage(GcsTierObjectStoreConfig config) {
        if (config.gcsCredFilePath.isPresent()) {
            try {
                GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(config.gcsCredFilePath.get()))
                        .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
                return StorageOptions.newBuilder()
                        .setCredentials(credentials).build()
                        .getService();
            } catch (IOException e) {
                throw new TierObjectStoreFatalException("Error in opening GCS credentials file", e);
            }
        } else
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
        if (!expectedRegion.equalsIgnoreCase(actualRegion)) {
            log.warn("Bucket region {} does not match expected region {}", actualRegion, expectedRegion);
        }
    }

    private String keyPath(ObjectStoreMetadata objectMetadata, TierObjectStore.FileType fileType) {
        return objectMetadata.toPath(prefix, fileType);
    }

    private static class GcsTierObjectStoreResponse implements TierObjectStoreResponse {
        private final InputStream inputStream;
        GcsTierObjectStoreResponse(ReadChannel channel, long startOffset,
                                   OptionalInt chunkSizeOpt) throws IOException {
            // we set chunk size to our estimate of the maximum amount of data required to read.
            // this avoids reading data that will be discarded as a result of being larger than
            // max partition fetch bytes, and results in similar read behavior to range fetches in S3
            int chunkSize = chunkSizeOpt.orElse(UNKNOWN_END_RANGE_CHUNK_SIZE);
            channel.seek(startOffset);
            channel.setChunkSize(chunkSize);
            this.inputStream = Channels.newInputStream(channel);
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
        }

        @Override
        public InputStream getInputStream() {
            return inputStream;
        }
    }
}
