/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.store;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.tier.exceptions.TierObjectStoreFatalException;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

public class AzureBlockBlobTierObjectStore implements TierObjectStore {
    private final static Logger log = LoggerFactory.getLogger(AzureBlockBlobTierObjectStore.class);
    private final BlobServiceClient blobServiceClient;
    private final BlobContainerClient blobContainerClient;
    private final Optional<String> clusterIdOpt;
    private final Optional<Integer> brokerIdOpt;
    private final String container;
    private final String prefix;
    private final int drainThreshold;

    public AzureBlockBlobTierObjectStore(AzureBlockBlobTierObjectStoreConfig config) {
        this.clusterIdOpt = config.clusterIdOpt;
        this.brokerIdOpt = config.brokerIdOpt;
        this.container = config.container;
        this.prefix = config.azureBlobPrefix;
        this.drainThreshold = config.drainThreshold;
        this.blobServiceClient = createServiceClient(config);
        this.blobContainerClient = createContainerClient(this.blobServiceClient, config);
    }

    @Override
    public Backend getBackend() {
        return Backend.AzureBlockBlob;
    }

    @Override
    public TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata,
                                             FileType fileType,
                                             Integer byteOffsetStart,
                                             Integer byteOffsetEnd) {
        final String key = keyPath(objectMetadata, fileType);
        final BlobClient blob = blobContainerClient.getBlobClient(key);
        if (byteOffsetStart != null && byteOffsetEnd != null && byteOffsetStart > byteOffsetEnd)
            throw new IllegalStateException("Invalid range of byteOffsetStart and byteOffsetEnd");
        if (byteOffsetStart == null && byteOffsetEnd != null)
            throw new IllegalStateException("Cannot specify a byteOffsetEnd without specifying a " + "byteOffsetStart");
        log.debug("Fetching object from {}/{}, with range of {} to {}", container, key,
                byteOffsetStart, byteOffsetEnd);
        long byteOffsetStartLong = byteOffsetStart == null ? 0 : byteOffsetStart.longValue();
        final BlobRange range = byteOffsetEnd != null ?
                new BlobRange(byteOffsetStartLong, byteOffsetEnd.longValue() - byteOffsetStartLong) :
                new BlobRange(byteOffsetStartLong);

        final InputStream inputStream = blob.getBlockBlobClient().openInputStream(range, new BlobRequestConditions());
        final long streamSize = byteOffsetEnd == null ?  Long.MAX_VALUE : byteOffsetEnd.longValue() - byteOffsetStartLong;
        return new AzureBlockBlobTierObjectStoreResponse(inputStream, drainThreshold, streamSize);
    }

    @Override
    public void putSegment(ObjectMetadata objectMetadata, File segmentData,
                           File offsetIndexData, File timestampIndexData,
                           Optional<File> producerStateSnapshotData,
                           Optional<ByteBuffer> transactionIndexData,
                           Optional<ByteBuffer> epochState) {
        final Map<String, String> metadata = objectMetadata.objectMetadata(clusterIdOpt, brokerIdOpt);
        try {
            putFile(keyPath(objectMetadata, FileType.SEGMENT), metadata, segmentData);
            putFile(keyPath(objectMetadata, FileType.OFFSET_INDEX), metadata, offsetIndexData);
            putFile(keyPath(objectMetadata, FileType.TIMESTAMP_INDEX), metadata, timestampIndexData);
            producerStateSnapshotData.ifPresent(file ->
                    putFile(keyPath(objectMetadata, FileType.PRODUCER_STATE), metadata, file));
            transactionIndexData.ifPresent(byteBuffer ->
                    putBuf(keyPath(objectMetadata, FileType.TRANSACTION_INDEX), metadata, byteBuffer));
            epochState.ifPresent(byteBuffer ->
                    putBuf(keyPath(objectMetadata, FileType.EPOCH_STATE), metadata, byteBuffer));
        } catch (UncheckedIOException e) {
            throw new TierObjectStoreRetriableException("Failed to upload segmtent " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when uploading segment: " + objectMetadata, e);
        }
    }

    @Override
    public void putObject(ObjectStoreMetadata objectMetadata, File file, FileType fileType) {
        Map<String, String> metadata = objectMetadata.objectMetadata(clusterIdOpt, brokerIdOpt);
        try {
            putFile(keyPath(objectMetadata, fileType), metadata, file);
        } catch (UncheckedIOException ex) {
            throw new TierObjectStoreRetriableException(
                    String.format("Failed to upload object %s, file %s, type %s",
                            objectMetadata, file, fileType), ex);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException(
                    String.format("Failed to upload object %s, file %s, type %s",
                            objectMetadata, file, fileType), e);
        }
    }

    @Override
    public void deleteSegment(ObjectMetadata objectMetadata) {
        for (FileType type : FileType.values()) {
            String key = keyPath(objectMetadata, type);
            try {
                BlobClient blob = blobContainerClient.getBlobClient(key);
                log.debug("Deleting " + key);
                blob.delete();
            } catch (BlobStorageException be) {
                // if blob is not found it was likely already deleted
                if (!be.getErrorCode().equals(BlobErrorCode.BLOB_NOT_FOUND)) {
                    throw new TierObjectStoreRetriableException("Failed to delete file " + key, be);
                }
            } catch (Exception e) {
                throw new TierObjectStoreFatalException("Unknown exception when deleting segment " + objectMetadata, e);
            }
        }
    }

    @Override
    public void close() {
      // Nothing to do here
    }
    
    private void putFile(String key, Map<String, String> metadata, File file) {
        final BlobClient blobClient = blobContainerClient.getBlobClient(key);
        final ParallelTransferOptions transferOptions = new ParallelTransferOptions(null, null, null);
        blobClient.uploadFromFile(file.getPath(), transferOptions, new BlobHttpHeaders(),
                metadata, AccessTier.HOT, new BlobRequestConditions(), null);
    }

    private void putBuf(String key, Map<String, String> metadata, ByteBuffer buf) {
        final BlobClient blobClient = blobContainerClient.getBlobClient(key);
        blobClient.getBlockBlobClient().uploadWithResponse(
                new ByteBufferInputStream(buf),
                buf.limit() - buf.position(),
                new BlobHttpHeaders(),
                metadata,
                AccessTier.HOT,
                null,
                new BlobRequestConditions(),
                null,
                null
        );
    }

    private static BlobServiceClient createServiceClient(AzureBlockBlobTierObjectStoreConfig config) {
        final BlobServiceClient blobServiceClient;
        if (config.azureCredFilePath.isPresent()) {
            try {
                final AzureCredentials azureCredentials = readCredFilePath(new FileReader(config.azureCredFilePath.get()));
                blobServiceClient = new BlobServiceClientBuilder()
                        .connectionString(azureCredentials.getConnectionString())
                        .buildClient();
            } catch (IOException ioException) {
                throw new TierObjectStoreFatalException("Failed to get credential from file: " + config.azureCredFilePath, ioException);
            }
        } else {
            final TokenCredential credential = new ManagedIdentityCredentialBuilder().build();
            blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint(config.endpoint.get())
                    .credential(credential).buildClient();
        }
        return blobServiceClient;
    }

    private static BlobContainerClient createContainerClient(BlobServiceClient blobServiceClient, AzureBlockBlobTierObjectStoreConfig config) {
        final BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(config.container);
        if (!blobContainerClient.exists())
            throw new TierObjectStoreFatalException("Container " + config.container + " does not exist or could not be found");
        return blobContainerClient;
    }

    private String keyPath(ObjectStoreMetadata objectMetadata, TierObjectStore.FileType fileType) {
        return objectMetadata.toPath(prefix, fileType);
    }

    private static class AzureBlockBlobTierObjectStoreResponse implements TierObjectStoreResponse {
        private final InputStream inputStream;
        AzureBlockBlobTierObjectStoreResponse(InputStream inputStream,
                                              int drainThreshold,
                                              long streamSize) {
            this.inputStream = new AutoAbortingGenericInputStream(inputStream, drainThreshold, streamSize);
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

    static AzureCredentials readCredFilePath(FileReader reader) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(reader, AzureCredentials.class);
    }

    private static class AzureCredentials {
        private String connectionString;

        @JsonCreator
        public AzureCredentials(@JsonProperty("connectionString") String connectionString) {
            this.connectionString = connectionString;
        }

        @JsonProperty
        public String getConnectionString() {
            return connectionString;
        }

        public void setConnectionString(String connectionString) {
            this.connectionString = connectionString;
        }
    }
}
