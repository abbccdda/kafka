/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import kafka.tier.exceptions.TierObjectStoreFatalException;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class S3TierObjectStore implements TierObjectStore {
    private final static Logger log = LoggerFactory.getLogger(S3TierObjectStore.class);
    private final Optional<String> clusterIdOpt;
    private final Optional<Integer> brokerIdOpt;
    private final AmazonS3 client;
    private final String bucket;
    private final String prefix;
    private final String sseAlgorithm;
    private final int autoAbortThresholdBytes;

    public S3TierObjectStore(S3TierObjectStoreConfig config) {
        this(client(config), config);
    }

    // used for testing
    S3TierObjectStore(AmazonS3 client, S3TierObjectStoreConfig config) {
        this.clusterIdOpt = config.clusterIdOpt;
        this.brokerIdOpt = config.brokerIdOpt;
        this.client = client;
        this.bucket = config.s3Bucket;
        this.prefix = config.s3Prefix;
        this.sseAlgorithm = config.s3SseAlgorithm;
        this.autoAbortThresholdBytes = config.s3AutoAbortThresholdBytes;
        expectBucket(bucket, config.s3Region);
    }

    @Override
    public Backend getBackend() {
        return Backend.S3;
    }

    @Override
    public TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata,
                                             FileType fileType,
                                             Integer byteOffsetStart,
                                             Integer byteOffsetEnd) {
        final String key = keyPath(objectMetadata, fileType);
        final GetObjectRequest request = new GetObjectRequest(bucket, key);
        if (byteOffsetStart != null && byteOffsetEnd != null)
            request.setRange(byteOffsetStart, byteOffsetEnd);
        else if (byteOffsetStart != null)
            request.setRange(byteOffsetStart);
        else if (byteOffsetEnd != null)
            throw new IllegalStateException("Cannot specify a byteOffsetEnd without specifying a "
                    + "byteOffsetStart");
        log.debug("Fetching object from s3://{}/{}, with range start {}", bucket, key, byteOffsetStart);

        S3Object object;
        try {
            object = client.getObject(request);
        } catch (AmazonClientException e) {
            throw new TierObjectStoreRetriableException(
                    String.format("Failed to fetch object, metadata: %s type: %s range %s-%s",
                            objectMetadata, fileType, byteOffsetStart, byteOffsetEnd), e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException(
                    String.format("Unknown exception when fetching object, metadata: %s type: %s range %s-%s",
                            objectMetadata, fileType, byteOffsetStart, byteOffsetEnd), e);
        }

        final S3ObjectInputStream inputStream = object.getObjectContent();
        return new S3TierObjectStoreResponse(inputStream, autoAbortThresholdBytes, object.getObjectMetadata().getContentLength());
    }

    @Override
    public void putObject(ObjectStoreMetadata objectMetadata, File file, FileType fileType) {
        Map<String, String> metadata = objectMetadata.objectMetadata(clusterIdOpt, brokerIdOpt);
        try {
            putFile(keyPath(objectMetadata, fileType), metadata, file);
        } catch (AmazonClientException e) {
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
    public void putSegment(ObjectMetadata objectMetadata, File segmentData,
                           File offsetIndexData, File timestampIndexData,
                           Optional<File> producerStateSnapshotData,
                           Optional<ByteBuffer> transactionIndexData,
                           Optional<File> epochState) {
        Map<String, String> metadata = objectMetadata.objectMetadata(clusterIdOpt, brokerIdOpt);

        try {
            putFile(keyPath(objectMetadata, FileType.SEGMENT), metadata, segmentData);
            putFile(keyPath(objectMetadata, FileType.OFFSET_INDEX), metadata, offsetIndexData);
            putFile(keyPath(objectMetadata, FileType.TIMESTAMP_INDEX), metadata, timestampIndexData);
            producerStateSnapshotData.ifPresent(file -> putFile(keyPath(objectMetadata, FileType.PRODUCER_STATE), metadata, file));
            transactionIndexData.ifPresent(abortedTxnsBuf -> putBuf(keyPath(objectMetadata,
                    FileType.TRANSACTION_INDEX), metadata, abortedTxnsBuf));
            epochState.ifPresent(file -> putFile(keyPath(objectMetadata, FileType.EPOCH_STATE), metadata, file));
        } catch (AmazonClientException e) {
            throw new TierObjectStoreRetriableException("Failed to upload segment: " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when uploading segment: " + objectMetadata, e);
        }
    }

    @Override
    public void deleteSegment(ObjectMetadata objectMetadata) {
        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
        for (FileType type : FileType.values()) {
            final String keyPath = keyPath(objectMetadata, type);
            log.debug("Deleting object s3://{}/{}", bucket, keyPath);
            keys.add(new DeleteObjectsRequest.KeyVersion(keyPath));
        }

        DeleteObjectsRequest request = new DeleteObjectsRequest(bucket).withKeys(keys);
        try {
            client.deleteObjects(request);
        } catch (AmazonClientException e) {
            throw new TierObjectStoreRetriableException("Failed to delete segment: " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when deleting segment: " + objectMetadata, e);
        }
    }

    @Override
    public void close() {
        this.client.shutdown();
    }

    private String keyPath(ObjectStoreMetadata objectMetadata, TierObjectStore.FileType fileType) {
        return objectMetadata.toPath(prefix, fileType);
    }

    private com.amazonaws.services.s3.model.ObjectMetadata putObjectMetadata(Map<String, String> userMetadata) {
        final com.amazonaws.services.s3.model.ObjectMetadata metadata = new com.amazonaws.services.s3.model.ObjectMetadata();
        if (sseAlgorithm != null)
            metadata.setSSEAlgorithm(sseAlgorithm);
        if (userMetadata != null)
            metadata.setUserMetadata(userMetadata);
        return metadata;
    }

    private void putFile(String key, Map<String, String> metadata, File file) {
        final PutObjectRequest request = new PutObjectRequest(bucket, key, file).withMetadata(putObjectMetadata(metadata));
        log.debug("Uploading object to s3://{}/{}", bucket, key);
        client.putObject(request);
    }

    private void putBuf(String key, Map<String, String> metadata, ByteBuffer buf) {
        final com.amazonaws.services.s3.model.ObjectMetadata s3metadata = putObjectMetadata(metadata);
        s3metadata.setContentLength(buf.limit() - buf.position());
        final PutObjectRequest request = new PutObjectRequest(bucket, key, new ByteBufferInputStream(buf), s3metadata);
        log.debug("Uploading object to s3://{}/{}", bucket, key);
        client.putObject(request);
    }

    private static AmazonS3 client(S3TierObjectStoreConfig config) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setUserAgentPrefix("APN/1.0 Confluent/1.0 TieredStorageS3/1.0");

        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.setClientConfiguration(clientConfiguration);

        if (config.s3SignerOverride.isPresent() && !config.s3SignerOverride.get().isEmpty())
            clientConfiguration.setSignerOverride(config.s3SignerOverride.get());

        if (config.s3EndpointOverride.isPresent() && !config.s3EndpointOverride.get().isEmpty()) {
            // If the endpoint is specified, we can set an endpoint and use path style access.
            builder.setEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                            config.s3EndpointOverride.get(),
                            Regions.fromName(config.s3Region).getName()));
            builder.setPathStyleAccessEnabled(true);
        } else if (config.s3Region != null && !config.s3Region.isEmpty()) {
            builder.setRegion(config.s3Region);
        }

        if (config.s3AwsAccessKeyId.isPresent() && config.s3AwsSecretAccessKey.isPresent()) {
            final BasicAWSCredentials credentials = new BasicAWSCredentials(config.s3AwsAccessKeyId.get(),
                    config.s3AwsSecretAccessKey.get());
            builder.setCredentials(new AWSStaticCredentialsProvider(credentials));
        } else {
            builder.setCredentials(new DefaultAWSCredentialsProviderChain());
        }

        return builder.build();
    }

    private void expectBucket(final String bucket, final String expectedRegion)
            throws TierObjectStoreFatalException {
        try {
            String actualRegion = client.getBucketLocation(bucket);

            // Buckets in us-east-1 return a value of "US" due to a backwards compatibility issue in S3.
            if (actualRegion.equals("US") && expectedRegion.equals("us-east-1"))
                return;

            if (!expectedRegion.equals(actualRegion)) {
                log.warn("Bucket region {} does not match expected region {}",
                        actualRegion, expectedRegion);
            }
        } catch (final AmazonClientException ex) {
            throw new TierObjectStoreFatalException("Failed to access bucket " + bucket, ex);
        }
    }

    private static class S3TierObjectStoreResponse implements TierObjectStoreResponse {
        private final AutoAbortingS3InputStream inputStream;

        S3TierObjectStoreResponse(S3ObjectInputStream inputStream,
                                  long autoAbortSize,
                                  long streamSize) {
            this.inputStream = new AutoAbortingS3InputStream(inputStream, autoAbortSize, streamSize);
        }

        @Override
        public void close() {
            inputStream.close();
        }

        @Override
        public InputStream getInputStream() {
            return inputStream;
        }
    }
}
