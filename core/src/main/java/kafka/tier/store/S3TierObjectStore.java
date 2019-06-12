/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.store;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.http.timers.client.ClientExecutionTimeoutException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import kafka.log.Log;
import kafka.tier.exceptions.TierObjectStoreFatalException;
import kafka.tier.exceptions.TierObjectStoreRetriableException;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class S3TierObjectStore implements TierObjectStore {
    // LOG_DATA_PREFIX is where segment, offset index, time index, transaction index, leader
    // epoch state checkpoint, and producer state snapshot data are stored.
    private final static String LOG_DATA_PREFIX = "0";
    private final static Logger log = LoggerFactory.getLogger(S3TierObjectStore.class);
    private final String clusterId;
    private final int brokerId;
    private final String bucket;
    private final String sseAlgorithm;
    private final int partUploadSize;
    private AmazonS3 client;

    public S3TierObjectStore(TierObjectStoreConfig config) {
        this(client(config), config);
    }

    // used for testing
    S3TierObjectStore(AmazonS3 client, TierObjectStoreConfig config) {
        this.clusterId = config.clusterId;
        this.brokerId = config.brokerId;
        this.client = client;
        this.bucket = config.s3bucket;
        this.sseAlgorithm = config.s3SseAlgorithm;
        this.partUploadSize = config.s3MultipartUploadSize;
        expectBucket(bucket, config.s3Region);
    }

    @Override
    public TierObjectStoreResponse getObject(ObjectMetadata objectMetadata,
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
        } catch (AmazonServiceException e) {
            throw new TierObjectStoreRetriableException("Failed to fetch segment " + objectMetadata, e);
        } catch (ClientExecutionTimeoutException e) {
            throw new TierObjectStoreRetriableException("Timeout when fetching segment " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when fetching segment " + objectMetadata, e);
        }

        final S3ObjectInputStream inputStream = object.getObjectContent();
        return new S3TierObjectStoreResponse(inputStream, object.getObjectMetadata().getContentLength());
    }

    @Override
    public void putSegment(ObjectMetadata objectMetadata, File segmentData,
                           File offsetIndexData, File timestampIndexData,
                           Optional<File> producerStateSnapshotData,
                           Optional<ByteBuffer> transactionIndexData,
                           Optional<File> epochState) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("metadata_version", Integer.toString(objectMetadata.version()));
        metadata.put("topic", objectMetadata.topicIdPartition().topic());
        metadata.put("cluster_id", clusterId);
        metadata.put("broker_id", Integer.toString(brokerId));

        try {
            if (segmentData.length() <= partUploadSize)
                putFile(keyPath(objectMetadata, FileType.SEGMENT), metadata, segmentData);
            else
                putFileMultipart(keyPath(objectMetadata, FileType.SEGMENT), metadata, segmentData);

            putFile(keyPath(objectMetadata, FileType.OFFSET_INDEX), metadata, offsetIndexData);
            putFile(keyPath(objectMetadata, FileType.TIMESTAMP_INDEX), metadata, timestampIndexData);
            producerStateSnapshotData.ifPresent(file -> putFile(keyPath(objectMetadata, FileType.PRODUCER_STATE), metadata, file));
            transactionIndexData.ifPresent(abortedTxnsBuf -> putBuf(keyPath(objectMetadata,
                    FileType.TRANSACTION_INDEX), metadata, abortedTxnsBuf));
            epochState.ifPresent(file -> putFile(keyPath(objectMetadata, FileType.EPOCH_STATE), metadata, file));
        } catch (AmazonServiceException e) {
            throw new TierObjectStoreRetriableException("Failed to upload segment " + objectMetadata, e);
        } catch (ClientExecutionTimeoutException e) {
            throw new TierObjectStoreRetriableException("Timeout when uploading segment " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when uploading segment " + objectMetadata, e);
        }
    }

    @Override
    public void deleteSegment(ObjectMetadata objectMetadata) {
        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
        for (FileType type : FileType.values())
            keys.add(new DeleteObjectsRequest.KeyVersion(keyPath(objectMetadata, type)));
        DeleteObjectsRequest request = new DeleteObjectsRequest(bucket).withKeys(keys);
        log.debug("Deleting " + keys);

        try {
            client.deleteObjects(request);
        } catch (AmazonServiceException e) {
            throw new TierObjectStoreRetriableException("Failed to delete segment " + objectMetadata, e);
        } catch (ClientExecutionTimeoutException e) {
            throw new TierObjectStoreRetriableException("Timeout when deleting segment " + objectMetadata, e);
        } catch (Exception e) {
            throw new TierObjectStoreFatalException("Unknown exception when deleting segment " + objectMetadata, e);
        }
    }

    @Override
    public void close() {
        this.client.shutdown();
    }

    public String keyPath(ObjectMetadata objectMetadata, FileType fileType) {
        return LOG_DATA_PREFIX
                + "/" + objectMetadata.objectIdAsBase64()
                + "/" + objectMetadata.topicIdPartition().topicIdAsBase64()
                + "/" + objectMetadata.topicIdPartition().partition()
                + "/" + Log.filenamePrefixFromOffset(objectMetadata.baseOffet())
                + "_" + objectMetadata.tierEpoch()
                + "_v" + objectMetadata.version()
                + "." + fileType.suffix();
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
        final PutObjectRequest request = new PutObjectRequest(bucket, key,
                new ByteBufferInputStream(buf), putObjectMetadata(metadata));
        log.debug("Uploading object to s3://{}/{}", bucket, key);
        client.putObject(request);
    }

    private void putFileMultipart(String key, Map<String, String> metadata, File file) {
        final long fileLength = file.length();
        long partSize = partUploadSize;
        log.debug("Uploading multipart object to s3://{}/{}", bucket, key);

        final List<PartETag> partETags = new ArrayList<>();
        final InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucket, key, putObjectMetadata(metadata));
        final InitiateMultipartUploadResult initiateMultipartUploadResult = client.initiateMultipartUpload(initiateMultipartUploadRequest);

        long filePosition = 0;
        for (int partNum = 1; filePosition < fileLength; partNum++) {
            partSize = Math.min(partSize, fileLength - filePosition);
            UploadPartRequest uploadPartRequest = new UploadPartRequest()
                    .withBucketName(bucket)
                    .withKey(key)
                    .withUploadId(initiateMultipartUploadResult.getUploadId())
                    .withPartNumber(partNum)
                    .withFile(file)
                    .withFileOffset(filePosition)
                    .withPartSize(partSize);

            UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
            partETags.add(uploadPartResult.getPartETag());
            filePosition += partSize;
        }

        final CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(
                        bucket, key, initiateMultipartUploadResult.getUploadId(), partETags);
        client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private static AmazonS3 client(TierObjectStoreConfig config) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.setClientConfiguration(clientConfiguration);

        if (config.s3SignerOverride != null && !config.s3SignerOverride.isEmpty())
            clientConfiguration.setSignerOverride(config.s3SignerOverride);

        if (config.s3EndpointOverride != null && !config.s3EndpointOverride.isEmpty()) {
            // If the endpoint is specified, we can set an endpoint and use path style access.
            builder.setEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(
                            config.s3EndpointOverride,
                            Regions.fromName(config.s3Region).getName()));
            builder.setPathStyleAccessEnabled(true);
        } else if (config.s3Region != null && !config.s3Region.isEmpty()) {
            builder.setRegion(config.s3Region);
        }

        if (config.s3AwsAccessKeyId != null && config.s3AwsSecretAccessKey != null) {
            final BasicAWSCredentials credentials = new BasicAWSCredentials(config.s3AwsAccessKeyId,
                    config.s3AwsSecretAccessKey);
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
        private final long objectSize;

        S3TierObjectStoreResponse(S3ObjectInputStream inputStream, long objectSize) {
            this.inputStream = new AutoAbortingS3InputStream(inputStream, objectSize);
            this.objectSize = objectSize;
        }

        @Override
        public void close() {
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
