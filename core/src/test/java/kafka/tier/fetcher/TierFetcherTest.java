/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.LogConfig;
import kafka.log.LogSegment;
import kafka.server.DelayedOperation;
import kafka.tier.TierTimestampAndOffset;
import kafka.tier.TopicIdPartition;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TierFetcherTest {
    private MockTime mockTime = new MockTime();
    @Test
    public void tierFetcherExceptionCausesOnComplete() throws Exception {
        ByteBuffer offsetIndexBuffer = ByteBuffer.allocate(1);
        ByteBuffer segmentFileBuffer = ByteBuffer.allocate(1);
        ByteBuffer timestampFileBuffer = ByteBuffer.allocate(1);

        MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer, offsetIndexBuffer, timestampFileBuffer);
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0);
        Metrics metrics = new Metrics();
        TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
        try {
            int maxBytes = 600;
            TierFetchMetadata fetchMetadata = new TierFetchMetadata(topicIdPartition.topicPartition(), 0,
                    Option.apply(1000L),
                    maxBytes, 1000L, true, tierObjectMetadata,
                    Option.empty(), 0, 1000);

            CompletableFuture<Boolean> f = new CompletableFuture<>();
            tierObjectStore.failNextRequest();
            assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);
            PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Arrays.asList(fetchMetadata)),
                    ignored -> f.complete(true));
            DelayedOperation delayedFetch = new MockDelayedFetch(pending);
            assertTrue(f.get(2000, TimeUnit.MILLISECONDS));

            // We fetched no bytes because there was an exception.
            assertEquals((double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0, 0);
        } finally {
            tierFetcher.close();
        }
    }

    private ByteBuffer getMemoryRecordsBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        ByteBuffer buffer2 = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        MemoryRecordsBuilder builder2 = MemoryRecords.builder(buffer2, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, 0L);
        IntStream.range(0, 50).forEach(i -> builder.appendWithOffset(i, 1L, "a".getBytes(), "v".getBytes()));
        IntStream.range(50, 101).forEach(i -> builder2.appendWithOffset(i, 1L, "a".getBytes(), "v".getBytes()));
        builder.build();
        builder2.build();
        buffer.flip();
        buffer2.flip();
        ByteBuffer combinedBuffer = ByteBuffer.allocate(2048 * 2048).put(buffer).put(buffer2);
        combinedBuffer.flip();
        return combinedBuffer;
    }

    class MockDelayedFetch extends DelayedOperation {
        PendingFetch fetch;
        MockDelayedFetch(PendingFetch fetch) {
            super(0, Option.empty());
            this.fetch = fetch;
        }

        @Override
        public void onExpiration() {

        }

        @Override
        public void onComplete() {
            fetch.finish();
        }

        @Override
        public boolean tryComplete() {
            if (fetch.isComplete()) {
                return this.forceComplete();
            } else {
                return false;
            }
        }
    }

    @Test
    public void tierFetcherRequestEmptyIndexTest() throws Exception {
        ByteBuffer combinedBuffer = getMemoryRecordsBuffer();
        TierObjectStore tierObjectStore = new MockedTierObjectStore(combinedBuffer,
                ByteBuffer.allocate(0), ByteBuffer.allocate(0));
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0);

        Metrics metrics = new Metrics();
        TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
        try {
            TierFetchMetadata fetchMetadata = new TierFetchMetadata(topicIdPartition.topicPartition(), 0,
                    Option.apply(1000L), 10000, 1000L, true, tierObjectMetadata,
                    Option.empty(), 0, 1000);

            CompletableFuture<Boolean> f = new CompletableFuture<>();
            assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);
            PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Arrays.asList(fetchMetadata)),
                    ignored -> f.complete(true));
            DelayedOperation delayedFetch = new MockDelayedFetch(pending);
            assertTrue(f.get(2000, TimeUnit.MILLISECONDS));

            Map<TopicPartition, TierFetchResult> fetchResults = pending.finish();
            assertNotNull("expected non-null fetch result", fetchResults);

            assertTrue((Double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue() > 0.0);
            assertTrue(delayedFetch.tryComplete());

            TierFetchResult fetchResult = fetchResults.get(topicIdPartition.topicPartition());
            Records records = fetchResult.records;

            long lastOffset = 0; // Start looking at offset 0
            for (Record record : records.records()) {
                assertEquals("Offset not expected", record.offset(), lastOffset);
                lastOffset += 1;
            }
        } finally {
            tierFetcher.close();
        }
    }

    private MemoryRecords buildWithOffset(long baseOffset) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        IntStream.range(0, 50).forEach(i -> builder.appendWithOffset(baseOffset + i, baseOffset + i,
                "a".getBytes(), "v".getBytes()));
        return builder.build();
    }

    @Test
    public void tierFetcherIndexTest() throws Exception {
        File logSegmentDir = TestUtils.tempDirectory();
        Properties logProps = new Properties();
        logProps.put(LogConfig.IndexIntervalBytesProp(), 1);
        Set<String> override = Collections.emptySet();
        LogConfig logConfig = LogConfig.apply(logProps, scala.collection.JavaConverters.asScalaSetConverter(override).asScala().toSet());
        LogSegment logSegment = LogSegment.open(logSegmentDir, 0, logConfig, mockTime, false, 4096, false, "");
        try {
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();

            File offsetIndexFile = logSegment.offsetIndex().file();
            ByteBuffer offsetIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(offsetIndexFile.toPath()));
            File timestampIndexFile = logSegment.offsetIndex().file();
            ByteBuffer timestampIndexBuffer =
                    ByteBuffer.wrap(Files.readAllBytes(timestampIndexFile.toPath()));
            File segmentFile = logSegment.log().file();
            ByteBuffer segmentFileBuffer = ByteBuffer.wrap(Files.readAllBytes(segmentFile.toPath()));

            TierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer,
                    offsetIndexBuffer, timestampIndexBuffer);
            TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
            TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0);
            Metrics metrics = new Metrics();

            TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
            try {
                TierFetchMetadata fetchMetadata = new TierFetchMetadata(topicIdPartition.topicPartition(), 100,
                        Option.apply(1000L), 10000, 1000L, true,
                        tierObjectMetadata, Option.empty(), 0, 1000);
                CompletableFuture<Boolean> f = new CompletableFuture<>();

                assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);
                PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Arrays.asList(fetchMetadata)),
                        ignored -> f.complete(true));
                DelayedOperation delayedFetch = new MockDelayedFetch(pending);
                assertTrue(f.get(2000, TimeUnit.MILLISECONDS));

                Map<TopicPartition, TierFetchResult> fetchResults = pending.finish();
                assertNotNull("expected non-null fetch result", fetchResults);

                assertTrue((Double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue() > 0.0);
                assertTrue(delayedFetch.tryComplete());

                TierFetchResult fetchResult = fetchResults.get(topicIdPartition.topicPartition());
                Records records = fetchResult.records;

                long lastOffset = 100L; // Start looking at offset 100
                for (Record record : records.records()) {
                    assertEquals("Offset not expected", lastOffset, record.offset());
                    lastOffset += 1;
                }
            } finally {
                tierFetcher.close();
            }

        } finally {
            logSegment.close();
        }
    }

    @Test
    public void tierTimestampIndexTest() throws Exception {
        File logSegmentDir = TestUtils.tempDirectory();
        Properties logProps = new Properties();
        logProps.put(LogConfig.IndexIntervalBytesProp(), 1);
        Set<String> override = Collections.emptySet();
        LogConfig logConfig = LogConfig.apply(logProps, scala.collection.JavaConverters.asScalaSetConverter(override).asScala().toSet());
        LogSegment logSegment = LogSegment.open(logSegmentDir, 0, logConfig, mockTime, false, 4096, false, "");
        try {
            MemoryRecords records1 = buildWithOffset(logSegment.readNextOffset());
            long largestOffset1 = logSegment.readNextOffset() + 49;
            logSegment.append(largestOffset1, largestOffset1, largestOffset1, records1);
            logSegment.flush();
            MemoryRecords records2 = buildWithOffset(logSegment.readNextOffset());
            long largestOffset2 = logSegment.readNextOffset() + 49;
            logSegment.append(largestOffset2, largestOffset2, largestOffset2, records2);
            logSegment.flush();
            MemoryRecords records3 = buildWithOffset(logSegment.readNextOffset());
            long largestOffset3 = logSegment.readNextOffset() + 49;
            logSegment.append(largestOffset3, largestOffset3, largestOffset3, records3);
            logSegment.flush();
            long largestOffset4 = logSegment.readNextOffset() + 49;
            logSegment.append(largestOffset4, largestOffset4, largestOffset4, records3);
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();
            logSegment.timeIndex().flush();
            logSegment.timeIndex().trimToValidSize();

            File offsetIndexFile = logSegment.offsetIndex().file();
            ByteBuffer offsetIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(offsetIndexFile.toPath()));
            File timestampIndexFile = logSegment.timeIndex().file();
            ByteBuffer timestampIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(timestampIndexFile.toPath()));
            File segmentFile = logSegment.log().file();
            ByteBuffer segmentFileBuffer = ByteBuffer.wrap(Files.readAllBytes(segmentFile.toPath()));


            MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer, offsetIndexBuffer, timestampIndexBuffer);
            TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
            TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0);
            Metrics metrics = new Metrics();

            TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
            try {
                // test success
                {
                    CompletableFuture<Boolean> f = new CompletableFuture<>();
                    HashMap<TopicPartition, TierTimestampAndOffset> timestamps = new HashMap<>();
                    timestamps.put(topicIdPartition.topicPartition(), new TierTimestampAndOffset(101L,
                            tierObjectMetadata));
                    PendingOffsetForTimestamp pending = tierFetcher.fetchOffsetForTimestamp(timestamps,
                            Optional.of(IsolationLevel.READ_UNCOMMITTED),
                            ignored -> f.complete(true));
                    f.get(2000, TimeUnit.MILLISECONDS);
                    assertEquals("incorrect offset for supplied timestamp returned",
                            Optional.of(new FileRecords.FileTimestampAndOffset(101, 101,
                                    Optional.empty())),
                            pending.results().get(topicIdPartition.topicPartition()));
                }
                // test failure
                {
                    tierObjectStore.failNextRequest();
                    CompletableFuture<Boolean> f = new CompletableFuture<>();
                    HashMap<TopicPartition, TierTimestampAndOffset> timestamps = new HashMap<>();
                    timestamps.put(topicIdPartition.topicPartition(), new TierTimestampAndOffset(101L,
                            tierObjectMetadata));
                    PendingOffsetForTimestamp pending = tierFetcher.fetchOffsetForTimestamp(timestamps,
                            Optional.of(IsolationLevel.READ_UNCOMMITTED),
                            ignored -> f.complete(true));
                    f.get(2000, TimeUnit.MILLISECONDS);
                    assertNotNull("tier object store through exception, pending result should "
                            + "have been completed exceptionally",
                            pending.results().get(topicIdPartition.topicPartition()).get().exception);
                }
            } finally {
                tierFetcher.close();
            }
        } finally {
            logSegment.close();
        }
    }

    @Test
    public void tierFetcherMaxBytesTest() throws Exception {
        File logSegmentDir = TestUtils.tempDirectory();
        Properties logProps = new Properties();
        logProps.put(LogConfig.IndexIntervalBytesProp(), 1);
        Set<String> override = Collections.emptySet();
        LogConfig logConfig = LogConfig.apply(logProps, scala.collection.JavaConverters.asScalaSetConverter(override).asScala().toSet());
        LogSegment logSegment = LogSegment.open(logSegmentDir, 0, logConfig, mockTime,
                false, 4096, false, "");
        try {
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset()));
            logSegment.flush();
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();

            File offsetIndexFile = logSegment.offsetIndex().file();
            ByteBuffer offsetIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(offsetIndexFile.toPath()));
            File timestampIndexFile = logSegment.timeIndex().file();
            ByteBuffer timestampIndexBuffer =
                    ByteBuffer.wrap(Files.readAllBytes(timestampIndexFile.toPath()));
            File segmentFile = logSegment.log().file();
            ByteBuffer segmentFileBuffer = ByteBuffer.wrap(Files.readAllBytes(segmentFile.toPath()));

            TierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer,
                    offsetIndexBuffer, timestampIndexBuffer);
            TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
            TopicPartition topicPartition = topicIdPartition.topicPartition();
            TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0);
            Metrics metrics = new Metrics();
            TierFetcher tierFetcher = new TierFetcher(tierObjectStore, metrics);
            try {
                int maxBytes = 600;
                TierFetchMetadata fetchMetadata =
                        new TierFetchMetadata(topicIdPartition.topicPartition(), 0,
                        Option.apply(1000L), maxBytes, 1000L, true,
                        tierObjectMetadata, Option.empty(), 0, 1000);

                CompletableFuture<Boolean> f = new CompletableFuture<>();

                assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);
                PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Arrays.asList(fetchMetadata)),
                        ignored -> f.complete(true));
                DelayedOperation delayedFetch = new MockDelayedFetch(pending);
                assertTrue(f.get(2000, TimeUnit.MILLISECONDS));

                Map<TopicPartition, TierFetchResult> fetchResults = pending.finish();
                assertNotNull("expected non-null fetch result", fetchResults);

                assertTrue((Double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue() > 0.0);
                assertTrue(delayedFetch.tryComplete());

                TierFetchResult fetchResult = fetchResults.get(topicPartition);
                Records records = fetchResult.records;
                assertTrue(fetchResult.records.sizeInBytes() <= maxBytes);

                long lastOffset = 0L; // Start looking at offset 0
                for (Record record : records.records()) {
                    assertEquals("Offset not expected", record.offset(), lastOffset);
                    lastOffset += 1;
                }
                assertEquals("When we set maxBytes low, we just read the first 50 records "
                        + "successfully.", 50, lastOffset);

            } finally {
                tierFetcher.close();
            }
        } finally {
            logSegment.close();
        }
    }

    class MockedTierObjectStore implements TierObjectStore {
        private final ByteBuffer segmentByteBuffer;
        private final ByteBuffer offsetByteBuffer;
        private final ByteBuffer timestampByteBuffer;
        private final AtomicBoolean failNextRequest = new AtomicBoolean(false);

        MockedTierObjectStore(ByteBuffer segmentByteBuffer,
                              ByteBuffer indexByteBuffer,
                              ByteBuffer timestampByteBuffer) {
            this.segmentByteBuffer = segmentByteBuffer;
            this.offsetByteBuffer = indexByteBuffer;
            this.timestampByteBuffer = timestampByteBuffer;
        }

        class MockTierObjectStoreResponse implements TierObjectStoreResponse {
            private final InputStream is;
            private final long size;

            MockTierObjectStoreResponse(InputStream is, long size) {
                this.is = is;
                this.size = size;
            }

            @Override
            public InputStream getInputStream() {
                return is;
            }

            @Override
            public Long getObjectSize() {
                return size;
            }

            @Override
            public void close() {
            }
        }

        void failNextRequest() {
            failNextRequest.set(true);
        }

        @Override
        public void close() {
        }

        @Override
        public TierObjectStoreResponse getObject(ObjectMetadata tierObjectMetadata,
                                                 FileType fileType,
                                                 Integer byteOffset,
                                                 Integer byteOffsetEnd) throws IOException {
            if (failNextRequest.compareAndSet(true, false)) {
                throw new IOException("Failed to retrieve object.");
            }
            ByteBuffer buffer;
            if (fileType == FileType.OFFSET_INDEX) {
                buffer = offsetByteBuffer;
            } else if (fileType == FileType.SEGMENT) {
                buffer = segmentByteBuffer;
            } else if (fileType == FileType.TIMESTAMP_INDEX) {
                buffer = timestampByteBuffer;
            } else {
                throw new UnsupportedOperationException();
            }

            int start = byteOffset == null ? 0 : byteOffset;
            int end = byteOffsetEnd == null ? buffer.array().length : byteOffsetEnd;
            int byteBufferSize = Math.min(end - start, buffer.array().length);
            ByteBuffer buf = ByteBuffer.allocate(byteBufferSize);
            buf.put(buffer.array(), start, end - start);
            buf.flip();

            return new MockTierObjectStoreResponse(new ByteBufferInputStream(buf), byteBufferSize);
        }

        @Override
        public void putSegment(ObjectMetadata objectMetadata,
                               File segmentData,
                               File offsetIndexData,
                               File timestampIndexData,
                               Optional<File> producerStateSnapshotData,
                               File transactionIndexData,
                               Optional<File> epochState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteSegment(ObjectMetadata objectMetadata) {
        }
    }
}