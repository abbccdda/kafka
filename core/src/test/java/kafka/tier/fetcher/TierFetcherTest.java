/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.LogConfig;
import kafka.log.LogSegment;
import kafka.server.DelayedOperation;
import kafka.server.KafkaConfig;
import kafka.tier.TierTimestampAndOffset;
import kafka.tier.TopicIdPartition;
import kafka.tier.fetcher.offsetcache.FetchOffsetCache;
import kafka.tier.store.MockInMemoryTierObjectStore;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import kafka.utils.KafkaScheduler;
import org.apache.kafka.common.IsolationLevel;
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
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import scala.compat.java8.OptionConverters;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertTrue;
import static org.easymock.EasyMock.createNiceMock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class TierFetcherTest {
    private MockTime mockTime = new MockTime();

    private boolean futureReady(long timeoutMs, CompletableFuture<?> future) {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {
            return false;
        }
        return true;
    }

    @Test
    public void tierFetcherCancellationUnblocksWaitingForMemory() throws InterruptedException {
        ByteBuffer combinedBuffer = getMemoryRecordsBuffer();
        TierObjectStore tierObjectStore = new MockedTierObjectStore(combinedBuffer,
                ByteBuffer.allocate(0), ByteBuffer.allocate(0));
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);
        Metrics metrics = new Metrics();
        KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
        TierFetcherConfig tierFetcherConfig = new TierFetcherConfig(1, Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE, 1L);
        TierFetcher tierFetcher = new TierFetcher(mockTime, tierFetcherConfig, tierObjectStore,
                kafkaScheduler, metrics, new LogContext());

        // first, drain the tierFetcher memory pool so it's negative
        tierFetcher.memoryTracker().newLease(CancellationContext.newContext(), 1024 * 1024);
        assertFalse("expected tierfetcher to have less than zero bytes available in pool, further"
                        + " lease attempts should fail",
                tierFetcher.memoryTracker().tryLease(100).isPresent());

        // Issue a tier fetch, it will be blocked on allocation from the memory pool
        int maxBytes = 600;
        TierFetchMetadata fetchMetadata = new TierFetchMetadata(topicIdPartition.topicPartition(), 0,
                maxBytes, 1000L, true, tierObjectMetadata,
                OptionConverters.toScala(Optional.empty()), 0, 1000);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Collections.singletonList(fetchMetadata)),
                IsolationLevel.READ_UNCOMMITTED,
                ignored -> future.complete(true),
                0);
        Thread runThread = new Thread(pending);

        assertFalse("expected fetch to be blocked on memory allocation", futureReady(100, future));
        pending.cancel();
        assertTrue("expected canceling the fetch to unblock the memory allocation", futureReady(1000, future));
        runThread.join();
    }

    @Test
    public void tierFetcherExceptionCausesOnComplete() throws Exception {
        ByteBuffer offsetIndexBuffer = ByteBuffer.allocate(0);
        ByteBuffer segmentFileBuffer = getMemoryRecordsBuffer();
        ByteBuffer timestampFileBuffer = ByteBuffer.allocate(0);

        MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer, offsetIndexBuffer, timestampFileBuffer);
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);
        Metrics metrics = new Metrics();
        KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
        TierFetcher tierFetcher = new TierFetcher(mockTime, tierObjectStore, kafkaScheduler, metrics);
        try {
            int maxBytes = 600;
            TierFetchMetadata fetchMetadata = new TierFetchMetadata(topicIdPartition.topicPartition(), 0,
                    maxBytes, 1000L, true, tierObjectMetadata,
                    OptionConverters.toScala(Optional.empty()), 0, 1000);

            CompletableFuture<Boolean> f = new CompletableFuture<>();
            tierObjectStore.failNextRequest();
            assertEquals(0.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0);
            assertEquals(0.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.fetchExceptionTotalMetricName).metricValue(), 0);
            PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Collections.singletonList(fetchMetadata)),
                    IsolationLevel.READ_UNCOMMITTED,
                    ignored -> f.complete(true),
                    0);
            assertTrue(f.get(2000, TimeUnit.MILLISECONDS));

            TierFetchResult result =
                    pending.finish().get(tierObjectMetadata.topicIdPartition().topicPartition());
            assertEquals("expected returned records to be empty due to exception thrown", ReclaimableMemoryRecords.EMPTY, result.records);

            // We fetched no bytes because there was an exception.
            assertEquals(0.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0);
            assertEquals(1.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.fetchExceptionTotalMetricName).metricValue(), 0);
        } finally {
            tierFetcher.close();
        }
        assertEquals("expected zero leased bytes", 0, tierFetcher.memoryTracker().leased());
    }

    @Test
    public void tierFetcherFetchCancelled() throws Exception {
        ByteBuffer offsetIndexBuffer = ByteBuffer.allocate(0);
        ByteBuffer segmentFileBuffer = getMemoryRecordsBuffer();
        ByteBuffer timestampFileBuffer = ByteBuffer.allocate(0);

        MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer, offsetIndexBuffer, timestampFileBuffer);
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);
        Metrics metrics = new Metrics();
        KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
        TierFetcher tierFetcher = new TierFetcher(mockTime, tierObjectStore, kafkaScheduler, metrics);
        try {
            int maxBytes = 600;
            TierFetchMetadata fetchMetadata =
                    new TierFetchMetadata(topicIdPartition.topicPartition(), 0, maxBytes, 1000L,
                            true, tierObjectMetadata, OptionConverters.toScala(Optional.empty()), 0, 1000);

            CompletableFuture<Boolean> f = new CompletableFuture<>();
            assertEquals(0.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0);
            assertEquals(0.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.fetchCancellationTotalMetricName).metricValue(), 0);
            PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Collections.singletonList(fetchMetadata)),
                    IsolationLevel.READ_UNCOMMITTED,
                    ignored -> f.complete(true),
                    0);
            pending.cancel();
            assertTrue(f.get(2000, TimeUnit.MILLISECONDS));
            Map<TopicPartition, TierFetchResult> results = pending.finish();
            TierFetchResult result = results.get(topicIdPartition.topicPartition());

            if (result.records.sizeInBytes() > 0) {
                assertTrue((double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue() > 0);
            }
            if (result.exception == null) {
                assertEquals(0.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.fetchExceptionTotalMetricName).metricValue(), 0);
            }
            pending.markFetchExpired();
            assertEquals("expected 1 cancellation", 1.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.fetchCancellationTotalMetricName).metricValue(), 0);
            result.records.release();
        } finally {
            tierFetcher.close();
        }

        assertEquals("expected zero leased bytes", 0, tierFetcher.memoryTracker().leased());
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

    static class MockDelayedFetch extends DelayedOperation {
        PendingFetch fetch;
        MockDelayedFetch(PendingFetch fetch) {
            super(0, OptionConverters.toScala(Optional.empty()));
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
        TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);

        Metrics metrics = new Metrics();
        KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
        TierFetcher tierFetcher = new TierFetcher(mockTime, tierObjectStore, kafkaScheduler, metrics);
        try {
            TierFetchMetadata fetchMetadata = new TierFetchMetadata(topicIdPartition.topicPartition(), 0,
                    10000, 1000L, true, tierObjectMetadata,
                    OptionConverters.toScala(Optional.empty()), 0, 1000);

            CompletableFuture<Boolean> f = new CompletableFuture<>();
            assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);
            PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Collections.singletonList(fetchMetadata)),
                    IsolationLevel.READ_UNCOMMITTED,
                    ignored -> f.complete(true),
                    0);
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
            fetchResult.records.release();
        } finally {
            tierFetcher.close();
        }
        assertEquals("expected zero leased bytes", 0, tierFetcher.memoryTracker().leased());
    }

    @Test
    public void tierFetcherLocateTargetOffsetTest() throws Exception {
        MemoryRecords[] recordArr = {
                buildWithOffset(0, 50),
                buildWithOffset(50, 50),
                buildWithOffset(100, 50),
                buildWithOffset(150, 50),
                buildWithOffset(200, 50)
        };

        int indexInterval = recordArr[0].sizeInBytes() + 1;
        File logSegmentDir = TestUtils.tempDirectory();
        Properties logProps = new Properties();
        logProps.put(LogConfig.IndexIntervalBytesProp(), indexInterval);
        Set<String> override = Collections.emptySet();
        LogConfig logConfig = LogConfig.apply(logProps, scala.collection.JavaConverters.asScalaSetConverter(override).asScala().toSet());
        LogSegment logSegment = LogSegment.open(logSegmentDir, 0, logConfig, mockTime, false, 4096, false, "");

        try {
            for (MemoryRecords records : recordArr)
                logSegment.append(records.batches().iterator().next().baseOffset(), 1L, 1, records);
            logSegment.flush();
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();

            long expectedEndOffset = logSegment.readNextOffset() - 1;

            File offsetIndexFile = logSegment.offsetIndex().file();
            ByteBuffer offsetIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(offsetIndexFile.toPath()));
            File timestampIndexFile = logSegment.offsetIndex().file();
            ByteBuffer timestampIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(timestampIndexFile.toPath()));
            File segmentFile = logSegment.log().file();
            ByteBuffer segmentFileBuffer = ByteBuffer.wrap(Files.readAllBytes(segmentFile.toPath()));

            MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer,
                    offsetIndexBuffer, timestampIndexBuffer);
            TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
            UUID objectId = UUID.randomUUID();
            TierObjectStore.ObjectMetadata tierObjectMetadata =
                    new TierObjectStore.ObjectMetadata(topicIdPartition, objectId, 0, 0, false, false, false);
            Metrics metrics = new Metrics();

            KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
            TierFetcher tierFetcher = new TierFetcher(mockTime, tierObjectStore, kafkaScheduler, metrics);
            try {
                int expectedCacheEntries = 0;
                long fetchOffset = 150L;

                while (fetchOffset < expectedEndOffset) {
                    TierFetchMetadata fetchMetadata =
                            new TierFetchMetadata(topicIdPartition.topicPartition(), fetchOffset,
                                    1000, 1000L, true,
                                    tierObjectMetadata, OptionConverters.toScala(Optional.empty()), 0, segmentFileBuffer.limit());
                    CompletableFuture<Boolean> f = new CompletableFuture<>();

                    PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Collections.singletonList(fetchMetadata)),
                            IsolationLevel.READ_UNCOMMITTED,
                            ignored -> f.complete(true),
                            0);
                    DelayedOperation delayedFetch = new MockDelayedFetch(pending);
                    assertTrue(f.get(4000, TimeUnit.MILLISECONDS));

                    Map<TopicPartition, TierFetchResult> fetchResults = pending.finish();
                    assertNotNull("expected non-null fetch result", fetchResults);

                    assertTrue((Double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue() > 0.0);
                    assertTrue(delayedFetch.tryComplete());

                    TierFetchResult fetchResult = fetchResults.get(topicIdPartition.topicPartition());
                    Records records = fetchResult.records;
                    for (Record record : records.records()) {
                        assertEquals("Offset not expected", fetchOffset, record.offset());
                        fetchOffset++;
                    }

                    // cache entry will not be inserted for final read, as there's nothing after
                    // the final read for the segment
                    if (fetchOffset < expectedEndOffset)
                        expectedCacheEntries++;

                    final long expected = expectedCacheEntries;
                    TestUtils.waitForCondition(() -> expected == tierFetcher.cache.size(),
                            "cache not updated by timeout");
                    fetchResult.records.release();
                }
                assertEquals(fetchOffset - 1, expectedEndOffset);
                assertEquals("offset index should have been used exactly once, for the initial fetch", 1, tierObjectStore.offsetIndexReads);
            } finally {
                tierFetcher.close();
            }
            assertEquals("expected zero leased bytes", 0, tierFetcher.memoryTracker().leased());
        } finally {
            logSegment.deleteIfExists();
        }
    }

    @Test
    public void tierFetcherRepeatedFetchesViaOffsetCacheTest() throws Exception {
        // this test performs repeated tier fetches from a single segment
        // to test the use of the offset cache. The first fetch will be from the start of the
        // segment, and this will seed the offset cache such that none of the remaining fetches
        // require offset index lookups to be completed.
        File logSegmentDir = TestUtils.tempDirectory();
        Properties logProps = new Properties();
        logProps.put(LogConfig.IndexIntervalBytesProp(), 1);
        Set<String> override = Collections.emptySet();
        LogConfig logConfig = LogConfig.apply(logProps, scala.collection.JavaConverters.asScalaSetConverter(override).asScala().toSet());
        LogSegment logSegment = LogSegment.open(logSegmentDir, 0, logConfig, mockTime, false, 4096, false, "");
        try {
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset(), 50));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset(), 50));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset(), 50));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset(), 50));
            logSegment.flush();
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();

            long expectedEndOffset = logSegment.readNextOffset() - 1;

            File offsetIndexFile = logSegment.offsetIndex().file();
            ByteBuffer offsetIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(offsetIndexFile.toPath()));
            File timestampIndexFile = logSegment.offsetIndex().file();
            ByteBuffer timestampIndexBuffer =
                    ByteBuffer.wrap(Files.readAllBytes(timestampIndexFile.toPath()));
            File segmentFile = logSegment.log().file();
            ByteBuffer segmentFileBuffer = ByteBuffer.wrap(Files.readAllBytes(segmentFile.toPath()));

            MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer,
                    offsetIndexBuffer, timestampIndexBuffer);
            TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
            UUID objectId = UUID.randomUUID();
            TierObjectStore.ObjectMetadata tierObjectMetadata =
                    new TierObjectStore.ObjectMetadata(topicIdPartition, objectId, 0, 0, false, false, false);
            Metrics metrics = new Metrics();

            KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
            TierFetcher tierFetcher = new TierFetcher(mockTime, tierObjectStore, kafkaScheduler, metrics);
            try {
                int expectedCacheEntries = 0;
                long fetchOffset = 0L;
                while (fetchOffset < expectedEndOffset) {
                    TierFetchMetadata fetchMetadata =
                            new TierFetchMetadata(topicIdPartition.topicPartition(), fetchOffset,
                                    1000, 1000L, true,
                                    tierObjectMetadata, OptionConverters.toScala(Optional.empty()), 0, segmentFileBuffer.limit());
                    CompletableFuture<Boolean> f = new CompletableFuture<>();

                    PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Collections.singletonList(fetchMetadata)),
                            IsolationLevel.READ_UNCOMMITTED,
                            ignored -> f.complete(true),
                            0);
                    DelayedOperation delayedFetch = new MockDelayedFetch(pending);
                    assertTrue(f.get(4000, TimeUnit.MILLISECONDS));

                    Map<TopicPartition, TierFetchResult> fetchResults = pending.finish();
                    assertNotNull("expected non-null fetch result", fetchResults);

                    assertTrue((Double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue() > 0.0);
                    assertTrue(delayedFetch.tryComplete());

                    TierFetchResult fetchResult = fetchResults.get(topicIdPartition.topicPartition());
                    Records records = fetchResult.records;
                    for (Record record : records.records()) {
                        assertEquals("Offset not expected", fetchOffset, record.offset());
                        fetchOffset++;
                    }

                    // cache entry will not be inserted for final read, as there's nothing after
                    // the final read for the segment
                    if (fetchOffset < expectedEndOffset)
                        expectedCacheEntries++;

                    final long expected = expectedCacheEntries;
                    TestUtils.waitForCondition(() -> expected == tierFetcher.cache.size(),
                            "cache not updated by timeout");
                    fetchResult.records.release();
                }
                assertEquals(fetchOffset - 1, expectedEndOffset);
                assertEquals(1.0, tierFetcher.cache.hitRatio(), 0.0001);
                assertEquals("offset index should not have been used", 0, tierObjectStore.offsetIndexReads);
            } finally {
                tierFetcher.close();
            }
            assertEquals("expected zero leased bytes", 0, tierFetcher.memoryTracker().leased());
        } finally {
            logSegment.deleteIfExists();
        }
    }

    private MemoryRecords buildWithOffset(long baseOffset, int numRecords) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, baseOffset);
        IntStream.range(0, numRecords).forEach(i -> builder.appendWithOffset(baseOffset + i, baseOffset + i,
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
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset(), 50));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset(), 50));
            logSegment.flush();
            logSegment.append(logSegment.readNextOffset() + 49, 1L, 1, buildWithOffset(logSegment.readNextOffset(), 50));
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
            TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);
            Metrics metrics = new Metrics();

            KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
            TierFetcher tierFetcher = new TierFetcher(mockTime, tierObjectStore, kafkaScheduler, metrics);
            try {
                TierFetchMetadata fetchMetadata = new TierFetchMetadata(topicIdPartition.topicPartition(), 100,
                        10000, 1000L, true,
                        tierObjectMetadata, OptionConverters.toScala(Optional.empty()), 0, 1000);
                CompletableFuture<Boolean> f = new CompletableFuture<>();

                assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);
                PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Collections.singletonList(fetchMetadata)),
                        IsolationLevel.READ_UNCOMMITTED,
                        ignored -> f.complete(true),
                        0);
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
                fetchResult.records.release();
            } finally {
                tierFetcher.close();
            }
            assertEquals("expected zero leased bytes", 0, tierFetcher.memoryTracker().leased());
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
            MemoryRecords records1 = buildWithOffset(logSegment.readNextOffset(), 50);
            long largestOffset1 = logSegment.readNextOffset() + 49;
            logSegment.append(largestOffset1, largestOffset1, largestOffset1, records1);
            logSegment.flush();
            MemoryRecords records2 = buildWithOffset(logSegment.readNextOffset(), 50);
            long largestOffset2 = logSegment.readNextOffset() + 49;
            logSegment.append(largestOffset2, largestOffset2, largestOffset2, records2);
            logSegment.flush();
            MemoryRecords records3 = buildWithOffset(logSegment.readNextOffset(), 50);
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
            TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);
            Metrics metrics = new Metrics();
            KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
            TierFetcher tierFetcher = new TierFetcher(mockTime, tierObjectStore, kafkaScheduler, metrics);
            try {
                // test success
                {
                    CompletableFuture<Boolean> f = new CompletableFuture<>();
                    HashMap<TopicPartition, TierTimestampAndOffset> timestamps = new HashMap<>();
                    timestamps.put(topicIdPartition.topicPartition(), new TierTimestampAndOffset(101L,
                            tierObjectMetadata, segmentFileBuffer.limit()));
                    PendingOffsetForTimestamp pending = tierFetcher.fetchOffsetForTimestamp(timestamps,
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
                            tierObjectMetadata, segmentFileBuffer.limit()));
                    PendingOffsetForTimestamp pending = tierFetcher.fetchOffsetForTimestamp(timestamps, ignored -> f.complete(true));
                    f.get(2000, TimeUnit.MILLISECONDS);
                    assertNotNull("tier object store through exception, pending result should "
                            + "have been completed exceptionally",
                            pending.results().get(topicIdPartition.topicPartition()).get().exception);
                    assertEquals(1.0, (double) metrics.metric(tierFetcher.tierFetcherMetrics.fetchOffsetForTimestampExceptionTotalMetricName).metricValue(), 0);
                }
            } finally {
                tierFetcher.close();
            }
            assertEquals("expected zero leased bytes", 0, tierFetcher.memoryTracker().leased());
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
            NavigableMap<Integer, Long> bytesToOffset = new TreeMap<>();

            MemoryRecords toAppend = buildWithOffset(0, 50);
            logSegment.append(49, 1L, 1, toAppend);
            bytesToOffset.put(logSegment.size(), 49L);

            toAppend = buildWithOffset(50, 50);
            logSegment.append(99, 1L, 1, toAppend);
            bytesToOffset.put(logSegment.size(), 99L);

            toAppend = buildWithOffset(100, 50);
            logSegment.append(149, 1L, 1, toAppend);
            bytesToOffset.put(logSegment.size(), 149L);

            logSegment.flush();
            logSegment.offsetIndex().flush();
            logSegment.offsetIndex().trimToValidSize();

            File offsetIndexFile = logSegment.offsetIndex().file();
            ByteBuffer offsetIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(offsetIndexFile.toPath()));
            File timestampIndexFile = logSegment.timeIndex().file();
            ByteBuffer timestampIndexBuffer = ByteBuffer.wrap(Files.readAllBytes(timestampIndexFile.toPath()));
            File segmentFile = logSegment.log().file();
            ByteBuffer segmentFileBuffer = ByteBuffer.wrap(Files.readAllBytes(segmentFile.toPath()));

            TierObjectStore tierObjectStore = new MockedTierObjectStore(segmentFileBuffer, offsetIndexBuffer, timestampIndexBuffer);
            TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
            TopicPartition topicPartition = topicIdPartition.topicPartition();
            TierObjectStore.ObjectMetadata tierObjectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);
            Metrics metrics = new Metrics();
            KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
            TierFetcher tierFetcher = new TierFetcher(mockTime, tierObjectStore, kafkaScheduler, metrics);
            try {
                int maxBytes = 600;
                int[] overrideMaxBytesArray = {0, 400, 600, segmentFileBuffer.remaining() - 1, segmentFileBuffer.remaining(), segmentFileBuffer.remaining() + 1};
                TierFetchMetadata fetchMetadata =
                        new TierFetchMetadata(topicIdPartition.topicPartition(), 0,
                        maxBytes, 1000L, true,
                        tierObjectMetadata, OptionConverters.toScala(Optional.empty()), 0, 1000);
                assertEquals(metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue(), 0.0);

                for (int overrideMaxBytes : overrideMaxBytesArray) {
                    CompletableFuture<Boolean> f = new CompletableFuture<>();
                    PendingFetch pending = tierFetcher.fetch(new ArrayList<>(Collections.singletonList(fetchMetadata)),
                            IsolationLevel.READ_UNCOMMITTED,
                            ignored -> f.complete(true),
                            overrideMaxBytes);
                    DelayedOperation delayedFetch = new MockDelayedFetch(pending);
                    assertTrue(f.get(2000, TimeUnit.MILLISECONDS));

                    Map<TopicPartition, TierFetchResult> fetchResults = pending.finish();
                    assertNotNull("expected non-null fetch result", fetchResults);

                    assertTrue((Double) metrics.metric(tierFetcher.tierFetcherMetrics.bytesFetchedTotalMetricName).metricValue() > 0.0);
                    assertTrue(delayedFetch.tryComplete());

                    TierFetchResult fetchResult = fetchResults.get(topicPartition);
                    Records records = fetchResult.records;

                    int expectedFetchBytes = Math.min(Math.max(maxBytes, overrideMaxBytes), segmentFileBuffer.remaining());
                    assertTrue(fetchResult.records.sizeInBytes() <= expectedFetchBytes);

                    long currentOffset = -1L;
                    for (Record record : records.records()) {
                        currentOffset += 1;
                        assertEquals("Offset not expected", record.offset(), currentOffset);
                    }

                    // Validate the last offset fetched based on the number of bytes fetched
                    long expectedLastOffset = bytesToOffset.floorEntry(expectedFetchBytes).getValue();
                    assertEquals("Unexpected lastOffset for overrideMaxBytes " + overrideMaxBytes,
                            expectedLastOffset, currentOffset);
                    fetchResult.records.release();
                }
            } finally {
                tierFetcher.close();
            }
            assertEquals("expected zero leased bytes", 0, tierFetcher.memoryTracker().leased());
        } finally {
            logSegment.close();
        }
    }

    /**
     * If there is an exception thrown from the TierObjectStore, test that the memory lease for the
     * initial fetch is released.
     */
    @Test
    public void testTierObjectStoreExceptionReleasesLease() {
        CancellationContext ctx = CancellationContext.newContext();
        ByteBuffer combinedBuffer = getMemoryRecordsBuffer();
        MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(combinedBuffer,
                ByteBuffer.allocate(0), ByteBuffer.allocate(0));
        FetchOffsetCache fetchOffsetCache = new FetchOffsetCache(mockTime, 0, 0);
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TierObjectStore.ObjectMetadata objectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);
        // MemoryTracker containing a single byte, this implies that we will have a single lease.
        MemoryTracker memoryTracker = new MemoryTracker(mockTime, 1);
        PendingFetch pendingFetch = new PendingFetch(ctx,
                tierObjectStore,
                fetchOffsetCache,
                Optional.empty(),
                objectMetadata,
                ignored -> { },
                0,
                1024,
                1024,
                IsolationLevel.READ_UNCOMMITTED,
                memoryTracker,
                Collections.emptyList());

        // fail the next request, this should result in all memory released
        tierObjectStore.failNextRequest();
        pendingFetch.run();
        Map<TopicPartition, TierFetchResult> results = pendingFetch.finish();
        TierFetchResult tierFetchResult = results.get(topicIdPartition.topicPartition());
        assertNotNull("expected fetch to return an exception", tierFetchResult.exception);
        assertEquals("expected all memory to be returned to the memory tracker", 0, memoryTracker.leased());
    }

    /**
     * Test that when we know the size of the first record batch, a more accurate memory lease is
     * used (and blocked on).
     */
    @Test
    public void memoryLeaseWithKnownFirstBatchSize() throws InterruptedException {
        CancellationContext ctx = CancellationContext.newContext();
        ByteBuffer combinedBuffer = getMemoryRecordsBuffer();
        MockedTierObjectStore tierObjectStore = new MockedTierObjectStore(combinedBuffer,
                ByteBuffer.allocate(0), ByteBuffer.allocate(0));
        FetchOffsetCache fetchOffsetCache = new FetchOffsetCache(mockTime, Integer.MAX_VALUE, Integer.MAX_VALUE);

        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TierObjectStore.ObjectMetadata objectMetadata = new TierObjectStore.ObjectMetadata(topicIdPartition, UUID.randomUUID(), 0, 0, false, false, false);
        int firstBatchSize = 2048;
        long memoryTrackerCapacity = 1024;
        // Add an offset cache entry for targetOffset == 1, this is larger than the memory tracker
        // capacity
        fetchOffsetCache.put(objectMetadata.objectId(), 1, 0, OptionalInt.of(firstBatchSize));
        // MemoryTracker containing a single byte, this implies that we will have a lease.
        MemoryTracker memoryTracker = new MemoryTracker(mockTime, memoryTrackerCapacity);
        // take out a lease to exhaust the memory tracker
        MemoryTracker.MemoryLease firstLease = memoryTracker.newLease(ctx, memoryTrackerCapacity * 2);
        PendingFetch pendingFetch = new PendingFetch(ctx,
                tierObjectStore,
                fetchOffsetCache,
                Optional.empty(),
                objectMetadata,
                ignored -> { },
                1, // targetOffset == 1
                1024,
                1024,
                IsolationLevel.READ_UNCOMMITTED,
                memoryTracker,
                Collections.emptyList());
        Thread running = new Thread(pendingFetch);
        running.start();
        assertFalse("expected pending fetch to be blocked on memory allocation", pendingFetch.isComplete());
        // release the initial lease, this should unblock the pendingfetch
        firstLease.release();
        memoryTracker.wakeup();
        running.join();
        Map<TopicPartition, TierFetchResult> results = pendingFetch.finish();
        TierFetchResult tierFetchResult = results.get(topicIdPartition.topicPartition());
        assertEquals("expected exactly firstBatchSize bytes to be leased", firstBatchSize, memoryTracker.leased());
        tierFetchResult.records.release();
        assertEquals("expected releasing the records returns leased memory to the MemoryTracker", 0, memoryTracker.leased());
    }

    @Test
    public void testResizeTierFetcherMemoryPoolDynamically() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(KafkaConfig.ZkConnectProp(), "127.0.0.1:0000");
        cfg.put(KafkaConfig.TierFetcherMemoryPoolSizeBytesProp(), "1024");
        KafkaConfig oldConfig = new KafkaConfig(cfg);
        TierFetcherConfig config = new TierFetcherConfig(oldConfig);
        KafkaScheduler kafkaScheduler = createNiceMock(KafkaScheduler.class);
        TierFetcher tierFetcher = new TierFetcher(mockTime, config, new MockInMemoryTierObjectStore(null), kafkaScheduler, new Metrics(), new LogContext());
        Assert.assertTrue("expected TierFetcher memory pool size to be reconfigurable", tierFetcher.reconfigurableConfigs().contains(KafkaConfig.TierFetcherMemoryPoolSizeBytesProp()));
        Assert.assertEquals("expected TierFetcher memory pool size to match what was set originally", tierFetcher.memoryTracker().poolSize(), 1024);

        cfg.put(KafkaConfig.TierFetcherMemoryPoolSizeBytesProp(), "0");
        tierFetcher.reconfigure(null, new KafkaConfig(cfg));

        Assert.assertEquals("expected TierFetcher memory pool size to be updated to the new size", tierFetcher.memoryTracker().poolSize(), 0);

    }

    class MockedTierObjectStore implements TierObjectStore {
        private final ByteBuffer segmentByteBuffer;
        private final ByteBuffer offsetByteBuffer;
        private final ByteBuffer timestampByteBuffer;
        private final AtomicBoolean failNextRequest = new AtomicBoolean(false);
        int segmentReads = 0;
        int offsetIndexReads = 0;
        int timestampIndexReads = 0;

        MockedTierObjectStore(ByteBuffer segmentByteBuffer,
                              ByteBuffer indexByteBuffer,
                              ByteBuffer timestampByteBuffer) {
            this.segmentByteBuffer = segmentByteBuffer;
            this.offsetByteBuffer = indexByteBuffer;
            this.timestampByteBuffer = timestampByteBuffer;
        }

        class MockTierObjectStoreResponse implements TierObjectStoreResponse {
            private final InputStream is;

            MockTierObjectStoreResponse(InputStream is) {
                this.is = is;
            }

            @Override
            public InputStream getInputStream() {
                return is;
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
        public Backend getBackend() {
            return Backend.Mock;
        }

        @Override
        public TierObjectStoreResponse getObject(ObjectStoreMetadata objectMetadata,
                                                 FileType fileType,
                                                 Integer byteOffset,
                                                 Integer byteOffsetEnd) throws IOException {
            if (failNextRequest.compareAndSet(true, false)) {
                throw new IOException("Failed to retrieve object.");
            }
            ByteBuffer buffer;
            if (fileType == FileType.OFFSET_INDEX) {
                offsetIndexReads++;
                buffer = offsetByteBuffer;
            } else if (fileType == FileType.SEGMENT) {
                segmentReads++;
                buffer = segmentByteBuffer;
            } else if (fileType == FileType.TIMESTAMP_INDEX) {
                timestampIndexReads++;
                buffer = timestampByteBuffer;
            } else {
                throw new UnsupportedOperationException();
            }

            int start = byteOffset == null ? 0 : byteOffset;
            int end = byteOffsetEnd == null ? buffer.limit() : Math.min(byteOffsetEnd, buffer.limit());
            int byteBufferSize = Math.min(end - start, buffer.array().length);
            ByteBuffer buf = ByteBuffer.allocate(byteBufferSize);
            buf.put(buffer.array(), start, byteBufferSize);
            buf.flip();

            return new MockTierObjectStoreResponse(new ByteBufferInputStream(buf));
        }

        @Override
        public void putSegment(ObjectMetadata objectMetadata,
                               File segmentData,
                               File offsetIndexData,
                               File timestampIndexData,
                               Optional<File> producerStateSnapshotData,
                               Optional<ByteBuffer> transactionIndexData,
                               Optional<ByteBuffer> epochState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putObject(ObjectStoreMetadata objectMetadata,
                              File file,
                              FileType fileType) throws IOException {
            throw new IOException("");
        }

        @Override
        public void deleteSegment(ObjectMetadata objectMetadata) {
        }
    }
}
