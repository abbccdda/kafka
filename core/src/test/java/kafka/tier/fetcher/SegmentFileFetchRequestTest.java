/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.LogConfig;
import kafka.log.LogSegment;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.serdes.State;
import kafka.tier.store.MockInMemoryTierObjectStore;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class SegmentFileFetchRequestTest {
    private MockTime mockTime = new MockTime();
    private Executor currentThreadExecutor = Runnable::run;
    private long baseTimestamp = 1500000000000L;

    @Test
    public void targetOffsetTest() {
        CancellationContext ctx = CancellationContext.newContext();
        TierObjectStore tierObjectStore =
                new MockInMemoryTierObjectStore(new TierObjectStoreConfig());
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo",
                UUID.randomUUID(), 0);
        TopicPartition topicPartition = topicIdPartition.topicPartition();

        LogSegment segment = createSegment(0L, 3, 50);

        try {
            TierObjectMetadata metadata = segmentMetadata(topicIdPartition, segment);
            Metrics metrics = new Metrics();
            putSegment(tierObjectStore, segment, metadata);
            Sensor recordBytesFetched = metrics.sensor("recordBytesFetched");

            long targetOffset = 149L;
            PendingFetch pendingFetch = new PendingFetch(topicPartition, ctx, tierObjectStore,
                    recordBytesFetched,
                    metadata, key -> { }, targetOffset, 1024, Long.MAX_VALUE, Collections.emptyList());
            currentThreadExecutor.execute(pendingFetch);
            TierFetchResult result = pendingFetch.finish().get(topicPartition);

            Assert.assertTrue("Records should be complete",
                    result.records.batches().iterator().hasNext());

            Assert.assertNotEquals("Should return records", result.records, MemoryRecords.EMPTY);

            RecordBatch firstRecordBatch = result.records.batches().iterator().next();
            Assert.assertTrue("Results should include target offset in the first record batch",
                    firstRecordBatch.baseOffset() <= targetOffset && firstRecordBatch.lastOffset() >= targetOffset);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception");
        } finally {
            ctx.close();
            segment.close();
            tierObjectStore.close();
        }
    }

    @Test
    public void targetOffsetOutOfRangeTest() {
        CancellationContext ctx = CancellationContext.newContext();
        TierObjectStore tierObjectStore =
                new MockInMemoryTierObjectStore(new TierObjectStoreConfig());
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo",
                UUID.randomUUID(), 0);
        TopicPartition topicPartition = topicIdPartition.topicPartition();

        LogSegment segment = createSegment(0L, 3, 50);

        try {
            TierObjectMetadata metadata = segmentMetadata(topicIdPartition, segment);

            putSegment(tierObjectStore, segment, metadata);

            Metrics metrics = new Metrics();
            Long targetOffset = 150L;
            Sensor recordBytesFetched = metrics.sensor("recordBytesFetched");
            PendingFetch pendingFetch = new PendingFetch(topicPartition,
                    ctx, tierObjectStore, recordBytesFetched,
                    metadata, key -> { }, targetOffset, 1024, Long.MAX_VALUE, Collections.emptyList());
            currentThreadExecutor.execute(pendingFetch);
            TierFetchResult result = pendingFetch.finish().get(topicPartition);

            Assert.assertFalse("Records should be incomplete",
                    result.records.batches().iterator().hasNext());

            Assert.assertEquals("Should return empty records", result.records, MemoryRecords.EMPTY);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception");
        } finally {
            ctx.close();
            segment.close();
            tierObjectStore.close();
        }
    }

    @Test
    public void targetOffsetAndMaxOffsetTest() {
        CancellationContext ctx = CancellationContext.newContext();
        TierObjectStore tierObjectStore =
                new MockInMemoryTierObjectStore(new TierObjectStoreConfig());
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo",
                UUID.randomUUID(), 0);
        TopicPartition topicPartition = topicIdPartition.topicPartition();

        LogSegment segment = createSegment(0L, 3, 50);

        try {
            TierObjectMetadata metadata = segmentMetadata(topicIdPartition, segment);

            putSegment(tierObjectStore, segment, metadata);
            Metrics metrics = new Metrics();
            Long targetOffset = 51L;
            Sensor recordBytesFetched = metrics.sensor("recordBytesFetched");
            PendingFetch pendingFetch = new PendingFetch(topicPartition, ctx, tierObjectStore,
                    recordBytesFetched, metadata, key -> { }, targetOffset, 1024,
                    100L, Collections.emptyList());

            currentThreadExecutor.execute(pendingFetch);
            TierFetchResult result = pendingFetch.finish().get(topicPartition);

            Assert.assertTrue("Records should be complete",
                    result.records.batches().iterator().hasNext());

            Assert.assertNotEquals("Should return records", result.records, MemoryRecords.EMPTY);

            Assert.assertFalse("Results should not include records at or beyond max offset",
                    StreamSupport.stream(result.records.records().spliterator(), false)
                            .anyMatch(r -> r.offset() >= 100L));

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception");
        } finally {
            ctx.close();
            segment.close();
            tierObjectStore.close();
        }
    }

    private TierObjectMetadata segmentMetadata(TopicIdPartition topicIdPartition,
                                               LogSegment logSegment) {
        return new TierObjectMetadata(
                topicIdPartition,
                0,
                logSegment.baseOffset(),
                (int) (logSegment.readNextOffset() - 1 - logSegment.baseOffset()),
                1L,
                logSegment.largestTimestamp(),
                logSegment.size(),
                true,
                false, false,
                State.AVAILABLE);
    }

    private MemoryRecords createRecords(long offset, int n) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords
                .builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME, offset);
        IntStream.range(0, n).forEach(i -> builder.appendWithOffset(offset + i, baseTimestamp + offset, "a".getBytes(), "v".getBytes()));
        return builder.build();
    }

    private LogSegment createSegment(long baseOffset, int batches, int recsPerBatch) {
        File logSegmentDir = TestUtils.tempDirectory();
        logSegmentDir.deleteOnExit();

        Properties logProps = new Properties();
        logProps.put(LogConfig.IndexIntervalBytesProp(), 1);

        LogConfig logConfig = LogConfig.apply(logProps, new scala.collection.immutable.HashSet<>());
        LogSegment segment = LogSegment
                .open(logSegmentDir, baseOffset, logConfig, mockTime, false, 4096, false, "");

        IntStream.range(0, batches).forEach(i -> {
            long nextOffset = segment.readNextOffset();
            MemoryRecords recs = createRecords(nextOffset, recsPerBatch);
            long largestOffset = nextOffset + recsPerBatch - 1;
            segment.append(largestOffset, baseTimestamp + largestOffset, recsPerBatch - 1, recs);
            segment.flush();
        });

        segment.offsetIndex().flush();
        segment.offsetIndex().trimToValidSize();

        return segment;
    }

    private void putSegment(TierObjectStore tierObjectStore, LogSegment segment, TierObjectMetadata metadata)
            throws IOException {
        tierObjectStore.putSegment(
                metadata, segment.log().file(), segment.offsetIndex().file(),
                segment.timeIndex().file(), segment.timeIndex().file(),
                segment.timeIndex().file(), Optional.of(segment.timeIndex().file()));
    }
}
