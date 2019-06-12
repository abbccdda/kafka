/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.AbortedTxn;
import kafka.log.LogConfig;
import kafka.log.LogSegment;
import kafka.tier.TopicIdPartition;
import kafka.tier.archiver.ArchiveTask;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.store.MockInMemoryTierObjectStore;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.JavaConversions;
import scala.compat.java8.OptionConverters;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
            TierObjectStore.ObjectMetadata metadata =
                    new TierObjectStore.ObjectMetadata(segmentMetadata(topicIdPartition, segment,
                     false));
            putSegment(tierObjectStore, segment, metadata, Optional.empty());

            long targetOffset = 149L;
            PendingFetch pendingFetch = new PendingFetch(ctx, tierObjectStore,
                    Optional.empty(),
                    metadata, key -> { }, targetOffset, 1024, Long.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED, Collections.emptyList());
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
            TierObjectStore.ObjectMetadata metadata =
                    new TierObjectStore.ObjectMetadata(segmentMetadata(topicIdPartition, segment,
                     false));
            putSegment(tierObjectStore, segment, metadata, Optional.empty());

            Long targetOffset = 150L;
            PendingFetch pendingFetch = new PendingFetch(ctx, tierObjectStore, Optional.empty(),
                    metadata, key -> { }, targetOffset, 1024, Long.MAX_VALUE, IsolationLevel.READ_UNCOMMITTED, Collections.emptyList());
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
            TierObjectStore.ObjectMetadata metadata =
                    new TierObjectStore.ObjectMetadata(segmentMetadata(topicIdPartition, segment,
                            false));
            putSegment(tierObjectStore, segment, metadata, Optional.empty());
            long targetOffset = 51L;
            PendingFetch pendingFetch = new PendingFetch(ctx, tierObjectStore,
                    Optional.empty(), metadata, key -> { }, targetOffset, 1024,
                    100L, IsolationLevel.READ_UNCOMMITTED, Collections.emptyList());

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
                                               LogSegment logSegment,
                                               boolean hasAbortedTxns) {
        return new TierObjectMetadata(
                topicIdPartition,
                0,
                UUID.randomUUID(),
                logSegment.baseOffset(),
                logSegment.readNextOffset() - 1,
                logSegment.largestTimestamp(),
                logSegment.size(),
                TierObjectMetadata.State.SEGMENT_UPLOAD_INITIATE, false, hasAbortedTxns, true
        );
    }

    private MemoryRecords createRecords(long offset, int n) {
        ByteBuffer buffer = ByteBuffer.allocate(2048);
        MemoryRecordsBuilder builder = MemoryRecords
                .builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
                        TimestampType.CREATE_TIME, offset, 0L);
        IntStream.range(0, n).forEach(i -> builder.appendWithOffset(offset + i, baseTimestamp + offset, "a".getBytes(), "v".getBytes()));
        return builder.build();
    }

    @Test // ensure that the test helper is working correctly
    public void testSerializingAbortedTransactions() {
        ByteBuffer buf = serializeAbortedTxns(
                new AbortedTxn(0, 0, 5, 0),
                new AbortedTxn(0, 10, 20, 0),
                new AbortedTxn(0, 50, 100, 0)).get();
        List<AbortedTxn> read = TierAbortedTxnReader.readInto(CancellationContext.newContext(),
                new ByteBufferInputStream(buf), 0, 100);
        Assert.assertEquals(
                Arrays.asList(
                        new AbortedTxn(0, 0, 5, 0),
                        new AbortedTxn(0, 10, 20, 0),
                        new AbortedTxn(0, 50, 100, 0)
                ), read);
    }

    @Test
    public void testReadCommittedEmptyBatch() {
        CancellationContext ctx = CancellationContext.newContext();
        TierObjectStore tierObjectStore =
                new MockInMemoryTierObjectStore(new TierObjectStoreConfig());
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TopicPartition topicPartition = topicIdPartition.topicPartition();

        LogSegment segment = createSegment(0L, 0, 0);
        try {
            Optional<ByteBuffer> serializedAbortedTxns = serializeAbortedTxns(
                    new AbortedTxn(0, 0, 5, 0),
                    new AbortedTxn(0, 10, 20, 0),
                    new AbortedTxn(0, 50, 100, 0),
                    new AbortedTxn(0, 101, 150, 0));

            TierObjectStore.ObjectMetadata metadata =
                    new TierObjectStore.ObjectMetadata(segmentMetadata(topicIdPartition, segment,
                            serializedAbortedTxns.isPresent()));
            putSegment(tierObjectStore, segment, metadata, serializedAbortedTxns);
            long targetOffset = 0L;
            PendingFetch pendingFetch = new PendingFetch(ctx, tierObjectStore, Optional.empty(),
                    metadata, key -> { }, targetOffset, 1024, 100,
                    IsolationLevel.READ_COMMITTED, Collections.emptyList());
            currentThreadExecutor.execute(pendingFetch);
            TierFetchResult result = pendingFetch.finish().get(topicPartition);

            Assert.assertFalse("expected to find 0 records",
                    result.records.records().iterator().hasNext());
            Assert.assertEquals("expected to find 0 aborted transactions overlapping the "
                            + "fetched range",
                    Collections.emptyList(),
                    result.abortedTxns);


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
    public void testFetchingReadCommitted() {
        CancellationContext ctx = CancellationContext.newContext();
        TierObjectStore tierObjectStore =
                new MockInMemoryTierObjectStore(new TierObjectStoreConfig());
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TopicPartition topicPartition = topicIdPartition.topicPartition();

        LogSegment segment = createSegment(0L, 3, 50);
        try {
            Optional<ByteBuffer> serializedAbortedTxns = serializeAbortedTxns(
                    new AbortedTxn(0, 0, 5, 0),
                    new AbortedTxn(0, 10, 20, 0),
                    new AbortedTxn(0, 50, 100, 0),
                    new AbortedTxn(0, 101, 150, 0));

            TierObjectStore.ObjectMetadata metadata =
                    new TierObjectStore.ObjectMetadata(segmentMetadata(topicIdPartition, segment,
                            serializedAbortedTxns.isPresent()));
            putSegment(tierObjectStore, segment, metadata, serializedAbortedTxns);
            long targetOffset = 0L;
            PendingFetch pendingFetch = new PendingFetch(ctx, tierObjectStore, Optional.empty(),
                    metadata, key -> { }, targetOffset, 1024, 100,
                    IsolationLevel.READ_COMMITTED, Collections.emptyList());
            currentThreadExecutor.execute(pendingFetch);
            TierFetchResult result = pendingFetch.finish().get(topicPartition);

            int recordCount = 0;
            for (Record record : result.records.records()) {
                Assert.assertTrue("Expected all records to be valid", record.isValid());
                recordCount += 1;
            }
            Assert.assertEquals("expected to find 100 records", 100, recordCount);
            Assert.assertEquals("expected to find the 3 aborted transactions overlapping the "
                            + "fetch range",
                    Arrays.asList(new AbortedTxn(0, 0, 5, 0),
                            new AbortedTxn(0, 10, 20, 0),
                            new AbortedTxn(0, 50, 100, 0)),
                    result.abortedTxns);

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception");
        } finally {
            ctx.close();
            segment.close();
            tierObjectStore.close();
        }
    }

    @Test // Tests when an exception is thrown fetching either the segment or aborted transactions,
          // no data is returned except the exception.
    public void testFetchingReadCommittedException() {
        CancellationContext ctx = CancellationContext.newContext();
        MockInMemoryTierObjectStore tierObjectStore =
                new MockInMemoryTierObjectStore(new TierObjectStoreConfig());
        TopicIdPartition topicIdPartition = new TopicIdPartition("foo", UUID.randomUUID(), 0);
        TopicPartition topicPartition = topicIdPartition.topicPartition();

        LogSegment segment = createSegment(0L, 3, 50);
        try {
            Optional<ByteBuffer> serializedAbortedTxns = serializeAbortedTxns(
                    new AbortedTxn(0, 0, 5, 0),
                    new AbortedTxn(0, 10, 20, 0),
                    new AbortedTxn(0, 50, 100, 0),
                    new AbortedTxn(0, 101, 150, 0));

            TierObjectStore.ObjectMetadata metadata =
                    new TierObjectStore.ObjectMetadata(segmentMetadata(topicIdPartition, segment,
                            serializedAbortedTxns.isPresent()));
            putSegment(tierObjectStore, segment, metadata, serializedAbortedTxns);

            // set the tier object store to throw when fetching the transaction index
            tierObjectStore.throwExceptionOnTransactionFetch = true;

            PendingFetch pendingFetch = new PendingFetch(ctx, tierObjectStore, Optional.empty(),
                    metadata, key -> { }, 0L, 1024, 100,
                    IsolationLevel.READ_COMMITTED, Collections.emptyList());
            currentThreadExecutor.execute(pendingFetch);
            TierFetchResult result = pendingFetch.finish().get(topicPartition);

            Assert.assertFalse("Expected to find 0 records, because an exception was thrown",
                    result.records.records().iterator().hasNext());
            Assert.assertTrue(result.exception instanceof IOException);
            Assert.assertEquals("Expected to find 0 aborted transaction because an exception was "
                    + "thrown", Collections.emptyList(), result.abortedTxns);

            // Test that if an exception is thrown during segment fetch, no transactions are
            // returned
            tierObjectStore.throwExceptionOnTransactionFetch = false;
            tierObjectStore.throwExceptionOnSegmentFetch = true;

            pendingFetch = new PendingFetch(ctx, tierObjectStore, Optional.empty(),
                    metadata, key -> { }, 0L, 1024, 100,
                    IsolationLevel.READ_COMMITTED, Collections.emptyList());
            currentThreadExecutor.execute(pendingFetch);
            result = pendingFetch.finish().get(topicPartition);

            Assert.assertFalse("Expected to find 0 records, because an exception was thrown",
                    result.records.records().iterator().hasNext());
            Assert.assertTrue(result.exception instanceof IOException);
            Assert.assertEquals("Expected to find 0 aborted transaction because an exception was "
                    + "thrown", Collections.emptyList(), result.abortedTxns);



        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail("Unexpected exception");
        } finally {
            ctx.close();
            segment.close();
            tierObjectStore.close();
        }
    }

    Optional<ByteBuffer> serializeAbortedTxns(AbortedTxn... abortedTxns) {
        return OptionConverters.toJava(ArchiveTask.serializeAbortedTransactions(
                JavaConversions.asScalaBuffer(Arrays.asList(abortedTxns))));
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

    private void putSegment(TierObjectStore tierObjectStore, LogSegment segment,
                            TierObjectStore.ObjectMetadata metadata,
                            Optional<ByteBuffer> abortedTxns)
            throws IOException {
        tierObjectStore.putSegment(
                metadata, segment.log().file(), segment.offsetIndex().file(),
                segment.timeIndex().file(), Optional.empty(),
                abortedTxns, Optional.of(segment.timeIndex().file()));
    }
}
