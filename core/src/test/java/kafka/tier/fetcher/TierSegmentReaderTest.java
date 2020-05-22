/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.TimeIndex;
import kafka.log.TimestampOffset;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class TierSegmentReaderTest {
    private final TierSegmentReader reader = new TierSegmentReader("");

    @Test
    public void homogenousRecordBatchTest() throws IOException {
        SimpleRecord[] simpleRecords = new SimpleRecord[] {
                new SimpleRecord(1L, "foo".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
        };
        ByteBuffer records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE,
                TimestampType.CREATE_TIME, simpleRecords).buffer();
        ByteBuffer records2 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 3L,
                CompressionType.NONE, TimestampType.CREATE_TIME, simpleRecords).buffer();
        ByteBuffer combinedBuffer = ByteBuffer.allocate(records.limit() + records2.limit());
        combinedBuffer.put(records);
        combinedBuffer.put(records2);
        combinedBuffer.flip();

        testExpected(combinedBuffer, 0L, 0L, 5L);
        testExpected(combinedBuffer, 1L, 0L, 5L);
        testExpected(combinedBuffer, 2L, 0L, 5L);
        testExpected(combinedBuffer, 3L, 3L, 5L);
        testExpected(combinedBuffer, 4L, 3L, 5L);
        testExpected(combinedBuffer, 5L, 3L, 5L);
        testThrows(reader, combinedBuffer, 6L, EOFException.class);
        testThrows(reader, combinedBuffer, 7L, EOFException.class);
    }

    @Test // Test that on EOF, the stream is no longer read
    public void testAbortOnEOF() {
        List<MemoryRecords> batches = createBatches();
        int size = batches.stream().mapToInt(MemoryRecords::sizeInBytes).sum();
        try (InputStream stream = toStream(batches)) {
            InputStream faulty = new InputStream() {
                private final InputStream inner = stream;
                private int bytesRead = -1;
                @Override
                public int read() throws IOException {
                    // Keep track of how much we've read from the input stream,
                    // if we try to read more than the size of the object, throw.
                    if (bytesRead >= (size - 1))
                        throw new IOException("hit eof!");
                    bytesRead += 1;
                    return inner.read();
                }
            };
            CancellationContext ctx = CancellationContext.newContext();
            TierSegmentReader reader = new TierSegmentReader("");
            // Perform a fetch for the first offset in the segment, but incorrectly state
            // the size of the segment as 2x the known size.
            // This will cause the fetch to attempt to continue reading from the input stream
            // past what the input stream contains.
            final long targetOffset = 100;
            TierSegmentReader.RecordsAndNextBatchMetadata recordsAndMetadata =
                    reader.readRecords(ctx, Optional.empty(), faulty, 1024 * 1024, targetOffset, 0,
                            size * 2);


            long expectedOffset = 100;
            for (RecordBatch batch : recordsAndMetadata.records.batches()) {
                for (Record record : batch) {
                    Assert.assertEquals(
                            "expected to find target offset 100 and all offsets after to be "
                                    + "linearly increasing.", expectedOffset, record.offset());
                    expectedOffset += 1;
                }
            }
        } catch (IOException e) {
            Assert.fail("expected no exception to be thrown");
        }
    }

    @Test
    public void testReadRecordsMissingRecordsBetweenBatches() throws IOException {
        SimpleRecord[] simpleRecords = new SimpleRecord[] {
                new SimpleRecord(1L, "foo".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
        };
        ByteBuffer records1 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 3L,
                CompressionType.NONE,
                TimestampType.CREATE_TIME, simpleRecords).buffer();
        ByteBuffer records2 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 6L,
                CompressionType.NONE, TimestampType.CREATE_TIME, simpleRecords).buffer();
        ByteBuffer records3 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 12L,
                CompressionType.NONE, TimestampType.CREATE_TIME, simpleRecords).buffer();
        ByteBuffer combinedBuffer = ByteBuffer.allocate(records1.limit() + records2.limit() + records3.limit());
        combinedBuffer.put(records1);
        combinedBuffer.put(records2);
        combinedBuffer.put(records3);
        combinedBuffer.flip();

        // first batch missing records for target offset
        testExpected(combinedBuffer, 0L, 3L, 14L);
        testExpected(combinedBuffer, 1L, 3L, 14L);
        testExpected(combinedBuffer, 2L, 3L, 14L);
        // target offset records exist
        testExpected(combinedBuffer, 3L, 3L, 14L);
        testExpected(combinedBuffer, 4L, 3L, 14L);
        testExpected(combinedBuffer, 5L, 3L, 14L);
        testExpected(combinedBuffer, 6L, 6L, 14L);
        testExpected(combinedBuffer, 7L, 6L, 14L);
        testExpected(combinedBuffer, 8L, 6L, 14L);
        // offset holes between batches
        testExpected(combinedBuffer, 9L, 12L, 14L);
        testExpected(combinedBuffer, 10L, 12L, 14L);
        testExpected(combinedBuffer, 11L, 12L, 14L);
        testExpected(combinedBuffer, 12L, 12L, 14L);
        testExpected(combinedBuffer, 13L, 12L, 14L);
        testExpected(combinedBuffer, 14L, 12L, 14L);
        // offset not present
        testThrows(reader, combinedBuffer, 15L, EOFException.class);
    }

    @Test
    public void testReadRecordsMissingRecordsWithinBatch() throws IOException {
        SimpleRecord[] simpleRecords = new SimpleRecord[] {
                new SimpleRecord(1L, "foo".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
        };
        ByteBuffer buffer = ByteBuffer.allocate(simpleRecords.length);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
                TimestampType.LOG_APPEND_TIME, 1L, System.currentTimeMillis(), 0);
        builder.appendWithOffset(3L, simpleRecords[0]);
        builder.appendWithOffset(5L, simpleRecords[1]);
        ByteBuffer records = builder.build().buffer();

        ByteBuffer combinedBuffer = ByteBuffer.allocate(records.limit());
        combinedBuffer.put(records);
        combinedBuffer.flip();

        // first batch missing records for target offset
        testExpected(combinedBuffer, 0L, 1L, 5L);
        testExpected(combinedBuffer, 1L, 1L, 5L);
        testExpected(combinedBuffer, 2L, 1L, 5L);
        // target offset records exist
        testExpected(combinedBuffer, 3L, 1L, 5L);
        testExpected(combinedBuffer, 5L, 1L, 5L);
        // offset holes between records
        testExpected(combinedBuffer, 4L, 1L, 5L);
        // offset not present
        testThrows(reader, combinedBuffer, 6L, EOFException.class);
    }


    @Test
    public void testReadRecordsOneBatchAlignedBoundaries() throws IOException {
        List<MemoryRecords> batches = createBatches();
        int batchSize = batches.get(0).sizeInBytes();
        testReadSegment(batches, batchSize);

        // test maxBytes < batchSize
        testReadSegment(batches, batchSize - 1);

        // test read multiple batches
        testReadSegment(batches, batchSize * 2);
    }

    @Test
    public void testReadRecordsOneBatchUnalignedBoundaries() throws IOException {
        List<MemoryRecords> batches = createBatches();
        int batchSize = batches.get(0).sizeInBytes();
        testReadSegment(batches, batchSize + 1);
    }

    @Test
    public void testReadRecordsOneBatchMaxLessThanBatchSize() throws IOException {
        List<MemoryRecords> batches = createBatches();
        int batchSize = batches.get(0).sizeInBytes();
        testReadSegment(batches, batchSize - 2);
    }

    @Test
    public void testReadRecordsMultipleBatchesAligned() throws IOException {
        List<MemoryRecords> batches = createBatches();
        int batchSize = batches.get(0).sizeInBytes();
        testReadSegment(batches, batchSize * 3);
    }

    @Test
    public void testReadRecordsMultipleBatchesUnaligned1() throws IOException {
        List<MemoryRecords> batches = createBatches();
        int batchSize = batches.get(0).sizeInBytes();
        testReadSegment(batches, batchSize * 3 + 2);
    }

    @Test
    public void testReadRecordsMultipleBatchesUnaligned2() throws IOException {
        List<MemoryRecords> batches = createBatches();
        int batchSize = batches.get(0).sizeInBytes();
        testReadSegment(batches, batchSize * 3 - 2);
    }

    @Test
    public void offsetForTimestampTest() {
        ByteBuffer records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2,
                0L,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "foo".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()))
                .buffer();

        ByteBuffer records2 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2,
                3L,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                new SimpleRecord(2L, "foo".getBytes(), "1".getBytes()),
                new SimpleRecord(5L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(6L, "c".getBytes(), "3".getBytes())
        ).buffer();
        ByteBuffer combinedBuffer = ByteBuffer.allocate(records.limit() + records2.limit());
        combinedBuffer.put(records);
        combinedBuffer.put(records2);

        assertCorrectOffsetForTimestamp(combinedBuffer, 1L, Optional.of(0L));
        assertCorrectOffsetForTimestamp(combinedBuffer, 2L, Optional.of(1L));
        assertCorrectOffsetForTimestamp(combinedBuffer, 3L, Optional.of(2L));
        assertCorrectOffsetForTimestamp(combinedBuffer, 5L, Optional.of(4L));
        assertCorrectOffsetForTimestamp(combinedBuffer, 6L, Optional.of(5L));
    }

    @Test
    public void timestampIndexIteratorTest() {
        try {
            File file = File.createTempFile("kafka", ".tmp");
            try {
                try {
                    long baseOffset = 1000;
                    List<TimestampOffset> expected = new ArrayList<>();
                    TimeIndex timeIndex = new TimeIndex(file, baseOffset, 800, true);
                    timeIndex.resize(800);
                    timeIndex.flush();
                    expected.add(new TimestampOffset(20000, 14000));
                    expected.add(new TimestampOffset(30000, 18000));
                    expected.add(new TimestampOffset(40000, 20000));
                    expected.add(new TimestampOffset(40001, 20001));

                    for (TimestampOffset tso : expected) {
                        timeIndex.maybeAppend(tso.timestamp(), tso.offset(), false);
                    }
                    timeIndex.flush();
                    timeIndex.close();

                    List<TimestampOffset> actual = new ArrayList<>();
                    TierTimestampIndexIterator iterator =
                            new TierTimestampIndexIterator(new FileInputStream(file), baseOffset);
                    while (iterator.hasNext()) {
                        actual.add(iterator.next());
                    }
                    assertEquals(expected, actual);
                } catch (IOException ioe) {
                    fail(ioe.getMessage());
                }
            } finally {
                file.delete();
            }
        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }

    private MemoryRecords createRecords(List<SimpleRecord> records, long baseOffset, int leaderEpoch) {
        ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records));
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE,
                TimestampType.LOG_APPEND_TIME, baseOffset, System.currentTimeMillis(), leaderEpoch);
        for (SimpleRecord record : records)
            builder.append(record);
        return builder.build();
    }

    private InputStream toStream(List<MemoryRecords> records) {
        int totalSize = records.stream().mapToInt(MemoryRecords::sizeInBytes).sum();
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (MemoryRecords record : records)
            buffer.put(record.buffer());
        buffer.flip();
        return new ByteBufferInputStream(buffer);
    }

    private List<MemoryRecords> createBatches() {
        List<SimpleRecord> records = new ArrayList<>();
        records.add(new SimpleRecord(1L, "k1".getBytes(), "v1".getBytes()));
        records.add(new SimpleRecord(2L, "k2".getBytes(), "v2".getBytes()));
        records.add(new SimpleRecord(3L, "k3".getBytes(), "v3".getBytes()));

        List<MemoryRecords> batches = new ArrayList<>();
        batches.add(createRecords(records, 100, 0));
        batches.add(createRecords(records, 103, 0));
        batches.add(createRecords(records, 106, 0));
        batches.add(createRecords(records, 109, 0));

        return batches;
    }

    private void testExpected(ByteBuffer combinedBuffer, Long target,
                              Long expectedStart, Long expectedEnd) throws IOException {
        combinedBuffer.position(0);
        ByteBufferInputStream is = new ByteBufferInputStream(combinedBuffer);
        CancellationContext cancellationContext = CancellationContext.newContext();
        MemoryRecords records = reader.readRecords(cancellationContext.subContext(), Optional.empty(),
                is, 1000, target, 0, combinedBuffer.limit()).records;

        Long firstOffset = null;
        Long lastOffset = null;
        if (records.sizeInBytes() != 0) {
            for (MutableRecordBatch batch : records.batches()) {
                if (firstOffset == null) {
                    firstOffset = batch.baseOffset();
                }
                lastOffset = batch.lastOffset();
            }
        }
        assertEquals(expectedStart, firstOffset);
        assertEquals(expectedEnd, lastOffset);
    }

    private static <T extends Throwable> void testThrows(TierSegmentReader reader,
                                                         ByteBuffer combinedBuffer,
                                                         long target,
                                                         Class<T> expectedThrowable) {
        combinedBuffer.position(0);
        ByteBufferInputStream is = new ByteBufferInputStream(combinedBuffer);
        CancellationContext cancellationContext = CancellationContext.newContext();

        assertThrows(expectedThrowable,
                () -> reader.readRecords(cancellationContext.subContext(), Optional.empty(), is, 1000,
                        target, 0, combinedBuffer.limit()));
    }

    private void testReadSegment(List<MemoryRecords> batches, int maxBytes) throws IOException {
        TreeMap<Long, BatchAndPosition> offsetToPosition = new TreeMap<>();
        List<Long> targetOffsets = new ArrayList<>();
        CancellationContext ctx = CancellationContext.newContext();
        int batchSize = batches.get(0).firstBatchSize();
        int segmentSize = batchSize * batches.size();

        int position = 0;
        for (MemoryRecords batch : batches) {
            RecordBatch firstBatch = batch.firstBatch();
            // insert into offset map
            offsetToPosition.put(firstBatch.baseOffset(), new BatchAndPosition(batch, position));

            // insert into targetOffsets
            targetOffsets.add(firstBatch.baseOffset());
            targetOffsets.add(firstBatch.baseOffset() + 1);

            // update position
            position += batch.sizeInBytes();
        }

        for (long targetOffset : targetOffsets) {
            long expectedFirstOffset = offsetToPosition.floorKey(targetOffset);
            BatchAndPosition expectedFirstBatchAndPosition = offsetToPosition.get(expectedFirstOffset);
            int expectedNumBatches = Math.max(1, maxBytes / expectedFirstBatchAndPosition.records.sizeInBytes());

            TreeSet<Long> expectedOffsets = offsetToPosition.navigableKeySet()
                    .tailSet(expectedFirstOffset)
                    .stream()
                    .limit(expectedNumBatches)
                    .collect(Collectors.toCollection(TreeSet::new));
            Long nextOffset = offsetToPosition.higherKey(expectedOffsets.last().longValue());

            InputStream stream = toStream(batches);

            TierSegmentReader.RecordsAndNextBatchMetadata result = reader.readRecords(ctx,
                    Optional.empty(), stream, maxBytes,
                    targetOffset, 0, segmentSize);

            if (expectedOffsets.isEmpty()) {
                assertNull(result);
            } else {
                Iterator<Long> expectedOffsetsIt = expectedOffsets.iterator();
                RecordBatch lastBatch = null;
                for (RecordBatch batch : result.records.batches()) {
                    assertEquals(expectedOffsetsIt.next().longValue(), batch.baseOffset());
                    lastBatch = batch;
                }

                if (nextOffset != null) {
                    BatchAndPosition nextBatch = offsetToPosition.get(nextOffset);

                    assertEquals(lastBatch.nextOffset(), result.nextOffsetAndBatchMetadata.nextOffset);
                    assertEquals(nextOffset.longValue(), result.nextOffsetAndBatchMetadata.nextOffset);
                    assertEquals(nextBatch.bytePosition, result.nextOffsetAndBatchMetadata.nextBatchMetadata.bytePosition);

                    // size of next batch is returned opportunistically
                    if (result.nextOffsetAndBatchMetadata.nextBatchMetadata.recordBatchSize.isPresent())
                        assertEquals(nextBatch.records.sizeInBytes(), result.nextOffsetAndBatchMetadata.nextBatchMetadata.recordBatchSize.getAsInt());
                } else {
                    assertNull(result.nextOffsetAndBatchMetadata);
                }
            }
        }
    }

    private void assertCorrectOffsetForTimestamp(ByteBuffer combinedBuffer, long targetTimestamp, Optional<Long> expectedOffset) {
        combinedBuffer.position(0);
        ByteBufferInputStream is = new ByteBufferInputStream(combinedBuffer);
        CancellationContext cancellationContext = CancellationContext.newContext();
        try {
            Optional<Long> timestamp = reader.offsetForTimestamp(cancellationContext.subContext(), is, targetTimestamp, combinedBuffer.limit());
            assertEquals(expectedOffset, timestamp);
        } catch (IOException ioe) {
            fail("IOexception encountered");
        }
    }

    private static class BatchAndPosition {
        MemoryRecords records;
        int bytePosition;

        BatchAndPosition(MemoryRecords records, int bytePosition) {
            this.records = records;
            this.bytePosition = bytePosition;
        }
    }
}
