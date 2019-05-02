/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.TimeIndex;
import kafka.log.TimestampOffset;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TierSegmentReaderTest {
    @Test
    public void homogenousRecordBatchTest() {
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

        testExpected(combinedBuffer, 0L, Long.MAX_VALUE, 0L, 5L);
        testExpected(combinedBuffer, 1L, Long.MAX_VALUE, 0L, 5L);
        testExpected(combinedBuffer, 2L, Long.MAX_VALUE, 0L, 5L);
        testExpected(combinedBuffer, 3L, Long.MAX_VALUE, 3L, 5L);
        testExpected(combinedBuffer, 4L, Long.MAX_VALUE, 3L, 5L);
        testExpected(combinedBuffer, 5L, Long.MAX_VALUE, 3L, 5L);
        testExpected(combinedBuffer, 6L, Long.MAX_VALUE, null, null);
        testExpected(combinedBuffer, 7L, Long.MAX_VALUE, null, null);
        testExpected(combinedBuffer, 0L, 3L, 0L, 2L);
        testExpected(combinedBuffer, 3L, 4L, null, null);
        testExpected(combinedBuffer, 4L, 5L, null, null);
    }

    private void testExpected(ByteBuffer combinedBuffer, Long target, Long maxOffset,
                              Long expectedStart, Long expectedEnd) {
        combinedBuffer.position(0);
        try {
            ByteBufferInputStream is = new ByteBufferInputStream(combinedBuffer);
            CancellationContext cancellationContext = CancellationContext.newContext();
            MemoryRecords records =
                    TierSegmentReader.loadRecords(cancellationContext.subContext(), is, 1000, maxOffset, target);

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
        } catch (IOException ioe) {
            fail("Exception should not be thrown: " + ioe);
        }
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

    private void assertCorrectOffsetForTimestamp(ByteBuffer combinedBuffer, long targetTimestamp, Optional<Long> expectedOffset) {
        combinedBuffer.position(0);
        ByteBufferInputStream is = new ByteBufferInputStream(combinedBuffer);
        CancellationContext cancellationContext = CancellationContext.newContext();
        try {
            Optional<Long> timestamp =
                    TierSegmentReader.offsetForTimestamp(cancellationContext.subContext(), is, targetTimestamp);
            assertEquals(expectedOffset, timestamp);
        } catch (IOException ioe) {
            fail("IOexception encountered");
        }
    }
}
