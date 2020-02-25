/*
 Copyright 2018 Confluent Inc.
 */

package org.apache.kafka.common.record;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.Collections;

import org.junit.Assert;

public class LazyDownConversionRecordsSendTest {
    class MockReleasableRecords implements Records {
        boolean released = false;

        @Override
        public long writeTo(GatheringByteChannel channel, long position, int length) {
            return 0;
        }

        @Override
        public Iterable<? extends RecordBatch> batches() {
            return Collections.emptyList();
        }

        @Override
        public AbstractIterator<? extends RecordBatch> batchIterator() {
            return new AbstractIterator<RecordBatch>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                protected RecordBatch makeNext() {
                    return null;
                }
            };
        }

        @Override
        public boolean hasMatchingMagic(byte magic) {
            return false;
        }

        @Override
        public boolean hasCompatibleMagic(byte magic) {
            return false;
        }

        @Override
        public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
            return new ConvertedRecords<>(MemoryRecords.EMPTY, new RecordConversionStats());
        }

        @Override
        public Iterable<Record> records() {
            return Collections.emptyList();
        }

        @Override
        public void release() {
            released = true;
        }

        @Override
        public int sizeInBytes() {
            return 0;
        }

        @Override
        public RecordsSend toSend(String destination) {
            return null;
        }
    }

    @Test
    public void testRecordsReleasedSend() throws IOException {
        MockReleasableRecords records = new MockReleasableRecords();
        TopicPartition topicPartition = new TopicPartition("foo", 1);
        LazyDownConversionRecords lazyDownConversionRecords = new LazyDownConversionRecords(topicPartition, records, (byte) 0, 0, new MockTime());
        LazyDownConversionRecordsSend send = new LazyDownConversionRecordsSend("", lazyDownConversionRecords);
        GatheringByteChannel channel = new ByteBufferChannel(1024);
        send.writeTo(channel);
        send.release();
        Assert.assertTrue("expected a released send to also release the backing records", records.released);
    }
}