/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.TimestampOffset;

import java.io.InputStream;
import java.nio.ByteBuffer;

class TierTimestampIndexIterator extends AbstractInputStreamFixedSizeIterator<TimestampOffset> {
    private final static int TIMESTAMP_ENTRY_SIZE = 8;
    private final static int RELATIVE_OFFSET_ENTRY_SIZE = 4;
    private final static int TIMESTAMP_OFFSET_ENTRY_SIZE =
            TIMESTAMP_ENTRY_SIZE + RELATIVE_OFFSET_ENTRY_SIZE;
    private final long base;
    TierTimestampIndexIterator(InputStream inputStream, long base) {
        super(inputStream, TIMESTAMP_OFFSET_ENTRY_SIZE);
        this.base = base;
    }

    @Override
    TimestampOffset toIndexEntry() {
        if (this.indexEntryBytes.length != TIMESTAMP_OFFSET_ENTRY_SIZE) {
            throw new IllegalArgumentException("TimestampIndex entries must be 12 bytes");
        } else {
            final ByteBuffer buf = ByteBuffer.wrap(indexEntryBytes);
            final long timestamp = buf.getLong();
            final int relativeOffset = buf.getInt();
            return TimestampOffset.apply(timestamp, relativeOffset + base);
        }
    }
}
