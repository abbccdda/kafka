/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.OffsetPosition;

import java.io.InputStream;
import java.nio.ByteBuffer;

class TierOffsetIndexIterator extends AbstractInputStreamFixedSizeIterator<OffsetPosition> {
    private final long base;
    TierOffsetIndexIterator(InputStream inputStream, long base) {
        super(inputStream, 8);
        this.base = base;
    }

    @Override
    OffsetPosition toIndexEntry() {
        if (this.indexEntryBytes.length != 8) {
            throw new IllegalArgumentException("OffsetIndex entries must be 8 bytes");
        } else {
            final ByteBuffer buf = ByteBuffer.wrap(indexEntryBytes);
            final long offset = base + buf.getInt();
            final int position = buf.getInt(4);
            return OffsetPosition.apply(offset, position);
        }
    }
}
