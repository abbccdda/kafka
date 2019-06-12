/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Iterator over InputStreams containing fixed-size serialized objects of type T
 */
abstract class AbstractInputStreamFixedSizeIterator<T> extends AbstractIterator<T> {
    private final InputStream inputStream;
    final byte[] indexEntryBytes;

    /**
     * Create an Iterator<T> over an InputStream. Override toIndexEntry() to parse byte[entrySize]
     * to T.
     *
     * @param inputStream InputStream to iterator over
     * @param entrySize   size of <T> in bytes
     */
    AbstractInputStreamFixedSizeIterator(InputStream inputStream, int entrySize) {
        this.inputStream = inputStream;
        this.indexEntryBytes = new byte[entrySize];
    }


    @Override
    protected T makeNext() {
        try {
            if (Utils.readFully(inputStream, indexEntryBytes) == indexEntryBytes.length)
                return toIndexEntry();
            else
                return allDone();
        } catch (IOException e) {
            allDone();
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Create a IndexEntry <T> from the current iterator state in indexEntryBytes.
     */
    abstract T toIndexEntry();

}