/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.AbortedTxn;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TierAbortedTxnReader {

    private static class AbortedTxnsIterator extends AbstractInputStreamFixedSizeIterator<AbortedTxn> {
        AbortedTxnsIterator(InputStream inputStream) {
            super(inputStream, AbortedTxn.TotalSize());
        }

        @Override
        AbortedTxn toIndexEntry() {
            final ByteBuffer buf = ByteBuffer.wrap(this.indexEntryBytes.clone());
            return new AbortedTxn(buf);
        }
    }

    /**
     * Read aborted transaction markers from the supplied inputstream
     */
    public static List<AbortedTxn> readInto(CancellationContext ctx,
                                            InputStream inputStream,
                                            long startOffset,
                                            long lastOffset) {
        final List<AbortedTxn> abortedTxns = new ArrayList<>();
        final AbortedTxnsIterator abortedTxnsIterator = new AbortedTxnsIterator(inputStream);
        final Iterable<AbortedTxn> iter = () -> abortedTxnsIterator;
        for (AbortedTxn abortedTxn : iter) {
            if (ctx.isCancelled()) {
                break;
            } else if (abortedTxnInRange(startOffset, lastOffset, abortedTxn)) {
                // if the aborted transaction is within our fetch range, include it.
                abortedTxns.add(abortedTxn);
            }
        }
        return abortedTxns;
    }

    /**
     * Returns true if there is an overlap between the fetched offset range and the aborted
     * transaction marker
     */
    static boolean abortedTxnInRange(long fetchStartOffset, long fetchLastOffset,
                                     AbortedTxn abortedTxn) {
        final long abortedTxnFirstOffset = abortedTxn.firstOffset();
        final long abortedTxnLastOffset = abortedTxn.lastOffset();
        return (fetchStartOffset <= abortedTxnLastOffset) && (abortedTxnFirstOffset <= fetchLastOffset);
    }
}
