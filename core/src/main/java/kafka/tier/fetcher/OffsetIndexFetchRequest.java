/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.OffsetPosition;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;

import java.io.InputStream;
import java.util.concurrent.CancellationException;

final class OffsetIndexFetchRequest {

    /**
     *  Find an OffsetPosition from a OffsetIndex InputStream. This method attempts to duplicate
     *  OffsetIndex.lookup() in that in the event an index entry is not found, or the index
     *  is empty, we return OffsetPosition(baseOffset, 0).
     *  This should allow for finding any given targetOffset as long as the targetOffset is located
     *  in the segment file.
     */
    static OffsetPosition fetchOffsetPositionForStartingOffset(CancellationContext cancellationContext,
                                                               TierObjectStore tierObjectStore,
                                                               TierObjectStore.ObjectMetadata tierObjectMetadata,
                                                               long targetOffset) throws Exception {
        final long startOffset = tierObjectMetadata.baseOffet();
        final OffsetPosition defaultOffsetPosition = new OffsetPosition(startOffset, 0);

        try (TierObjectStoreResponse response = tierObjectStore.getObject(
                tierObjectMetadata,
                TierObjectStore.FileType.OFFSET_INDEX)) {
            final InputStream responseInputStream = response.getInputStream();
            OffsetPosition currentOffsetPosition = defaultOffsetPosition;
            final TierOffsetIndexIterator tierOffsetIndexIterator = new TierOffsetIndexIterator(responseInputStream, startOffset);
            while (tierOffsetIndexIterator.hasNext()) {
                if (cancellationContext.isCancelled())
                    throw new CancellationException("Tiered offset index fetch request cancelled");
                final OffsetPosition op = tierOffsetIndexIterator.next();
                if (targetOffset == op.offset()) {
                    currentOffsetPosition = op;
                    break;
                } else if (targetOffset > op.offset()) {
                    currentOffsetPosition = op;
                } else if (targetOffset < op.offset()) {
                    break;
                }
            }
            return currentOffsetPosition;
        }
    }

}
