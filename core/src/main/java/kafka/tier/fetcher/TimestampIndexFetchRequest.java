/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.TimestampOffset;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;

import java.io.InputStream;
import java.util.concurrent.CancellationException;

final class TimestampIndexFetchRequest {

    /**
     *  Find a TimestampOffset from a TimestampIndex InputStream, corresponding to a target
     *  timestamp.
     * @param cancellationContext cancellation context to allow for request cancellation
     * @param tierObjectStore object store implementation to use to get the timestamp index
     * @param tierObjectMetadata object metadata used to locate the timestamp index in the object
     *                           store
     * @param targetTimestamp target timestamp to search
     * @return TimestampOffset containing the offset corresponding to the latest offset in the
     *         offset index prior to the target timestamp
     */
    static TimestampOffset fetchOffsetForTimestamp(CancellationContext cancellationContext,
                                                   TierObjectStore tierObjectStore,
                                                   TierObjectStore.ObjectMetadata tierObjectMetadata,
                                                   long targetTimestamp) throws Exception {
        final long startOffset = tierObjectMetadata.baseOffset();
        TimestampOffset found = new TimestampOffset(targetTimestamp, tierObjectMetadata.baseOffset());
        try (TierObjectStoreResponse timestampResponse = tierObjectStore.getObject(
                tierObjectMetadata,
                TierObjectStore.FileType.TIMESTAMP_INDEX)) {
            final InputStream timestampStream = timestampResponse.getInputStream();
            final TierTimestampIndexIterator tierTimestampIterator = new TierTimestampIndexIterator(timestampStream, startOffset);
            while (tierTimestampIterator.hasNext()) {
                if (cancellationContext.isCancelled())
                    throw new CancellationException("Tiered timestamp index fetch request "
                            + "cancelled");
                
                TimestampOffset timestampOffset = tierTimestampIterator.next();
                if (timestampOffset.timestamp() == targetTimestamp) {
                    return timestampOffset;
                } else if (timestampOffset.timestamp() < targetTimestamp) {
                    found = timestampOffset;
                } else {
                    return found;
                }
            }
        }
        return found;
    }
}
