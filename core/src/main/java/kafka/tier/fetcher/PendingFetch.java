/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.AbortedTxn;
import kafka.log.OffsetPosition;
import kafka.server.DelayedOperationKey;
import kafka.server.TierFetchOperationKey;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.IsolationLevel;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class PendingFetch implements Runnable {
    private final CancellationContext cancellationContext;
    private final TierObjectStore tierObjectStore;
    private final Optional<Sensor> recordBytesFetched;
    private final TierObjectStore.ObjectMetadata objectMetadata;
    private final Consumer<DelayedOperationKey> fetchCompletionCallback;
    private final long targetOffset;
    private final int maxBytes;
    private final List<TopicPartition> ignoredTopicPartitions;
    private final UUID requestId = UUID.randomUUID();
    private final CompletableFuture<TierFetchResult> transferPromise;
    private final IsolationLevel isolationLevel;

    PendingFetch(CancellationContext cancellationContext,
                 TierObjectStore tierObjectStore,
                 Optional<Sensor> recordBytesFetched,
                 TierObjectStore.ObjectMetadata objectMetadata,
                 Consumer<DelayedOperationKey> fetchCompletionCallback,
                 long targetOffset,
                 int maxBytes,
                 IsolationLevel isolationLevel,
                 List<TopicPartition> ignoredTopicPartitions) {
        this.cancellationContext = cancellationContext;
        this.tierObjectStore = tierObjectStore;
        this.recordBytesFetched = recordBytesFetched;
        this.objectMetadata = objectMetadata;
        this.fetchCompletionCallback = fetchCompletionCallback;
        this.targetOffset = targetOffset;
        this.maxBytes = maxBytes;
        this.ignoredTopicPartitions = ignoredTopicPartitions;
        this.transferPromise = new CompletableFuture<>();
        this.isolationLevel = isolationLevel;
    }

    /**
     * Generate DelayedFetch purgatory operation keys
     * @return list of DelayedOperationKeys that correspond to this request.
     */
    public List<DelayedOperationKey> delayedOperationKeys() {
        return Collections.singletonList(new TierFetchOperationKey(objectMetadata.topicIdPartition().topicPartition(), requestId));
    }

    /**
     * Checks if the pending fetch has finished
     * @return boolean for whether the fetch is complete
     */
    public boolean isComplete() {
        return this.transferPromise.isDone();
    }

    private OffsetPosition fetchOffsetPosition() throws Exception {
        return OffsetIndexFetchRequest.fetchOffsetPositionForStartingOffset(
                        cancellationContext,
                        tierObjectStore,
                        objectMetadata,
                        targetOffset);
    }

    private TierObjectStoreResponse fetchSegment(OffsetPosition offsetPosition) throws IOException {
        return tierObjectStore.getObject(objectMetadata,
                TierObjectStore.FileType.SEGMENT, offsetPosition.position());
    }

    private List<AbortedTxn> fetchAbortedTxns(MemoryRecords records) throws Exception {
        Long startOffset = null;
        long lastOffset = 0; // walk the record batches to find the base offset and last offset of the fetch range
        for (RecordBatch recordBatch : records.batches()) {
            if (startOffset == null)
                startOffset = recordBatch.baseOffset();
            lastOffset = recordBatch.lastOffset();
        }

        if (startOffset == null || lastOffset == 0) {
            return Collections.emptyList();
        }

        try (final TierObjectStoreResponse abortedTransactionsResponse =
                     tierObjectStore.getObject(objectMetadata,
                             TierObjectStore.FileType.TRANSACTION_INDEX)) {
            return TierAbortedTxnReader.readInto(cancellationContext,
                    abortedTransactionsResponse.getInputStream(),
                    startOffset,
                    lastOffset);
        }
    }

    @Override
    public void run() {
        try {
            if (!cancellationContext.isCancelled()) {
                final OffsetPosition offsetPosition = fetchOffsetPosition();
                try (final TierObjectStoreResponse response = fetchSegment(offsetPosition)) {
                    final MemoryRecords records = TierSegmentReader.loadRecords(cancellationContext,
                            response.getInputStream(), maxBytes, targetOffset);
                    if (objectMetadata.hasAbortedTxns() && isolationLevel == IsolationLevel.READ_COMMITTED) {
                        List<AbortedTxn> abortedTxns = fetchAbortedTxns(records);
                        completeFetch(records, abortedTxns, null);
                    } else {
                        completeFetch(records, Collections.emptyList(), null);
                    }
                }
            } else {
                completeFetch(MemoryRecords.EMPTY, Collections.emptyList(), null);
            }
        } catch (Exception e) {
            completeFetch(MemoryRecords.EMPTY, Collections.emptyList(), e);
        }
    }

    /**
     * Block on a fetch request finishing (or canceling), returning either complete MemoryRecords
     * for the fetch, or empty records.
     */
    public Map<TopicPartition, TierFetchResult> finish() {
        final HashMap<TopicPartition, TierFetchResult> resultMap = new HashMap<>();
        try {
            final TierFetchResult tierFetchResult = transferPromise.get();
            recordBytesFetched.ifPresent(sensor -> sensor.record(tierFetchResult.records.sizeInBytes()));
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(), tierFetchResult);
        } catch (InterruptedException e) {
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(), TierFetchResult.emptyFetchResult());
        } catch (ExecutionException e) {
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(),
                    new TierFetchResult(MemoryRecords.EMPTY, Collections.emptyList(), e.getCause()));
        }

        for (TopicPartition ignoredTopicPartition : ignoredTopicPartitions) {
            resultMap.put(ignoredTopicPartition, TierFetchResult.emptyFetchResult());
        }
        return resultMap;
    }

    /**
     * Cancel the pending fetch.
     */
    public void cancel() {
        this.cancellationContext.cancel();
    }

    /**
     * Complete this fetch by making the provided records available through the transferPromise
     * and calling the fetchCompletionCallback.
     */
    private void completeFetch(MemoryRecords records,
                               List<AbortedTxn> abortedTxns,
                               Throwable throwable) {
        final TierFetchResult tierFetchResult = new TierFetchResult(records, abortedTxns,
                throwable);
        this.transferPromise.complete(tierFetchResult);
        if (fetchCompletionCallback != null) {
            for (DelayedOperationKey key : delayedOperationKeys())
                fetchCompletionCallback.accept(key);
        }
    }

}
