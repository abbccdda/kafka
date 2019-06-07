/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.OffsetPosition;
import kafka.server.DelayedOperationKey;
import kafka.server.TierFetchOperationKey;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class PendingFetch implements Runnable {
    private final CancellationContext cancellationContext;
    private final TierObjectStore tierObjectStore;
    private final Sensor recordBytesFetched;
    private final TierObjectStore.ObjectMetadata objectMetadata;
    private final Consumer<DelayedOperationKey> fetchCompletionCallback;
    private final long targetOffset;
    private final int maxBytes;
    private final long maxOffset;
    private final List<TopicPartition> ignoredTopicPartitions;
    private final UUID requestId = UUID.randomUUID();
    private final CompletableFuture<MemoryRecords> transferPromise;
    private Exception exception;

    PendingFetch(CancellationContext cancellationContext,
                 TierObjectStore tierObjectStore,
                 Sensor recordBytesFetched,
                 TierObjectStore.ObjectMetadata objectMetadata,
                 Consumer<DelayedOperationKey> fetchCompletionCallback,
                 long targetOffset,
                 int maxBytes,
                 long maxOffset,
                 List<TopicPartition> ignoredTopicPartitions) {
        this.cancellationContext = cancellationContext;
        this.tierObjectStore = tierObjectStore;
        this.recordBytesFetched = recordBytesFetched;
        this.objectMetadata = objectMetadata;
        this.fetchCompletionCallback = fetchCompletionCallback;
        this.targetOffset = targetOffset;
        this.maxBytes = maxBytes;
        this.maxOffset = maxOffset;
        this.ignoredTopicPartitions = ignoredTopicPartitions;
        this.transferPromise = new CompletableFuture<>();
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

    @Override
    public void run() {
        try {
            final OffsetPosition offsetPosition = OffsetIndexFetchRequest
                    .fetchOffsetPositionForStartingOffset(
                            cancellationContext,
                            tierObjectStore,
                            objectMetadata,
                            targetOffset);
            if (!cancellationContext.isCancelled()) {
                try (final TierObjectStoreResponse response =
                             tierObjectStore.getObject(objectMetadata,
                                     TierObjectStore.FileType.SEGMENT,
                                     offsetPosition.position())) {

                    final MemoryRecords records = TierSegmentReader.loadRecords(cancellationContext,
                            response.getInputStream(), maxBytes, maxOffset, targetOffset);
                    completeFetch(records);
                }
            } else {
                completeFetch(MemoryRecords.EMPTY);
            }
        } catch (CancellationException e) {
            completeFetch(MemoryRecords.EMPTY);
        } catch (Exception e) {
            this.exception = e;
            completeFetch(MemoryRecords.EMPTY);
        }
    }

    /**
     * Block on a fetch request finishing (or canceling), returning either complete MemoryRecords
     * for the fetch, or empty records.
     */
    public Map<TopicPartition, TierFetchResult> finish() {
        final HashMap<TopicPartition, TierFetchResult> resultMap = new HashMap<>();
        try {
            final Records records = transferPromise.get();
            final TierFetchResult tierFetchResult = new TierFetchResult(records, exception);
            recordBytesFetched.record(records.sizeInBytes());
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(), tierFetchResult);
        } catch (InterruptedException e) {
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(), TierFetchResult.emptyFetchResult());
        } catch (ExecutionException e) {
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(),
                    new TierFetchResult(MemoryRecords.EMPTY, e.getCause()));
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
    private void completeFetch(MemoryRecords records) {
        this.transferPromise.complete(records);
        if (fetchCompletionCallback != null) {
            for (DelayedOperationKey key : delayedOperationKeys())
                fetchCompletionCallback.accept(key);
        }
    }

}
