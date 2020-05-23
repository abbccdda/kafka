/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.AbortedTxn;
import kafka.log.OffsetPosition;
import kafka.server.DelayedOperationKey;
import kafka.server.TierFetchOperationKey;
import kafka.tier.fetcher.offsetcache.FetchOffsetCache;
import kafka.tier.fetcher.offsetcache.FetchOffsetMetadata;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class PendingFetch implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(PendingFetch.class);
    private final CancellationContext cancellationContext;
    private final TierObjectStore tierObjectStore;
    private final Optional<TierFetcherMetrics> tierFetcherMetrics;
    private final TierObjectStore.ObjectMetadata objectMetadata;
    private final Consumer<DelayedOperationKey> fetchCompletionCallback;
    private final long targetOffset;
    private final int maxBytes;
    private final int segmentSize;
    private final List<TopicPartition> ignoredTopicPartitions;
    private final UUID requestId = UUID.randomUUID();
    private final CompletableFuture<TierFetchResult> transferPromise;
    private final IsolationLevel isolationLevel;
    private final FetchOffsetCache cache;
    private final FetchOffsetMetadata fetchOffsetMetadata;
    private final TierSegmentReader reader;
    private final String logPrefix;
    private final MemoryTracker memoryTracker;

    PendingFetch(CancellationContext cancellationContext,
                 TierObjectStore tierObjectStore,
                 FetchOffsetCache cache,
                 Optional<TierFetcherMetrics> tierFetcherMetrics,
                 TierObjectStore.ObjectMetadata objectMetadata,
                 Consumer<DelayedOperationKey> fetchCompletionCallback,
                 long targetOffset,
                 int maxBytes,
                 int segmentSize,
                 IsolationLevel isolationLevel,
                 MemoryTracker memoryTracker,
                 List<TopicPartition> ignoredTopicPartitions) {
        this.cancellationContext = cancellationContext;
        this.tierObjectStore = tierObjectStore;
        this.tierFetcherMetrics = tierFetcherMetrics;
        this.objectMetadata = objectMetadata;
        this.fetchCompletionCallback = fetchCompletionCallback;
        this.targetOffset = targetOffset;
        this.maxBytes = maxBytes;
        this.segmentSize = segmentSize;
        this.cache = cache;
        this.ignoredTopicPartitions = ignoredTopicPartitions;
        this.transferPromise = new CompletableFuture<>();
        this.isolationLevel = isolationLevel;
        this.logPrefix = "PendingFetch(requestId=" + requestId + ")";
        this.reader = new TierSegmentReader(logPrefix);

        if (targetOffset == objectMetadata.baseOffset())
            this.fetchOffsetMetadata = new FetchOffsetMetadata(0, OptionalInt.empty());
        else
            this.fetchOffsetMetadata = cache.get(objectMetadata.objectId(), targetOffset);
        this.memoryTracker = memoryTracker;
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
        return transferPromise.isDone();
    }

    private OffsetPosition fetchOffsetPosition() throws Exception {
        if (fetchOffsetMetadata != null) {
            log.debug("{} using fetch position {}", logPrefix, fetchOffsetMetadata);
            return new OffsetPosition(targetOffset, fetchOffsetMetadata.bytePosition);
        }

        // lookup closest offset via offset index
        log.debug("{} fetching offset index", logPrefix);
        return OffsetIndexFetchRequest.fetchOffsetPositionForStartingOffset(
                        cancellationContext,
                        tierObjectStore,
                        objectMetadata,
                        targetOffset);
    }

    private Integer getEndRange(OffsetPosition offsetPosition) {
        if (fetchOffsetMetadata != null && fetchOffsetMetadata.recordBatchSize.isPresent()) {
            // We know the first batch size, so we can perform an object store range request as
            // we can guarantee that we will be able to read at least one batch with the request.
            // We will attempt to read the next batch header in excess of what we return so we know
            // the next batch size (and thus the range) for the next follow up request.
            int length = Math.max(fetchOffsetMetadata.recordBatchSize.getAsInt() + Records.HEADER_SIZE_UP_TO_MAGIC, maxBytes);
            return offsetPosition.position() + length;
        }

        return null;
    }

    private TierObjectStoreResponse fetchSegment(OffsetPosition offsetPosition) throws IOException {
        Integer endRange = getEndRange(offsetPosition);
        if (endRange != null) {
            log.debug("{} fetching segment startPosition: {}, endPosition: {}", logPrefix, offsetPosition, endRange);
            return tierObjectStore.getObject(objectMetadata, TierObjectStore.FileType.SEGMENT,
                    offsetPosition.position(), endRange);
        }

        log.debug("{} fetching segment startPosition: {}", logPrefix, offsetPosition);
        return tierObjectStore.getObject(objectMetadata, TierObjectStore.FileType.SEGMENT,
                offsetPosition.position());
    }

    private List<AbortedTxn> fetchAbortedTxns(ReclaimableMemoryRecords records) throws Exception {
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

    private Optional<MemoryTracker.MemoryLease> waitOnMemoryLease() {
        if (memoryTracker.isDisabled()) {
            return Optional.empty();
        }
        log.debug("{} acquiring memory lease", logPrefix);
        if (fetchOffsetMetadata != null && fetchOffsetMetadata.recordBatchSize.isPresent()) {
            int amount = fetchOffsetMetadata.recordBatchSize.getAsInt();
            return Optional.of(memoryTracker.newLease(cancellationContext, amount));
        } else {
            return Optional.of(memoryTracker.newLease(cancellationContext,
                    Records.HEADER_SIZE_UP_TO_MAGIC));
        }
    }

    @Override
    public void run() {
        try {
            log.debug("Starting tiered fetch. requestId={}, objectMetadata={}, targetOffset={}, "
                            + "maxBytes={}, isolationLevel={}.",
                    requestId, objectMetadata, targetOffset, maxBytes, isolationLevel);

            Optional<MemoryTracker.MemoryLease> lease;
            if (!cancellationContext.isCancelled()) {
                final OffsetPosition offsetPosition = fetchOffsetPosition();
                lease = waitOnMemoryLease();
                try (final TierObjectStoreResponse response = fetchSegment(offsetPosition)) {
                    TierSegmentReader.RecordsAndNextBatchMetadata recordsAndNextBatchMetadata = reader.readRecords(cancellationContext,
                            lease,
                            response.getInputStream(),
                            maxBytes,
                            targetOffset,
                            offsetPosition.position(),
                            segmentSize);
                    ReclaimableMemoryRecords records = recordsAndNextBatchMetadata.records;
                    updateCache(recordsAndNextBatchMetadata.nextOffsetAndBatchMetadata);
                    if (objectMetadata.hasAbortedTxns() && isolationLevel == IsolationLevel.READ_COMMITTED) {
                        final List<AbortedTxn> abortedTxns = fetchAbortedTxns(records);
                        completeFetch(records, abortedTxns, null);
                    } else {
                        completeFetch(records, Collections.emptyList(), null);
                    }
                } catch (Throwable t) {
                    // If there is any exception thrown when attempting to fetch, ensure the memory
                    // lease is relinquished.
                    lease.ifPresent(MemoryTracker.MemoryLease::release);
                    throw t;
                }
            } else {
                completeFetch(ReclaimableMemoryRecords.EMPTY, Collections.emptyList(), null);
            }
        } catch (CancellationException e) {
            completeFetch(ReclaimableMemoryRecords.EMPTY, Collections.emptyList(), null);
        } catch (Exception e) {
            completeFetch(ReclaimableMemoryRecords.EMPTY, Collections.emptyList(), e);
        }
    }

    private void updateCache(TierSegmentReader.NextOffsetAndBatchMetadata nextOffsetAndBatchMetadata) {
        if (nextOffsetAndBatchMetadata != null) {
            long nextOffset = nextOffsetAndBatchMetadata.nextOffset;
            FetchOffsetMetadata nextBatchMetadata = nextOffsetAndBatchMetadata.nextBatchMetadata;

            if (nextBatchMetadata != null) {
                log.debug("{} updating cache. metadata: {}", logPrefix, nextOffsetAndBatchMetadata);
                cache.put(objectMetadata.objectId(), nextOffset, nextBatchMetadata);
            }
        }
    }

    /**
     * Block on a fetch request finishing (or canceling), returning either complete MemoryRecords
     * for the fetch, or empty records. This will also cancel any further work from being performed
     * by the in-progress fetch.
     */
    public Map<TopicPartition, TierFetchResult> finish() {
        // Cancel the CancellationContext in the event the fetch is not finished as the caller
        // will be blocked on the transferPromise being completed.
        cancel();

        // Note: Ensure that if any exception is thrown, the memory lease contained within the
        // TierFetchResult is relinquished.
        final HashMap<TopicPartition, TierFetchResult> resultMap = new HashMap<>();
        try {
            final TierFetchResult tierFetchResult = transferPromise.get();
            tierFetcherMetrics.ifPresent(metrics -> metrics.bytesFetched().record(tierFetchResult.records.sizeInBytes()));
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(), tierFetchResult);
        } catch (InterruptedException e) {
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(), TierFetchResult.emptyFetchResult());
        } catch (ExecutionException e) {
            log.warn("Failed exceptionally while finishing pending fetch request for partition {} from tiered storage." +
                    " This exception is unexpected as the promise in not completed exceptionally ",
                    objectMetadata.topicIdPartition().topicPartition(), e);
            tierFetcherMetrics.ifPresent(metrics -> metrics.fetchException().record());
            resultMap.put(objectMetadata.topicIdPartition().topicPartition(),
                    new TierFetchResult(ReclaimableMemoryRecords.EMPTY, Collections.emptyList(), e.getCause()));
        }

        for (TopicPartition ignoredTopicPartition : ignoredTopicPartitions) {
            resultMap.put(ignoredTopicPartition, TierFetchResult.emptyFetchResult());
        }
        return resultMap;
    }

    public void markFetchExpired() {
        tierFetcherMetrics.ifPresent(metrics -> metrics.fetchCancelled().record());
    }

    /**
     * Cancel the pending fetch.
     */
    public void cancel() {
        cancellationContext.cancel();
        // MemoryTracker requests require a CancellationContext as part of the request to
        // control how long a request for memory will block. By canceling the CancellationContext
        // and waking up the memory tracker here, a canceled request will not block indefinitely.
        memoryTracker.wakeup();
    }

    /**
     * Complete this fetch by making the provided records available through the transferPromise
     * and calling the fetchCompletionCallback.
     */
    private void completeFetch(ReclaimableMemoryRecords records,
                               List<AbortedTxn> abortedTxns,
                               Throwable throwable) {
        if (throwable != null) {
            log.error("{} tier fetch objectMetadata={}, targetOffset={}, maxBytes={}, "
                            + "isolationLevel={} completed with exception", logPrefix,
                    objectMetadata, targetOffset, maxBytes, isolationLevel, throwable);
            tierFetcherMetrics.ifPresent(metrics -> metrics.fetchException().record());
        }

        final TierFetchResult tierFetchResult = new TierFetchResult(records, abortedTxns, throwable);
        transferPromise.complete(tierFetchResult);
        if (fetchCompletionCallback != null) {
            for (DelayedOperationKey key : delayedOperationKeys())
                fetchCompletionCallback.accept(key);
        }
    }
}
