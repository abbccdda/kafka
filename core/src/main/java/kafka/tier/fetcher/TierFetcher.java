/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.server.DelayedOperationKey;
import kafka.tier.TierTimestampAndOffset;
import kafka.tier.store.TierObjectStore;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;
import scala.compat.java8.OptionConverters;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TierFetcher {
    private final CancellationContext cancellationContext;
    private final Logger logger;
    private final TierObjectStore tierObjectStore;
    private final ThreadPoolExecutor executorService;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    public final TierFetcherMetrics tierFetcherMetrics;

    public TierFetcher(TierFetcherConfig tierFetcherConfig,
                       TierObjectStore tierObjectStore,
                       Metrics metrics,
                       LogContext logContext) {
        this.cancellationContext = CancellationContext.newContext();
        this.tierObjectStore = tierObjectStore;
        this.logger = logContext.logger(TierFetcher.class);
        this.executorService = (ThreadPoolExecutor) (Executors.newFixedThreadPool(tierFetcherConfig.numFetchThreads));
        this.tierFetcherMetrics = new TierFetcherMetrics(metrics, executorService);
    }

    TierFetcher(TierObjectStore tierObjectStore, Metrics metrics) {
        this(new TierFetcherConfig(), tierObjectStore, metrics, new LogContext());
    }

    /**
     * Seal the TierFetcher from accepting new fetches, and cancel all in-progress fetches. The
     * fetched data is not removed from the TierFetcher and is still accessible by requestId.
     */
    public void close() {
        logger.info("Closing TierFetcher");
        if (stopped.compareAndSet(false, true)) {
            cancellationContext.cancel();
            executorService.shutdownNow();
        }
    }

    /**
     * Execute a read for a single partition from Tiered Storage.
     * The provided UUID can be used at any time to cancel the in-progress fetch and retrieve
     * any data fetched. fetchCompletionCallback will be called with the
     * DelayedOperationKey of the completed fetch.
     * <p>
     * Returns a list of TierFetcherOperationKey to be used when registering a DelayedOperation
     * which depends on this fetch.
     */
    public PendingFetch fetch(List<TierFetchMetadata> tierFetchMetadataList,
                              IsolationLevel isolationLevel,
                              Consumer<DelayedOperationKey> fetchCompletionCallback) {
        if (!tierFetchMetadataList.isEmpty()) {
            // For now, we only fetch the first requested partition
            // This is subject to change in the future.
            final TierFetchMetadata firstFetchMetadata = tierFetchMetadataList.get(0);
            final List<TopicPartition> ignoredTopicPartitions =
                    tierFetchMetadataList.subList(1, tierFetchMetadataList.size())
                            .stream()
                            .map(tierFetchMetadata -> tierFetchMetadata.topicPartition())
                            .collect(Collectors.toList());

            if (firstFetchMetadata == null) {
                throw new IllegalStateException("No TierFetchMetadata supplied, cannot start fetch");
            } else if (!stopped.get()) {
                logger.debug("Fetching " + firstFetchMetadata.topicPartition() + " from tiered storage");
                final long targetOffset = firstFetchMetadata.fetchStartOffset();
                final int maxBytes = firstFetchMetadata.maxBytes();
                final long maxOffset = OptionConverters.toJava(firstFetchMetadata.maxOffset()).map(v -> (Long) v).orElse(Long.MAX_VALUE);
                final CancellationContext cancellationContext = this.cancellationContext.subContext();
                final PendingFetch newFetch =
                        new PendingFetch(
                                cancellationContext,
                                tierObjectStore,
                                Optional.of(tierFetcherMetrics.bytesFetched()),
                                firstFetchMetadata.segmentMetadata(),
                                fetchCompletionCallback,
                                targetOffset,
                                maxBytes,
                                maxOffset,
                                isolationLevel,
                                ignoredTopicPartitions);
                executorService.execute(newFetch);
                return newFetch;
            } else {
                throw new IllegalStateException("TierFetcher is shutting down, request was not scheduled");
            }
        } else {
            throw new IllegalStateException("No TierFetchMetadata supplied to TierFetcher fetch "
                    + "request");
        }
    }

    public PendingOffsetForTimestamp fetchOffsetForTimestamp(Map<TopicPartition, TierTimestampAndOffset> tierTimestampAndOffsets,
                                                             Consumer<DelayedOperationKey> fetchCompletionCallback) {
        final CancellationContext cancellationContext = this.cancellationContext.subContext();
        final PendingOffsetForTimestamp pending = new PendingOffsetForTimestamp(cancellationContext,
                tierObjectStore,
                tierTimestampAndOffsets,
                fetchCompletionCallback);
        executorService.execute(pending);
        return pending;
    }
}
