/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.server.BrokerReconfigurable;
import kafka.server.DelayedOperationKey;
import kafka.server.KafkaConfig;
import kafka.tier.TierTimestampAndOffset;
import kafka.tier.fetcher.offsetcache.FetchOffsetCache;
import kafka.tier.store.TierObjectStore;
import kafka.utils.KafkaScheduler;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static scala.compat.java8.JFunction.func;

public class TierFetcher implements BrokerReconfigurable {
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final CancellationContext cancellationContext;
    private final Logger logger;
    private final TierObjectStore tierObjectStore;
    private final ThreadPoolExecutor executorService;
    public final TierFetcherMetrics tierFetcherMetrics;
    // package private for testing
    final FetchOffsetCache cache;
    private final MemoryTracker memoryTracker;

    @SuppressWarnings("deprecation")
    public TierFetcher(Time time,
                       TierFetcherConfig tierFetcherConfig,
                       TierObjectStore tierObjectStore,
                       KafkaScheduler scheduler,
                       Metrics metrics,
                       LogContext logContext) {
        this.cancellationContext = CancellationContext.newContext();
        this.tierObjectStore = tierObjectStore;
        this.logger = logContext.logger(TierFetcher.class);
        this.executorService = (ThreadPoolExecutor) (Executors.newFixedThreadPool(tierFetcherConfig.numFetchThreads));
        this.cache = new FetchOffsetCache(time, tierFetcherConfig.offsetCacheSize, tierFetcherConfig.offsetCacheExpirationMs);
        this.tierFetcherMetrics = new TierFetcherMetrics(metrics, executorService, cache);
        this.memoryTracker = new MemoryTracker(time, metrics, tierFetcherConfig.memoryPoolSizeBytes);
        scheduler.schedule("tier-fetcher-clear-fetch-offset-cache",
                func(() -> {
                    cache.expireEntries();
                    return null;
                  }),
                tierFetcherConfig.offsetCacheExpiryPeriodMs,
                tierFetcherConfig.offsetCacheExpiryPeriodMs,
                TimeUnit.MILLISECONDS);
    }

    TierFetcher(Time time, TierObjectStore tierObjectStore, KafkaScheduler scheduler, Metrics metrics) {
        this(time, new TierFetcherConfig(), tierObjectStore, scheduler, metrics, new LogContext());
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
            // unblock any pending memory acquisitions
            memoryTracker.close();
        }
    }

    // public for jmh benchmarking purposes
    public PendingFetch buildFetch(List<TierFetchMetadata> tierFetchMetadataList,
                                   IsolationLevel isolationLevel,
                                   Consumer<DelayedOperationKey> fetchCompletionCallback) {
        if (!tierFetchMetadataList.isEmpty()) {
            // For now, we only fetch the first requested partition
            // This is subject to change in the future.
            final TierFetchMetadata firstFetchMetadata = tierFetchMetadataList.get(0);
            final List<TopicPartition> ignoredTopicPartitions =
                    tierFetchMetadataList.subList(1, tierFetchMetadataList.size())
                            .stream()
                            .map(TierFetchMetadata::topicPartition)
                            .collect(Collectors.toList());

            if (firstFetchMetadata == null) {
                throw new IllegalStateException("No TierFetchMetadata supplied, cannot start fetch");
            } else if (!stopped.get()) {
                logger.debug("Fetching " + firstFetchMetadata.topicPartition() + " from tiered storage");
                final long targetOffset = firstFetchMetadata.fetchStartOffset();
                final int maxBytes = firstFetchMetadata.maxBytes();
                final CancellationContext cancellationContext = this.cancellationContext.subContext();
                return new PendingFetch(
                                cancellationContext,
                                tierObjectStore,
                                cache,
                                Optional.of(tierFetcherMetrics),
                                firstFetchMetadata.segmentMetadata(),
                                fetchCompletionCallback,
                                targetOffset,
                                maxBytes,
                                firstFetchMetadata.segmentSize(),
                                isolationLevel,
                                memoryTracker,
                                ignoredTopicPartitions);
            } else {
                throw new IllegalStateException("TierFetcher is shutting down, request was not scheduled");
            }
        } else {
            throw new IllegalStateException("No TierFetchMetadata supplied to TierFetcher fetch "
                    + "request");
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
        PendingFetch fetch = buildFetch(tierFetchMetadataList, isolationLevel, fetchCompletionCallback);
        executorService.execute(fetch);
        return fetch;
    }

    public PendingOffsetForTimestamp fetchOffsetForTimestamp(Map<TopicPartition, TierTimestampAndOffset> tierTimestampAndOffsets,
                                                             Consumer<DelayedOperationKey> fetchCompletionCallback) {
        final CancellationContext cancellationContext = this.cancellationContext.subContext();
        final PendingOffsetForTimestamp pending = new PendingOffsetForTimestamp(cancellationContext,
                tierObjectStore,
                tierTimestampAndOffsets,
                Optional.of(tierFetcherMetrics),
                fetchCompletionCallback);
        executorService.execute(pending);
        return pending;
    }

    // visible for testing
    public MemoryTracker memoryTracker() {
        return this.memoryTracker;
    }

    // dynamic configuration
    @SuppressWarnings("deprecation")
    public static scala.collection.Set<String> reconfigurableConfigs =
            scala.collection.JavaConverters.asScalaSet(new HashSet<>(Arrays.asList(KafkaConfig.TierFetcherMemoryPoolSizeBytesProp())));
    @Override
    public scala.collection.Set<String> reconfigurableConfigs() {
        return reconfigurableConfigs;
    }

    @Override
    public void validateReconfiguration(KafkaConfig newConfig) {
        long newMemoryBytesValue = newConfig.tierFetcherMemoryPoolSizeBytes();
        if (newMemoryBytesValue < 0) {
            throw new ConfigException(String.format("%s should not be less than 0",
                    KafkaConfig.TierFetcherMemoryPoolSizeBytesProp()));
        }
    }

    @Override
    public void reconfigure(KafkaConfig oldConfig, KafkaConfig newConfig) {
        TierFetcherConfig newTierFetcherConfig = new TierFetcherConfig(newConfig);
        memoryTracker.setPoolSize(newTierFetcherConfig.memoryPoolSizeBytes);
    }
}
