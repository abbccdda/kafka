/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.log.OffsetPosition;
import kafka.log.TimestampOffset;
import kafka.server.DelayedOperationKey;
import kafka.server.TierOffsetForTimestampOperationKey;
import kafka.tier.TierTimestampAndOffset;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.store.TierObjectStore;
import kafka.tier.store.TierObjectStoreResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.record.FileRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class PendingOffsetForTimestamp implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(PendingOffsetForTimestamp.class);
    private final CancellationContext cancellationContext;
    private final TierObjectStore tierObjectStore;
    private final Map<TopicPartition, TierTimestampAndOffset> timestamps;
    private final Consumer<DelayedOperationKey> fetchCompletionCallback;
    private final ConcurrentHashMap<TopicPartition,
            Optional<FileRecords.FileTimestampAndOffset>> results = new ConcurrentHashMap<>();
    private final UUID requestId = UUID.randomUUID();

    PendingOffsetForTimestamp(CancellationContext cancellationContext,
                              TierObjectStore tierObjectStore,
                              Map<TopicPartition, TierTimestampAndOffset> timestamps,
                              Consumer<DelayedOperationKey> fetchCompletionCallback) {
        this.cancellationContext = cancellationContext;
        this.tierObjectStore = tierObjectStore;
        this.timestamps = Collections.unmodifiableMap(timestamps);
        this.fetchCompletionCallback = fetchCompletionCallback;
    }

    /**
     * @return true if the the request has either been completed or has been canceled
     */
    public boolean isDone() {
        return cancellationContext.isCancelled() || isComplete();
    }

    /**
     * @return true if all FileTimestampAndOffsets have been fetched or completed exceptionally
     */
    public boolean isComplete() {
        return results.size() == timestamps.size();
    }

    /**
     * @return Return the results map containing all (TopicPartition, FileTimestampAndOffset) that have
     *         either been successfully fetched or completed exceptionally
     */
    public Map<TopicPartition, Optional<FileRecords.FileTimestampAndOffset>> results() {
        return Collections.unmodifiableMap(results);
    }

    /**
     * Complete a TimestampAndOffset request exceptionally. Generally used when the leader
     * for a topic partition has changed.
     *
     * @param topicPartition the TopicPartition for the request
     * @param exception      the exception to wrap in a FileTimestampAndOffset
     */
    public void completeExceptionally(TopicPartition topicPartition, Exception exception) {
        TierTimestampAndOffset timestampAndOffset = timestamps.get(topicPartition);
        if (timestampAndOffset != null)
            results.put(topicPartition,
                    Optional.of(new FileRecords.FileTimestampAndOffset(timestampAndOffset.timestamp,
                            timestampAndOffset.leaderEpoch(),
                            exception)));
    }

    /**
     * Generate DelayedListOffsets purgatory operation keys
     * @return list of DelayedOperationKeys that correspond to this request.
     */
    public List<DelayedOperationKey> delayedOperationKeys() {
        return Collections.singletonList(new TierOffsetForTimestampOperationKey(requestId));
    }

    /**
     * Complete this fetch by making the provided records available through the transferPromise
     * and calling the fetchCompletionCallback.
     */
    private void complete() {
        if (fetchCompletionCallback != null) {
            for (DelayedOperationKey key : delayedOperationKeys()) {
                fetchCompletionCallback.accept(key);
            }
        }
    }

    /** TierTimestampAndOffsets that will be resolved by this fetch
     * @return TierTimestampAndOffset map
     */
    public Map<TopicPartition, TierTimestampAndOffset> tierTimestampAndOffsets() {
        return timestamps;
    }

    @Override
    public void run() {
        for (Map.Entry<TopicPartition, TierTimestampAndOffset> entry : timestamps.entrySet()) {
            final TopicPartition topicPartition = entry.getKey();
            final TierTimestampAndOffset tierTimestampAndOffset = entry.getValue();
            final TierObjectMetadata objectMetadata = entry.getValue().metadata;
            final long targetTimestamp = tierTimestampAndOffset.timestamp;
            try {
                if (fetchable(topicPartition)) {
                    final TimestampOffset indexOffsetTimestamp = TimestampIndexFetchRequest
                            .fetchOffsetForTimestamp(
                                    cancellationContext,
                                    tierObjectStore,
                                    tierTimestampAndOffset.metadata,
                                    targetTimestamp);
                    if (fetchable(topicPartition)) {
                        final OffsetPosition offsetPosition = OffsetIndexFetchRequest
                                .fetchOffsetPositionForStartingOffset(
                                        cancellationContext,
                                        tierObjectStore,
                                        objectMetadata,
                                        indexOffsetTimestamp.offset());
                        if (fetchable(topicPartition)) {
                            try (final TierObjectStoreResponse response =
                                         tierObjectStore.getObject(objectMetadata,
                                                 TierObjectStore.TierObjectStoreFileType.SEGMENT,
                                                 offsetPosition.position())) {
                                final Optional<Long> offsetOpt =
                                        TierSegmentReader.offsetForTimestamp(cancellationContext,
                                                response.getInputStream(),
                                                targetTimestamp);
                                results.putIfAbsent(objectMetadata.topicPartition(),
                                        offsetOpt.map(offset ->
                                                new FileRecords.FileTimestampAndOffset(targetTimestamp, offset,
                                                        tierTimestampAndOffset.leaderEpoch())));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.debug("Failed to fetch TierTimestampAndOffset {} from tiered storage",
                        tierTimestampAndOffset, e);
                results.putIfAbsent(topicPartition,
                        Optional.of(new FileRecords.FileTimestampAndOffset(targetTimestamp,
                                tierTimestampAndOffset.leaderEpoch(),
                                new KafkaStorageException(e))));
            }
        }
        complete();
    }


    /**
     * A tier timestamp and offset lookup can be skipped if it has already
     * been completed exceptionally, or the overall request has been cancelled
     */
    private boolean fetchable(TopicPartition topicPartition) {
        return !cancellationContext.isCancelled() && !results.containsKey(topicPartition);
    }

    /**
     * Cancel the request
     */
    public void cancel() {
        this.cancellationContext.cancel();
    }
}
