/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.exceptions.TierMetadataFatalException;
import kafka.tier.state.TierPartitionState;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * Class to track outstanding requests and signal back to the TierTopicManager
 * user when their metadata requests have been read and materialized.
 */
class TierTopicListeners {
    private final Map<TopicIdPartition, Map<UUID, CompletableFuture<TierPartitionState.AppendResult>>> results = new HashMap<>();

    /**
     * Checks whether a given tier index entry is being tracked. If so,
     * returns a CompletableFuture to be completed to signal back to the sender.
     *
     * @param metadata tier index topic entry we are trying to complete
     * @return CompletableFuture for this index entry if one exists.
     */
    synchronized Optional<CompletableFuture<TierPartitionState.AppendResult>> getAndRemoveTracked(AbstractTierMetadata metadata) {
        Map<UUID, CompletableFuture<TierPartitionState.AppendResult>> entries = results.get(metadata.topicIdPartition());

        if (entries != null) {
            CompletableFuture<TierPartitionState.AppendResult> future = entries.remove(metadata.messageId());

            if (entries.size() == 0)
                results.remove(metadata.topicIdPartition());

            return Optional.ofNullable(future);
        }
        return Optional.empty();
    }

    /**
     * Track a tier topic index entry's materialization into the tier topic.
     * If an index entry is already being tracked, then we exceptionally
     * complete the existing future before adding the new entry and future.
     *
     * @param metadata tier index topic entry to track materialization of
     * @param future future to complete when the entry has been materialized
     */
    synchronized void addTracked(AbstractTierMetadata metadata,
                                 CompletableFuture<TierPartitionState.AppendResult> future) {
        results.putIfAbsent(metadata.topicIdPartition(), new HashMap<>());
        Map<UUID, CompletableFuture<TierPartitionState.AppendResult>> entries = results.get(metadata.topicIdPartition());

        CompletableFuture previous = entries.put(metadata.messageId(), future);
        if (previous != null)
            previous.completeExceptionally(new TierMetadataFatalException(
                    "A new index entry is being tracked for messageId " + metadata.messageId() +
                            " obsoleting this request."));
    }

    /**
     * Return the number of registered listeners.
     * @return number of registered listeners
     */
    synchronized long numListeners() {
        return results
                .values()
                .stream()
                .mapToLong(Map::size)
                .sum();
    }

    /**
     * Shutdown listeners. Completes any outstanding listener exceptionally.
     */
    synchronized void shutdown() {
        for (Map<UUID, CompletableFuture<TierPartitionState.AppendResult>> entries : results.values()) {
            for (CompletableFuture<TierPartitionState.AppendResult> future : entries.values())
                future.completeExceptionally(new CancellationException("TierTopicListeners shutting down"));
        }
        results.clear();
    }
}
