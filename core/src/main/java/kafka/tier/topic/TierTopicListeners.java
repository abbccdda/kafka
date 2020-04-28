/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierRecordType;
import kafka.tier.exceptions.TierMetadataFatalException;
import kafka.tier.state.TierPartitionState;
import kafka.utils.CoreUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Class to track outstanding requests and signal back to the TierTopicManager
 * user when their metadata requests have been read and materialized.
 */
class TierTopicListeners {
    private final Map<TopicIdPartition, Map<MaterializationKey,
            CompletableFuture<TierPartitionState.AppendResult>>> results = new HashMap<>();

    /**
     * Checks whether a given tier index entry is being tracked. If so,
     * returns a CompletableFuture to be completed to signal back to the sender.
     *
     * @param metadata tier index topic entry we are trying to complete
     * @return CompletableFuture for this index entry if one exists.
     */
    synchronized Optional<CompletableFuture<TierPartitionState.AppendResult>> getAndRemoveTracked(AbstractTierMetadata metadata) {
        Map<MaterializationKey, CompletableFuture<TierPartitionState.AppendResult>> entries = results.get(metadata.topicIdPartition());

        if (entries != null) {
            CompletableFuture<TierPartitionState.AppendResult> future =
                    entries.remove(new MaterializationKey(metadata.type(), metadata.messageId(), -1L));

            if (entries.size() == 0)
                results.remove(metadata.topicIdPartition());

            return Optional.ofNullable(future);
        }
        return Optional.empty();
    }

    /**
     * Get and remove all listeners for a particular TopicIdPartition
     *
     * @param topicIdPartition
     * @return Collection of CompletableFuture(s) for the supplied TopicIdPartition
     */
    synchronized Collection<CompletableFuture<TierPartitionState.AppendResult>> getAndRemoveAll(TopicIdPartition topicIdPartition) {
        Map<MaterializationKey, CompletableFuture<TierPartitionState.AppendResult>> entries = results.remove(topicIdPartition);
        return entries == null ? new ArrayList<>() : entries.values();
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
        Map<MaterializationKey, CompletableFuture<TierPartitionState.AppendResult>> entries =
                results.get(metadata.topicIdPartition());

        MaterializationKey key = new MaterializationKey(metadata.type(), metadata.messageId(), System.nanoTime());
        CompletableFuture<TierPartitionState.AppendResult> previous = entries.put(key, future);
        if (previous != null)
            previous.completeExceptionally(new TierMetadataFatalException(
                    "A new index entry is being tracked " + key + " obsoleting this request."));
    }

    /**
     * @return Return the time the oldest listener has been listening for metadata materialization.
     */
    synchronized Optional<Long> maxListenerTimeNanos() {
        return results.values()
                .stream()
                .map(m -> m.keySet().stream().map(MaterializationKey::startTimeNs).min(Long::compareTo))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .min(Long::compare)
                .map(t -> System.nanoTime() - t);
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
        for (Map<MaterializationKey, CompletableFuture<TierPartitionState.AppendResult>> entries : results.values()) {
            for (CompletableFuture<TierPartitionState.AppendResult> future : entries.values())
                future.completeExceptionally(new CancellationException("TierTopicListeners shutting down"));
        }
        results.clear();
    }

    /**
     * Track materialization by (messageType, messageId) to ensure that a retried message
     * will not complete a future for a later metadata with the same messageId.
     * This is necessary as we do not use the idempotent producer
     */
    private static class MaterializationKey {
        TierRecordType type;
        UUID messageId;
        // startTimeNs is included for metrics purposes, and is
        // not used in hash and equality checks
        long startTimeNs;
        MaterializationKey(TierRecordType type, UUID messageId, long startTimeNs) {
            this.type = type;
            this.messageId = messageId;
            this.startTimeNs = startTimeNs;
        }

        public long startTimeNs() {
            return startTimeNs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, messageId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            MaterializationKey other = (MaterializationKey) obj;
            return type.equals(other.type) && messageId.equals(other.messageId);
        }

        @Override
        public String toString() {
            long ageMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs);
            return "MaterializationKey: type(" + type + ") uuid(" + messageId + ") uuidAsBase64("
                    + CoreUtils.uuidToBase64(messageId) + ") ageMs(" + ageMs + ")";
        }
    }
}
