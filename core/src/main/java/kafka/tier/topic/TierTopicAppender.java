/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.state.TierPartitionState;
import java.util.concurrent.CompletableFuture;

/**
 * TierTopicAppender allows interaction with a TierTopic. This interface expresses both the
 * capability to write new TierTopic entries, as well as retrieve the results of materializing
 * those entries.
 */
public interface TierTopicAppender {
    /**
     * Performs a write to the tier topic to attempt to become leader for the tiered topic partition.
     *
     * @param topicIdPartition the topic partition for which the sender wishes to become the
     *                         archive leader.
     * @param tierEpoch      the archiver epoch
     * @return a CompletableFuture which returns the result of the send and subsequent materialization.
     */
    CompletableFuture<TierPartitionState.AppendResult> becomeArchiver(TopicIdPartition topicIdPartition,
                                                                      int tierEpoch) throws IllegalAccessException;
    /**
     * Write an AbstractTierMetadata to the Tier Topic, returning a
     * CompletableFuture that tracks the result of the materialization after the
     * message has been read from the tier topic, allowing the sender to determine
     * whether the write was fenced, or the send failed.
     *
     * @param entry the tier topic entry to be written to the tier topic.
     * @return a CompletableFuture which returns the result of the send and subsequent materialization.
     */
    CompletableFuture<TierPartitionState.AppendResult> addMetadata(AbstractTierMetadata entry) throws IllegalAccessException;

    /**
     * Return whether TierTopicManager is ready to accept writes.
     *
     * @return boolean
     */
    boolean isReady();
}
