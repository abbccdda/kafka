/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TierMetadataManager;
import kafka.tier.TopicIdPartition;
import kafka.tier.state.TierPartitionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * Provides abstractions for the catchup consumer.
 * This class is not thread safe, except the {@link #wakeup()} method.
 */
class TierCatchupConsumer {
    private static final Logger log = LoggerFactory.getLogger(TierCatchupConsumer.class);

    private final TierMetadataManager tierMetadataManager;
    private final Supplier<Consumer<byte[], byte[]>> consumerSupplier;

    private volatile Consumer<byte[], byte[]> consumer;

    private Set<TopicIdPartition> partitionsCatchingUp;

    TierCatchupConsumer(TierMetadataManager tierMetadataManager, Supplier<Consumer<byte[], byte[]>> consumerSupplier) {
        this.tierMetadataManager = tierMetadataManager;
        this.consumerSupplier = consumerSupplier;
    }

    /**
     * Poll the underlying consumer, and return any records.
     * @param pollDuration The poll duration passed into {@link org.apache.kafka.clients.consumer.KafkaConsumer#poll(Duration)}
     * @return Consumed records if active; null otherwise
     */
    public ConsumerRecords<byte[], byte[]> poll(Duration pollDuration) {
        if (active())
            return consumer.poll(pollDuration);
        return null;
    }

    /**
     * Check if the consumer is active.
     * @return true if active; false otherwise
     */
    public boolean active() {
        return consumer != null;
    }

    /**
     * Startup the consumer, throwing an exception if unable to do so.
     * @param tierTopic tier topic instance
     * @param partitionStates tier partition state for partitions to catchup on
     */
    void doStart(TierTopic tierTopic, Set<TierPartitionState> partitionStates) {
        if (!maybeStartConsumer(tierTopic, partitionStates))
            throw new IllegalStateException("Unable to startup catchup consumer");
    }

    /**
     * Startup the consumer, if it is not already active.
     * @param tierTopic tier topic instance
     * @param partitionStates tier partition state for partitions to catchup on
     * @return true if consumer was started; false otherwise
     */
    boolean maybeStartConsumer(TierTopic tierTopic, Set<TierPartitionState> partitionStates) {
        if (active() || partitionStates.isEmpty())
            return false;

        partitionStates.forEach(TierPartitionState::beginCatchup);

        Set<TopicIdPartition> partitions = toTopicIdPartitions(partitionStates);
        consumer = consumerSupplier.get();

        Collection<TopicPartition> tierTopicPartitions = tierTopic.toTierTopicPartitions(partitions);
        log.info("Seeking catchup consumer to beginning for {}", tierTopicPartitions);

        consumer.assign(tierTopicPartitions);
        consumer.seekToBeginning(consumer.assignment());

        partitionsCatchingUp = partitions;
        return true;
    }

    /**
     * Try to complete catchup, if we have caught up completely with the provided primary consumer.
     * @param primaryConsumer the consumer to catch up to
     * @return true if catchup complete; false otherwise
     */
    boolean tryComplete(Consumer<byte[], byte[]> primaryConsumer) {
        if (!active())
            return false;

        Set<TopicPartition> assignment = consumer.assignment();
        boolean hasCaughtUp;

        // check if we have caught up with the primary consumer
        try {
            hasCaughtUp = assignment
                    .stream()
                    .allMatch(tp -> primaryConsumer.position(tp) <= consumer.position(tp));
        } catch (TimeoutException e) {
            log.warn("Timed out when determining consumer position");
            return false;
        }

        if (hasCaughtUp) {
            // complete catchup for partitions
            for (TopicIdPartition topicIdPartition : partitionsCatchingUp) {
                Optional<TierPartitionState> partitionStateOpt = tierMetadataManager.tierPartitionState(topicIdPartition);
                partitionStateOpt.ifPresent(partitionState -> {
                    switch (partitionState.status()) {
                        case CATCHUP:
                            partitionState.onCatchUpComplete();
                            break;

                        case INIT:
                        case ONLINE:
                            log.warn("Expected " + topicIdPartition + " to be in catchup state but is in " + partitionState.status());
                            break;

                        default:
                            log.debug("Ignoring catchup completion for " + topicIdPartition + " as current state is " + partitionState.status());
                    }
                });
            }
            partitionsCatchingUp = null;

            // close consumer
            close();
            return true;
        }

        return false;
    }

    /**
     * Wakeup the consumer if it is active. This method is thread-safe.
     */
    synchronized void wakeup() {
        if (active())
            consumer.wakeup();
    }

    /**
     * Close the consumer.
     */
    void close() {
        try {
            if (active())
                consumer.close();
        } catch (Exception e) {
            log.warn("Ignoring exception when closing consumer", e);
        } finally {
            synchronized (this) {
                consumer = null;
            }
        }
    }

    // package-private for testing
    Consumer<byte[], byte[]> consumer() {
        return consumer;
    }

    private static Set<TopicIdPartition> toTopicIdPartitions(Set<TierPartitionState> states) {
        return states
                .stream()
                .map(TierPartitionState::topicIdPartition)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());
    }
}
