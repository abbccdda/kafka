/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;


/**
 * Provides abstractions for the catchup consumer.
 * This class is not thread safe, except the {@link #wakeup()} method.
 */
class TierCatchupConsumer {
    private static final Logger log = LoggerFactory.getLogger(TierCatchupConsumer.class);

    private final Supplier<Consumer<byte[], byte[]>> consumerSupplier;

    private volatile Consumer<byte[], byte[]> consumer;

    TierCatchupConsumer(Supplier<Consumer<byte[], byte[]>> consumerSupplier) {
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
     * @param tierTopicPartitions Tier topic partitions to start consuming
     */
    void doStart(Set<TopicPartition> tierTopicPartitions) {
        if (!maybeStartConsumer(tierTopicPartitions))
            throw new IllegalStateException("Unable to startup catchup consumer");
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

    /**
     * Startup the consumer, if it is not already active.
     * @param tierTopicPartitions Tier topic partitions to start consuming
     * @return true if consumer was started; false otherwise
     */
    private boolean maybeStartConsumer(Set<TopicPartition> tierTopicPartitions) {
        if (active() || tierTopicPartitions.isEmpty())
            return false;

        consumer = consumerSupplier.get();
        log.info("Seeking catchup consumer to beginning for {}", tierTopicPartitions);
        consumer.assign(tierTopicPartitions);
        consumer.seekToBeginning(consumer.assignment());
        return true;
    }
}
