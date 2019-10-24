/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.tier.TopicIdPartition;
import kafka.tier.client.TierTopicProducerSupplier;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.exceptions.TierMetadataFatalException;
import kafka.tier.exceptions.TierMetadataRetriableException;
import kafka.tier.state.TierPartitionState.AppendResult;
import kafka.zk.AdminZkClient;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A metadata store for tiered storage. Exposes APIs to maintain and materialize metadata for tiered segments. The metadata
 * store is implemented as a Kafka topic. The message types stored in this topic are defined in {@link kafka.tier.domain.TierRecordType}.
 * The TierTopicManager is also responsible for making all the tiering related metadata available to all brokers in the
 * cluster. It does this by consuming from the tier topic and materializing relevant state into the TierPartitionState
 * files.
 */
public class TierTopicManager implements Runnable, TierTopicAppender {
    private static final Logger log = LoggerFactory.getLogger(TierTopicManager.class);
    private static final int TOPIC_CREATION_BACKOFF_MS = 5000;

    private final TierTopicManagerConfig config;
    private final Supplier<String> bootstrapServersSupplier;
    private final Supplier<Producer<byte[], byte[]>> producerSupplier;
    private final TierTopic tierTopic;
    private final TierTopicConsumer tierTopicConsumer;
    private final AtomicLong heartbeat = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ReentrantReadWriteLock sendLock = new ReentrantReadWriteLock();
    private final Map<AbstractTierMetadata, CompletableFuture<AppendResult>> queuedRequests = new LinkedHashMap<>();
    private final Thread becomeReadyThread = new KafkaThread("TierTopicManagerThread", this, false);

    private volatile Producer<byte[], byte[]> producer;

    /**
     * Instantiate TierTopicManager. Once created, startup() must be called in order to start normal operation.
     * @param config tier topic manager configurations
     * @param tierTopicConsumer tier topic consumer instance
     * @param producerSupplier supplier for producer instances
     * @param bootstrapServersSupplier supplier for bootstrap server
     */
    public TierTopicManager(TierTopicManagerConfig config,
                            TierTopicConsumer tierTopicConsumer,
                            Supplier<Producer<byte[], byte[]>> producerSupplier,
                            Supplier<AdminZkClient> adminZkClientSupplier,
                            Supplier<String> bootstrapServersSupplier) {
        if (config.logDirs.size() > 1)
            throw new UnsupportedOperationException("Tiered storage does not support multiple log directories");

        this.config = config;
        this.bootstrapServersSupplier = bootstrapServersSupplier;

        this.tierTopicConsumer = tierTopicConsumer;
        this.tierTopic = new TierTopic(config.tierNamespace, adminZkClientSupplier);
        this.producerSupplier = producerSupplier;
    }

    /**
     * Primary public constructor for TierTopicManager.
     * @param config TierTopicManagerConfig containing tiering configuration.
     * @param tierTopicConsumer tier topic consumer instance
     * @param metrics Kafka metrics to track TierTopicManager metrics
     */
    public TierTopicManager(TierTopicManagerConfig config,
                            TierTopicConsumer tierTopicConsumer,
                            Supplier<AdminZkClient> adminZkClientSupplier,
                            Supplier<String> bootstrapServersSupplier,
                            Metrics metrics) {
        this(config,
                tierTopicConsumer,
                new TierTopicProducerSupplier(config),
                adminZkClientSupplier,
                bootstrapServersSupplier);
        setupMetrics(metrics);
    }

    public void startup() {
        becomeReadyThread.start();
    }

    @Override
    public void run() {
        try {
            while (!ready.get() && !shutdown.get()) {
                if (!tryBecomeReady(true)) {
                    log.warn("Failed to become ready. Retrying in {}ms.", TOPIC_CREATION_BACKOFF_MS);
                    Thread.sleep(TOPIC_CREATION_BACKOFF_MS);
                }
            }
        } catch (Exception e) {
            if (shutdown.get())
                log.debug("Ignoring exception caught during shutdown", e);
            else
                log.error("Caught fatal exception in TierTopicManager", e);
        } finally {
            log.info("TierTopicManager thread exited. ready: {} shutdown: {}", ready.get(), shutdown.get());
        }
    }

    /**
     * Write an AbstractTierMetadata to the Tier Topic, returning a
     * CompletableFuture that tracks the result of the materialization after the
     * message has been read from the tier topic, allowing the sender to determine
     * whether the write was fenced, or the send failed.
     *
     * @param metadata metadata to be written to the tier topic
     * @return a CompletableFuture which returns the result of the send and subsequent materialization.
     */
    @Override
    public CompletableFuture<AppendResult> addMetadata(AbstractTierMetadata metadata) {
        CompletableFuture<AppendResult> future = new CompletableFuture<>();
        addMetadata(metadata, future);
        return future;
    }

    /**
     * Performs a write to the tier topic to attempt to become leader for the tiered topic partition.
     *
     * @param topicIdPartition the topic partition for which the sender wishes to become the archive leader.
     * @param tierEpoch the archiver epoch
     * @return a CompletableFuture which returns the result of the send and subsequent materialization.
     */
    @Override
    public CompletableFuture<AppendResult> becomeArchiver(TopicIdPartition topicIdPartition, int tierEpoch) {
        // Generate a unique ID in order to track the leader request under scenarios
        // where we maintain the same leader ID.
        // This is possible when there is a single broker, and is primarily for defensive reasons.
        final UUID messageId = UUID.randomUUID();
        final TierTopicInitLeader initRecord = new TierTopicInitLeader(topicIdPartition, tierEpoch, messageId, config.brokerId);
        return addMetadata(initRecord);
    }

    /**
     * Return whether TierTopicManager is ready to accept writes.
     *
     * @return boolean
     */
    @Override
    public boolean isReady() {
        return ready.get() && tierTopicConsumer.isReady();
    }

    /**
     * @return All of the partitions for the Tier Topic
     * visible for testing
     */
    public static Set<TopicPartition> partitions(String topicName, int numPartitions) {
        return IntStream
                .range(0, numPartitions)
                .mapToObj(partitionId -> new TopicPartition(topicName, partitionId))
                .collect(Collectors.toSet());
    }

    /**
     * Try to move the TierTopicManager to ready state. This will first try to create the tier state
     * topic if it has not been created yet, and check that the topic has the expected number of
     * partitions. It will then call startProduceConsume, which will setup tier topic producer.
     * @return boolean for whether TierTopicManager moved to ready state
     *
     * visible for testing
     */
    public boolean tryBecomeReady(boolean startConsumerThread) {
        // wait until we have a non-empty bootstrap server
        if (bootstrapServersSupplier.get().isEmpty()) {
            log.info("Could not resolve bootstrap server. Will retry.");
            return false;
        }

        // ensure tier topic is created; create one if not
        try {
            tierTopic.ensureTopic(config.configuredNumPartitions, config.configuredReplicationFactor);
        } catch (Exception e) {
            log.info("Caught exception when ensuring tier topic is created. Will retry.", e);
            return false;
        }

        // start producer and consumer
        startProduceConsume(startConsumerThread);
        return true;
    }

    /**
     * Shutdown the tier topic manager.
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            try {
                becomeReadyThread.join();
            } catch (InterruptedException e) {
                log.error("Shutdown interrupted", e);
            } finally {
                cleanup();
            }
        }
    }

    /**
     * Setup backing consumers and producer
     */
    private void startProduceConsume(boolean startConsumerThread) {
        // startup the producer
        producer = producerSupplier.get();

        // start the consumer
        tierTopicConsumer.startConsume(startConsumerThread, tierTopic);

        // Now that the producers and consumers have been setup, we are ready to accept requests. Reprocess any queued
        // metadata requests.
        synchronized (this) {
            ready.set(true);

            for (Map.Entry<AbstractTierMetadata, CompletableFuture<AppendResult>> entry : queuedRequests.entrySet())
                addMetadata(entry.getKey(), entry.getValue());
            queuedRequests.clear();
        }
    }

    private void cleanup() {
        sendLock.writeLock().lock();
        try {
            ready.set(false);

            if (producer != null)
                producer.close(Duration.ofSeconds(1));

            for (CompletableFuture<AppendResult> future : queuedRequests.values())
                future.completeExceptionally(new TierMetadataFatalException("Tier topic manager shutting down"));
            queuedRequests.clear();
        } finally {
            tierTopicConsumer.shutdown();
            sendLock.writeLock().unlock();
        }
    }

    private void addMetadata(AbstractTierMetadata metadata, CompletableFuture<AppendResult> future) {
        sendLock.readLock().lock();
        try {
            if (shutdown.get())
                throw new IllegalStateException("TierTopicManager thread has exited. Cannot add metadata.");

            synchronized (this) {
                if (!ready.get()) {
                    queuedRequests.put(metadata, future);
                    return;
                }
            }

            TopicIdPartition topicPartition = metadata.topicIdPartition();

            // track this entry's materialization
            tierTopicConsumer.trackMaterialization(metadata, future);

            // identify the tier topic partition and produce the message
            TopicPartition tierTopicPartition = tierTopic.toTierTopicPartition(topicPartition);
            producer.send(new ProducerRecord<>(tierTopicPartition.topic(),
                            tierTopicPartition.partition(),
                            metadata.serializeKey(),
                            metadata.serializeValue()),
                    (recordMetadata, exception) -> {
                        if (exception != null) {
                            tierTopicConsumer.cancelTracked(metadata);
                            if (retriable(exception)) {
                                future.completeExceptionally(
                                        new TierMetadataRetriableException(
                                                "Retriable exception sending tier metadata.",
                                                exception));
                            } else {
                                future.completeExceptionally(
                                        new TierMetadataFatalException(
                                                "Fatal exception sending tier metadata.",
                                                exception));
                            }
                        }
                    });
        } finally {
            sendLock.readLock().unlock();
        }
    }

    /**
     * Setup metrics for the tier topic manager.
     */
    private void setupMetrics(Metrics metrics) {
        metrics.addMetric(new MetricName("HeartbeatMs",
                        "TierTopicManager",
                        "Time since last heartbeat in milliseconds.",
                        new HashMap<>()),
                (MetricConfig config, long now) -> now - heartbeat.get());
    }

    /**
     * Determine whether tiering is retriable or whether hard exit should occur
     *
     * @param e The exception
     * @return true if retriable, false otherwise.
     */
    private static boolean retriable(Exception e) {
        return e instanceof RetriableException;
    }
}
