/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.server.LogDirFailureChannel;
import kafka.tier.TierMetadataManager;
import kafka.tier.TierTopicManagerCommitter;
import kafka.tier.TopicIdPartition;
import kafka.tier.client.TierTopicConsumerSupplier;
import kafka.tier.client.TierTopicProducerSupplier;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierRecordType;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.exceptions.TierMetadataDeserializationException;
import kafka.tier.exceptions.TierMetadataFatalException;
import kafka.tier.exceptions.TierMetadataRetriableException;
import kafka.tier.state.TierPartitionState;
import kafka.tier.state.TierPartitionState.AppendResult;
import kafka.tier.state.TierPartitionStatus;
import kafka.zk.AdminZkClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
    private final TierMetadataManager tierMetadataManager;
    private final Supplier<String> bootstrapServersSupplier;
    private final TierTopicManagerCommitter committer;
    private final Supplier<Consumer<byte[], byte[]>> primaryConsumerSupplier;
    private final Supplier<Producer<byte[], byte[]>> producerSupplier;
    private final TierTopic tierTopic;
    private final AtomicLong heartbeat = new AtomicLong(System.currentTimeMillis());
    private final CountDownLatch shutdownInitiated = new CountDownLatch(2);
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean cleaned = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final TierTopicListeners resultListeners = new TierTopicListeners();
    private final ReentrantReadWriteLock sendLock = new ReentrantReadWriteLock();
    private final Map<AbstractTierMetadata, CompletableFuture<AppendResult>> queuedRequests = new LinkedHashMap<>();
    private final AtomicLong deletedPartitions = new AtomicLong(0);  // temporary tracking for deleted partitions until we implement logic to delete them

    private volatile Consumer<byte[], byte[]> primaryConsumer;
    private volatile Producer<byte[], byte[]> producer;
    private volatile boolean pendingImmigrations = false;

    private KafkaThread committerThread;
    private KafkaThread managerThread;
    private TierCatchupConsumer catchupConsumer;

    /**
     * Instantiate TierTopicManager. Once created, startup() must be called in order to start normal operation.
     *
     * @param config tier topic manager configurations
     * @param primaryConsumerSupplier supplier for primary consumer instances
     * @param catchupConsumerSupplier supplier for catchup consumer instances
     * @param producerSupplier supplier for producer instances
     * @param adminZkClientSupplier supplier for admin zk client
     * @param bootstrapServersSupplier supplier for bootstrap server
     * @param tierMetadataManager TierMetadataManager instance
     * @param logDirFailureChannel log dir failure channel instance
     */
    public TierTopicManager(TierTopicManagerConfig config,
                            Supplier<Consumer<byte[], byte[]>> primaryConsumerSupplier,
                            Supplier<Consumer<byte[], byte[]>> catchupConsumerSupplier,
                            Supplier<Producer<byte[], byte[]>> producerSupplier,
                            Supplier<AdminZkClient> adminZkClientSupplier,
                            Supplier<String> bootstrapServersSupplier,
                            TierMetadataManager tierMetadataManager,
                            LogDirFailureChannel logDirFailureChannel) {
        if (config.logDirs.size() > 1)
            throw new UnsupportedOperationException("Tiered storage does not support multiple log directories");

        this.config = config;
        this.tierMetadataManager = tierMetadataManager;
        this.bootstrapServersSupplier = bootstrapServersSupplier;
        this.committer = new TierTopicManagerCommitter(config, tierMetadataManager, logDirFailureChannel, shutdownInitiated);

        this.tierTopic = new TierTopic(config.tierNamespace, adminZkClientSupplier);
        this.primaryConsumerSupplier = primaryConsumerSupplier;
        this.producerSupplier = producerSupplier;

        this.catchupConsumer = new TierCatchupConsumer(tierMetadataManager, catchupConsumerSupplier);

        tierMetadataManager.addListener(this.getClass(), new TierMetadataManager.ChangeListener() {
            @Override
            public void onBecomeLeader(TopicIdPartition topicIdPartition, int leaderEpoch) {
                synchronized (TierTopicManager.this) {
                    pendingImmigrations = true;
                }
            }

            @Override
            public void onBecomeFollower(TopicIdPartition topicIdPartition) {
                synchronized (TierTopicManager.this) {
                    pendingImmigrations = true;
                }
            }

            @Override
            public void onDelete(TopicIdPartition topicIdPartition) {
            }
        });
    }

    /**
     * Primary public constructor for TierTopicManager.
     *
     * @param tierMetadataManager TierMetadataManager instance
     * @param config TierTopicManagerConfig containing tiering configuration.
     * @param adminZkClientSupplier Supplier for admin zk client
     * @param logDirFailureChannel Log dir failure channel
     * @param metrics Kafka metrics to track TierTopicManager metrics
     */
    public TierTopicManager(TierMetadataManager tierMetadataManager,
                            TierTopicManagerConfig config,
                            Supplier<AdminZkClient> adminZkClientSupplier,
                            Supplier<String> bootstrapServersSupplier,
                            LogDirFailureChannel logDirFailureChannel,
                            Metrics metrics) {
        this(config,
                new TierTopicConsumerSupplier(config, "primary"),
                new TierTopicConsumerSupplier(config, "catchup"),
                new TierTopicProducerSupplier(config),
                adminZkClientSupplier,
                bootstrapServersSupplier,
                tierMetadataManager,
                logDirFailureChannel);

        setupMetrics(metrics);
    }

    /**
     * Startup the tier topic manager.
     */
    public void startup() {
        managerThread = new KafkaThread("TierTopicManager", this, false);
        managerThread.start();
        committerThread = new KafkaThread("TierTopicManagerCommitter", committer, false);
        committerThread.start();
    }

    /**
     * Shutdown the tier topic manager.
     */
    public void shutdown() {
        shutdown.set(true);
        tierMetadataManager.removeListener(this.getClass());
        if (primaryConsumer != null)
            primaryConsumer.wakeup();
        catchupConsumer.wakeup();

        try {
            if (managerThread != null && managerThread.isAlive()) { // if the manager thread never
                // started, there's nothing
                shutdownInitiated.await(); // to await.
            }
            if (committerThread != null && committerThread.isAlive()) {
                committer.shutdown();
                committerThread.join();
            }
        } catch (InterruptedException e) {
            log.debug("Ignoring exception caught during shutdown", e);
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
     * Return the TierPartitionState for a given topic partition.
     *
     * @param topicIdPartition tiered topic partition
     * @return TierPartitionState for this partition.
     */
    @Override
    public TierPartitionState partitionState(TopicIdPartition topicIdPartition) {
        return tierMetadataManager.tierPartitionState(topicIdPartition)
                .orElseThrow(() -> new IllegalStateException("Tier partition state for " + topicIdPartition + " not found"));
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
        return ready.get();
    }

    /**
     * tier topic manager work loop
     */
    @Override
    public void run() {
        try {
            while (!ready.get() && !shutdown.get()) {
                if (!tryBecomeReady()) {
                    log.warn("Failed to become ready. Retrying in {}ms", TOPIC_CREATION_BACKOFF_MS);
                    Thread.sleep(TOPIC_CREATION_BACKOFF_MS);
                }
            }
            while (!shutdown.get()) {
                doWork();
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!shutdown.get()) {
                throw e;
            }
        } catch (Exception e) {
            log.error("Unrecoverable exception in TierTopicManager", e);
        } finally {
            cleanup();
        }
    }

    /**
     * Work cycle
     * public for testing purposes.
     */
    public boolean doWork() throws TierMetadataDeserializationException {
        catchupConsumer.tryComplete(primaryConsumer);
        processPendingImmigrations();

        boolean primaryProcessed = processRecords(primaryConsumer.poll(config.pollDuration), TierPartitionStatus.ONLINE, true);
        boolean catchUpProcessed = processRecords(catchupConsumer.poll(config.pollDuration), TierPartitionStatus.CATCHUP, false);

        heartbeat.set(System.currentTimeMillis());

        return primaryProcessed || catchUpProcessed;
    }

    /**
     * Setup backing consumers and producer
     */
    private void startProduceConsume() {
        Set<TopicPartition> tierTopicPartitions = partitions(tierTopic.topicName(), tierTopic.numPartitions().getAsInt());

        // startup the primary consumer
        primaryConsumer = primaryConsumerSupplier.get();
        primaryConsumer.assign(tierTopicPartitions);
        for (TopicPartition partition : tierTopicPartitions) {
            Long position = committer.positions().get(partition.partition());
            if (position != null) {
                log.info("seeking primary consumer to committed offset {} for partition {}", position, partition);
                primaryConsumer.seek(partition, position);
            } else {
                log.info("primary consumer missing committed offset for partition {}. Seeking to beginning", partition);
                primaryConsumer.seekToBeginning(Collections.singletonList(partition));
            }
        }

        // startup the catchup consumer if needed
        Set<TierPartitionState> partitionsToCatchup = collectPartitionsWithStatus(
                new HashSet<>(Arrays.asList(TierPartitionStatus.INIT, TierPartitionStatus.CATCHUP))
        );
        catchupConsumer.maybeStartConsumer(tierTopic, partitionsToCatchup);

        // startup the producer
        producer = producerSupplier.get();

        // Now that the producers and consumers have been setup, we are ready to accept requests. Reprocess any queued
        // metadata requests.
        synchronized (this) {
            ready.set(true);

            for (Map.Entry<AbstractTierMetadata, CompletableFuture<AppendResult>> entry : queuedRequests.entrySet())
                addMetadata(entry.getKey(), entry.getValue());
            queuedRequests.clear();
        }
    }

    // visible for testing
    public long trackedDeletedPartitions() {
        return deletedPartitions.get();
    }

    // visible for testing
    public boolean catchingUp() {
        return catchupConsumer.active();
    }

    // visible for testing
    TierTopic tierTopic() {
        return tierTopic;
    }

    private void cleanup() {
        sendLock.writeLock().lock();
        try {
            if (cleaned.compareAndSet(false, true)) {
                ready.set(false);
                if (primaryConsumer != null)
                    primaryConsumer.close();
                catchupConsumer.close();

                if (producer != null)
                    producer.close(Duration.ofSeconds(1));

                committer.shutdown();
                for (CompletableFuture<AppendResult> future : queuedRequests.values())
                    future.completeExceptionally(new TierMetadataFatalException("Tier topic manager shutting down"));
                queuedRequests.clear();
                resultListeners.shutdown();

                shutdownInitiated.countDown();
            }
        } finally {
            sendLock.writeLock().unlock();
        }
    }

    /**
     * Process any pending immigrations, if they have occurred.
     *
     * If the catch up consumer is stopped, and partitions have been immigrated, check whether any partitions are in the
     * INIT state, and if so transition them to CATCHUP and start the catch up consumer.
     */
    private void processPendingImmigrations() {
        if (!catchupConsumer.active() && pendingImmigrations) {
            Set<TierPartitionState> immigratedPartitionStates;

            synchronized (this) {
                immigratedPartitionStates = collectPartitionsWithStatus(Collections.singleton(TierPartitionStatus.INIT));
                pendingImmigrations = false;
            }

            if (!immigratedPartitionStates.isEmpty())
                catchupConsumer.doStart(tierTopic, immigratedPartitionStates);
        }
    }

    // package-private for testing
    long numResultListeners() {
        return resultListeners.numListeners();
    }

    private Set<TierPartitionState> collectPartitionsWithStatus(Set<TierPartitionStatus> transitionStatuses) {
        Set<TierPartitionState> tierPartitionStates = new HashSet<>();
        tierMetadataManager.tierEnabledPartitionStateIterator().forEachRemaining(tps -> {
            if (transitionStatuses.contains(tps.status()))
                tierPartitionStates.add(tps);
        });
        return tierPartitionStates;
    }

    TierTopicManagerCommitter committer() {
        return committer;
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
     * Poll a consumer, materializing Tier Topic entries to TierPartition state.
     *
     * @param records consumed records to process
     * @param requiredState the tier partition must be in this state or else the metadata will be ignored.
     * @param commitPositions boolean denoting whether to send the consumer positions to the
     *                        committer. Only the primary consumer should commit offsets.
     * @return boolean denoting whether messages were processed
     */
    private boolean processRecords(ConsumerRecords<byte[], byte[]> records,
                                   TierPartitionStatus requiredState,
                                   boolean commitPositions) {
        if (records == null)
            return false;

        boolean processedMessages = false;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            final Optional<AbstractTierMetadata> entryOpt = AbstractTierMetadata.deserialize(record.key(), record.value());
            if (entryOpt.isPresent()) {
                AbstractTierMetadata entry = entryOpt.get();
                log.trace("Read {} at offset {} of partition {}", entry, record.offset(), record.partition());
                processEntry(entry, record.partition(), record.offset(), requiredState);

                if (commitPositions)
                    committer.updatePosition(record.partition(), record.offset() + 1);

                processedMessages = true;
            } else {
                log.info("Skipping message at offset {} of partition {}. Message for {} "
                        + "and type: {} cannot be deserialized.",
                        record.offset(),
                        record.partition(),
                        AbstractTierMetadata.deserializeKey(record.key()),
                        AbstractTierMetadata.getTypeId(record.value()));
            }
        }
        return processedMessages;
    }

    private void addMetadata(AbstractTierMetadata metadata, CompletableFuture<AppendResult> future) {
        sendLock.readLock().lock();
        try {
            if (cleaned.get())
                throw new IllegalStateException("TierTopicManager thread has exited. Cannot add "
                        + "metadata");

            synchronized (this) {
                if (!ready.get()) {
                    queuedRequests.put(metadata, future);
                    return;
                }
            }

            TopicIdPartition topicPartition = metadata.topicIdPartition();

            // track this entry's materialization
            resultListeners.addTracked(metadata, future);

            // identify the tier topic partition and produce the message
            TopicPartition tierTopicPartition = tierTopic.toTierTopicPartition(topicPartition);
            producer.send(new ProducerRecord<>(tierTopicPartition.topic(),
                            tierTopicPartition.partition(),
                            metadata.serializeKey(),
                            metadata.serializeValue()),
                    (recordMetadata, exception) -> {
                        if (exception != null) {
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
                            resultListeners.getAndRemoveTracked(metadata);
                        }
                    });
        } finally {
            sendLock.readLock().unlock();
        }
    }

    /**
     * Try to move the TierTopicManager to ready state. This will first try to create the tier state
     * topic if it has not been created yet, and check that the topic has the expected number of
     * partitions. It will then call startProduceConsume, which will setup tier topic producer and consumers.
     * @return boolean for whether TierTopicManager moved to ready state
     *
     * visible for testing
     */
    public boolean tryBecomeReady() {
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
        startProduceConsume();
        return true;
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
     * Materialize a tier topic entry into the corresponding tier partition status.
     * @param entry The tier topic entry read from the tier topic.
     * @param partition source partition for this metadata entry
     * @param offset source offset for this metadata entry
     * @param requiredState TierPartitionState must be in this status in order to modify it; otherwise the entry will be ignored.
     */
    private void processEntry(AbstractTierMetadata entry,
                              int partition,
                              long offset,
                              TierPartitionStatus requiredState) throws TierMetadataFatalException {
        final TopicIdPartition tpid = entry.topicIdPartition();
        try {
            if (entry.type() == TierRecordType.PartitionDeleteInitiate) {
                deletedPartitions.incrementAndGet();
                resultListeners.getAndRemoveTracked(entry).ifPresent(c -> c.complete(AppendResult.ACCEPTED));
            } else {
                final Optional<TierPartitionState> tierPartitionStateOpt = tierMetadataManager.tierPartitionState(tpid);
                if (tierPartitionStateOpt.isPresent()) {
                    TierPartitionState tierPartitionState = tierPartitionStateOpt.get();
                    TierPartitionStatus currentState = tierPartitionState.status();

                    switch (currentState) {
                        case DISK_OFFLINE:
                            resultListeners
                                    .getAndRemoveTracked(entry)
                                    .ifPresent(c -> c.completeExceptionally(new TierMetadataFatalException("Partition " + tpid + " is offline")));
                            break;

                        default:
                            if (currentState == requiredState) {
                                AppendResult result = tierPartitionState.append(entry);
                                resultListeners.getAndRemoveTracked(entry).ifPresent(c -> c.complete(result));
                            } else {
                                // We partition the materialization between the primary and catchup consumer based on the
                                // current state of the tier partition. Primary consumer can materialize metadata for
                                // ONLINE partitions; catchup consumer can materialize metadata for CATCHUP partitions.
                                // If we fall in this case, it means that the current state does not match the consumer
                                // we are processing this record for. For example,
                                //
                                // 1. The primary consumer is processing message for a partition in CATCHUP state. The
                                //    catchup consumer will eventually read this message and complete the listener.
                                // 2. The catchup consumer is processing message for a partition in ONLINE state. This
                                //    message may already have been materialized or not, depending on where the primary
                                //    consumer is relative to the catchup consumer. In either case, the primary consumer
                                //    would have or will materialize this message.
                                // 3. The partition was reassigned back to this broker and is now in INIT state. We will
                                //    wait for the catchup consumer to be restarted and reconsume this message.
                                //
                                // In all cases, we have an implicit guarantee that one of the two consumers will consume
                                // this message and find the tier partition either in the right state or to be deleted,
                                // which will trigger materialization and completion of listener.
                                log.debug("Ignoring metadata {}. currentState: {} requiredState: {}", entry, currentState, requiredState);
                            }
                            break;
                    }
                } else {
                    resultListeners.getAndRemoveTracked(entry).ifPresent(c -> c.completeExceptionally(
                            new TierMetadataRetriableException("Tier partition state for " + tpid + " does not exist")));
                }
            }
        } catch (Exception e) {
            throw new TierMetadataFatalException(String.format("Error processing "
                    + "message %s at offset %d, partition %d", entry, offset, partition), e);
        }
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
