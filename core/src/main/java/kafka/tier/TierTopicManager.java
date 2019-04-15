/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.tier.client.ConsumerBuilder;
import kafka.tier.client.ProducerBuilder;
import kafka.tier.client.TierTopicConsumerBuilder;
import kafka.tier.client.TierTopicProducerBuilder;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import kafka.tier.exceptions.TierMetadataDeserializationException;
import kafka.tier.exceptions.TierMetadataFatalException;
import kafka.tier.exceptions.TierMetadataRetriableException;
import kafka.tier.state.TierPartitionState;
import kafka.tier.state.TierPartitionState.AppendResult;
import kafka.tier.state.TierPartitionStatus;
import kafka.tier.topic.TierTopicAdmin;
import kafka.tier.topic.TierTopicPartitioner;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
public class TierTopicManager implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TierTopicManager.class);
    private static final int TOPIC_CREATION_BACKOFF_MS = 5000;
    private final String topicName;
    private final TierTopicManagerConfig config;
    private final TierMetadataManager tierMetadataManager;
    private final Supplier<String> bootstrapServersSupplier;
    private final TierTopicListeners resultListeners = new TierTopicListeners();
    private final TierTopicManagerCommitter committer;
    private final ConcurrentLinkedQueue<MigrationEntry> migrations = new ConcurrentLinkedQueue<>();
    private final TierTopicConsumerBuilder consumerBuilder;
    private final TierTopicProducerBuilder producerBuilder;
    private final AtomicLong heartbeat = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private TierTopicPartitioner partitioner;
    private final CountDownLatch shutdownInitiated = new CountDownLatch(2);
    private Consumer<byte[], byte[]> primaryConsumer;
    private Consumer<byte[], byte[]> catchUpConsumer;
    private Producer<byte[], byte[]> producer;
    private volatile boolean ready = false;
    private KafkaThread committerThread;
    private KafkaThread managerThread;

    /**
     * Instantiate TierTopicManager. Once created, startup() must be called in order to start normal operation.
     *
     * @param config TierTopicManagerConfig containing tiering configuration.
     * @param consumerBuilder builder to create consumer instances.
     * @param producerBuilder producer to create producer instances.
     * @param tierMetadataManager Tier Metadata Manager instance
     * @throws IOException on logdir write failures
     */
    public TierTopicManager(TierTopicManagerConfig config,
                            TierTopicConsumerBuilder consumerBuilder,
                            TierTopicProducerBuilder producerBuilder,
                            Supplier<String> bootstrapServersSupplier,
                            TierMetadataManager tierMetadataManager) throws IOException {
        this.config = config;
        this.topicName = topicName(config.tierNamespace);
        this.tierMetadataManager = tierMetadataManager;
        this.bootstrapServersSupplier = bootstrapServersSupplier;
        this.committer = new TierTopicManagerCommitter(config, tierMetadataManager, shutdownInitiated);
        if (config.logDirs.size() > 1) {
            throw new UnsupportedOperationException("Multiple log.dirs detected. Tiered "
                    + "storage currently supports single logdir configuration.");
        }
        this.consumerBuilder = consumerBuilder;
        this.producerBuilder = producerBuilder;
        tierMetadataManager.addListener(new TierMetadataManager.ChangeListener() {
            @Override
            public void onBecomeLeader(TopicPartition topicPartition, int leaderEpoch) {
                immigratePartitions(Collections.singletonList(topicPartition));
            }

            @Override
            public void onBecomeFollower(TopicPartition topicPartition) {
                immigratePartitions(Collections.singletonList(topicPartition));
            }

            @Override
            public void onDelete(TopicPartition topicPartition) {
                emigratePartitions(Collections.singletonList(topicPartition));
            }
        });
    }

    /**
     * Primary public constructor for TierTopicManager.
     *
     * @param tierMetadataManager Tier Metadata Manager instance
     * @param config TierTopicManagerConfig containing tiering configuration.
     * @param metrics kafka metrics to track TierTopicManager metrics
     */
    public TierTopicManager(TierMetadataManager tierMetadataManager,
                            TierTopicManagerConfig config,
                            Supplier<String> bootstrapServersSupplier,
                            Metrics metrics) throws IOException {
        this(config,
                new ConsumerBuilder(config),
                new ProducerBuilder(config),
                bootstrapServersSupplier,
                tierMetadataManager);
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
        primaryConsumer.wakeup();
        if (catchUpConsumer != null)
            catchUpConsumer.wakeup();
        producer.close(Duration.ofSeconds(1));
        try {
            if (managerThread != null && managerThread.isAlive()) { // if the manager thread never
                // started, there's nothing
                shutdownInitiated.await(); // to await.
            }
        } catch (InterruptedException ie) {
            log.debug("shutdownInitiated latch count reached zero. Shutdown called.");
        }
    }

    /**
     * Generate the tier topic name, namespaced if tierNamespace is non-empty.
     *
     * @param tierNamespace Tier Topic namespace for placing tier topic on external cluster.
     * @return The topic name.
     */
    public static String topicName(String tierNamespace) {
        return tierNamespace != null && !tierNamespace.isEmpty()
                ? String.format("%s_%s", Topic.TIER_TOPIC_NAME, tierNamespace)
                : Topic.TIER_TOPIC_NAME;
    }

    /**
     * Write an AbstractTierMetadata to the Tier Topic, returning a
     * CompletableFuture that tracks the result of the materialization after the
     * message has been read from the tier topic, allowing the sender to determine
     * whether the write was fenced, or the send failed.
     *
     * @param entry the tier topic entry to be written to the tier topic.
     * @return a CompletableFuture which returns the result of the send and subsequent materialization.
     */
    public CompletableFuture<AppendResult> addMetadata(AbstractTierMetadata entry) throws IllegalAccessException {
        ensureReady();

        final TopicPartition tp = entry.topicPartition();
        // track this entry's materialization
        final CompletableFuture<AppendResult> result = resultListeners.addTracked(tp, entry);
        producer.send(new ProducerRecord<>(topicName, partitioner.partitionId(tp),
                        entry.serializeKey(),
                        entry.serializeValue()),
                (recordMetadata, exception) -> {
                    if (exception != null) {
                        if (retriable(exception)) {
                            result.completeExceptionally(
                                    new TierMetadataRetriableException(
                                            "Retriable exception sending tier metadata.",
                                            exception));
                        } else {
                            result.completeExceptionally(
                                    new TierMetadataFatalException(
                                            "Fatal exception sending tier metadata.",
                                            exception));
                        }
                        resultListeners.getAndRemoveTracked(tp, entry);
                    }
                });
        return result;
    }

    /**
     * Return the TierPartitionState for a given topic partition.
     *
     * @param topicPartition tiered topic partition
     * @return TierPartitionState for this partition.
     */
    public TierPartitionState partitionState(TopicPartition topicPartition) {
        TierPartitionState tierPartitionState = tierMetadataManager.tierPartitionState(topicPartition)
                .orElseThrow(() -> new IllegalStateException("Tier partition state for " + topicPartition + " not found"));
        return tierPartitionState;
    }

    /**
     * Performs a write to the tier topic to attempt to become leader for the tiered topic partition.
     *
     * @param topicPartition the topic partition for which the sender wishes to become the archive leader.
     * @param tierEpoch      the archiver epoch
     * @return a CompletableFuture which returns the result of the send and subsequent materialization.
     */
    public CompletableFuture<AppendResult> becomeArchiver(TopicPartition topicPartition,
                                                          int tierEpoch) throws IllegalAccessException {
        ensureReady();
        // Generate a unique ID in order to track the leader request under scenarios
        // where we maintain the same leader ID.
        // This is possible when there is a single broker, and is primarily for defensive reasons.
        final UUID messageId = UUID.randomUUID();
        final TierTopicInitLeader initRecord = new TierTopicInitLeader(topicPartition, tierEpoch, messageId, config.brokerId);
        return addMetadata(initRecord);
    }

    /**
     * Return whether TierTopicManager is ready to accept writes.
     *
     * @return boolean
     */
    public boolean isReady() {
        return ready;
    }

    /**
     * tier topic manager work loop
     */
    public void run() {
        try {
            while (!ready && !shutdown.get()) {
                String bootstrapServers = this.bootstrapServersSupplier.get();
                if (bootstrapServers.isEmpty()) {
                    log.warn("Failed to lookup bootstrap servers. Retrying in {}", TOPIC_CREATION_BACKOFF_MS);
                    Thread.sleep(TOPIC_CREATION_BACKOFF_MS);
                } else if (TierTopicAdmin.ensureTopicCreated(bootstrapServers, topicName,
                        config.numPartitions, config.replicationFactor)) {
                    becomeReady(bootstrapServers);
                    final int producerPartitions = producer.partitionsFor(topicName).size();
                    if (producerPartitions != config.numPartitions) {
                        log.error("Number of partitions {} on tier topic: {} " +
                                        "does not match the number of partitions configured {}.",
                                producerPartitions, topicName, config.numPartitions);
                        Exit.exit(1);
                    }
                } else {
                    log.warn("Failed to ensure tier topic has been created. Retrying in {}",
                            TOPIC_CREATION_BACKOFF_MS);
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
        } catch (IOException io) {
            log.error("Unrecoverable IOException in TierTopicManager", io);
            Exit.exit(1);
        } catch (AuthenticationException | AuthorizationException e) {
            log.error("Unrecoverable authentication or authorization issue in TierTopicManager", e);
            Exit.exit(1);
        } catch (KafkaException | IllegalStateException e) {
            log.error("Unrecoverable error in work cycle", e);
            Exit.exit(1);
        } catch (InterruptedException ie) {
            log.error("Topic manager interrupted", ie);
            Exit.exit(1);
        } catch (TierMetadataDeserializationException de) {
            log.error("Tier topic: deserialization error encountered materializing tier topic.",
                    de);
            Exit.exit(1);
        } finally {
            if (primaryConsumer != null)
                primaryConsumer.close();
            if (catchUpConsumer != null)
                catchUpConsumer.close();
            committer.shutdown();
            shutdownInitiated.countDown();
        }
    }

    /**
     * @return boolean denoting whether catch up consumer is currently materializing the tier topic.
     */
    public boolean catchingUp() {
        return catchUpConsumer != null;
    }

    /**
     * Adds partitions to the migration queue to be immigrated.
     * @param partitions
     */
    private void immigratePartitions(List<TopicPartition> partitions) {
        for (TopicPartition tp : partitions)
            migrations.add(new MigrationEntry(tp, MigrationEntry.Type.IMMIGRATION));
    }

    /**
     * Adds partitions to the migration queue to be emigrated.
     * @param partitions
     */
    private void emigratePartitions(List<TopicPartition> partitions) {
        for (TopicPartition tp : partitions)
            migrations.add(new MigrationEntry(tp, MigrationEntry.Type.EMIGRATION));
    }

    /**
     * work cycle
     */
    // public for testing
    public boolean doWork() throws TierMetadataDeserializationException, IOException {
        processMigrations();
        checkCatchingUpComplete();
        final boolean primaryProcessed = pollConsumer(primaryConsumer, TierPartitionStatus.ONLINE);
        final boolean catchUpProcessed = catchUpConsumer != null
                && pollConsumer(catchUpConsumer, TierPartitionStatus.CATCHUP);

        heartbeat.set(System.currentTimeMillis());
        return primaryProcessed || catchUpProcessed;
    }

    /**
     * Ensure tier topic has been created and setup the backing consumer
     * and producer before signalling ready.
     */
    // pubic for testing
    public void becomeReady(String boostrapServers) {
        primaryConsumer = consumerBuilder.setupConsumer(boostrapServers, committer, topicName, "primary");
        primaryConsumer.assign(partitions());
        for (Map.Entry<Integer, Long> entry : committer.positions().entrySet()) {
            primaryConsumer.seek(new TopicPartition(topicName, entry.getKey()), entry.getValue());
        }

        producer = producerBuilder.setupProducer(boostrapServers);
        partitioner = new TierTopicPartitioner(config.numPartitions);
        ready = true;
    }


    TierTopicManagerCommitter committer() {
        return committer;
    }

    /**
     * @return All of the partitions for the Tier Topic
     */
    private Collection<TopicPartition> partitions() {
        return IntStream
                .range(0, config.numPartitions)
                .mapToObj(part -> new TopicPartition(topicName, part))
                .collect(Collectors.toList());
    }

    /**
     * Generate the tier topic partitions containing data for tiered partitions.
     *
     * @param tieredPartitions partitions that have been tiered
     * @return The partitions on the Tier Topic containing data for tieredPartitions
     */
    private Collection<TopicPartition> requiredPartitions(Collection<TopicPartition> tieredPartitions) {
        return tieredPartitions
                .stream()
                .map(tp -> new TopicPartition(topicName, partitioner.partitionId(tp)))
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Computes the offset distance between the positions of two consumers
     *
     * @return Optional distance, Optional.empty if no partitions are assigned to catch up consumer.
     */
    private Long catchUpConsumerLag() {
        Set<TopicPartition> catchUpAssignment = catchUpConsumer.assignment();
        return primaryConsumer
                .assignment()
                .stream()
                .filter(catchUpAssignment::contains)
                .map(tp -> Math.max(0, primaryConsumer.position(tp) - catchUpConsumer.position(tp)))
                .reduce(Long::sum)
                .orElse(0L);
    }

    /**
     * Checks whether catch up consumer has caught up to primary consumer.
     * If caught up, shuts down the catch up consumer.
     */
    private void checkCatchingUpComplete() {
        if (catchingUp() && catchUpConsumerLag() == 0) {
            completeCatchUp();
        }
    }

    /**
     * When all tier partition states have caught up, switch consumers.
     */
    private void completeCatchUp() {
        log.info("Completed adding partitions. Setting states online. Switching catchup consumer to primary consumer.");
        tierMetadataManager.tierEnabledPartitionStateIterator().forEachRemaining(tierPartitionState -> {
            if (tierPartitionState.status() == TierPartitionStatus.CATCHUP)
                tierPartitionState.onCatchUpComplete();
        });
        catchUpConsumer.close();
        catchUpConsumer = null;
    }

    /**
     * Drains the migration queue of entries if a catch up consumer is not already materializing.
     * For immigrating partitions, sets TierPartition state to CATCHUP
     * and instantiates the catch up consumer, assigning it the Tier Topic partitions
     * containing data for the immigrating partitions.
     */
    private void processMigrations() {
        if (!catchingUp() && !migrations.isEmpty()) {
            HashSet<TopicPartition> migrationCandidates = pollMigrations();
            HashSet<TopicPartition> transitioned = new HashSet<>();
            if (!migrationCandidates.isEmpty()) {
                for (TopicPartition tp : migrationCandidates) {
                    log.debug("Adding {} to catchingUp partition states.", tp);
                    Optional<TierMetadataManager.PartitionMetadata> partitionMetadataOpt = tierMetadataManager.tierPartitionMetadata(tp);
                    if (!partitionMetadataOpt.isPresent())
                        continue;
                    TierMetadataManager.PartitionMetadata partitionMetadata = partitionMetadataOpt.get();
                    TierPartitionState tierPartitionState = partitionMetadata.tierPartitionState();

                    switch (tierPartitionState.status()) {
                        case READ_ONLY:
                            if (partitionMetadata.tieringEnabled())
                                tierPartitionState.beginCatchup();
                            transitioned.add(tp);
                            break;

                        case CLOSED:
                            throw new IllegalStateException("Partition " + tp + " in invalid state during migration");

                        default:
                            log.debug("Ignoring migration of {} in state {}", tp, tierPartitionState.status());
                    }
                }

                if (!transitioned.isEmpty()) {
                    catchUpConsumer = consumerBuilder.setupConsumer(bootstrapServersSupplier.get(),
                            committer, topicName,
                            "catchup");
                    catchUpConsumer.assign(requiredPartitions(transitioned));

                    log.info("Seeking consumer to beginning.");

                    // TODO: upon adding snapshot support, we should seek to the earliest point
                    // required to restore all required snapshots
                    catchUpConsumer.seekToBeginning(catchUpConsumer.assignment());
                }
            }
        }
    }

    /**
     * polls the migration queue, removing emigrated partitions
     * and returning a set of the added partitions.
     *
     * @return HashSet containing immigrated partitions.
     */
    private HashSet<TopicPartition> pollMigrations() {
        HashSet<TopicPartition> added = new HashSet<>();
        while (!migrations.isEmpty()) {
            MigrationEntry entry = migrations.poll();
            switch (entry.type) {
                case IMMIGRATION:
                    added.add(entry.topicPartition);
                    break;
                case EMIGRATION:
                    // there is no need to catch up to a partition
                    // that has been emigrated prior to being processed
                    added.remove(entry.topicPartition);
                    resultListeners.remove(entry.topicPartition);
                    break;
            }
        }
        return added;
    }


    /**
     * Poll a consumer, materializing Tier Topic entries to TierPartition state.
     *
     * @param consumer      the consumer to poll
     * @param requiredState The TierPartition must be in this state or else the metadata will be ignored.
     * @return boolean for whether any messages were processed
     * @throws IOException if error occurred writing to pier partition state/logdir.
     */
    private boolean pollConsumer(Consumer<byte[], byte[]> consumer,
                                 TierPartitionStatus requiredState) throws IOException {
        boolean processedMessages = false;
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(config.pollDuration)) {
            final Optional<AbstractTierMetadata> entry =
                    AbstractTierMetadata.deserialize(record.key(), record.value());
            if (entry.isPresent()) {
                processEntry(entry.get(), requiredState);
                committer.updatePosition(record.partition(), record.offset() + 1);
                processedMessages = true;
            }
        }
        return processedMessages;
    }

    /**
     * Sanity check to ensure TierTopicManager is ready before performing operations.
     *
     * @throws IllegalAccessException
     */
    private void ensureReady() throws IllegalAccessException {
        if (!ready) {
            throw new IllegalAccessException("Tier Topic manager is not ready.");
        }
    }

    /**
     * Setup metrics for the tier topic manager.
     */
    private void setupMetrics(Metrics metrics) {
        metrics.addMetric(new MetricName("HeartbeatMs",
                        "TierTopicManager",
                        "Time since last heartbeat in milliseconds.",
                        new java.util.HashMap<>()),
                (MetricConfig config, long now) -> now - heartbeat.get());
    }

    /**
     * Materialize a tier topic entry into the corresponding tier partition status.
     *
     * @param entry         the tier topic entry read from the tier topic.
     * @param requiredState TierPartitionState must be in this status in order to modify it.
     *                      Otherwise the entry will be ignored.
     */
    private void processEntry(AbstractTierMetadata entry, TierPartitionStatus requiredState) throws IOException {
        final TopicPartition tp = entry.topicPartition();
        final Optional<TierPartitionState> tierPartitionStateOpt = tierMetadataManager.tierPartitionState(tp);
        if (!tierPartitionStateOpt.isPresent())
            return;

        TierPartitionState tierPartitionState = tierPartitionStateOpt.get();
        if (tierPartitionState.status() == requiredState) {
            final AppendResult result = tierPartitionState.append(entry);
            log.debug("Read entry {}, append result {}", entry, result);
            // signal completion of this tier topic entry if this topic manager was the sender
            resultListeners.getAndRemoveTracked(tp, entry)
                    .ifPresent(c -> c.complete(result));
        } else {
            log.debug("TierPartitionState {} not in required state {}. Ignoring metadata {}.",
                    tp, requiredState, entry);
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

    private static class MigrationEntry {
        public enum Type {
            // supplied tier partition has been added to broker
            IMMIGRATION,
            // supplied tier partition has been removed from the broker
            EMIGRATION
        }

        public final TopicPartition topicPartition;
        public final Type type;

        MigrationEntry(TopicPartition topicPartition, Type type) {
            this.topicPartition = topicPartition;
            this.type = type;
        }
    }

    /**
     * Class to track outstanding requests and signal back to the TierTopicManager
     * user when their metadata requests have been read and materialized.
     */
    private static class TierTopicListeners {
        private final ConcurrentHashMap<TopicPartition, Entry> results = new ConcurrentHashMap<>();

        /**
         * Checks whether a given tier index entry is being tracked. If so,
         * returns a CompletableFuture to be completed to signal back to the sender.
         *
         * @param tp    tiered topic partition
         * @param entry tier index topic entry we are trying to complete
         * @return CompletableFuture for this index entry if one exists.
         */
        Optional<CompletableFuture<AppendResult>>
        getAndRemoveTracked(TopicPartition tp, AbstractTierMetadata entry) {
            final Entry complete = results.get(tp);
            if (complete != null && complete.key.equals(listenerKey(entry))) {
                results.remove(tp, complete);
                return Optional.of(complete.future);
            }
            return Optional.empty();
        }

        /**
         * Track a tier topic index entry's materialization into the tier topic.
         * If an index entry is already being tracked, then we exceptionally
         * complete the existing future before adding the new entry and future.
         *
         * @param tp    tiered topic partition
         * @param entry tier index topic entry to track materialization of.
         * @return future that will be completed when the entry has been materialized.
         */
        CompletableFuture<AppendResult> addTracked(TopicPartition tp,
                                                   AbstractTierMetadata entry) {
            final CompletableFuture<AppendResult> result = new CompletableFuture<>();
            final Entry complete = new Entry(listenerKey(entry), result);
            final Entry found = results.get(tp);
            if (found != null) {
                found.future.completeExceptionally(
                        new TierMetadataFatalException(
                                "A new index entry is being tracked for this topic partition"
                                        + ", obsoleting this request."));
            }
            results.put(tp, complete);
            return result;
        }

        /**
         * Stop tracking this partition after partition emigration
         *
         * @param tp topic partition.
         */
        void remove(TopicPartition tp) {
            final Entry found = results.get(tp);
            if (found != null) {
                found.future.completeExceptionally(new TierMetadataFatalException("TierPartitionState has"
                        + " been immigrated by the topic manager."));
                results.remove(tp, found);
            }
        }

        private static class Entry {
            public final TierMetadataListener key;
            public final CompletableFuture<AppendResult> future;

            Entry(TierMetadataListener key, CompletableFuture<AppendResult> future) {
                this.key = key;
                this.future = future;
            }
        }

        /**
         * Select a subset of the data in the tier index entry for use in tracking
         * the result of materialization. Reduces memory consumption vs tracking the entire
         * index entry.
         *
         * @return The key.
         */
        TierMetadataListener listenerKey(AbstractTierMetadata message) {
            if (message instanceof TierObjectMetadata) {
                TierObjectMetadata metadata = (TierObjectMetadata) message;
                return new TierObjectMetadataListener(metadata.topicPartition(),
                        metadata.tierEpoch(), metadata.startOffset(),
                        metadata.endOffsetDelta());
            } else if (message instanceof TierTopicInitLeader) {
                TierTopicInitLeader initLeader = (TierTopicInitLeader) message;
                return new TierInitLeaderListener(initLeader.messageId());
            } else {
                throw new IllegalArgumentException(
                        "Tier topic message type unsupported in metadata listener "
                                + message.getClass().getName());
            }
        }

        interface TierMetadataListener {
        }

        class TierObjectMetadataListener implements TierMetadataListener {
            private final TopicPartition topicPartition;
            private final int tierEpoch;
            private final long startOffset;
            private final int endOffsetDelta;

            TierObjectMetadataListener(TopicPartition topicPartition, int tierEpoch, long startOffset, int endOffsetDelta) {
                this.topicPartition = topicPartition;
                this.tierEpoch = tierEpoch;
                this.startOffset = startOffset;
                this.endOffsetDelta = endOffsetDelta;
            }

            public int hashCode() {
                return Objects.hash(topicPartition, tierEpoch, startOffset, endOffsetDelta);
            }

            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                TierObjectMetadataListener that = (TierObjectMetadataListener) o;
                return Objects.equals(topicPartition, that.topicPartition)
                        && Objects.equals(tierEpoch, that.tierEpoch)
                        && Objects.equals(startOffset, that.startOffset)
                        && Objects.equals(endOffsetDelta, that.endOffsetDelta);
            }
        }

        class TierInitLeaderListener implements TierMetadataListener {
            final private UUID messageId;

            TierInitLeaderListener(UUID messageId) {
                this.messageId = messageId;
            }

            public int hashCode() {
                return Objects.hash(messageId);
            }

            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }

                if (o == null || getClass() != o.getClass()) {
                    return false;
                }

                TierInitLeaderListener that = (TierInitLeaderListener) o;
                return Objects.equals(messageId, that.messageId);
            }
        }

    }
}
