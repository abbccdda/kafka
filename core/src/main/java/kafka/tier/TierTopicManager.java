/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.tier.client.ConsumerBuilder;
import kafka.tier.client.ProducerBuilder;
import kafka.tier.client.TierTopicConsumerBuilder;
import kafka.tier.client.TierTopicProducerBuilder;
import kafka.tier.domain.AbstractTierMetadata;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.time.Duration;
import java.util.LinkedHashMap;
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
public class TierTopicManager implements Runnable, TierTopicAppender {
    private static final Logger log = LoggerFactory.getLogger(TierTopicManager.class);
    private static final int TOPIC_CREATION_BACKOFF_MS = 5000;

    private final String topicName;
    private final TierTopicManagerConfig config;
    private final TierMetadataManager tierMetadataManager;
    private final Supplier<String> bootstrapServersSupplier;
    private final TierTopicManagerCommitter committer;
    private final TierTopicConsumerBuilder consumerBuilder;
    private final TierTopicProducerBuilder producerBuilder;

    private final AtomicLong heartbeat = new AtomicLong(System.currentTimeMillis());
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final CountDownLatch shutdownInitiated = new CountDownLatch(2);
    private final TierTopicListeners resultListeners = new TierTopicListeners();
    private final Map<AbstractTierMetadata, CompletableFuture<AppendResult>> queuedRequests = new LinkedHashMap<>();
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private volatile Consumer<byte[], byte[]> primaryConsumer;
    private volatile Consumer<byte[], byte[]> catchUpConsumer;
    private volatile Producer<byte[], byte[]> producer;
    private volatile boolean partitionsImmigrated = false;
    private volatile boolean partitionsEmigrated = false;

    private TierTopicPartitioner partitioner;
    private KafkaThread committerThread;
    private KafkaThread managerThread;

    /**
     * Instantiate TierTopicManager. Once created, startup() must be called in order to start normal operation.
     *
     * @param config              TierTopicManagerConfig containing tiering configuration.
     * @param consumerBuilder     builder to create consumer instances.
     * @param producerBuilder     producer to create producer instances.
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
            public void onBecomeLeader(TopicIdPartition topicIdPartition, int leaderEpoch) {
                immigratePartitions(Collections.singletonList(topicIdPartition));
            }

            @Override
            public void onBecomeFollower(TopicIdPartition topicIdPartition) {
                immigratePartitions(Collections.singletonList(topicIdPartition));
            }

            @Override
            public void onDelete(TopicIdPartition topicIdPartition) {
                emigratePartitions(Collections.singletonList(topicIdPartition));
            }
        });
    }

    /**
     * Primary public constructor for TierTopicManager.
     *
     * @param tierMetadataManager Tier Metadata Manager instance
     * @param config              TierTopicManagerConfig containing tiering configuration.
     * @param metrics             kafka metrics to track TierTopicManager metrics
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
        if (primaryConsumer != null)
            primaryConsumer.wakeup();
        if (catchUpConsumer != null)
            catchUpConsumer.wakeup();
        if (producer != null)
            producer.close(Duration.ofSeconds(1));
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
     * Generate the tier topic name, namespaced if tierNamespace is non-empty.
     *
     * @param tierNamespace Tier Topic namespace for placing tier topic on external cluster.
     * @return The topic name.
     */
    public static String topicName(String tierNamespace) {
        return tierNamespace != null && !tierNamespace.isEmpty() ?
                String.format("%s-%s", Topic.TIER_TOPIC_NAME, tierNamespace) :
                Topic.TIER_TOPIC_NAME;
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
                    maybeStartCatchUpConsumer(new HashSet<>(Arrays.asList(TierPartitionStatus.INIT, TierPartitionStatus.CATCHUP)));
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
            cleanup();
        }
    }

    /**
     * @return boolean denoting whether catch up consumer is currently materializing the tier topic.
     */
    public boolean catchingUp() {
        return catchUpConsumer != null;
    }

    /**
     * Work cycle
     * public for testing purposes.
     */
    public boolean doWork() throws TierMetadataDeserializationException, IOException {
        checkCatchingUpComplete();
        processMigrations();
        final boolean primaryProcessed = pollConsumer(primaryConsumer, TierPartitionStatus.ONLINE, true);
        final boolean catchUpProcessed = catchUpConsumer != null
                && pollConsumer(catchUpConsumer, TierPartitionStatus.CATCHUP, false);

        heartbeat.set(System.currentTimeMillis());
        return primaryProcessed || catchUpProcessed;
    }

    /**
     * Ensure tier topic has been created and setup the backing consumer
     * and producer before signalling ready.
     * @param boostrapServers the brokers to bootstrap the tier topic consumer and producer
     */
    // pubic for testing
    public void becomeReady(String boostrapServers) {
        primaryConsumer = consumerBuilder.setupConsumer(boostrapServers, topicName, "primary");
        primaryConsumer.assign(partitions());
        for (Map.Entry<Integer, Long> entry : committer.positions().entrySet())
            primaryConsumer.seek(new TopicPartition(topicName, entry.getKey()), entry.getValue());

        producer = producerBuilder.setupProducer(boostrapServers);
        partitioner = new TierTopicPartitioner(config.numPartitions);

        synchronized (ready) {
            ready.set(true);

            for (Map.Entry<AbstractTierMetadata, CompletableFuture<AppendResult>> entry : queuedRequests.entrySet())
                addMetadata(entry.getKey(), entry.getValue());
            queuedRequests.clear();
        }
    }

    private void cleanup() {
        ready.set(false);

        if (primaryConsumer != null)
            primaryConsumer.close();
        if (catchUpConsumer != null)
            catchUpConsumer.close();
        committer.shutdown();

        for (CompletableFuture<AppendResult> future : queuedRequests.values())
            future.completeExceptionally(new TierMetadataFatalException("Tier topic manager shutting down"));
        queuedRequests.clear();

        shutdownInitiated.countDown();
    }

    /**
     * Sets a flag to trigger immigration of the supplied partitions in
     * the main worker thread.
     *
     * package-private for testing purposes.
     * @param partitions the TopicPartitions to immigrate
     */
    private void immigratePartitions(List<TopicIdPartition> partitions) {
        if (!partitions.isEmpty())
            partitionsImmigrated = true;
    }

    /**
     * Sets a flag to trigger emigrations of the supplied partitions in
     * the main worker thread.
     *
     * package-private for testing purposes.
     * @param partitions the TopicPartitions to emigrate
     */
    private void emigratePartitions(List<TopicIdPartition> partitions) {
        if (!partitions.isEmpty())
            partitionsEmigrated = true;
    }

    /**
     * Process any migrations if any have occurred.
     * If the catch up consumer has been started, and partitions have been emigrated,
     * check whether the catch up consumer is still required, and if not, stop it.
     *
     * If the catch up consumer is stopped, and partitions have been immigrated,
     * check whether any partitions are in the INIT state, and if so transition them to CATCHUP
     * and start the catch up consumer.
     *
     * package-private for testing purposes
     */
    void processMigrations() {
        if (catchingUp()) {
            if (partitionsEmigrated) {
                partitionsEmigrated = false;
                reconcileCatchUpConsumer();
            }
        } else {
            if (partitionsImmigrated) {
                partitionsImmigrated = false;
                maybeStartCatchUpConsumer(new HashSet<>(Collections.singletonList(TierPartitionStatus.INIT)));
            }
        }
    }

    private List<TierPartitionState> collectPartitionsWithStatus(Set<TierPartitionStatus> transitionStatuses) {
        ArrayList<TierPartitionState> tierPartitionStates = new ArrayList<>();
        tierMetadataManager.tierEnabledPartitionStateIterator().forEachRemaining(tps -> {
            if (transitionStatuses.contains(tps.status()))
                tierPartitionStates.add(tps);
        });
        return tierPartitionStates;
    }

    /**
     * Reconciles the catch up consumer, with the current TierPartitionStates
     * that are being caught up. If no tier topic partitions need to be consumed, the
     * catch up consumer will be shutdown. If tier topic partitions no longer need to be read
     * the catch up consumer's assignment will be updated.
     */
    private void reconcileCatchUpConsumer() {
        Set<TierPartitionStatus> catchUpStatuses
                = new HashSet<>(Collections.singletonList(TierPartitionStatus.CATCHUP));
        List<TierPartitionState> states = collectPartitionsWithStatus(catchUpStatuses);
        if (states.isEmpty()) {
            stopCatchUpConsumer();
        } else {
            List<TopicIdPartition> catchUpPartitions = getCatchUpPartititions(states);
            log.info("Assigning tier topic partitions to catch up consumer {}", catchUpPartitions);
            catchUpConsumer.assign(requiredPartitions(catchUpPartitions));
        }
    }

    private void maybeStartCatchUpConsumer(Set<TierPartitionStatus> transitionStatuses) {
        if (!catchingUp()) {
            List<TierPartitionState> states = collectPartitionsWithStatus(transitionStatuses);
            if (!states.isEmpty()) {
                for (TierPartitionState state : states)
                    state.beginCatchup();
                List<TopicIdPartition> catchUpPartitions = getCatchUpPartititions(states);
                catchUpConsumer = consumerBuilder.setupConsumer(bootstrapServersSupplier.get(), topicName, "catchup");
                catchUpConsumer.assign(requiredPartitions(catchUpPartitions));

                log.info("Seeking consumer to beginning.");
                catchUpConsumer.seekToBeginning(catchUpConsumer.assignment());
            }
        }
    }

    private List<TopicIdPartition> getCatchUpPartititions(List<TierPartitionState> states) {
        return states
                .stream()
                .map(TierPartitionState::topicIdPartition)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
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
                .mapToObj(partitionId -> new TopicPartition(topicName, partitionId))
                .collect(Collectors.toList());
    }

    /**
     * Generate the tier topic partitions containing data for tiered partitions.
     *
     * @param tieredPartitions partitions that have been tiered
     * @return The partitions on the Tier Topic containing data for tieredPartitions
     */
    private Collection<TopicPartition> requiredPartitions(Collection<TopicIdPartition> tieredPartitions) {
        return tieredPartitions
                .stream()
                .map(tpid -> new TopicPartition(topicName, partitioner.partitionId(tpid)))
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
        if (catchingUp() && catchUpConsumerLag() == 0)
            completeCatchUp();
    }

    private void stopCatchUpConsumer() {
        catchUpConsumer.close();
        catchUpConsumer = null;
    }

    /**
     * When all tier partition states have caught up, transition their statuses,
     * shutdown catch up consumer, and check whether any TierPartitionStates have been added
     * that require catch up since the last pass started.
     */
    private void completeCatchUp() {
        log.info("Completed adding partitions. Setting states for catch up topic partitions to ONLINE.");
        stopCatchUpConsumer();
        Iterator<TierPartitionState> iterator = tierMetadataManager.tierEnabledPartitionStateIterator();
        while (iterator.hasNext()) {
            TierPartitionState state = iterator.next();
            if (state.status() == TierPartitionStatus.CATCHUP)
                state.onCatchUpComplete();
        }
    }

    /**
     * Poll a consumer, materializing Tier Topic entries to TierPartition state.
     *
     * @param consumer      the consumer to poll
     * @param requiredState The TierPartition must be in this state or else the metadata will be ignored.
     * @param commitPositions boolean denoting whether to send the consumer positions to the
     *                        committer. Only the primary consumer should commit offsets.
     * @return boolean denoting whether messages were processed
     * @throws IOException if error occurred writing to pier partition state/logdir.
     */
    private boolean pollConsumer(Consumer<byte[], byte[]> consumer,
                                 TierPartitionStatus requiredState,
                                 boolean commitPositions) throws IOException {
        boolean processedMessages = false;
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(config.pollDuration)) {
            final Optional<AbstractTierMetadata> entry =
                    AbstractTierMetadata.deserialize(record.key(), record.value());
            if (entry.isPresent()) {
                processEntry(entry.get(), requiredState);
                if (commitPositions)
                    committer.updatePosition(record.partition(), record.offset() + 1);

                processedMessages = true;
            }
        }
        return processedMessages;
    }

    private void addMetadata(AbstractTierMetadata metadata, CompletableFuture<AppendResult> future) {
        synchronized (ready) {
            if (!ready.get()) {
                queuedRequests.put(metadata, future);
                return;
            }
        }

        TopicIdPartition topicPartition = metadata.topicIdPartition();
        // track this entry's materialization
        resultListeners.addTracked(metadata, future);
        producer.send(new ProducerRecord<>(topicName, partitioner.partitionId(topicPartition),
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
     * @param requiredState TierPartitionState must be in this status in order to modify it; otherwise the entry will be ignored.
     */
    private void processEntry(AbstractTierMetadata entry, TierPartitionStatus requiredState) throws IOException {
        final TopicIdPartition tpid = entry.topicIdPartition();
        final Optional<TierPartitionState> tierPartitionStateOpt = tierMetadataManager.tierPartitionState(tpid);
        if (tierPartitionStateOpt.isPresent()) {
            TierPartitionState tierPartitionState = tierPartitionStateOpt.get();
            if (tierPartitionState.status() == requiredState) {
                final AppendResult result = tierPartitionState.append(entry);
                log.debug("Read entry {}, append result {}", entry, result);
                // signal completion of this tier topic entry if this topic manager was the sender
                resultListeners.getAndRemoveTracked(entry).ifPresent(c -> c.complete(result));
            } else {
                log.debug("TierPartitionState {} not in required state {}. Ignoring metadata {}.", tpid, requiredState, entry);
            }
        } else {
            resultListeners.getAndRemoveTracked(entry).ifPresent(c -> c.completeExceptionally(
                    new TierMetadataRetriableException("Tier partition state for " + tpid + " does not exist")));
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

    /**
     * Class to track outstanding requests and signal back to the TierTopicManager
     * user when their metadata requests have been read and materialized.
     */
    private static class TierTopicListeners {
        private final Map<TopicIdPartition, Map<UUID, CompletableFuture<AppendResult>>> results = new ConcurrentHashMap<>();

        /**
         * Checks whether a given tier index entry is being tracked. If so,
         * returns a CompletableFuture to be completed to signal back to the sender.
         *
         * @param metadata tier index topic entry we are trying to complete
         * @return CompletableFuture for this index entry if one exists.
         */
        Optional<CompletableFuture<AppendResult>> getAndRemoveTracked(AbstractTierMetadata metadata) {
            Map<UUID, CompletableFuture<AppendResult>> entries = results.get(metadata.topicIdPartition());

            if (entries != null) {
                CompletableFuture<AppendResult> future = entries.remove(metadata.messageId());
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
        void addTracked(AbstractTierMetadata metadata, CompletableFuture<AppendResult> future) {
            results.putIfAbsent(metadata.topicIdPartition(), new ConcurrentHashMap<>());
            Map<UUID, CompletableFuture<AppendResult>> entries = results.get(metadata.topicIdPartition());

            CompletableFuture previous = entries.put(metadata.messageId(), future);
            if (previous != null)
                previous.completeExceptionally(new TierMetadataFatalException(
                        "A new index entry is being tracked for messageId " + metadata.messageId() +
                                " obsoleting this request."));
        }
    }
}
