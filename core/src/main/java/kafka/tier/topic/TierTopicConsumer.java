/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.topic;

import kafka.server.LogDirFailureChannel;
import kafka.tier.TierTopicManagerCommitter;
import kafka.tier.TopicIdPartition;
import kafka.tier.client.TierTopicConsumerSupplier;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierRecordType;
import kafka.tier.exceptions.TierMetadataFatalException;
import kafka.tier.exceptions.TierMetadataRetriableException;
import kafka.tier.state.TierPartitionState;
import kafka.tier.state.TierPartitionStatus;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Tier topic consumer. Responsible for consuming the tier topic and materializing relevant state to the appropriate
 * target. Provides abstractions to {@link #register} and {@link #deregister} materialization of tier partition
 * states.
 */
public class TierTopicConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TierTopicConsumer.class);

    private final TierTopicManagerConfig config;
    private final Optional<Metrics> metrics;
    private final TierTopicListeners resultListeners = new TierTopicListeners();

    /**
     * Map of {TopicIdPartition -> ClientCtx}, where each ClientCtx status can be any of the available
     * TierPartitionStatus values. This map contains partitions that have been registered, but are
     * yet to be moved into either primaryConsumerPartitions or catchUpConsumerPartitions.
     *
     * THREAD SAFETY: Access to this attribute must be synchronized.
     */
    private final Map<TopicIdPartition, ClientCtx> immigratingPartitions = new HashMap<>();

    /**
     * Map of {TopicIdPartition -> ClientCtx}, where each ClientCtx status can be one among the
     * following values: {TierPartitionStatus.ONLINE, TierPartitionStatus.ERROR}.
     * TierTopic events supplied only by the primary consumer are applied to these partitions.
     *
     * THREAD SAFETY: Access to this attribute must be synchronized.
     */
    private final Map<TopicIdPartition, ClientCtx> primaryConsumerPartitions = new HashMap<>();

    /**
     * Map of {TopicIdPartition -> ClientCtx}, where each ClientCtx status can be one among the
     * following values: {TierPartitionStatus.CATCHUP, TierPartitionStatus.ERROR}.
     * TierTopic events supplied only by the catchup consumer are applied to these partitions.
     *
     * THREAD SAFETY: Access to this attribute must be synchronized.
     */
    private final Map<TopicIdPartition, ClientCtx> catchUpConsumerPartitions = new HashMap<>();

    /**
     * Set of TopicIdPartition, where, each such partition's ClientCtx had reached
     * TierPartitionStatus.ERROR status at some point. These may have reached an error state, when
     * processing events via either the primary consumer or catch up consumer. Currently this
     * attribute is used only for monitoring purposes.
     *
     * THREAD SAFETY: Access to this attribute must be synchronized.
     */
    private final Set<TopicIdPartition> errorPartitions = new HashSet<>();    // accesses must be synchronized

    private final Thread consumerThread = new KafkaThread("TierTopicConsumer", this, false);
    private final Supplier<Consumer<byte[], byte[]>> primaryConsumerSupplier;
    private final TierTopicManagerCommitter committer;
    private final MetricName errorPartitionsMetricName = new MetricName("ErrorPartitions",
        "TierTopicConsumer",
        "Number of tiered partitions with ERROR materialization state",
        new HashMap<>());

    private volatile Consumer<byte[], byte[]> primaryConsumer;
    private volatile boolean ready = true;
    private volatile boolean shutdown = false;

    private InitializedTierTopic tierTopic;
    private TierCatchupConsumer catchupConsumer;

    public TierTopicConsumer(TierTopicManagerConfig config,
                             LogDirFailureChannel logDirFailureChannel,
                             Metrics metrics) {
        this(config,
                new TierTopicConsumerSupplier(config, "primary"),
                new TierTopicConsumerSupplier(config, "catchup"),
                new TierTopicManagerCommitter(config, logDirFailureChannel),
                Optional.of(metrics));
    }

    // used for testing
    public TierTopicConsumer(TierTopicManagerConfig config,
                             Supplier<Consumer<byte[], byte[]>> primaryConsumerSupplier,
                             Supplier<Consumer<byte[], byte[]>> catchupConsumerSupplier,
                             TierTopicManagerCommitter committer,
                             Optional<Metrics> metrics) {
        this.config = config;
        this.committer = committer;
        this.primaryConsumerSupplier = primaryConsumerSupplier;
        this.catchupConsumer = new TierCatchupConsumer(catchupConsumerSupplier);
        this.metrics = metrics;
        setupMetrics();
    }

    /**
     * Register topic partition to be materialized. Note that multiple registrations for the topic partition are not
     * permissible.
     * @param partition Topic partition to register
     * @param clientCtx Client context for this registration
     */
    public synchronized void register(TopicIdPartition partition, ClientCtx clientCtx) {
        if (!immigratingPartitions.containsKey(partition) &&
                !primaryConsumerPartitions.containsKey(partition) &&
                !catchUpConsumerPartitions.containsKey(partition)) {
            immigratingPartitions.put(partition, clientCtx);
            if (clientCtx.status() == TierPartitionStatus.ERROR) {
                // We add the partition to errorPartitions immediately, as it is useful to surface
                // error partitions in monitoring as soon as they are registered.
                errorPartitions.add(partition);
            }
        } else {
            throw new IllegalStateException("Duplicate registration for " + partition);
        }
    }

    /**
     * Register topic partitions to be materialized. This is similar to {@link #register(TopicIdPartition, ClientCtx)}
     * but allows registration for multiple topic partitions atomically, to ensure all topic partitions can begin
     * materialization at the same time.
     * @param partitionsToRegister Topic partitions to register
     */
    public synchronized void register(Map<TopicIdPartition, ClientCtx> partitionsToRegister) {
        for (Map.Entry<TopicIdPartition, ClientCtx> partitionToRegister : partitionsToRegister.entrySet())
            register(partitionToRegister.getKey(), partitionToRegister.getValue());
    }

    /**
     * Deregister this topic partition and stop materialization.
     * @param partition Topic partition to deregister
     */
    public synchronized void deregister(TopicIdPartition partition) {
        immigratingPartitions.remove(partition);
        primaryConsumerPartitions.remove(partition);
        catchUpConsumerPartitions.remove(partition);
        errorPartitions.remove(partition);
    }

    /**
     * Track materialization of provided metadata.
     * @param metadata Metadata to track materialization for
     * @param future Corresponding future; the future is completed after successful materialization of the metadata. It
     *               may be completed exceptionally if the partition is no longer being materialized or if we ran into
     *               unexpected state.
     */
    public void trackMaterialization(AbstractTierMetadata metadata, CompletableFuture<TierPartitionState.AppendResult> future) {
        resultListeners.addTracked(metadata, future);
    }

    /**
     * Cancel materialization tracking for provided metadata.
     * @param metadata Metadata to cancel materialization tracking for
     */
    public void cancelTracked(AbstractTierMetadata metadata) {
        resultListeners.getAndRemoveTracked(metadata);
    }

    /**
     * Start consuming the tier topic. Caller must ensure that the tier topic has already been created.
     * @param startConsumeThread Whether to start the background consumption thread. This is mainly exposed so that tests
     *                           could drive the consumption thread in foreground if needed.
     * @param tierTopic An instance of {@link InitializedTierTopic}.
     */
    public void startConsume(boolean startConsumeThread, InitializedTierTopic tierTopic) {
        Set<TopicPartition> tierTopicPartitions = TierTopicManager.partitions(tierTopic.topicName(), tierTopic.numPartitions().getAsInt());

        // startup the primary consumer
        primaryConsumer = primaryConsumerSupplier.get();
        primaryConsumer.assign(tierTopicPartitions);
        for (TopicPartition topicPartition : tierTopicPartitions) {
            Long position = committer.positionFor(topicPartition.partition());
            if (position != null) {
                log.info("seeking primary consumer to committed offset {} for partition {}", position, topicPartition);
                primaryConsumer.seek(topicPartition, position);
            } else {
                log.info("primary consumer missing committed offset for partition {}. Seeking to beginning", topicPartition);
                primaryConsumer.seekToBeginning(Collections.singletonList(topicPartition));
            }
        }

        if (startConsumeThread)
            consumerThread.start();

        this.tierTopic = tierTopic;
        ready = true;
    }

    /**
     * Commit positions for the provided tier partition states.
     * @param tierPartitionStateIterator Iterator over all tier partition states.
     */
    public void commitPositions(Iterator<TierPartitionState> tierPartitionStateIterator) {
        committer.flush(tierPartitionStateIterator);
    }

    public boolean isReady() {
        return ready;
    }

    public void shutdown() {
        shutdown = true;

        if (primaryConsumer != null)
            primaryConsumer.wakeup();
        catchupConsumer.wakeup();

        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
        }
        resultListeners.shutdown();
        removeMetrics();
    }

    public void cleanup() {
        if (primaryConsumer != null)
            primaryConsumer.close();
        catchupConsumer.close();
    }

    @Override
    public void run() {
        try {
            while (!shutdown)
                doWork();
        } catch (Exception e) {
            if (shutdown)
                log.debug("Exception caught during shutdown", e);
            else
                log.error("Fatal exception in TierTopicConsumer", e);
        } finally {
            ready = false;
        }
    }

    // visible for testing
    public void doWork() {
        if (catchupConsumer.tryComplete(primaryConsumer)) {
            synchronized (this) {
                // Complete catchup of ClientCtx, only if did not reach an error status during
                // catchup. Such error partitions should be already part of errorPartitions by this
                // point.
                for (ClientCtx ctx : catchUpConsumerPartitions.values()) {
                    if (!ctx.status().hasError())
                        ctx.completeCatchup();
                }

                primaryConsumerPartitions.putAll(catchUpConsumerPartitions);
                catchUpConsumerPartitions.clear();
            }
        }

        processPendingImmigrations();

        processRecords(primaryConsumer.poll(config.pollDuration), TierPartitionStatus.ONLINE, true);
        processRecords(catchupConsumer.poll(config.pollDuration), TierPartitionStatus.CATCHUP, false);
    }

    // visible for testing
    InitializedTierTopic tierTopic() {
        return tierTopic;
    }

    /**
     * Process any pending immigrations, if they have occurred.
     *
     * If the catch up consumer is stopped, and partitions have been immigrated, then do the following:
     * 1. Check whether any partitions are in the INIT or CATCHUP or ERROR state. If there are any, then register
     *    these partitions into the catchup consumer's group of partitions. Then, transition only the ones in INIT
     *    status to CATCHUP, and start the catch up consumer for all such new partitions.
     * 2. Check whether any partitions are in ONLINE state. If there are any, then register these partitions into
     *    the primary consumer's group of partitions.
     *
     * Note: We don't know the position of the partitions in ERROR status, that is why we simply register
     * these into the catchup consumer first.
     */
    private void processPendingImmigrations() {
        Map<TopicIdPartition, ClientCtx> newCatchupPartitions = new HashMap<>();
        Map<TopicIdPartition, ClientCtx> newOnlinePartitions = new HashMap<>();

        if (!catchupConsumer.active()) {
            synchronized (this) {
                for (Map.Entry<TopicIdPartition, ClientCtx> entry : immigratingPartitions.entrySet()) {
                    TopicIdPartition partition = entry.getKey();
                    ClientCtx clientCtx = entry.getValue();
                    TierPartitionStatus status = clientCtx.status();

                    if (status == TierPartitionStatus.INIT ||
                        status == TierPartitionStatus.CATCHUP ||
                        status == TierPartitionStatus.ERROR) {
                        newCatchupPartitions.put(partition, clientCtx);
                    } else if (status == TierPartitionStatus.ONLINE) {
                        newOnlinePartitions.put(partition, clientCtx);
                    } else {
                        log.debug("Ignoring immigration of partition {} in state {}", partition,
                            status);
                    }
                }

                catchUpConsumerPartitions.putAll(newCatchupPartitions);
                primaryConsumerPartitions.putAll(newOnlinePartitions);
                immigratingPartitions.clear();
            }
        }

        if (!newCatchupPartitions.isEmpty())
            beginCatchup(newCatchupPartitions);
    }

    private void beginCatchup(Map<TopicIdPartition, ClientCtx> partitionsToCatchup) {
        for (ClientCtx ctx : partitionsToCatchup.values())
            if (!ctx.status().hasError())
                ctx.beginCatchup();

        Set<TopicPartition> tierTopicPartitions = tierTopic.toTierTopicPartitions(partitionsToCatchup.keySet());
        catchupConsumer.doStart(tierTopicPartitions);
    }

    /**
     * Poll a consumer, materializing Tier Topic entries to TierPartition state.
     *
     * @param records consumed records to process
     * @param requiredState the tier partition must be in this state or else the metadata will be ignored.
     * @param commitPositions boolean denoting whether to send the consumer positions to the
     *                        committer. Only the primary consumer should commit offsets.
     * @throws TierMetadataFatalException
     */
    private void processRecords(ConsumerRecords<byte[], byte[]> records,
                                TierPartitionStatus requiredState,
                                boolean commitPositions) {
        if (records == null)
            return;

        for (ConsumerRecord<byte[], byte[]> record : records) {
            try {
                final Optional<AbstractTierMetadata> entryOpt =
                        AbstractTierMetadata.deserialize(record.key(), record.value());

                if (entryOpt.isPresent()) {
                    AbstractTierMetadata entry = entryOpt.get();
                    log.trace("Read {} at offset {} of partition {} requiredState {}", entry, record.offset(), record.partition(), requiredState);
                    processEntry(entry, record.partition(), record.offset(), requiredState);

                    if (commitPositions)
                        committer.updatePosition(record.partition(), record.offset() + 1);
                } else {
                    throw new TierMetadataFatalException(
                            String.format("Fatal Exception message for %s and unknown type: %d cannot be deserialized (requiredState:%s).",
                                AbstractTierMetadata.deserializeKey(record.key()).toString(),
                                AbstractTierMetadata.getTypeId(record.value()),
                                requiredState));
                }
            } catch (Exception e) {
                throw new TierMetadataFatalException(
                        String.format("Unable to process message at offset %d of partition %d, requiredState %s",
                                record.offset(), record.partition(), requiredState), e);
            }
        }
    }

    private void setupMetrics() {
        metrics.ifPresent(m -> {
            m.addMetric(errorPartitionsMetricName, (MetricConfig config, long now) -> {
                synchronized (this) {
                    return errorPartitions.size();
                }
            });
        });
    }

    private void removeMetrics() {
        metrics.ifPresent(m -> {
            m.removeMetric(errorPartitionsMetricName);
        });
    }

    private static boolean checkClientCtxStatus(ClientCtx ctx, TierPartitionStatus status) {
        return (ctx != null) && EnumSet.of(status, TierPartitionStatus.ERROR).contains(ctx.status());
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

        try {
            TopicIdPartition topicIdPartition = entry.topicIdPartition();
            ClientCtx clientCtx = null;
            boolean matchesRequiredState = false;

            // Retrieve the client context, if any
            synchronized (this) {
                if (primaryConsumerPartitions.containsKey(topicIdPartition)) {
                    clientCtx = primaryConsumerPartitions.get(topicIdPartition);
                    matchesRequiredState =
                        (requiredState == TierPartitionStatus.ONLINE) &&
                        checkClientCtxStatus(clientCtx, requiredState);
                } else {
                    clientCtx = catchUpConsumerPartitions.get(topicIdPartition);
                    matchesRequiredState =
                        (requiredState == TierPartitionStatus.CATCHUP) &&
                        checkClientCtxStatus(clientCtx, requiredState);
                }
            }

            if (clientCtx != null) {
                TierPartitionStatus currentState = clientCtx.status();
                switch (currentState) {
                    case DISK_OFFLINE:
                        resultListeners
                                .getAndRemoveTracked(entry)
                                .ifPresent(c -> c.completeExceptionally(
                                        new TierMetadataFatalException("Partition " + topicIdPartition + " is offline")));
                        break;

                    default:
                        if (matchesRequiredState) {
                            TierPartitionState.AppendResult result = processEntry(
                                clientCtx, topicIdPartition, entry, offset);
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
                // Partition deletion messages do not require a corresponding target for successful completion. For example,
                // the controller would attempt to initiate deletion of a partition but would not necessarily have the
                // partition registered for materialization. We complete their materialization successfully. All other
                // messages require an active target. If there is none, we complete the future exceptionally.
                if (entry.type() == TierRecordType.PartitionDeleteInitiate || entry.type() == TierRecordType.PartitionDeleteComplete)
                    resultListeners.getAndRemoveTracked(entry).ifPresent(c -> c.complete(TierPartitionState.AppendResult.ACCEPTED));
                else
                    resultListeners.getAndRemoveTracked(entry).ifPresent(c -> c.completeExceptionally(
                            new TierMetadataRetriableException("Tier partition state for " + topicIdPartition + " does not exist")));
            }
        } catch (Exception e) {
            throw new TierMetadataFatalException(
                    String.format("Error processing message %s at offset %d, partition %d, requiredState %s", entry, offset, partition, requiredState), e);
        }
    }

    private TierPartitionState.AppendResult processEntry(
        ClientCtx clientCtx, TopicIdPartition topicIdPartition, AbstractTierMetadata entry, long offset) {
        try {
            return clientCtx.process(entry, offset);
        } finally {
            if (clientCtx.status() == TierPartitionStatus.ERROR) {
                synchronized (this) {
                    errorPartitions.add(topicIdPartition);
                }
            }
        }
    }

    // visible for testing
    synchronized Map<TopicIdPartition, ClientCtx> immigratingPartitions() {
        return new HashMap<>(immigratingPartitions);
    }

    // visible for testing
    synchronized Map<TopicIdPartition, ClientCtx> primaryConsumerPartitions() {
        return new HashMap<>(primaryConsumerPartitions);
    }

    // visible for testing
    synchronized Map<TopicIdPartition, ClientCtx> catchUpConsumerPartitions() {
        return new HashMap<>(catchUpConsumerPartitions);
    }

    // visible for testing
    synchronized long numListeners() {
        return resultListeners.numListeners();
    }

    // visible for testing
    synchronized Set<TopicIdPartition> errorPartitions() {
        return new HashSet<>(errorPartitions);
    }

    public interface ClientCtx {
        /**
         * Process metadata for this context.
         * @param metadata Metadata to process
         * @param tierTopicPartitionOffset  Metadata's offset in TierTopicPartition
         * @return Result of processing
         */
        TierPartitionState.AppendResult process(AbstractTierMetadata metadata, long tierTopicPartitionOffset);

        /**
         * Retrieve status of tiered partition.
         * @return The current status
         */
        TierPartitionStatus status();

        /**
         * Begin {@link TierPartitionStatus#CATCHUP} phase for this context.
         */
        void beginCatchup();

        /**
         * Complete catchup phase for this context and transition to {@link TierPartitionStatus#ONLINE} status.
         */
        void completeCatchup();
    }
}
