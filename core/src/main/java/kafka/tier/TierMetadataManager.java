/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.log.LogConfig;
import kafka.server.LogDirFailureChannel;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.state.TierPartitionState;
import kafka.tier.state.TierPartitionStateFactory;
import kafka.tier.store.TierObjectStore;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction0;
import static scala.compat.java8.JFunction.func;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Central place to maintain lifecycle of partitions for tiered storage. Tracks the lifecycle (addition, deletion, leader
 * election, becoming follower, change in topic configuration) for all partitions for which tiering is enabled. Also
 * maintains a mapping of topic-partition to the associated {@link TierPartitionState}. This serves as the central place to tie
 * all tiering components with other Kafka components.
 *
 * For a particular topic partition, {@link kafka.log.Log} layer initializes the {@link TierPartitionState} by invoking
 * {@link #initState}. When the partition is made a replica, either one of {@link #becomeLeader} or {@link #becomeFollower}
 * is invoked. Finally {@link #delete} is invoked when the replica is deleted from the broker. {@link #onConfigChange}
 * is invoked to track topic configuration changes.
 *
 * Listeners can be registered using {@link #addListener} and appropriate callbacks are fired for each lifecycle stage.
 * Notably, {@link kafka.tier.archiver.TierArchiver} and {@link TierTopicManager} register listeners so they could track
 * the set of partitions for which tiering is enabled, and whether the broker is a leader or not.
 */
public class TierMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(TierMetadataManager.class);

    private final TierPartitionStateFactory tierPartitionStateFactory;
    private final Optional<TierObjectStore> tierObjectStore;
    private final ConcurrentHashMap<TopicIdPartition, PartitionMetadata> tierMetadataById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicPartition, PartitionMetadata> tierMetadata = new ConcurrentHashMap<>();
    private final LogDirFailureChannel logDirFailureChannel;
    private final boolean tierFeatureEnabled;
    private final Map<Class, ChangeListener> changeListeners = new ConcurrentHashMap<>();

    public TierMetadataManager(TierPartitionStateFactory tierPartitionStateFactory,
                               Optional<TierObjectStore> tierObjectStore,
                               LogDirFailureChannel logDirFailureChannel,
                               boolean tierFeatureEnabled) {
        this.tierPartitionStateFactory = tierPartitionStateFactory;
        this.tierObjectStore = tierObjectStore;
        this.logDirFailureChannel = logDirFailureChannel;
        this.tierFeatureEnabled = tierFeatureEnabled;
    }

    /**
     * Initialize tier state for given topic partition. This is called only once per topic partition during Log
     * initialization. This method also fires off registered change listeners if tiering is enabled for the topic partition.
     * @param topicPartition Topic partition for which tier metadata needs to be initialized
     * @param dir Log directory
     * @param logConfig Log configuration
     * @return Initialized tier partition state
     * @throws IOException
     */
    public synchronized TierPartitionState initState(TopicPartition topicPartition,
                                                     File dir,
                                                     LogConfig logConfig) throws IOException {
        PartitionMetadata partitionMetadata = new PartitionMetadata(tierPartitionStateFactory, dir, topicPartition, logConfig, tierFeatureEnabled);
        tierMetadata.put(new TopicPartition(topicPartition.topic(), topicPartition.partition()), partitionMetadata);
        // The TierTopicManager will start materializing ONLINE partitions on startup.
        // We must ensure that the tierMetadata and tierMetadataById maps are in sync when a
        // given partition is ONLINE or the TierTopicManager will miss metadata updates
        partitionMetadata.tierPartitionState().topicIdPartition().ifPresent(tpid -> tierMetadataById.put(tpid, partitionMetadata));
        return partitionMetadata.tierPartitionState;
    }

    public Future<TierObjectMetadata> materializeUntilOffset(TopicPartition topicPartition,
                                                             Long target) throws IOException {
        return tierMetadata.get(topicPartition).tierPartitionState.materializationListener(target);
    }

    /**
     * Delete tier metadata for given topic partition. Called when the partition is deleted from this broker. Fires off
     * registered change listeners if partition was enabled for tiering.
     * @param topicPartition Topic partition to delete tier metadata for
     */
    public synchronized void delete(TopicPartition topicPartition) {
        PartitionMetadata partitionMetadata = tierMetadata.get(topicPartition);
        if (partitionMetadata != null) {
            if (partitionMetadata.tierable()) {
                log.debug("Firing onDelete listeners for tiered topic {}", topicPartition);
                Optional<TopicIdPartition> topicIdPartition =
                        partitionMetadata.tierPartitionState.topicIdPartition();

                topicIdPartition.ifPresent(tpid -> changeListeners.values().forEach(listener -> listener.onDelete(tpid)));
            }

            File dir = partitionMetadata.tierPartitionState.dir();
            try {
                partitionMetadata.tierPartitionState.delete();
            } catch (IOException e) {
                handleIOException(dir, e, "Storage exception when deleting tier partition state");
            } finally {
                partitionMetadata.tierPartitionState
                        .topicIdPartition()
                        .map(tierMetadataById::remove);
                tierMetadata.remove(topicPartition);
            }
        }
    }

    /**
     * Called when replica is elected to be the leader. Fires off registered change listeners if partition is enabled
     * for tiering.
     * @param topicPartition Topic id partition being elected leader
     * @param leaderEpoch Leader epoch
     */
    public synchronized void becomeLeader(TopicPartition topicPartition, int leaderEpoch) {
        PartitionMetadata partitionMetadata = tierMetadata.get(topicPartition);
        if (partitionMetadata == null)
            throw new IllegalStateException("Tier metadata must exist for " + topicPartition);

        partitionMetadata.epochIfLeader = OptionalInt.of(leaderEpoch);
        maybeFireListeners(topicPartition, partitionMetadata);
    }

    /**
     * Called when replica becomes follower. Fires off registered change listeners if partition is enabled for tiering.
     * @param topicPartition Topic partition becoming follower
     */
    public synchronized void becomeFollower(TopicPartition topicPartition) {
        PartitionMetadata partitionMetadata = tierMetadata.get(topicPartition);
        if (partitionMetadata == null)
            throw new IllegalStateException("Tier metadata must exist for " + topicPartition);

        partitionMetadata.epochIfLeader = OptionalInt.empty();
        maybeFireListeners(topicPartition, partitionMetadata);
    }

    public synchronized void ensureTopicIdPartition(TopicIdPartition topicIdPartition) {
        PartitionMetadata partitionMetadata = tierMetadata.get(topicIdPartition.topicPartition());
        try {

            if (partitionMetadata.setTopicIdPartition(topicIdPartition)) {
                // a TopicIdPartition was not previously set, so we need to setup our
                // tierMetadataById map and fire any listeners that would have been skipped in prior
                // becomeLeader/becomeFollower calls
                tierMetadataById.put(topicIdPartition, partitionMetadata);
                maybeFireListeners(topicIdPartition.topicPartition(), partitionMetadata);
            }
        } catch (IOException ioe) {
            logDirFailureChannel.maybeAddOfflineLogDir(partitionMetadata.tierPartitionState().dir().getParent(),
                    func(() -> "error setting TopicIdPartition " + topicIdPartition + " on "
                            + "TierPartitionState"), ioe);
        }
    }

    /**
     * Called when log configuration for a topic partition is changed. Fires off registered change listeners if partition
     * is enabled for tiering.
     * @param topicPartition Topic partition
     * @param config New log configuration
     */
    public synchronized void onConfigChange(TopicPartition topicPartition, LogConfig config) {
        PartitionMetadata partitionMetadata = tierMetadata.get(topicPartition);
        if (partitionMetadata == null)
            throw new IllegalStateException("Tier metadata must exist for " + topicPartition);

        File dir = partitionMetadata.tierPartitionState.dir();
        try {
            if (partitionMetadata.updateConfig(config, tierFeatureEnabled))
                maybeFireListeners(topicPartition, partitionMetadata);
        } catch (IOException e) {
            handleIOException(dir, e, "Storage exception on configuration change");
        }
    }

    private void maybeFireListeners(TopicPartition topicPartition, PartitionMetadata partitionMetadata) {
        if (partitionMetadata.tierable()) {
            partitionMetadata.tierPartitionState.topicIdPartition().ifPresent(topicIdPartition -> {
                OptionalInt leaderEpoch = partitionMetadata.epochIfLeader;
                if (leaderEpoch.isPresent()) {
                    int epoch = leaderEpoch.getAsInt();
                    log.debug("Firing onBecomeLeader listeners on change for tiered topic {} leaderEpoch: {}", topicPartition, epoch);
                    changeListeners.values().forEach(listener -> listener.onBecomeLeader(topicIdPartition,
                            epoch));
                } else {
                    log.debug("Firing onBecomeFollower listeners on change for tiered topic {}", topicPartition);
                    changeListeners.values().forEach(listener -> listener.onBecomeFollower(topicIdPartition));
                }
            });
        }
    }

    public synchronized void close() {
        for (PartitionMetadata partitionMetadata : tierMetadata.values()) {
            try {
                partitionMetadata.tierPartitionState.close();
            } catch (Throwable t) {
                log.warn("Ignoring exception when closing tier partition state", t);
            }
        }
        tierMetadata.clear();
        tierMetadataById.clear();
    }

    /**
     * Retrieve the tier partition state for a particular topic partition, if present. Note that the presence of tier
     * partition state does not indicate tiering is enabled for that topic partition.
     * @param topicPartition Topic partition
     * @return Tier partition state
     */
    public Optional<TierPartitionState> tierPartitionState(TopicPartition topicPartition) {
        Optional<PartitionMetadata> partitionMetadata = tierPartitionMetadata(topicPartition);
        return partitionMetadata.map(PartitionMetadata::tierPartitionState);
    }

    /**
     * Retrieve the tier partition state for a particular topic partition, if present. Note that the presence of tier
     * partition state does not indicate tiering is enabled for that topic partition.
     * @param topicIdPartition Topic partition
     * @return Tier partition state
     */
    public Optional<TierPartitionState> tierPartitionState(TopicIdPartition topicIdPartition) {
        Optional<PartitionMetadata> partitionMetadata = tierPartitionMetadata(topicIdPartition);
        return partitionMetadata.map(PartitionMetadata::tierPartitionState);
    }


    /**
     * Retrieve the partition metadata, if present.
     * @param topicPartition Topic partition
     * @return Tier partition metadata
     */
    public Optional<PartitionMetadata> tierPartitionMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(tierMetadata.get(topicPartition));
    }

    public Optional<PartitionMetadata> tierPartitionMetadata(TopicIdPartition topicIdPartition) {
        return Optional.ofNullable(tierMetadataById.get(topicIdPartition));
    }

    /**
     * Get an iterator over states for all topic partitions for which tiering is enabled.
     * @return Iterator over tier state for topic partitions with tiering enabled
     */
    public Iterator<TierPartitionState> tierEnabledPartitionStateIterator() {
        return tierMetadata.values()
                .stream()
                .filter(PartitionMetadata::tierable)
                .map(partitionMetadata -> partitionMetadata.tierPartitionState)
                .iterator();
    }

    /**
     * Get an iterator over states for all topic partitions for which tiering is enabled and the current broker is the
     * leader of.
     */
    public Iterator<TierPartitionState> tierEnabledLeaderPartitionStateIterator() {
        return tierMetadata.values()
                .stream()
                .filter(partitionMetadata -> partitionMetadata.tierable() && partitionMetadata.epochIfLeader.isPresent())
                .map(partitionMetadata -> partitionMetadata.tierPartitionState)
                .iterator();
    }

    /**
     * Register a change listener.
     * @param clazz class to register the listener under
     * @param listener Listener to register
     */
    public synchronized void addListener(Class clazz, ChangeListener listener) {
        changeListeners.put(clazz, listener);
    }

    /**
     * Unregister a change listener.
     * @param clazz class to unregister the listener under
     */
    public synchronized void removeListener(Class clazz) {
        changeListeners.remove(clazz);
    }

    /**
     * Get the tier object store handle. This method assumes that tiering is enabled for the broker and a tier object
     * store exists.
     * @return Tier object store handle
     */
    public Optional<TierObjectStore> tierObjectStore() {
        return tierObjectStore;
    }

    // Handle IO exceptions by marking the log directory offline
    private void handleIOException(File logDir, IOException e, String message) {
        logDirFailureChannel.maybeAddOfflineLogDir(logDir.getParent(),
                new AbstractFunction0<String>() {
                    @Override
                    public String apply() {
                        return message + " {" + logDir + "}";
                    }
                }, e);
        throw new KafkaStorageException(e);
    }

    /**
     * Interface to register callbacks on the lifecycle of tiering enabled topic partitions. The lifecycle typically
     * follows the following cycle:
     *
     * (onBecomeLeader|onBecomeFollower <-> onBecomeFollower|onBecomeLeader) -> onDelete ->
     *     (possibly back to onBecomeLeader|onBecomeFollower if a new topic partition with the same name is recreated)
     */
    public interface ChangeListener {
        /**
         * Fired when this topic partition becomes leader.
         */
        void onBecomeLeader(TopicIdPartition topicIdPartition, int leaderEpoch);

        /**
         * Fired when this topic partition becomes follower.
         */
        void onBecomeFollower(TopicIdPartition topicIdPartition);

        /**
         * Fired when this topic partition is deleted.
         * @param topicIdPartition
         */
        void onDelete(TopicIdPartition topicIdPartition);
    }

    /**
     * Tiering metadata for a particular topic-partition.
     */
    public static class PartitionMetadata {
        private OptionalInt epochIfLeader = OptionalInt.empty();
        private final TierPartitionState tierPartitionState;

        private PartitionMetadata(TierPartitionStateFactory tierPartitionStateFactory,
                                  File stateDir,
                                  TopicPartition topicPartition,
                                  LogConfig config,
                                  boolean tierFeatureEnabled) throws IOException {
            boolean tieringEnabled = isTieringEnabled(topicPartition, config, tierFeatureEnabled);
            this.tierPartitionState = tierPartitionStateFactory.initState(stateDir, topicPartition, tieringEnabled);
        }

        private boolean setTopicIdPartition(TopicIdPartition topicIdPartition) throws IOException {
            return tierPartitionState.setTopicIdPartition(topicIdPartition);
        }

        // Change tiering enabled configuration
        private boolean updateConfig(LogConfig newConfig, boolean tierFeatureEnabled) throws IOException {
            boolean currentTieringEnabled = tieringEnabled();
            boolean newTieringEnabled = isTieringEnabled(tierPartitionState.topicPartition(), newConfig, tierFeatureEnabled);

            if (!currentTieringEnabled && newTieringEnabled) {
                tierPartitionState.onTieringEnable();
                return true;
            } else if (currentTieringEnabled && !newTieringEnabled) {
                throw new IllegalStateException("Cannot disable tiering on a topic that already has been tiered");
            }

            return false;
        }

        boolean tieringEnabled() {
            return tierPartitionState.tieringEnabled();
        }

        boolean tierable() {
            return tieringEnabled() && tierPartitionState.topicIdPartition().isPresent();
        }

        private boolean isTieringEnabled(TopicPartition topicPartition, LogConfig config, boolean tierFeatureEnabled) {
            if (tierFeatureEnabled && config.tierEnable()) {
                if (config.compact()) {
                    log.warn("Tiering cannot be enabled for compacted topic " + topicPartition);
                    return false;
                } else if (Topic.isInternal(topicPartition.topic())) {
                    log.warn("Tiering cannot be enabled for internal topic " + topicPartition);
                    return false;
                }
                return true;
            }
            return false;
        }

        public TierPartitionState tierPartitionState() {
            return tierPartitionState;
        }

        public OptionalInt epochIfLeader() {
            return epochIfLeader;
        }
    }
}
