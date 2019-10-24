/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.tier.state.TierPartitionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Replica manager for tiered storage. Mainly deals with propagating replica state (leader, follower, deletion) to
 * relevant components.
 *
 * Listeners can be registered using {@link #addListener} and appropriate callbacks are fired for each lifecycle stage.
 * Notably, {@link kafka.tier.tasks.TierTasks} registers listeners so it can track the set of partitions for which tiering
 * is enabled, and drive archiver and retention tasks based on that.
 */
public class TierReplicaManager {
    private static final Logger log = LoggerFactory.getLogger(TierReplicaManager.class);
    private final List<ChangeListener> changeListeners = new ArrayList<>();

    /**
     * Called when replica is elected to be the leader. Fires off registered change listeners if partition is enabled
     * for tiering.
     * @param tierPartitionState Tier state file for partition becoming leader
     * @param leaderEpoch Leader epoch
     */
    public synchronized void becomeLeader(TierPartitionState tierPartitionState, int leaderEpoch) {
        if (tierPartitionState.isTieringEnabled()) {
            log.debug("Firing becomeLeader listeners for tiered topic {}", tierPartitionState.topicIdPartition());
            changeListeners.forEach(listener -> listener.onBecomeLeader(tierPartitionState.topicIdPartition().get(), leaderEpoch));
        }
    }

    /**
     * Called when replica becomes follower. Fires off registered change listeners if partition is enabled for tiering.
     * @param tierPartitionState Tier state file for partition becoming leader
     */
    public synchronized void becomeFollower(TierPartitionState tierPartitionState) {
        if (tierPartitionState.isTieringEnabled()) {
            log.debug("Firing becomeFollower listeners for tiered topic {}", tierPartitionState.topicIdPartition());
            changeListeners.forEach(listener -> listener.onBecomeFollower(tierPartitionState.topicIdPartition().get()));
        }
    }

    /**
     * Called when the partition is deleted from this broker. Fires off registered change listeners if partition was
     * enabled for tiering.
     * @param topicIdPartition Topic partition being deleted
     */
    public synchronized void delete(TopicIdPartition topicIdPartition) {
        log.debug("Firing onDelete listeners for tiered topic {}", topicIdPartition);
        changeListeners.forEach(listener -> listener.onDelete(topicIdPartition));
    }

    /**
     * Register a change listener.
     * @param listener Listener to register
     */
    public synchronized void addListener(ChangeListener listener) {
        changeListeners.add(listener);
    }

    /**
     * Interface to register callbacks on the lifecycle of tiering enabled topic partitions. The lifecycle typically
     * follows the following cycle:
     *
     * (onBecomeLeader|onBecomeFollower <-> onBecomeFollower|onBecomeLeader) -> onDelete
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
}
