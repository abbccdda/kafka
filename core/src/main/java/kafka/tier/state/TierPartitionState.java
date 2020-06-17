/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierPartitionForceRestore;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface TierPartitionState {
    /**
     * The result of an attempt to append a tier metadata entry.
     */
    enum AppendResult {
        // the tier partition status has not been initialized, or it has been closed/deleted
        NOT_TIERABLE,
        // the entry was materialized but was fenced
        FENCED,
        // the entry was materialized
        ACCEPTED,
        // the entry could not be materialized
        FAILED,
        // the entry was fenced due to a later restore offset and epoch
        RESTORE_FENCED
    }

    /**
     * The result of an attempt to restore a TierPartitionState via a PartitionForceRestore event
     */
    enum RestoreResult {
        // the restore attempt succeeded and the state is now the new state
        SUCCEEDED,
        // the restore attempted failed
        FAILED
    }

    /**
     * The topic-partition corresponding to this TierPartition.
     * @return topic-partition
     */
    TopicPartition topicPartition();

    /**
     * Optional TopicIdPartition corresponding to this TierPartition
     * If one has not been set, returns empty
     * @return TopicIdPartition
     */
    Optional<TopicIdPartition> topicIdPartition();

    /**
     * The directory where the TierPartition is stored on disk.
     * @return file handle for the directory
     */
    File dir();

    /**
     * Determine start offset spanned by the TierPartitionState.
     * @return start offset
     * @throws IOException
     */
    Optional<Long> startOffset() throws IOException;

    /**
     * Sets the TopicIdPartition for this TierPartitionState.
     * Will open the TierPartitionState if tiering is enabled, and a TopicIdPartition was not
     * previously set or able to be read from the FileTierPartitionState header.
     * @param topicId The topic id to assign
     * @return true if a TopicIdPartition was not set and one has now been set, false otherwise
     * @throws IOException
     * @throws IllegalStateException on topic id mismatch, if it had already been set before
     */
    boolean setTopicId(UUID topicId) throws IOException;

    /**
     * Return the end offset spanned by the TierPartitionState that has been committed to disk.
     * @return end offset or -1 if not set
     * @throws IOException
     */
    long committedEndOffset() throws IOException;

    /**
     * Return the uncommitted end offset spanned by the TierPartitionState.
     * @return end offset or -1 if not set
     * @throws IOException
     */
    long endOffset() throws IOException;

    /**
     * Return the last materialized source offset and epoch
     * @throws IOException
     */
    OffsetAndEpoch lastLocalMaterializedSrcOffsetAndEpoch();

    /**
     * Scan the ObjectMetadata (segment) entries in this tier partition, and return the count.
     * @return number of tiered segments
     */
    int numSegments();

    /**
     * Count of the number of segments lying within a range
     * @param from Start of the range, include segment which contains "from" (inclusive)
     * @param to End of the range, upper bound exclusive offset to include or the end of the log if "to" is past the end
     * @return number of tiered segments
     */
    int numSegments(long from, long to);

    /**
     * Get an iterator for all readable tiered segments. The returned list is in order of base offset.
     * The iterator returned must be closed after usage ends to ensure that a read lock is released
     * @return an iterator of tiered segments. NOTE: the iterator *must* be closed by the caller after usage.
     */
    CloseableIterator<TierObjectMetadata> segments();

    /**
     * Get an iterator for all readable tiered segments in a given range. The iterator is sorted by base offset.
     * The iterator returned must be closed after usage ends to ensure that a read lock is released
     * @param from Start of the range, include segment which contains "from" (inclusive)
     * @param to End of the range, upper bound exclusive offset to include or the end of the log if "to" is past the end
     * @return an iterator of tiered segments. NOTE: the iterator *must* be closed by the caller after usage.
     */
    CloseableIterator<TierObjectMetadata> segments(long from, long to);

    /**
     * Lookup the TierObjectMetadata which will contain data for a target offset.
     * @param targetOffset the target offset to lookup the overlapping or next metadata for.
     * @return The TierObjectMetadata, if any.
     * @throws IOException if disk error encountered
     */
    Optional<TierObjectMetadata> metadata(long targetOffset) throws IOException;

    Collection<TierObjectMetadata> fencedSegments();

    /**
     * Appends abstract metadata to the tier partition.
     * Dispatches to more specific append method.
     * When appending a TierTopicInitLeader entry, it may advance the tierEpoch.
     * When appending a TierObjectMetadata entry, it may append the tier metadata to the tier
     * partition log file.
     * @param tierMetadata AbstractTierMetadata entry to be appended to the tier partition log.
     * @param sourceOffsetAndEpoch Offset and epoch corresponding to this metadata entry
     * @return Returns an AppendResult denoting the result of the append action.
     */
    AppendResult append(AbstractTierMetadata tierMetadata, OffsetAndEpoch sourceOffsetAndEpoch);


    /**
     * Performs a TierPartitionState restore, swapping the current state file
     * from buffer and replacing any internal state with the contents of the new TierPartitionState,
     * setting the TierPartitionStatus of the state to status.
     * @param metadata the TierPartitionForceRestore metadata including the coordinates and
     *                 metadata about the state to be restored.
     * @param targetState ByteBuffer containing the contents of the TierPartitionState to restore
     * @param targetStatus the status that the TierPartitionState will be transitioned to upon
     *                     successful restoration
     * @param sourceOffsetAndEpoch Offset and epoch corresponding to this metadata entry
     * @return Returns a RestoreResult:
     *           - When SUCCEEDED, it means the restore was successful, and the `TierPartitionState` status has been set to the `targetState`.
     *           - When FAILED, it means the restore failed, and the `TierPartitionState` status has been set to the `ERROR` status.
     *             Apart from the status change, it is expected that the underlying state has not undergone other significant modifications during this failure.
     */
    RestoreResult restoreState(TierPartitionForceRestore metadata,
                               ByteBuffer targetState,
                               TierPartitionStatus targetStatus,
                               OffsetAndEpoch sourceOffsetAndEpoch);

    /**
     * Sum the size of all segment spanned by this TierPartitionState.
     * @return total size
     * @throws IOException
     */
    long totalSize() throws IOException;

    /**
     * Return the current tierEpoch.
     * Metadata will only be added to TierPartitionState if the metadata's
     * tierEpoch is equal to the TierPartitionState's tierEpoch.
     * @return tierEpoch
     */
    int tierEpoch() throws IOException;

    boolean isTieringEnabled();

    /**
     * Indicates if a partition may have some tiered data, essentially whether tiered storage is currently enabled or was enabled
     * earlier for this partition.
     * This method is called by work flows that require access to existing tiered segments, no matter the current status of
     * tierEnable configuration. For example, when tierEnable is switched to false for a given topic partition, we still need
     * to access tiered segments to serve fetch requests and open the TierPartitionState channel for keeping track of current
     * leader epoch and tiered segments as they get deleted.
     * @return true when tiering is enabled currently, or at any time in the past; false otherwise
     */
    boolean mayContainTieredData();

    /**
     * Called when a replica receives OFFSET_TIERED exception while replicating from leader. In case when tier storage has
     * been disabled for the partition, a new replica added to the assignment will neither see tier.enable config set, nor
     * will it have the flushed(persistent) TierPartitionState file from when tier.enable was true. These are the conditions
     * to open up a TierPartitionState file for materializing the tier state for a tiered partition. This method opens the
     * TierPartitionState file for such replicas. Caller will use the returned value to register FileTierPartitionState with
     * TierTopicConsumer for materialization.
     * @return true, if TierPartitionState file was opened by this method; false otherwise.
     * @throws IOException
     */
    boolean maybeOpenChannelOnOffsetTieredException() throws IOException;

    /**
     * Called when tiering is enabled for this tier topic partition.
     * @return true if the TierPartitionState channel is opened by this method; false if the channel was already open.
     * Caller will use this flag to register TierPartitionState with TierTopicConsumer for materialization.
     * @throws IOException
     */
    boolean enableTierConfig() throws IOException;

    /**
     * Reset the tier(ing) enabled flag at TierPartitionState. This function is called when tiered storage is disabled
     * for the partition.
     */
    void disableTierConfig();

    /**
     * flush data contained in this TierPartitionState to disk.
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Begin catchup phase for tier partition state.
     */
    void beginCatchup();

    /**
     * Mark catchup completed for tier partition state.
     */
    void onCatchUpComplete();

    /**
     * Sets up a listener for this tier partition state for use by the replica fetcher.
     * Returns a future that is completed when TierPartitionState endOffset >= targetOffset
     * with the TierObjectMetadata that covers targetOffset.
     *
     * @param targetOffset the offset awaiting materialization
     * @return future containing the TierObjectMetadata.
     * @throws IOException
     */
    Future<TierObjectMetadata> materializeUpto(long targetOffset) throws IOException;

    /**
     * Sets up a listener for this tier partition state.
     * Returns a future that is completed when tierEpoch() >= targetEpoch
     *
     * @param targetEpoch the leader epoch awaiting materialization
     * @return CompletableFuture containing Optional TierObjectMetadata for last segment.
     * @throws IOException
     */
    CompletableFuture<Optional<TierObjectMetadata>> materializeUptoEpoch(int targetEpoch) throws IOException;

    /**
     * Return the current status of the TierPartitionState.
     * @return TierPartitionStatus
     */
    TierPartitionStatus status();

    /**
     * Return the current tier materialization lag.
     * @return long
     */
    long materializationLag();

    /**
     * Update the directory reference for the log and indices in this segment. This would typically be called after a
     * directory is renamed.
     * @param dir The new directory
     */
    void updateDir(File dir);

    /**
     * Delete this TierPartitionState from local storage.
     * @throws IOException
     */
    void delete() throws IOException;

    /**
     * Close TierPartition, flushing to disk.
     * @throws IOException
     */
    void close() throws IOException;

    /**
     *
     * @throws IOException
     */
    void closeHandlers() throws IOException;
}
