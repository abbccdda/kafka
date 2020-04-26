/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
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
     * Get an iterator for all readable tiered segments. The returned list is in order of base
     * offset
     * @return list of tiered seggments
     */
    Iterator<TierObjectMetadata> segments();

    /**
     * Get an iterator for all readable tiered segments in a given range. The iterator is sorted by base offset.
     * @param from Start of the range, include segment which contains "from" (inclusive)
     * @param to End of the range, upper bound exclusive offset to include or the end of the log if "to" is past the end
     * @return list of tiered segments
     */
    Iterator<TierObjectMetadata> segments(long from, long to);

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
     * Called when tiering is enabled for this tier topic partition.
     * @throws IOException
     */
    void enableTierConfig() throws IOException;

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
    Future<TierObjectMetadata> materializationListener(long targetOffset) throws IOException;

    /**
     * Return the current status of the TierPartitionState.
     * @return TierPartitionStatus
     */
    TierPartitionStatus status();

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
