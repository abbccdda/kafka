/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.state;

import kafka.log.Log$;
import kafka.tier.TopicIdPartition;
import kafka.tier.domain.AbstractTierMetadata;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.domain.TierTopicInitLeader;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryTierPartitionState implements TierPartitionState {
    private final ConcurrentNavigableMap<Long, TierObjectMetadata> segmentMap = new ConcurrentSkipListMap<>();
    private final AtomicInteger currentEpoch = new AtomicInteger(-1);
    private final TopicPartition topicPartition;
    private TopicIdPartition topicIdPartition;

    private File dir;
    private Object segmentMapLock = new Object();

    private volatile TierPartitionStatus status;
    private volatile boolean tieringEnabled;
    private volatile boolean closed = false;
    private volatile Long committedEndOffset = null;

    public MemoryTierPartitionState(File dir, TopicPartition topicPartition, boolean tieringEnabled) {
        this.dir = dir;
        this.status = TierPartitionStatus.CLOSED;
        this.topicPartition = topicPartition;
        this.tieringEnabled = tieringEnabled;
        maybeOpen();
    }

    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public File dir() {
        return dir;
    }

    public void setTopicIdPartition(TopicIdPartition topicIdPartition) {
        if (this.topicIdPartition != null && ! this.topicIdPartition.equals(topicIdPartition))
            throw new IllegalStateException("TierPartitionState assigned a different "
                    + "topicIdPartition than already assigned (" + topicIdPartition
                    + " " + this.topicIdPartition);

        this.topicIdPartition = topicIdPartition;
    }

    public Optional<TopicIdPartition> topicIdPartition() {
        return Optional.ofNullable(topicIdPartition);
    }

    Optional<TierObjectMetadata> lastSegmentMetadata() {
        Map.Entry<Long, TierObjectMetadata> lastEntry = segmentMap.lastEntry();
        if (lastEntry != null) {
            return metadata(lastEntry.getKey());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Long> committedEndOffset() {
        return Optional.ofNullable(committedEndOffset);
    }

    @Override
    public Optional<Long> endOffset() {
        return lastSegmentMetadata().map(TierObjectMetadata::endOffset);
    }

    @Override
    public Optional<Long> startOffset() {
        Map.Entry<Long, TierObjectMetadata> firstEntry = segmentMap.firstEntry();
        if (firstEntry != null)
            return Optional.of(firstEntry.getKey());
        return Optional.empty();
    }

    @Override
    public Future<TierObjectMetadata> materializationListener(long targetOffset) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("offsetListener not supported for MemoryTierPartitionState.");
    }

    @Override
    public TierPartitionStatus status() {
        return status;
    }

    @Override
    public void updateDir(File dir) {
        this.dir = dir;
    }

    @Override
    public long totalSize() {
        long size = 0;
        for (TierObjectMetadata tierObjectMetadata : segmentMap.values())
            size += tierObjectMetadata.size();
        return size;
    }

    @Override
    public int tierEpoch() {
        return currentEpoch.get();
    }

    @Override
    public boolean tieringEnabled() {
        return tieringEnabled;
    }

    @Override
    public void onTieringEnable() {
        tieringEnabled = true;
        maybeOpen();
    }

    @Override
    public TierPartitionState.AppendResult append(AbstractTierMetadata entry) {
        if (!status.isOpenForWrite() || closed) {
            return AppendResult.ILLEGAL;
        } else if (entry instanceof TierTopicInitLeader) {
            return append((TierTopicInitLeader) entry);
        } else if (entry instanceof TierObjectMetadata) {
            return append((TierObjectMetadata) entry);
        } else {
            throw new RuntimeException(String.format("Unknown AbstractTierMetadataType %s", entry));
        }
    }

    @Override
    public String path() {
        return dir.getAbsolutePath();
    }

    @Override
    public int numSegments() {
        return segmentMap.size();
    }

    @Override
    public NavigableSet<Long> segmentOffsets() {
        return segmentMap.keySet();
    }

    @Override
    public NavigableSet<Long> segmentOffsets(long from, long to) {
        return Log$.MODULE$.logSegments(segmentMap, from, to, segmentMapLock).keySet();
    }

    @Override
    public Optional<TierObjectMetadata> metadata(long targetOffset) {
        Map.Entry<Long, TierObjectMetadata> entry = segmentMap.floorEntry(targetOffset);
        if (entry != null)
            return Optional.of(entry.getValue());
        else
            return Optional.empty();
    }

    public void flush() {
        committedEndOffset = endOffset().orElse(null);
    }

    @Override
    public void beginCatchup() {
        if (!tieringEnabled)
            throw new IllegalStateException("Illegal state for tier partition state");
        status = TierPartitionStatus.CATCHUP;
    }

    @Override
    public void onCatchUpComplete() {
        if (!tieringEnabled)
            throw new IllegalStateException("Illegal state for tier partition state");
        status = TierPartitionStatus.ONLINE;
    }

    public void close() {
        synchronized (segmentMapLock) {
            segmentMap.clear();
            closed = true;
        }
    }

    @Override
    public void closeHandlers() {
        close();
    }

    public void delete() {
        close();
    }

    private void maybeOpen() {
        if (tieringEnabled)
            status = TierPartitionStatus.INIT;
    }

    private AppendResult append(TierObjectMetadata objectMetadata) {
        if (objectMetadata.tierEpoch() == tierEpoch()) {
            Optional<Long> endOffset = endOffset();
            if (!endOffset.isPresent() || objectMetadata.endOffset() > endOffset.get()) {
                // As there may be arbitrary overlap between segments, it is possible for a new
                // segment to completely overlap a previous segment. We rely on on lookup via the
                // start offset, and if we insert into the lookup map with the raw offset, it is possible
                // for portions of a segment to be unfetchable unless we bound overlapping segments
                // in the lookup map. e.g. if [100 - 200] is in the map at 100, and we insert [50 - 250]
                // at 50, the portion 201 - 250 will be inaccessible.
                segmentMap.put(Math.max(endOffset().orElse(-1L) + 1, objectMetadata.startOffset()), objectMetadata);
                return AppendResult.ACCEPTED;
            }
        }
        return AppendResult.FENCED;
    }

    private AppendResult append(TierTopicInitLeader initLeader) {
        if (initLeader.tierEpoch() >= currentEpoch.get()) {
            currentEpoch.set(initLeader.tierEpoch());
            return AppendResult.ACCEPTED;
        } else {
            return AppendResult.FENCED;
        }
    }
}
