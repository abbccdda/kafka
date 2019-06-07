package kafka.tier.state;

import kafka.log.TierLogSegment;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.store.TierObjectStore;
import org.apache.kafka.common.errors.KafkaStorageException;

import java.io.IOException;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class TierUtils {
    private static Optional<MetadataWithOffset> metadataForOffset(TierPartitionState partitionState,
                                                                  long offset) throws KafkaStorageException {
        try {
            return partitionState.metadata(offset).map(metadata -> new MetadataWithOffset(offset, metadata));
        } catch (IOException e) {
            throw new KafkaStorageException(e);
        }
    }

    public static NavigableSet<TierLogSegment> tieredSegments(NavigableSet<Long> tieredOffsets,
                                                              TierPartitionState partitionState,
                                                              Optional<TierObjectStore> objectStore) {
        return tieredOffsets
                .stream()
                .map(offset -> metadataForOffset(partitionState, offset))
                .filter(metadataOpt -> metadataOpt.isPresent())
                .map(metadataOpt -> new TierLogSegment(metadataOpt.get().metadata, metadataOpt.get().startOffset, objectStore.get()))
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparingLong(segment -> segment.startOffset()))));
    }

    private static class MetadataWithOffset {
        long startOffset;  // the start offset of a segment may be different from the base offset, as there could be overlaps
        TierObjectMetadata metadata;

        MetadataWithOffset(long startOffset, TierObjectMetadata metadata) {
            this.startOffset = startOffset;
            this.metadata = metadata;
        }
    }
}
