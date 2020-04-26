package kafka.tier.state;

import kafka.log.TierLogSegment;
import kafka.tier.domain.TierObjectMetadata;
import kafka.tier.store.TierObjectStore;
import org.apache.kafka.common.errors.KafkaStorageException;

import java.io.IOException;
import java.util.Optional;

public class TierUtils {
    private static Optional<MetadataWithOffset> metadataForOffset(TierPartitionState partitionState,
                                                                  long offset) throws KafkaStorageException {
        try {
            return partitionState.metadata(offset).map(metadata -> new MetadataWithOffset(offset, metadata));
        } catch (IOException e) {
            throw new KafkaStorageException(e);
        }
    }

    public static Optional<TierLogSegment> tierLogSegmentForOffset(TierPartitionState partitionState,
                                                                   long offset,
                                                                   Optional<TierObjectStore> objectStore) {
        return metadataForOffset(partitionState, offset)
                .map(metadataOpt -> new TierLogSegment(metadataOpt.metadata, metadataOpt.startOffset, objectStore.get()));
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
