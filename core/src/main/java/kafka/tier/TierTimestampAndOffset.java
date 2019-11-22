/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.tier.store.TierObjectStore;
import org.apache.kafka.common.record.FileRecords;

import java.util.Objects;
import java.util.Optional;

public class TierTimestampAndOffset implements FileRecords.TimestampAndOffset {
    public final TierObjectStore.ObjectMetadata metadata;
    public final long timestamp;
    public final int segmentSize;

    public TierTimestampAndOffset(long timestamp,
                                  TierObjectStore.ObjectMetadata metadata,
                                  int segmentSize) {
        this.timestamp = timestamp;
        this.metadata = metadata;
        this.segmentSize = segmentSize;
    }


    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public Optional<Integer> leaderEpoch() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TierTimestampAndOffset that = (TierTimestampAndOffset) o;
        return timestamp == that.timestamp &&
                metadata.equals(that.metadata) &&
                segmentSize == that.segmentSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, metadata, segmentSize);
    }

    @Override
    public String toString() {
        return "TierTimestampAndOffset(" +
                "timestamp=" + timestamp +
                ", metadata=" + metadata +
                ", segmentSize=" + segmentSize +
                ')';
    }
}
