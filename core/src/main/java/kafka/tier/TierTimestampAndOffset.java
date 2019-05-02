/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier;

import kafka.tier.domain.TierObjectMetadata;
import org.apache.kafka.common.record.FileRecords;

import java.util.Objects;
import java.util.Optional;

public class TierTimestampAndOffset implements FileRecords.TimestampAndOffset {
    public final TierObjectMetadata metadata;
    public final long timestamp;

    public TierTimestampAndOffset(long timestamp, TierObjectMetadata metadata) {
        this.timestamp = timestamp;
        this.metadata = metadata;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TierTimestampAndOffset that = (TierTimestampAndOffset) o;
        return timestamp == that.timestamp &&
                metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, metadata);
    }

    @Override
    public String toString() {
        return "TierTimestampAndOffset(" +
                "timestamp=" + timestamp +
                ", metadata=" + metadata +
                ')';
    }
}
