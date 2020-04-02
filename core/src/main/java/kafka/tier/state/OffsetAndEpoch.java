/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.state;

import java.util.Objects;
import java.util.Optional;

public final class OffsetAndEpoch {
    private final long offset;
    private final Optional<Integer> epoch;

    public static final OffsetAndEpoch EMPTY = new OffsetAndEpoch(-1, Optional.empty());

    public OffsetAndEpoch(long offset, Optional<Integer> epoch) {
        this.offset = offset;
        this.epoch = epoch;
    }

    public long offset() {
        return offset;
    }

    public Optional<Integer> epoch() {
        return epoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        OffsetAndEpoch that = (OffsetAndEpoch) o;
        return offset == that.offset &&
                Objects.equals(epoch, that.epoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, epoch);
    }

    @Override
    public String toString() {
        return "OffsetAndEpoch(" +
                "offset=" + offset +
                ", epoch=" + epoch +
                ')';
    }
}
