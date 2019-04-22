/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.state.TierPartitionStatus;
import kafka.tier.serdes.TierPartitionStateHeader;

import java.nio.ByteBuffer;
import java.util.Objects;

public class FileTierPartitionStateHeader {
    private final TierPartitionStateHeader header;

    public FileTierPartitionStateHeader(TierPartitionStateHeader header) {
        this.header = header;
    }

    public FileTierPartitionStateHeader(byte version,
                                        int tierEpoch,
                                        TierPartitionStatus status) {
        if (tierEpoch < -1) {
            throw new IllegalArgumentException(String.format("Illegal tierEpoch supplied %d.", tierEpoch));
        }

        final FlatBufferBuilder builder = new FlatBufferBuilder(100).forceDefaults(true);
        TierPartitionStateHeader.startTierPartitionStateHeader(builder);
        TierPartitionStateHeader.addTierEpoch(builder, tierEpoch);
        TierPartitionStateHeader.addVersion(builder, version);
        TierPartitionStateHeader.addStatus(builder, TierPartitionStatus.toByte(status));
        final int entryId = kafka.tier.serdes.TierPartitionStateHeader.endTierPartitionStateHeader(builder);
        builder.finish(entryId);
        this.header = TierPartitionStateHeader.getRootAsTierPartitionStateHeader(builder.dataBuffer());
    }

    public TierPartitionStateHeader header() {
        return header;
    }

    public ByteBuffer payloadBuffer() {
        return header.getByteBuffer().duplicate();
    }

    public int tierEpoch() {
        return header.tierEpoch();
    }

    public TierPartitionStatus status() {
        return TierPartitionStatus.fromByte(header.status());
    }

    public short version() {
        return header.version();
    }

    @Override
    public String toString() {
        return String.format("FileTierPartitionStateHeader(tierEpoch='%s', status=%s,"
                        + " version=%s", tierEpoch(), status(), version());
    }

    public int hashCode() {
        return Objects.hash(version(), tierEpoch(), status());
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FileTierPartitionStateHeader that = (FileTierPartitionStateHeader) o;
        return Objects.equals(version(), that.version())
                && Objects.equals(tierEpoch(), that.tierEpoch())
                && Objects.equals(status(), that.status());
    }
}
