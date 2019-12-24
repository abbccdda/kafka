/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.state;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.serdes.MaterializationTrackingInfo;
import kafka.tier.serdes.TierPartitionStateHeader;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * Header for the tier partition state file. The schema for this is defined in
 * <a href="file:core/src/main/resources/serde/mutable/tier_partition_state_header.fbs">tier_partition_state.fbs</a>
 */
public class Header {
    static final int HEADER_LENGTH_LENGTH = 2;

    private final TierPartitionStateHeader header;
    private final MaterializationInfo materializationInfo;

    Header(TierPartitionStateHeader header) {
        this.header = header;
        this.materializationInfo = new MaterializationInfo(header.materializationInfo());
    }

    Header(UUID topicId,
           byte version,
           int tierEpoch,
           TierPartitionStatus status,
           long endOffset,
           long localMaterializedOffset) {
        if (tierEpoch < -1)
            throw new IllegalArgumentException("Illegal tierEpoch " + tierEpoch);

        final FlatBufferBuilder builder = new FlatBufferBuilder(100).forceDefaults(true);
        final int materializedInfo = MaterializationTrackingInfo.createMaterializationTrackingInfo(
                builder,
                -1,
                localMaterializedOffset);
        TierPartitionStateHeader.startTierPartitionStateHeader(builder);
        int topicIdOffset = kafka.tier.serdes.UUID.createUUID(builder,
                topicId.getMostSignificantBits(),
                topicId.getLeastSignificantBits());
        TierPartitionStateHeader.addTopicId(builder, topicIdOffset);
        TierPartitionStateHeader.addTierEpoch(builder, tierEpoch);
        TierPartitionStateHeader.addVersion(builder, version);
        TierPartitionStateHeader.addStatus(builder, TierPartitionStatus.toByte(status));
        TierPartitionStateHeader.addEndOffset(builder, endOffset);
        TierPartitionStateHeader.addMaterializationInfo(builder, materializedInfo);
        final int entryId = kafka.tier.serdes.TierPartitionStateHeader.endTierPartitionStateHeader(builder);
        builder.finish(entryId);
        this.header = TierPartitionStateHeader.getRootAsTierPartitionStateHeader(builder.dataBuffer());
        this.materializationInfo = new MaterializationInfo(header.materializationInfo());
    }

    ByteBuffer payloadBuffer() {
        return header.getByteBuffer().duplicate();
    }

    int tierEpoch() {
        return header.tierEpoch();
    }

    UUID topicId() {
        return new UUID(header.topicId().mostSignificantBits(),
                header.topicId().leastSignificantBits());
    }

    TierPartitionStatus status() {
        return TierPartitionStatus.fromByte(header.status());
    }

    long size() {
        return payloadBuffer().remaining() + HEADER_LENGTH_LENGTH;
    }

    short version() {
        return header.version();
    }

    long endOffset() {
        return header.endOffset();
    }

    long localMaterializedOffset() {
        return materializationInfo.localMaterializedOffset();
    }

    long globalMaterializedOffset() {
        return materializationInfo.globalMaterializedOffset();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Header that = (Header) o;
        return Objects.equals(version(), that.version()) &&
                Objects.equals(topicId(), that.topicId()) &&
                Objects.equals(tierEpoch(), that.tierEpoch()) &&
                Objects.equals(status(), that.status()) &&
                Objects.equals(endOffset(), that.endOffset()) &&
                Objects.equals(materializationInfo, that.materializationInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version(), topicId(), tierEpoch(), status(), endOffset(), materializationInfo);
    }

    @Override
    public String toString() {
        return "Header(" +
                "version=" + version() + ", " +
                "topicId=" + topicId() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "status=" + status() + ", " +
                "endOffset=" + endOffset() + ", " +
                "materializationInfo=" + materializationInfo +
                ")";
    }

    private static class MaterializationInfo {
        private final MaterializationTrackingInfo info;

        MaterializationInfo(MaterializationTrackingInfo info) {
            if (info == null) {
                // MaterializationInfo was added in v2 so it is possible for it to not exist. Build a buffer with
                // default values in this case.
                final FlatBufferBuilder builder = new FlatBufferBuilder(100);
                MaterializationTrackingInfo.startMaterializationTrackingInfo(builder);
                final int entryId = MaterializationTrackingInfo.endMaterializationTrackingInfo(builder);
                builder.finish(entryId);
                this.info = MaterializationTrackingInfo.getRootAsMaterializationTrackingInfo(builder.dataBuffer());
            } else {
                this.info = info;
            }
        }

        long localMaterializedOffset() {
            return info.localMaterializedOffset();
        }

        long globalMaterializedOffset() {
            return info.globalMaterializedOffset();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            MaterializationInfo that = (MaterializationInfo) o;
            return Objects.equals(localMaterializedOffset(), that.localMaterializedOffset()) &&
                    Objects.equals(globalMaterializedOffset(), that.globalMaterializedOffset());
        }

        @Override
        public int hashCode() {
            return Objects.hash(localMaterializedOffset(), globalMaterializedOffset());
        }

        @Override
        public String toString() {
            return "MaterializationInfo(" +
                    "localMaterializedOffset=" + localMaterializedOffset() + ", " +
                    "globalMaterializedOffset=" + globalMaterializedOffset() +
                    ")";
        }
    }
}
