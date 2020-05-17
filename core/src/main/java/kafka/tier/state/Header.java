/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.state;

import com.google.flatbuffers.FlatBufferBuilder;

import kafka.tier.serdes.MaterializationTrackingInfo;
import kafka.tier.serdes.TierPartitionStateHeader;
import static kafka.tier.serdes.OffsetAndEpoch.createOffsetAndEpoch;
import kafka.utils.CoreUtils;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Header for the tier partition state file. The schema for this is defined in
 * <a href="file:core/src/main/resources/serde/mutable/tier_partition_state_header.fbs">tier_partition_state.fbs</a>
 */
public class Header {
    // Length (in bytes) of the header section containing the length of the header.
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
           long startOffset,
           long endOffset,
           OffsetAndEpoch globalMaterializedOffsetAndEpoch,
           OffsetAndEpoch localMaterializedOffsetAndEpoch,
           OffsetAndEpoch errorOffsetAndEpoch,
           OffsetAndEpoch lastRestoredOffsetAndEpoch) {
        if (tierEpoch < -1)
            throw new IllegalArgumentException("Illegal tierEpoch " + tierEpoch);

        final FlatBufferBuilder builder = new FlatBufferBuilder(100).forceDefaults(true);
        final int materializedInfo = MaterializationTrackingInfo.createMaterializationTrackingInfo(
                builder,
                globalMaterializedOffsetAndEpoch.offset(),
                localMaterializedOffsetAndEpoch.offset(),
                globalMaterializedOffsetAndEpoch.epoch().orElse(-1),
                localMaterializedOffsetAndEpoch.epoch().orElse(-1));

        TierPartitionStateHeader.startTierPartitionStateHeader(builder);
        final int topicIdOffset = kafka.tier.serdes.UUID.createUUID(builder,
                topicId.getMostSignificantBits(),
                topicId.getLeastSignificantBits());
        TierPartitionStateHeader.addTopicId(builder, topicIdOffset);
        TierPartitionStateHeader.addTierEpoch(builder, tierEpoch);
        TierPartitionStateHeader.addVersion(builder, version);
        TierPartitionStateHeader.addStatus(builder, TierPartitionStatus.toByte(status));
        TierPartitionStateHeader.addStartOffset(builder, startOffset);
        TierPartitionStateHeader.addEndOffset(builder, endOffset);
        TierPartitionStateHeader.addMaterializationInfo(builder, materializedInfo);
        final int errorOffsetAndEpochId = createOffsetAndEpoch(
                builder,
                errorOffsetAndEpoch.offset(),
                errorOffsetAndEpoch.epoch().orElse(-1));
        TierPartitionStateHeader.addErrorOffsetAndEpoch(builder, errorOffsetAndEpochId);
        final int restoreId = kafka.tier.serdes.OffsetAndEpoch.createOffsetAndEpoch(
                builder, lastRestoredOffsetAndEpoch.offset(),
                lastRestoredOffsetAndEpoch.epoch().orElse(-1));
        TierPartitionStateHeader.addRestoreOffsetAndEpoch(builder, restoreId);
        final int entryId = kafka.tier.serdes.TierPartitionStateHeader.endTierPartitionStateHeader(builder);
        builder.finish(entryId);
        this.header = TierPartitionStateHeader.getRootAsTierPartitionStateHeader(builder.dataBuffer());
        this.materializationInfo = new MaterializationInfo(header.materializationInfo());
    }

    ByteBuffer payloadBuffer() {
        return header.getByteBuffer().duplicate();
    }

    public int tierEpoch() {
        return header.tierEpoch();
    }

    public UUID topicId() {
        return new UUID(header.topicId().mostSignificantBits(),
                header.topicId().leastSignificantBits());
    }

    public TierPartitionStatus status() {
        return TierPartitionStatus.fromByte(header.status());
    }

    public long startOffset() {
        return header.startOffset();
    }

    public long endOffset() {
        return header.endOffset();
    }

    long size() {
        return payloadBuffer().remaining() + HEADER_LENGTH_LENGTH;
    }

    short version() {
        return header.version();
    }

    public OffsetAndEpoch localMaterializedOffsetAndEpoch() {
        return materializationInfo.localMaterializedOffsetAndEpoch();
    }

    public OffsetAndEpoch globalMaterializedOffsetAndEpoch() {
        return materializationInfo.globalMaterializedOffsetAndEpoch();
    }

    public OffsetAndEpoch restoreOffsetAndEpoch() {
        return header.restoreOffsetAndEpoch() == null ?
                OffsetAndEpoch.EMPTY :
                new OffsetAndEpoch(header.restoreOffsetAndEpoch());
    }

    public OffsetAndEpoch errorOffsetAndEpoch() {
        return header.errorOffsetAndEpoch() == null ?
            OffsetAndEpoch.EMPTY :
            new OffsetAndEpoch(header.errorOffsetAndEpoch());
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
                Objects.equals(startOffset(), that.startOffset()) &&
                Objects.equals(endOffset(), that.endOffset()) &&
                Objects.equals(errorOffsetAndEpoch(), that.errorOffsetAndEpoch()) &&
                Objects.equals(restoreOffsetAndEpoch(), that.restoreOffsetAndEpoch()) &&
                Objects.equals(materializationInfo, that.materializationInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            version(), topicId(), tierEpoch(), status(), startOffset(), endOffset(),
                restoreOffsetAndEpoch(), errorOffsetAndEpoch(), materializationInfo);
    }

    @Override
    public String toString() {
        return "Header(" +
                "version=" + version() + ", " +
                "topicId=" + CoreUtils.uuidToBase64(topicId()) + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "status=" + status() + ", " +
                "startOffset=" + startOffset() + ", " +
                "endOffset=" + endOffset() + ", " +
                "errorOffsetAndEpoch=" + errorOffsetAndEpoch() + ", " +
                "restoreOffsetAndEpoch=" + restoreOffsetAndEpoch() + ", " +
                "materializationInfo=" + materializationInfo +
                ")";
    }

    private static OffsetAndEpoch toOffsetAndEpoch(long offset, int epoch) {
        Optional<Integer> epochOpt = (epoch == -1) ? Optional.empty() : Optional.of(epoch);
        return new OffsetAndEpoch(offset, epochOpt);
    }

    static class MaterializationInfo {
        OffsetAndEpoch globalMaterializedOffsetAndEpoch;
        OffsetAndEpoch localMaterializedOffsetAndEpoch;

        MaterializationInfo(MaterializationTrackingInfo info) {
            if (info == null) {
                // MaterializationInfo was added in v2 so it is possible for it to not exist. Build a buffer with
                // default values in this case.
                final FlatBufferBuilder builder = new FlatBufferBuilder(100);
                MaterializationTrackingInfo.startMaterializationTrackingInfo(builder);
                final int entryId = MaterializationTrackingInfo.endMaterializationTrackingInfo(builder);
                builder.finish(entryId);
                info = MaterializationTrackingInfo.getRootAsMaterializationTrackingInfo(builder.dataBuffer());
            }

            globalMaterializedOffsetAndEpoch = toOffsetAndEpoch(info.globalMaterializedOffset(), info.globalMaterializedEpoch());
            localMaterializedOffsetAndEpoch = toOffsetAndEpoch(info.localMaterializedOffset(), info.localMaterializedEpoch());
        }

        OffsetAndEpoch globalMaterializedOffsetAndEpoch() {
            return globalMaterializedOffsetAndEpoch;
        }

        OffsetAndEpoch localMaterializedOffsetAndEpoch() {
            return localMaterializedOffsetAndEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            MaterializationInfo that = (MaterializationInfo) o;
            return Objects.equals(localMaterializedOffsetAndEpoch(), that.localMaterializedOffsetAndEpoch())
                    && Objects.equals(globalMaterializedOffsetAndEpoch(), that.globalMaterializedOffsetAndEpoch());
        }

        @Override
        public int hashCode() {
            return Objects.hash(localMaterializedOffsetAndEpoch(), globalMaterializedOffsetAndEpoch());
        }

        @Override
        public String toString() {
            return "MaterializationInfo(" +
                    "localMaterializedOffset=" + localMaterializedOffsetAndEpoch() + ", " +
                    "globalMaterializedOffset=" + globalMaterializedOffsetAndEpoch() +
                    ")";
        }
    }
}
