/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.SegmentDeleteComplete;
import kafka.tier.state.OffsetAndEpoch;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;

import static kafka.tier.serdes.OffsetAndEpoch.createOffsetAndEpoch;
import static kafka.tier.serdes.UUID.createUUID;

/**
 * Delete complete metadata. The schema for this file is defined in
 * <a href="file:core/src/main/resources/serde/immutable/segment_delete_complete.fbs">segment_delete_complete.fbs</a>
 */
public class TierSegmentDeleteComplete extends AbstractTierSegmentMetadata {
    // version v1 added stateOffsetAndEpoch for use in state restoration
    private final static byte VERSION_V1 = 1;
    private final static byte CURRENT_VERSION = VERSION_V1;
    private final static int INITIAL_BUFFER_SIZE = 64;

    private final TopicIdPartition topicIdPartition;
    private final SegmentDeleteComplete metadata;

    public TierSegmentDeleteComplete(TopicIdPartition topicIdPartition,
                                     int tierEpoch,
                                     UUID objectId,
                                     Optional<OffsetAndEpoch> stateOffsetAndEpoch) {
        FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);
        SegmentDeleteComplete.startSegmentDeleteComplete(builder);

        SegmentDeleteComplete.addVersion(builder, CURRENT_VERSION);
        SegmentDeleteComplete.addTierEpoch(builder, tierEpoch);
        stateOffsetAndEpoch.ifPresent(offsetAndEpoch -> {
            int offsetAndEpochId = createOffsetAndEpoch(builder, offsetAndEpoch.offset(),
                    offsetAndEpoch.epoch().orElse(-1));
            SegmentDeleteComplete.addStateOffsetAndEpoch(builder, offsetAndEpochId);
        });
        int objectIdOffset = createUUID(builder, objectId.getMostSignificantBits(), objectId.getLeastSignificantBits());
        SegmentDeleteComplete.addObjectId(builder, objectIdOffset);

        int entryId = SegmentDeleteComplete.endSegmentDeleteComplete(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = SegmentDeleteComplete.getRootAsSegmentDeleteComplete(builder.dataBuffer());
    }

    public TierSegmentDeleteComplete(TopicIdPartition topicIdPartition, SegmentDeleteComplete metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    public TierSegmentDeleteComplete(TopicIdPartition topicIdPartition,
                                     int tierEpoch,
                                     UUID objectId,
                                     OffsetAndEpoch stateOffset) {
        this(topicIdPartition, tierEpoch, objectId, Optional.of(stateOffset));
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.SegmentDeleteComplete;
    }

    @Override
    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    @Override
    public int tierEpoch() {
        return metadata.tierEpoch();
    }

    @Override
    public OffsetAndEpoch stateOffsetAndEpoch() {
        return metadata.stateOffsetAndEpoch() == null ?
                OffsetAndEpoch.EMPTY :
                new OffsetAndEpoch(metadata.stateOffsetAndEpoch());
    }

    @Override
    public ByteBuffer payloadBuffer() {
        return metadata.getByteBuffer().duplicate();
    }

    @Override
    public TierObjectMetadata.State state() {
        return TierObjectMetadata.State.SEGMENT_DELETE_COMPLETE;
    }

    @Override
    public UUID messageId() {
        return new UUID(metadata.objectId().mostSignificantBits(), metadata.objectId().leastSignificantBits());
    }

    @Override
    public int expectedSizeLatestVersion() {
        return INITIAL_BUFFER_SIZE;
    }

    @Override
    public String toString() {
        return "TierSegmentDeleteComplete(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "objectIdAsBase64=" + objectIdAsBase64() + ", " +
                "stateOffsetAndEpoch=" + stateOffsetAndEpoch() +
                ")";
    }
}
