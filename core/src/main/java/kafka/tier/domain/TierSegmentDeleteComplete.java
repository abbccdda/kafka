/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.SegmentDeleteComplete;

import java.nio.ByteBuffer;
import java.util.UUID;

import static kafka.tier.serdes.UUID.createUUID;

public class TierSegmentDeleteComplete extends AbstractTierSegmentMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int INITIAL_BUFFER_SIZE = 60;

    private final TopicIdPartition topicIdPartition;
    private final SegmentDeleteComplete metadata;

    public TierSegmentDeleteComplete(TopicIdPartition topicIdPartition,
                                     int tierEpoch,
                                     UUID objectId) {
        FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);
        SegmentDeleteComplete.startSegmentDeleteComplete(builder);

        SegmentDeleteComplete.addVersion(builder, CURRENT_VERSION);
        SegmentDeleteComplete.addTierEpoch(builder, tierEpoch);
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
    public String toString() {
        return "TierSegmentDeleteComplete(" +
                "version=" + metadata.version() + ", " +
                "topic=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "objectId=" + objectId() + ")";
    }
}
