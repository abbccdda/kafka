/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.SegmentDeleteInitiate;

import java.nio.ByteBuffer;
import java.util.UUID;

import static kafka.tier.serdes.UUID.createUUID;

public class TierSegmentDeleteInitiate extends AbstractTierSegmentMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int INITIAL_BUFFER_SIZE = 60;

    private final TopicIdPartition topicIdPartition;
    private final SegmentDeleteInitiate metadata;

    public TierSegmentDeleteInitiate(TopicIdPartition topicIdPartition,
                                     int tierEpoch,
                                     UUID objectId) {
        FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);

        SegmentDeleteInitiate.startSegmentDeleteInitiate(builder);

        SegmentDeleteInitiate.addVersion(builder, CURRENT_VERSION);
        SegmentDeleteInitiate.addTierEpoch(builder, tierEpoch);
        int objectIdOffset = createUUID(builder, objectId.getMostSignificantBits(), objectId.getLeastSignificantBits());
        SegmentDeleteInitiate.addObjectId(builder, objectIdOffset);

        int entryId = SegmentDeleteInitiate.endSegmentDeleteInitiate(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = SegmentDeleteInitiate.getRootAsSegmentDeleteInitiate(builder.dataBuffer());
    }

    public TierSegmentDeleteInitiate(TopicIdPartition topicIdPartition, SegmentDeleteInitiate metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.SegmentDeleteInitiate;
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
    public UUID messageId() {
        return new UUID(metadata.objectId().mostSignificantBits(), metadata.objectId().leastSignificantBits());
    }

    @Override
    public TierObjectMetadata.State state() {
        return TierObjectMetadata.State.SEGMENT_DELETE_INITIATE;
    }

    @Override
    public String toString() {
        return "TierSegmentDeleteInitiate(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "objectId=" + objectId() + ")";
    }
}
