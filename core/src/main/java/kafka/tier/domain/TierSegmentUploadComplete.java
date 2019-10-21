/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.SegmentUploadComplete;

import java.nio.ByteBuffer;
import java.util.UUID;

import static kafka.tier.serdes.UUID.createUUID;

public class TierSegmentUploadComplete extends AbstractTierSegmentMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int INITIAL_BUFFER_SIZE = 60;

    private final TopicIdPartition topicIdPartition;
    private final SegmentUploadComplete metadata;

    public TierSegmentUploadComplete(TopicIdPartition topicIdPartition,
                                     int tierEpoch,
                                     UUID objectId) {
        FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);

        SegmentUploadComplete.startSegmentUploadComplete(builder);
        SegmentUploadComplete.addVersion(builder, CURRENT_VERSION);
        SegmentUploadComplete.addTierEpoch(builder, tierEpoch);
        int objectIdOffset = createUUID(builder, objectId.getMostSignificantBits(), objectId.getLeastSignificantBits());
        SegmentUploadComplete.addObjectId(builder, objectIdOffset);

        int entryId = SegmentUploadComplete.endSegmentUploadComplete(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = SegmentUploadComplete.getRootAsSegmentUploadComplete(builder.dataBuffer());
    }

    public TierSegmentUploadComplete(TierSegmentUploadInitiate uploadInitiate) {
        this(uploadInitiate.topicIdPartition(), uploadInitiate.tierEpoch(), uploadInitiate.objectId());
    }

    public TierSegmentUploadComplete(TopicIdPartition topicIdPartition, SegmentUploadComplete metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.SegmentUploadComplete;
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
        return TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE;
    }

    @Override
    public UUID messageId() {
        return new UUID(metadata.objectId().mostSignificantBits(), metadata.objectId().leastSignificantBits());
    }

    @Override
    public String toString() {
        return "TierSegmentUploadComplete(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "objectIdAsBase64=" + objectIdAsBase64() + ")";
    }
}
