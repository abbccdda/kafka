/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.SegmentUploadComplete;
import kafka.tier.state.OffsetAndEpoch;

import java.nio.ByteBuffer;
import java.util.UUID;

import static kafka.tier.serdes.OffsetAndEpoch.createOffsetAndEpoch;
import static kafka.tier.serdes.UUID.createUUID;


/**
 * Upload complete metadata. The schema for this file is defined in
 * <a href="file:core/src/main/resources/serde/immutable/segment_upload_complete.fbs">segment_upload_complete.fbs</a>
 */
public class TierSegmentUploadComplete extends AbstractTierSegmentMetadata {
    // version v1 added stateOffsetAndEpoch for use in state restoration
    private final static byte VERSION_V1 = 1;
    private final static byte CURRENT_VERSION = VERSION_V1;
    private final static int INITIAL_BUFFER_SIZE = 64;

    private final TopicIdPartition topicIdPartition;
    private final SegmentUploadComplete metadata;

    public TierSegmentUploadComplete(TopicIdPartition topicIdPartition,
                                     int tierEpoch,
                                     UUID objectId,
                                     OffsetAndEpoch stateOffset) {
        FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);

        SegmentUploadComplete.startSegmentUploadComplete(builder);
        SegmentUploadComplete.addVersion(builder, CURRENT_VERSION);
        SegmentUploadComplete.addTierEpoch(builder, tierEpoch);
        int offsetAndEpochId = createOffsetAndEpoch(builder, stateOffset.offset(), stateOffset.epoch().orElse(-1));
        SegmentUploadComplete.addStateOffsetAndEpoch(builder, offsetAndEpochId);
        int objectIdOffset = createUUID(builder, objectId.getMostSignificantBits(), objectId.getLeastSignificantBits());
        SegmentUploadComplete.addObjectId(builder, objectIdOffset);

        int entryId = SegmentUploadComplete.endSegmentUploadComplete(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = SegmentUploadComplete.getRootAsSegmentUploadComplete(builder.dataBuffer());
    }

    public TierSegmentUploadComplete(TierSegmentUploadInitiate uploadInitiate) {
        this(uploadInitiate.topicIdPartition(), uploadInitiate.tierEpoch(),
                uploadInitiate.objectId(),
                uploadInitiate.stateOffsetAndEpoch());
    }

    public TierSegmentUploadComplete(TopicIdPartition topicIdPartition, SegmentUploadComplete metadata) {
        if (metadata.version() >= VERSION_V1 && metadata.stateOffsetAndEpoch() == null)
            throw new IllegalArgumentException(String.format("TierSegmentUploadComplete version "
                    + "%d must contain a stateOffsetAndEpoch.", metadata.version()));

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
        return TierObjectMetadata.State.SEGMENT_UPLOAD_COMPLETE;
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
        return "TierSegmentUploadComplete(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "objectIdAsBase64=" + objectIdAsBase64() + ", " +
                "stateOffsetAndEpoch=" + stateOffsetAndEpoch() + ")";
    }
}
