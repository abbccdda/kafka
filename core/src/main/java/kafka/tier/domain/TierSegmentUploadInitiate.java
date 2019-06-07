/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.SegmentUploadInitiate;

import java.nio.ByteBuffer;
import java.util.UUID;

import static kafka.tier.serdes.UUID.createUUID;

public class TierSegmentUploadInitiate extends AbstractTierSegmentMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int INITIAL_BUFFER_SIZE = 60;

    private final TopicIdPartition topicIdPartition;
    private final SegmentUploadInitiate metadata;

    public TierSegmentUploadInitiate(TopicIdPartition topicIdPartition,
                                     int tierEpoch,
                                     UUID objectId,
                                     long baseOffset,
                                     long endOffset,
                                     long maxTimestamp,
                                     int size,
                                     boolean hasEpochState,
                                     boolean hasAbortedTxns,
                                     boolean hasProducerState) {
        FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);

        SegmentUploadInitiate.startSegmentUploadInitiate(builder);

        SegmentUploadInitiate.addVersion(builder, CURRENT_VERSION);
        SegmentUploadInitiate.addTierEpoch(builder, tierEpoch);
        int objectIdOffset = createUUID(builder, objectId.getMostSignificantBits(), objectId.getLeastSignificantBits());
        SegmentUploadInitiate.addObjectId(builder, objectIdOffset);
        SegmentUploadInitiate.addBaseOffset(builder, baseOffset);
        SegmentUploadInitiate.addEndOffsetDelta(builder, (int) (endOffset - baseOffset));
        SegmentUploadInitiate.addMaxTimestamp(builder, maxTimestamp);
        SegmentUploadInitiate.addSize(builder, size);
        SegmentUploadInitiate.addHasEpochState(builder, hasEpochState);
        SegmentUploadInitiate.addHasAbortedTxns(builder, hasAbortedTxns);
        SegmentUploadInitiate.addHasProducerState(builder, hasProducerState);

        int entryId = SegmentUploadInitiate.endSegmentUploadInitiate(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = SegmentUploadInitiate.getRootAsSegmentUploadInitiate(builder.dataBuffer());
    }

    public TierSegmentUploadInitiate(TopicIdPartition topicIdPartition, SegmentUploadInitiate metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    @Override
    public TierObjectMetadata.State state() {
        return TierObjectMetadata.State.SEGMENT_UPLOAD_INITIATE;
    }

    public long baseOffset() {
        return metadata.baseOffset();
    }

    public long endOffset() {
        return metadata.baseOffset() + metadata.endOffsetDelta();
    }

    public long maxTimestamp() {
        return metadata.maxTimestamp();
    }

    public int size() {
        return metadata.size();
    }

    public boolean hasEpochState() {
        return metadata.hasEpochState();
    }

    public boolean hasAbortedTxns() {
        return metadata.hasAbortedTxns();
    }

    public boolean hasProducerState() {
        return metadata.hasProducerState();
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.SegmentUploadInitiate;
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
    public String toString() {
        return "TierSegmentUploadInitiate(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "objectId=" + objectId() + ", " +
                "baseOffset=" + baseOffset() + ", " +
                "endOffset=" + endOffset() + ", " +
                "maxTimestamp=" + maxTimestamp() + ", " +
                "size=" + size() + ", " +
                "hasEpochState=" + hasEpochState() + ", " +
                "hasAbortedTxns=" + hasAbortedTxns() + ", " +
                "hasProducerState=" + hasProducerState() + ")";
    }
}
