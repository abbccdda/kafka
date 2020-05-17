/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.SegmentUploadInitiate;
import kafka.tier.state.OffsetAndEpoch;

import java.nio.ByteBuffer;
import java.util.UUID;

import static kafka.tier.serdes.UUID.createUUID;
import static kafka.tier.serdes.OffsetAndEpoch.createOffsetAndEpoch;

/**
 * Upload initiate metadata. The schema for this file is defined in
 * <a href="file:core/src/main/resources/serde/immutable/segment_upload_initiate.fbs">segment_upload_initiate.fbs</a>
 */
public class TierSegmentUploadInitiate extends AbstractTierSegmentMetadata {
    // version v1 added stateOffsetAndEpoch for use in state restoration
    private final static byte VERSION_V1 = 1;
    private final static byte CURRENT_VERSION = VERSION_V1;
    private final static int INITIAL_BUFFER_SIZE = 112;

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
                                     boolean hasProducerState,
                                     OffsetAndEpoch stateOffset) {
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
        int offsetAndEpochId = createOffsetAndEpoch(builder, stateOffset.offset(), stateOffset.epoch().orElse(-1));
        SegmentUploadInitiate.addStateOffsetAndEpoch(builder, offsetAndEpochId);

        int entryId = SegmentUploadInitiate.endSegmentUploadInitiate(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = SegmentUploadInitiate.getRootAsSegmentUploadInitiate(builder.dataBuffer());
    }

    public TierSegmentUploadInitiate(TopicIdPartition topicIdPartition, SegmentUploadInitiate metadata) {
        if (metadata.version() >= VERSION_V1 && metadata.stateOffsetAndEpoch() == null)
            throw new IllegalArgumentException(String.format("TierSegmentUploadInitiate version "
                    + "%d must contain a stateOffsetAndEpoch.", metadata.version()));

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
    public UUID messageId() {
        return new UUID(metadata.objectId().mostSignificantBits(), metadata.objectId().leastSignificantBits());
    }

    @Override
    public int expectedSizeLatestVersion() {
        return INITIAL_BUFFER_SIZE;
    }

    @Override
    public String toString() {
        return "TierSegmentUploadInitiate(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "objectIdAsBase64=" + objectIdAsBase64() + ", " +
                "baseOffset=" + baseOffset() + ", " +
                "endOffset=" + endOffset() + ", " +
                "maxTimestamp=" + maxTimestamp() + ", " +
                "size=" + size() + ", " +
                "hasEpochState=" + hasEpochState() + ", " +
                "hasAbortedTxns=" + hasAbortedTxns() + ", " +
                "hasProducerState=" + hasProducerState() + ", " +
                "stateOffsetAndEpoch=" + stateOffsetAndEpoch() + ")";
    }
}
