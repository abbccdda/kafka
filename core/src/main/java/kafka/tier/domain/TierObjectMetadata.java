/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.ObjectMetadata;
import kafka.utils.CoreUtils;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

public class TierObjectMetadata extends AbstractTierMetadata {
    public final static byte ID = 1;
    private final TopicIdPartition topicIdPartition;
    private final ObjectMetadata metadata;
    private final static byte VERSION_VO = 0;
    private final static byte CURRENT_VERSION = VERSION_VO;
    private final static int BASE_BUFFER_SIZE = 108;

    public TierObjectMetadata(TopicIdPartition topicIdPartition, ObjectMetadata metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    public TierObjectMetadata(TopicIdPartition topicIdPartition, int tierEpoch,
                              long startOffset, int endOffsetDelta,
                              long lastStableOffset, long maxTimestamp,
                              int size, boolean hasEpochState,
                              boolean hasProducerState,
                              boolean hasAborts, byte state) {
        // Random ID to provide uniqueness when generating object store paths.
        final UUID messageId = UUID.randomUUID();

        if (tierEpoch < 0) {
            throw new IllegalArgumentException(String.format("Illegal tierEpoch supplied %d.", tierEpoch));
        }

        this.topicIdPartition = topicIdPartition;
        final FlatBufferBuilder builder = new FlatBufferBuilder(BASE_BUFFER_SIZE)
                .forceDefaults(true);
        ObjectMetadata.startObjectMetadata(builder);
        ObjectMetadata.addTierEpoch(builder, tierEpoch);
        ObjectMetadata.addStartOffset(builder, startOffset);
        ObjectMetadata.addEndOffsetDelta(builder, endOffsetDelta);
        ObjectMetadata.addLastStableOffset(builder, lastStableOffset);
        ObjectMetadata.addMaxTimestamp(builder, maxTimestamp);
        int messageIdOffset = kafka.tier.serdes.UUID.createUUID(builder, messageId.getMostSignificantBits(), messageId.getLeastSignificantBits());
        ObjectMetadata.addMessageId(builder, messageIdOffset);
        ObjectMetadata.addSize(builder, size);
        ObjectMetadata.addHasEpochState(builder, hasEpochState);
        ObjectMetadata.addHasAborts(builder, hasAborts);
        ObjectMetadata.addVersion(builder, CURRENT_VERSION);
        ObjectMetadata.addState(builder, state);
        ObjectMetadata.addHasProducerState(builder, hasProducerState);
        final int entryId = ObjectMetadata.endObjectMetadata(builder);
        builder.finish(entryId);
        this.metadata = ObjectMetadata.getRootAsObjectMetadata(builder.dataBuffer());
    }

    public ObjectMetadata objectMetadata() {
        return metadata;
    }

    public byte type() {
        return ID;
    }

    public ByteBuffer payloadBuffer() {
        return metadata.getByteBuffer().duplicate();
    }

    public int tierEpoch() {
        return metadata.tierEpoch();
    }

    public long startOffset() {
        return metadata.startOffset();
    }

    public int endOffsetDelta() {
        return metadata.endOffsetDelta();
    }

    /**
     * Random ID associated with each TierObjectMetadata entry.
     */
    public UUID messageId() {
        return new UUID(metadata.messageId().mostSignificantBits(), metadata.messageId().leastSignificantBits());
    }

    /**
     * @return Base64 string representation of metadata message ID
     */
    public String messageIdAsBase64() {
        return CoreUtils.uuidToBase64(messageId());
    }

    public long endOffset() {
        return startOffset() + endOffsetDelta();
    }

    public long lastStableOffset() {
        return metadata.lastStableOffset();
    }

    public long maxTimestamp() {
        return metadata.maxTimestamp();
    }

    public int size() {
        return metadata.size();
    }

    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    public boolean hasEpochState() {
        return metadata.hasEpochState();
    }

    public boolean hasAborts() {
        return metadata.hasAborts();
    }

    public byte state() {
        return metadata.state();
    }

    public boolean hasProducerState() {
        return metadata.hasProducerState();
    }

    public short version() {
        return metadata.version();
    }

    @Override
    public String toString() {
        return String.format("TierObjectMetadata(topic='%s', topicId='%s', partition=%s,"
                        + " tierEpoch=%s, version=%s, startOffset=%s,"
                        + " endOffsetDelta=%s, lastStableOffset=%s, hasAborts=%s,"
                        + " maxTimestamp=%s, messageId=%s, size=%s, status=%s,"
                        + " hasProducerState=%s)",
                topicIdPartition.topic(), topicIdPartition.topicIdAsBase64(),
                topicIdPartition.partition(), tierEpoch(), version(), startOffset(),
                endOffsetDelta(), lastStableOffset(), hasAborts(), maxTimestamp(),
                messageIdAsBase64(), size(), state(), hasProducerState());
    }

    public int hashCode() {
        return Objects.hash(topicIdPartition, tierEpoch(),
                startOffset(), endOffsetDelta(), lastStableOffset(),
                hasAborts(), maxTimestamp(), messageId(), size(),
                version(), state(), hasProducerState());
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TierObjectMetadata that = (TierObjectMetadata) o;
        return Objects.equals(topicIdPartition, that.topicIdPartition)
                && Objects.equals(tierEpoch(), that.tierEpoch())
                && Objects.equals(startOffset(), that.startOffset())
                && Objects.equals(endOffsetDelta(), that.endOffsetDelta())
                && Objects.equals(lastStableOffset(), that.lastStableOffset())
                && Objects.equals(hasAborts(), that.hasAborts())
                && Objects.equals(maxTimestamp(), that.maxTimestamp())
                && Objects.equals(messageId(), that.messageId())
                && Objects.equals(size(), that.size())
                && Objects.equals(version(), that.version())
                && Objects.equals(state(), that.state());
    }
}
