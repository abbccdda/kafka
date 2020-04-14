/*
 Copyright 2020 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.UUID;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.PartitionFence;

public class TierPartitionFence extends AbstractTierMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int INITIAL_BUFFER_SIZE = 60;

    private final TopicIdPartition topicIdPartition;
    private final PartitionFence metadata;

    public TierPartitionFence(TopicIdPartition topicIdPartition, UUID messageId) {
        final FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);

        PartitionFence.startPartitionFence(builder);
        PartitionFence.addVersion(builder, CURRENT_VERSION);
        int messageIdOffset = kafka.tier.serdes.UUID.createUUID(
            builder, messageId.getMostSignificantBits(), messageId.getLeastSignificantBits());
        PartitionFence.addMessageId(builder, messageIdOffset);
        int entryId = PartitionFence.endPartitionFence(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = PartitionFence.getRootAsPartitionFence(builder.dataBuffer());
    }

    public TierPartitionFence(TopicIdPartition topicIdPartition, PartitionFence metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.PartitionFence;
    }

    @Override
    public ByteBuffer payloadBuffer() {
        return metadata.getByteBuffer().duplicate();
    }

    @Override
    public UUID messageId() {
        return new UUID(metadata.messageId().mostSignificantBits(), metadata.messageId().leastSignificantBits());
    }

    @Override
    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    @Override
    public int tierEpoch() {
        return -1;
    }

    private byte version() {
        return metadata.version();
    }

    @Override
    public String toString() {
        return "TierPartitionFence(" +
            "version=" + version() + ", " +
            "topicIdPartition=" + topicIdPartition() + ", " +
            "messageIdAsBase64=" + messageIdAsBase64() +
            ")";
    }
}
