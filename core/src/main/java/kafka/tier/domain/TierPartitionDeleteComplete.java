/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.PartitionDeleteComplete;

import java.nio.ByteBuffer;
import java.util.UUID;

public class TierPartitionDeleteComplete extends AbstractTierMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int INITIAL_BUFFER_SIZE = 60;

    private final TopicIdPartition topicIdPartition;
    private final PartitionDeleteComplete metadata;

    public TierPartitionDeleteComplete(TopicIdPartition topicIdPartition, UUID messageId) {
        FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);

        PartitionDeleteComplete.startPartitionDeleteComplete(builder);

        PartitionDeleteComplete.addVersion(builder, CURRENT_VERSION);
        int messageIdOffset = kafka.tier.serdes.UUID.createUUID(builder, messageId.getMostSignificantBits(), messageId.getLeastSignificantBits());
        PartitionDeleteComplete.addMessageId(builder, messageIdOffset);

        int entryId = PartitionDeleteComplete.endPartitionDeleteComplete(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = PartitionDeleteComplete.getRootAsPartitionDeleteComplete(builder.dataBuffer());
    }

    public TierPartitionDeleteComplete(TopicIdPartition topicIdPartition, PartitionDeleteComplete metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.PartitionDeleteComplete;
    }

    @Override
    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    @Override
    public ByteBuffer payloadBuffer() {
        return metadata.getByteBuffer().duplicate();
    }

    @Override
    public int tierEpoch() {
        return -1;
    }

    @Override
    public UUID messageId() {
        return new UUID(metadata.messageId().mostSignificantBits(), metadata.messageId().leastSignificantBits());
    }

    @Override
    public String toString() {
        return "TierPartitionDeleteComplete(" +
                "version=" + metadata.version() + ", " +
                "topic=" + topicIdPartition() + ", " +
                "messageId=" + messageId() + ")";
    }
}
