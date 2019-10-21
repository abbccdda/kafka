/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.InitLeader;

import java.nio.ByteBuffer;
import java.util.UUID;

public class TierTopicInitLeader extends AbstractTierMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int INITIAL_BUFFER_SIZE = 60;
    private final TopicIdPartition topicIdPartition;
    private final InitLeader metadata;

    public TierTopicInitLeader(TopicIdPartition topicIdPartition,
                               int tierEpoch,
                               UUID messageId,
                               int brokerId) {
        if (tierEpoch < 0)
            throw new IllegalArgumentException("Illegal tierEpoch: " + tierEpoch);

        this.topicIdPartition = topicIdPartition;
        final FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);
        InitLeader.startInitLeader(builder);
        int messageIdOffset = kafka.tier.serdes.UUID.createUUID(builder, messageId.getMostSignificantBits(), messageId.getLeastSignificantBits());
        InitLeader.addMessageId(builder, messageIdOffset);
        InitLeader.addTierEpoch(builder, tierEpoch);
        InitLeader.addBrokerId(builder, brokerId);
        InitLeader.addVersion(builder, CURRENT_VERSION);
        int entryId = InitLeader.endInitLeader(builder);
        builder.finish(entryId);
        this.metadata = InitLeader.getRootAsInitLeader(builder.dataBuffer());
    }

    public TierTopicInitLeader(TopicIdPartition topicIdPartition, InitLeader metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.InitLeader;
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
        return metadata.tierEpoch();
    }

    private byte version() {
        return metadata.version();
    }

    public int brokerId() {
        return metadata.brokerId();
    }

    @Override
    public String toString() {
        return "TierInitLeader(" +
                "version=" + version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "messageIdAsBase64=" + messageIdAsBase64() +
                "brokerId=" + brokerId() +
                ")";
    }
}
