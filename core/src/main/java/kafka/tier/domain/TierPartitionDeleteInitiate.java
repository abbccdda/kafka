/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.PartitionDeleteInitiate;
import kafka.tier.state.OffsetAndEpoch;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Partition delete initiate metadata. The schema for this file is defined in
 * <a href="file:core/src/main/resources/serde/immutable/partition_delete_initiate.fbs">partition_delete_initiate.fbs</a>
 */
public class TierPartitionDeleteInitiate extends AbstractTierMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int INITIAL_BUFFER_SIZE = 48;

    private final TopicIdPartition topicIdPartition;
    private final PartitionDeleteInitiate metadata;

    public TierPartitionDeleteInitiate(TopicIdPartition topicIdPartition,
                                       int controllerEpoch,
                                       UUID messageId) {
        FlatBufferBuilder builder = new FlatBufferBuilder(INITIAL_BUFFER_SIZE).forceDefaults(true);

        PartitionDeleteInitiate.startPartitionDeleteInitiate(builder);

        PartitionDeleteInitiate.addVersion(builder, CURRENT_VERSION);
        PartitionDeleteInitiate.addControllerEpoch(builder, controllerEpoch);
        int messageIdOffset = kafka.tier.serdes.UUID.createUUID(builder, messageId.getMostSignificantBits(), messageId.getLeastSignificantBits());
        PartitionDeleteInitiate.addMessageId(builder, messageIdOffset);

        int entryId = PartitionDeleteInitiate.endPartitionDeleteInitiate(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = PartitionDeleteInitiate.getRootAsPartitionDeleteInitiate(builder.dataBuffer());
    }

    public TierPartitionDeleteInitiate(TopicIdPartition topicIdPartition, PartitionDeleteInitiate metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.PartitionDeleteInitiate;
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
        return metadata.controllerEpoch();
    }

    @Override
    public OffsetAndEpoch stateOffsetAndEpoch() {
        return OffsetAndEpoch.EMPTY;
    }

    @Override
    public UUID messageId() {
        return new UUID(metadata.messageId().mostSignificantBits(), metadata.messageId().leastSignificantBits());
    }

    @Override
    public int expectedSizeLatestVersion() {
        return INITIAL_BUFFER_SIZE;
    }

    @Override
    public String toString() {
        return "TierPartitionDeleteInitiate(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "controllerEpoch=" + metadata.controllerEpoch() + ", " +
                "messageIdAsBase64=" + messageIdAsBase64() + ")";
    }
}
