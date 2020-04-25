/*
 Copyright 2019 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.PartitionForceRestore;
import kafka.tier.state.OffsetAndEpoch;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.UUID;

import static kafka.tier.serdes.OffsetAndEpoch.createOffsetAndEpoch;

public class TierPartitionForceRestore extends AbstractTierMetadata {
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;

    private final TopicIdPartition topicIdPartition;
    private final PartitionForceRestore metadata;

    public TierPartitionForceRestore(TopicIdPartition topicIdPartition,
                                     UUID messageId,
                                     long startOffset,
                                     long endOffset,
                                     OffsetAndEpoch stateValidityOffsetAndEpoch,
                                     String contentHash) {
        FlatBufferBuilder builder = new FlatBufferBuilder().forceDefaults(true);
        int contentHashId = builder.createString(contentHash);
        PartitionForceRestore.startPartitionForceRestore(builder);
        PartitionForceRestore.addVersion(builder, CURRENT_VERSION);
        PartitionForceRestore.addStartOffset(builder, startOffset);
        PartitionForceRestore.addEndOffset(builder, endOffset);
        int messageIdOffset = kafka.tier.serdes.UUID.createUUID(
                builder, messageId.getMostSignificantBits(), messageId.getLeastSignificantBits());
        PartitionForceRestore.addMessageId(builder, messageIdOffset);
        int offsetAndEpochId = createOffsetAndEpoch(builder, stateValidityOffsetAndEpoch.offset(),
                stateValidityOffsetAndEpoch.epoch().orElse(-1));
        PartitionForceRestore.addStateValidityOffsetAndEpoch(builder, offsetAndEpochId);
        PartitionForceRestore.addContentHash(builder, contentHashId);
        int entryId = PartitionForceRestore.endPartitionForceRestore(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = PartitionForceRestore.getRootAsPartitionForceRestore(builder.dataBuffer());
    }

    public TierPartitionForceRestore(TopicIdPartition topicIdPartition, PartitionForceRestore metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    public long startOffset() {
        return metadata.startOffset();
    }

    public long endOffset() {
        return metadata.endOffset();
    }

    public String contentHash() {
        return metadata.contentHash();
    }

    public byte version() {
        return metadata.version();
    }

    @Override
    public TierRecordType type() {
        return TierRecordType.PartitionForceRestore;
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

    public Optional<OffsetAndEpoch> stateValidityOffsetAndEpoch() {
        return Optional.of(offsetAndEpoch(metadata.stateValidityOffsetAndEpoch()));
    }

    @Override
    public UUID messageId() {
        return new UUID(metadata.messageId().mostSignificantBits(), metadata.messageId().leastSignificantBits());
   }

    @Override
    public String toString() {
        return "TierPartitionForceRestore(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "messageIdAsBase64=" + messageIdAsBase64() + ", " +
                "startOffset=" + startOffset() + ", " +
                "endOffset=" + endOffset() + ", " +
                "stateValidityOffsetAndEpoch=" + stateValidityOffsetAndEpoch() + ", " +
                "contentHash=" + contentHash() + ")";
    }
}
