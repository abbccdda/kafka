/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.InitLeader;
import kafka.tier.exceptions.TierMetadataDeserializationException;
import kafka.tier.serdes.PartitionDeleteInitiate;
import kafka.tier.serdes.TierKafkaKey;
import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.serdes.SegmentDeleteComplete;
import kafka.tier.serdes.SegmentDeleteInitiate;
import kafka.tier.serdes.SegmentUploadComplete;
import kafka.tier.serdes.SegmentUploadInitiate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public abstract class AbstractTierMetadata {
    private static final Logger log = LoggerFactory.getLogger(AbstractTierMetadata.class);
    // initial key length will not be enough to contain all topic names (256 bytes + type +
    // string length), however the byte buffer will grow when necessary
    private static final int KEY_INITIAL_LENGTH = 200;
    private static final int TYPE_LENGTH = 1;

    public byte[] serializeKey() {
        final FlatBufferBuilder builder = new FlatBufferBuilder(KEY_INITIAL_LENGTH);
        int topicNameOffset = builder.createString(topicIdPartition().topic());
        final int topicIdOffset = kafka.tier.serdes.UUID.createUUID(builder,
                topicIdPartition().topicId().getMostSignificantBits(),
                topicIdPartition().topicId().getLeastSignificantBits());
        TierKafkaKey.startTierKafkaKey(builder);
        TierKafkaKey.addTopicId(builder, topicIdOffset);
        TierKafkaKey.addPartition(builder, topicIdPartition().topicPartition().partition());
        TierKafkaKey.addTopicName(builder, topicNameOffset);
        final int entryId = TierKafkaKey.endTierKafkaKey(builder);
        builder.finish(entryId);
        final ByteBuffer buffer = builder.dataBuffer();
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public byte[] serializeValue() {
        final ByteBuffer payload = payloadBuffer();
        final ByteBuffer buf = ByteBuffer.allocate(payload.remaining() + TYPE_LENGTH);
        buf.put(TierRecordType.toByte(type()));
        buf.put(payload);
        return buf.array();
    }

    /**
     * Deserializes byte key and value read from Tier Topic into Tier Metadata.
     * @param key Key containing archived topic partition
     * @param value Value containing tier metadata.
     * @return AbstractTierMetadata if one could be deserialized. Empty if Tier Metadata ID unrecognized.
     * @throws TierMetadataDeserializationException
     */
    public static Optional<AbstractTierMetadata> deserialize(byte[] key, byte[] value) throws TierMetadataDeserializationException {
        final ByteBuffer keyBuf = ByteBuffer.wrap(key);
        final ByteBuffer valueBuf = ByteBuffer.wrap(value);
        final TierKafkaKey tierKey = TierKafkaKey.getRootAsTierKafkaKey(keyBuf);
        final TopicIdPartition topicIdPartition = new TopicIdPartition(tierKey.topicName(),
                new UUID(tierKey.topicId().mostSignificantBits(),
                        tierKey.topicId().leastSignificantBits()),
                tierKey.partition());

        // deserialize value header with record type and tierEpoch
        final TierRecordType type = TierRecordType.toType(valueBuf.get());
        switch (type) {
            case InitLeader:
                final InitLeader initLeader = InitLeader.getRootAsInitLeader(valueBuf);
                return Optional.of(new TierTopicInitLeader(topicIdPartition, initLeader));
            case SegmentUploadInitiate:
                final SegmentUploadInitiate uploadInitiate = SegmentUploadInitiate.getRootAsSegmentUploadInitiate(valueBuf);
                return Optional.of(new TierSegmentUploadInitiate(topicIdPartition, uploadInitiate));
            case SegmentUploadComplete:
                final SegmentUploadComplete uploadComplete = SegmentUploadComplete.getRootAsSegmentUploadComplete(valueBuf);
                return Optional.of(new TierSegmentUploadComplete(topicIdPartition, uploadComplete));
            case SegmentDeleteInitiate:
                final SegmentDeleteInitiate deleteInitiate = SegmentDeleteInitiate.getRootAsSegmentDeleteInitiate(valueBuf);
                return Optional.of(new TierSegmentDeleteInitiate(topicIdPartition, deleteInitiate));
            case SegmentDeleteComplete:
                final SegmentDeleteComplete deleteComplete = SegmentDeleteComplete.getRootAsSegmentDeleteComplete(valueBuf);
                return Optional.of(new TierSegmentDeleteComplete(topicIdPartition, deleteComplete));
            case PartitionDeleteInitiate:
                final PartitionDeleteInitiate partitionDeleteInitiate = PartitionDeleteInitiate.getRootAsPartitionDeleteInitiate(valueBuf);
                return Optional.of(new TierPartitionDeleteInitiate(topicIdPartition, partitionDeleteInitiate));
            default:
                log.debug("Unknown tier metadata type with ID {}. Ignoring record.", type);
                return Optional.empty();
        }
    }

    /**
     * @return byte ID for this metadata entry type.
     */
    public abstract TierRecordType type();

    /**
     * Topic-partition corresponding to this tier metadata.
     * @return topic partition
     */
    public abstract TopicIdPartition topicIdPartition();

    /**
     * @return backing payload buffer for this metadata.
     */
    public abstract ByteBuffer payloadBuffer();

    /**
     * tierEpoch for the tier metadata
     * @return tierEpoch
     */
    public abstract int tierEpoch();

    /**
     * Get a unique id for this message. This is a unique fingerprint that identifies the message.
     * @return the message id
     */
    public abstract UUID messageId();

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        AbstractTierMetadata that = (AbstractTierMetadata) o;
        return type().equals(that.type()) &&
                topicIdPartition().equals(that.topicIdPartition()) &&
                payloadBuffer().equals(that.payloadBuffer());
    }

    @Override
    public int hashCode() {
        return Objects.hash(type(), topicIdPartition(), payloadBuffer());
    }
}
