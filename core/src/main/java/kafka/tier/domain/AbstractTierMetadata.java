/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.InitLeader;
import kafka.tier.serdes.ObjectMetadata;
import kafka.tier.exceptions.TierMetadataDeserializationException;
import kafka.tier.serdes.TierKafkaKey;
import com.google.flatbuffers.FlatBufferBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
        buf.put(type());
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
    public static Optional<AbstractTierMetadata> deserialize(byte[] key, byte[] value)
            throws TierMetadataDeserializationException {
        final ByteBuffer keyBuf = ByteBuffer.wrap(key);
        final ByteBuffer valueBuf = ByteBuffer.wrap(value);
        final TierKafkaKey tierKey = TierKafkaKey.getRootAsTierKafkaKey(keyBuf);
        final TopicIdPartition topicIdPartition = new TopicIdPartition(tierKey.topicName(),
                new UUID(tierKey.topicId().mostSignificantBits(),
                        tierKey.topicId().leastSignificantBits()),
                tierKey.partition());

        // deserialize value header with record type and tierEpoch
        final byte type = valueBuf.get();
        switch (type) {
            case TierTopicInitLeader.ID:
                final InitLeader init = InitLeader.getRootAsInitLeader(valueBuf);
                return Optional.of(new TierTopicInitLeader(topicIdPartition, init));
            case TierObjectMetadata.ID:
                final ObjectMetadata metadata = ObjectMetadata.getRootAsObjectMetadata(valueBuf);
                return Optional.of(new TierObjectMetadata(topicIdPartition, metadata));
            default:
                log.debug("Unknown tier metadata type with ID {}. Ignoring record.", type);
                return Optional.empty();
        }
    }

    /**
     * @return byte ID for this metadata entry type.
     */
    public abstract byte type();

    /**
     * Topic-partition corresponding to this tier metadata.
     * @return topic partition
     */
    public abstract TopicIdPartition topicIdPartition();

    /**
     * tierEpoch for the tier metadata
     * @return tierEpoch
     */
    public abstract int tierEpoch();

    /**
     * @return backing payload buffer for this metadata.
     */
    public abstract ByteBuffer payloadBuffer();
}
