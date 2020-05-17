/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.PartitionFence;
import kafka.tier.serdes.InitLeader;
import kafka.tier.exceptions.TierMetadataDeserializationException;
import kafka.tier.serdes.PartitionDeleteComplete;
import kafka.tier.serdes.PartitionDeleteInitiate;
import kafka.tier.serdes.PartitionForceRestore;
import kafka.tier.serdes.TierKafkaKey;
import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.serdes.SegmentDeleteComplete;
import kafka.tier.serdes.SegmentDeleteInitiate;
import kafka.tier.serdes.SegmentUploadComplete;
import kafka.tier.serdes.SegmentUploadInitiate;
import kafka.tier.state.OffsetAndEpoch;
import kafka.utils.CoreUtils;
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
     * Deserializes just the type from a value read from a Tier State Topic
     * @param value value bytes
     * @return TierRecordType
     */
    public static byte getTypeId(byte[] value) {
        return value[0];
    }

    public static TopicIdPartition deserializeKey(byte[] key) {
        return deserializeKey(ByteBuffer.wrap(key));
    }

    public static TopicIdPartition deserializeKey(ByteBuffer key) {
        TierKafkaKey tierKey = TierKafkaKey.getRootAsTierKafkaKey(key);

        return new TopicIdPartition(tierKey.topicName(),
                new UUID(tierKey.topicId().mostSignificantBits(),
                        tierKey.topicId().leastSignificantBits()),
                tierKey.partition());
    }

    /**
     * Deserializes byte key and value read from Tier State Topic into Tier Metadata.
     * Current implementation maps any exception while deserialization as TierMetadataDeserializationException. In
     * future, we may adopt more stronger checks before throwing TierMetadataDeserializationException.
     * @param key Key containing archived topic partition
     * @param value Value containing tier metadata.
     * @return AbstractTierMetadata if one could be deserialized. Empty if Tier Metadata ID unrecognized.
     * @throws TierMetadataDeserializationException
     */
    public static Optional<AbstractTierMetadata> deserialize(byte[] key, byte[] value) throws TierMetadataDeserializationException {
        try {
            return deserialize(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
        } catch (Exception e) {
            throw new TierMetadataDeserializationException(String.format("Deserialization error [%s]", e.getMessage()), e);
        }
    }

    public static Optional<AbstractTierMetadata> deserialize(ByteBuffer key, ByteBuffer value) throws TierMetadataDeserializationException {
        final TopicIdPartition topicIdPartition = deserializeKey(key);

        // deserialize value header with record type and tierEpoch
        final TierRecordType type = TierRecordType.toType(value.get());
        switch (type) {
            case InitLeader:
                final InitLeader initLeader = InitLeader.getRootAsInitLeader(value);
                return Optional.of(new TierTopicInitLeader(topicIdPartition, initLeader));
            case SegmentUploadInitiate:
                final SegmentUploadInitiate uploadInitiate = SegmentUploadInitiate.getRootAsSegmentUploadInitiate(value);
                return Optional.of(new TierSegmentUploadInitiate(topicIdPartition, uploadInitiate));
            case SegmentUploadComplete:
                final SegmentUploadComplete uploadComplete = SegmentUploadComplete.getRootAsSegmentUploadComplete(value);
                return Optional.of(new TierSegmentUploadComplete(topicIdPartition, uploadComplete));
            case SegmentDeleteInitiate:
                final SegmentDeleteInitiate deleteInitiate = SegmentDeleteInitiate.getRootAsSegmentDeleteInitiate(value);
                return Optional.of(new TierSegmentDeleteInitiate(topicIdPartition, deleteInitiate));
            case SegmentDeleteComplete:
                final SegmentDeleteComplete deleteComplete = SegmentDeleteComplete.getRootAsSegmentDeleteComplete(value);
                return Optional.of(new TierSegmentDeleteComplete(topicIdPartition, deleteComplete));
            case PartitionDeleteInitiate:
                final PartitionDeleteInitiate partitionDeleteInitiate = PartitionDeleteInitiate.getRootAsPartitionDeleteInitiate(value);
                return Optional.of(new TierPartitionDeleteInitiate(topicIdPartition, partitionDeleteInitiate));
            case PartitionDeleteComplete:
                final PartitionDeleteComplete partitionDeleteComplete = PartitionDeleteComplete.getRootAsPartitionDeleteComplete(value);
                return Optional.of(new TierPartitionDeleteComplete(topicIdPartition, partitionDeleteComplete));
            case PartitionFence:
                final PartitionFence partitionFence = PartitionFence.getRootAsPartitionFence(value);
                return Optional.of(new TierPartitionFence(topicIdPartition, partitionFence));
            case PartitionForceRestore:
                final PartitionForceRestore partitionForceRestore = PartitionForceRestore.getRootAsPartitionForceRestore(value);
                return Optional.of(new TierPartitionForceRestore(topicIdPartition, partitionForceRestore));
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

    /**
     * The lastMaterializedOffsetAndEpoch for the tier partition state at the time this metadata was generated.
     * This is used to ensure that the metadata is intended to be applied to this state, and that
     * it has not undergone a restore.
     * This metadata may be successfully applied to a tier state with
     * restoreOffsetAndEpoch <= stateOffsetAndEpoch.
     * OffsetAndEpoch.EMPTY indicates that the metadata is an older version that does not contain
     * this field, or that it may be applied without a check against restoreOffsetAndEpoch.
     * @return OffsetAndEpoch
     */
    public abstract OffsetAndEpoch stateOffsetAndEpoch();

    /**
     * Encode messageId with Base64
     * @return string representing messageId encoded in Base64
     */
    public String messageIdAsBase64() {
        return CoreUtils.uuidToBase64(messageId());
    }

    /**
     * Test method for validating initial buffer size allocation
     * @return the expected size of a the buffer once fully written
     */
    public abstract int expectedSizeLatestVersion();

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
