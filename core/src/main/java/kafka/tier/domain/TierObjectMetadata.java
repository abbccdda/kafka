/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

import com.google.flatbuffers.FlatBufferBuilder;
import kafka.tier.TopicIdPartition;
import kafka.tier.serdes.ObjectState;
import kafka.tier.serdes.TierPartitionStateEntry;
import kafka.utils.CoreUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static kafka.tier.serdes.UUID.createUUID;

public class TierObjectMetadata {
    private final TopicIdPartition topicIdPartition;
    private final TierPartitionStateEntry metadata;
    private final static byte VERSION_V0 = 0;
    private final static byte CURRENT_VERSION = VERSION_V0;
    private final static int BASE_BUFFER_SIZE = 108;

    public enum State {
        SEGMENT_UPLOAD_INITIATE(ObjectState.SEGMENT_UPLOAD_INITIATE, Arrays.asList(ObjectState.SEGMENT_UPLOAD_COMPLETE, ObjectState.SEGMENT_DELETE_INITIATE, ObjectState.SEGMENT_FENCED)),
        SEGMENT_UPLOAD_COMPLETE(ObjectState.SEGMENT_UPLOAD_COMPLETE, Collections.singletonList(ObjectState.SEGMENT_DELETE_INITIATE)),
        SEGMENT_DELETE_INITIATE(ObjectState.SEGMENT_DELETE_INITIATE, Arrays.asList(ObjectState.SEGMENT_DELETE_COMPLETE, ObjectState.SEGMENT_FENCED)),
        SEGMENT_DELETE_COMPLETE(ObjectState.SEGMENT_DELETE_COMPLETE, Collections.emptyList()),
        SEGMENT_FENCED(ObjectState.SEGMENT_FENCED, Collections.singletonList(ObjectState.SEGMENT_DELETE_INITIATE));

        private final static Map<Byte, State> VALUES = new HashMap<>();

        private final byte id;
        private final List<Byte> nextStates;

        static {
            for (State state : State.values()) {
                State previousState = VALUES.put(state.id, state);
                if (previousState != null)
                    throw new ExceptionInInitializerError("Conflicting VALUES for " + previousState + " and " + state);
            }
        }

        State(byte id, List<Byte> nextStates) {
            this.id = id;
            this.nextStates = nextStates;
        }

        public byte id() {
            return id;
        }

        public static State toState(byte id) {
            State state = VALUES.get(id);
            if (state == null)
                throw new IllegalArgumentException("No mapping found for " + id);
            return state;
        }

        /**
         * Check if we can safely transition to the given state.
         * @param newState The state to transition to
         * @return True if transition to given state is permissible; false otherwise
         * @throws IllegalStateException If attempt to transition to given state is illegal
         */
        public boolean canTransitionTo(State newState) {
            raiseIfIllegal(this, newState);
            return nextStates.contains(newState.id);
        }

        private static void raiseIfIllegal(State currentState, State newState) {
            if (newState == SEGMENT_DELETE_COMPLETE && currentState != SEGMENT_DELETE_INITIATE)
                throw new IllegalStateException("Illegal transition from " + currentState + " to " + newState);
        }
    }

    public TierObjectMetadata(TopicIdPartition topicIdPartition, TierPartitionStateEntry metadata) {
        this.topicIdPartition = topicIdPartition;
        this.metadata = metadata;
    }

    public TierObjectMetadata(TopicIdPartition topicIdPartition,
                              int tierEpoch,
                              UUID objectId,
                              long baseOffset,
                              long endOffset,
                              long maxTimestamp,
                              int size,
                              State state,
                              boolean hasEpochState,
                              boolean hasAbortedTxns,
                              boolean hasProducerState) {
        if (tierEpoch < 0)
            throw new IllegalArgumentException(String.format("Illegal tierEpoch supplied %d.", tierEpoch));

        FlatBufferBuilder builder = new FlatBufferBuilder(BASE_BUFFER_SIZE).forceDefaults(true);

        TierPartitionStateEntry.startTierPartitionStateEntry(builder);
        TierPartitionStateEntry.addVersion(builder, CURRENT_VERSION);
        TierPartitionStateEntry.addTierEpoch(builder, tierEpoch);
        // Random ID to provide uniqueness when generating object store paths
        int objectIdOffset = createUUID(builder, objectId.getMostSignificantBits(), objectId.getLeastSignificantBits());
        TierPartitionStateEntry.addObjectId(builder, objectIdOffset);
        TierPartitionStateEntry.addBaseOffset(builder, baseOffset);
        TierPartitionStateEntry.addEndOffsetDelta(builder, (int) (endOffset - baseOffset));
        TierPartitionStateEntry.addMaxTimestamp(builder, maxTimestamp);
        TierPartitionStateEntry.addSize(builder, size);
        TierPartitionStateEntry.addState(builder, state.id());
        TierPartitionStateEntry.addHasEpochState(builder, hasEpochState);
        TierPartitionStateEntry.addHasAbortedTxns(builder, hasAbortedTxns);
        TierPartitionStateEntry.addHasProducerState(builder, hasProducerState);

        int entryId = TierPartitionStateEntry.endTierPartitionStateEntry(builder);
        builder.finish(entryId);

        this.topicIdPartition = topicIdPartition;
        this.metadata = TierPartitionStateEntry.getRootAsTierPartitionStateEntry(builder.dataBuffer());
    }

    public TierObjectMetadata(TierSegmentUploadInitiate uploadInitiate) {
        this(uploadInitiate.topicIdPartition(), uploadInitiate.tierEpoch(), uploadInitiate.objectId(),
                uploadInitiate.baseOffset(), uploadInitiate.endOffset(), uploadInitiate.maxTimestamp(),
                uploadInitiate.size(), uploadInitiate.state(), uploadInitiate.hasEpochState(),
                uploadInitiate.hasAbortedTxns(), uploadInitiate.hasProducerState());
    }

    public TierObjectMetadata duplicate() {
        return new TierObjectMetadata(topicIdPartition, tierEpoch(), objectId(), baseOffset(), endOffset(),
                maxTimestamp(), size(), state(), hasEpochState(), hasAbortedTxns(), hasProducerState());
    }

    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    public ByteBuffer payloadBuffer() {
        return metadata.getByteBuffer().duplicate();
    }

    public int tierEpoch() {
        return metadata.tierEpoch();
    }

    public long baseOffset() {
        return metadata.baseOffset();
    }

    /**
     * Random ID associated with each TierObjectMetadata entry.
     */
    public UUID objectId() {
        return new UUID(metadata.objectId().mostSignificantBits(), metadata.objectId().leastSignificantBits());
    }

    /**
     * @return Base64 string representation of metadata message ID
     */
    public String objectIdAsBase64() {
        return CoreUtils.uuidToBase64(objectId());
    }

    public long endOffset() {
        return baseOffset() + endOffsetDelta();
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

    public State state() {
        return State.toState(metadata.state());
    }

    public void mutateState(State newState) {
        if (!state().canTransitionTo(newState))
            throw new IllegalStateException("Cannot transition from " + state() + " to " + newState);
        metadata.mutateState(newState.id);
    }

    public boolean hasProducerState() {
        return metadata.hasProducerState();
    }

    public short version() {
        return metadata.version();
    }

    private int endOffsetDelta() {
        return metadata.endOffsetDelta();
    }

    @Override
    public String toString() {
        return "TierObjectMetadata(" +
                "version=" + metadata.version() + ", " +
                "topicIdPartition=" + topicIdPartition() + ", " +
                "tierEpoch=" + tierEpoch() + ", " +
                "objectId=" + objectId() + ", " +
                "baseOffset=" + baseOffset() + ", " +
                "endOffset=" + endOffset() + ", " +
                "maxTimestamp=" + maxTimestamp() + ", " +
                "size=" + size() + ", " +
                "state=" + state() + ", " +
                "hasEpochState=" + hasEpochState() + ", " +
                "hasAbortedTxns=" + hasAbortedTxns() + ", " +
                "hasProducerState=" + hasProducerState() + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicIdPartition(), payloadBuffer());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TierObjectMetadata that = (TierObjectMetadata) o;
        return Objects.equals(topicIdPartition(), that.topicIdPartition()) &&
                Objects.equals(payloadBuffer(), that.payloadBuffer());
    }
}
