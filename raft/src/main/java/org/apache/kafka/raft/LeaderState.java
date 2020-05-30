/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft;

import org.apache.kafka.common.message.DescribeQuorumResponseData;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

public class LeaderState implements EpochState {
    private final int localId;
    private final int epoch;
    private final long epochStartOffset;

    private Optional<LogOffsetMetadata> highWatermark = Optional.empty();
    private final Map<Integer, VoterState> voterReplicaStates = new HashMap<>();
    private final Map<Integer, ReplicaState> observerReplicaStates = new HashMap<>();
    private static final long OBSERVER_SESSION_TIMEOUT_MS = 300_000L;

    protected LeaderState(int localId, int epoch, long epochStartOffset, Set<Integer> voters) {
        this.localId = localId;
        this.epoch = epoch;
        this.epochStartOffset = epochStartOffset;

        for (int voterId : voters) {
            boolean hasEndorsedLeader = voterId == localId;
            this.voterReplicaStates.put(voterId, new VoterState(voterId, epochStartOffset, hasEndorsedLeader));
        }
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public ElectionState election() {
        return ElectionState.withElectedLeader(epoch, localId, voterReplicaStates.keySet());
    }

    @Override
    public int epoch() {
        return epoch;
    }

    public Set<Integer> followers() {
        return voterReplicaStates.keySet().stream().filter(id -> id != localId).collect(Collectors.toSet());
    }

    public int localId() {
        return localId;
    }

    public Set<Integer> nonEndorsingFollowers() {
        Set<Integer> nonEndorsing = new HashSet<>();
        for (VoterState state : voterReplicaStates.values()) {
            if (!state.hasEndorsedLeader)
                nonEndorsing.add(state.nodeId);
        }
        return nonEndorsing;
    }

    private boolean updateHighWatermark() {
        // Find the largest offset which is replicated to a majority of replicas (the leader counts)
        List<VoterState> followersByDescendingFetchOffset = followersByDescendingFetchOffset();

        int indexOfHw = voterReplicaStates.size() / 2;
        Optional<LogOffsetMetadata> highWatermarkUpdateOpt = followersByDescendingFetchOffset.get(indexOfHw).endOffset;

        if (highWatermarkUpdateOpt.isPresent()) {
            // When a leader is first elected, it cannot know the high watermark of the previous
            // leader. In order to avoid exposing a non-monotonically increasing value, we have
            // to wait for followers to catch up to the start of the leader's epoch.
            long highWatermarkUpdate = highWatermarkUpdateOpt.get().offset;
            if (highWatermarkUpdate >= epochStartOffset) {
                highWatermark = highWatermarkUpdateOpt;
                return true;
            }
        }
        return false;
    }

    private OptionalLong quorumMajorityFetchTimestamp() {
        // Find the latest timestamp which is fetched by a majority of replicas (the leader counts)
        ArrayList<ReplicaState> followersByDescendingFetchTimestamp = new ArrayList<>(this.voterReplicaStates.values());
        followersByDescendingFetchTimestamp.sort(FETCH_TIMESTAMP_COMPARATOR);
        int indexOfTimestamp = voterReplicaStates.size() / 2;
        return followersByDescendingFetchTimestamp.get(indexOfTimestamp).lastFetchTimestamp;
    }

    /**
     * @return The updated lower bound of fetch timestamps for a majority of quorum; -1 indicating that we have
     *         not received fetch from the majority yet
     */
    public OptionalLong updateReplicaFetchState(int nodeId,
                                                long fetchOffset,
                                                long leaderEndOffset,
                                                long timestamp) {
        ReplicaState state = getReplicaState(nodeId, leaderEndOffset);

        state.updateFetchState(fetchOffset, leaderEndOffset, timestamp);

        return isVoter(nodeId) ? quorumMajorityFetchTimestamp() : OptionalLong.empty();
    }

    public List<Integer> nonLeaderVotersByDescendingFetchOffset() {
        return followersByDescendingFetchOffset().stream()
                   .filter(state -> state.nodeId != localId)
                   .map(state -> state.nodeId)
                   .collect(Collectors.toList());
    }

    private List<VoterState> followersByDescendingFetchOffset() {
        return new ArrayList<>(this.voterReplicaStates.values())
            .stream().sorted().collect(Collectors.toList());
    }

    /**
     * @return true if the high watermark is updated too
     */
    public boolean updateEndOffset(int remoteNodeId, LogOffsetMetadata endOffsetMetadata) {
        // Ignore fetches from negative replica id, as it indicates
        // the fetch is from non-replica. For example, a consumer.
        if (remoteNodeId < 0) {
            return false;
        }

        ReplicaState state = getReplicaState(remoteNodeId, endOffsetMetadata.offset);

        state.endOffset.ifPresent(currentEndOffset -> {
            if (currentEndOffset.offset > endOffsetMetadata.offset)
                throw new IllegalArgumentException("Non-monotonic update to end offset for nodeId " + remoteNodeId);
        });

        state.endOffset = Optional.of(endOffsetMetadata);

        if (isVoter(remoteNodeId)) {
            ((VoterState) state).hasEndorsedLeader = true;
            addEndorsementFrom(remoteNodeId);
            return updateHighWatermark();
        }
        return false;
    }

    public void addEndorsementFrom(int remoteNodeId) {
        VoterState voterState = ensureValidVoter(remoteNodeId);
        voterState.hasEndorsedLeader = true;
    }

    private VoterState ensureValidVoter(int remoteNodeId) {
        VoterState state = voterReplicaStates.get(remoteNodeId);
        if (state == null)
            throw new IllegalArgumentException("Unexpected endorsement from non-voter " + remoteNodeId);
        return state;
    }

    ReplicaState getReplicaState(int remoteNodeId, long leaderEndOffset) {
        ReplicaState state = voterReplicaStates.get(remoteNodeId);
        if (state == null) {
            observerReplicaStates.putIfAbsent(remoteNodeId, new ReplicaState(remoteNodeId, leaderEndOffset));
            return observerReplicaStates.get(remoteNodeId);
        }
        return state;
    }

    List<DescribeQuorumResponseData.ReplicaState> getVoterStates() {
        return convertReplicaStates(voterReplicaStates);
    }

    List<DescribeQuorumResponseData.ReplicaState> getObserverStates() {
        clearInactiveObservers();
        return convertReplicaStates(observerReplicaStates);
    }

    private static <R extends ReplicaState> List<DescribeQuorumResponseData.ReplicaState> convertReplicaStates(
        Map<Integer, R> replicaStates) {
        return replicaStates.entrySet().stream()
                   .map(entry -> new DescribeQuorumResponseData.ReplicaState()
                                     .setReplicaId(entry.getKey())
                                     .setLogEndOffset(entry.getValue().endOffset
                                         .map(logOffsetMetadata -> logOffsetMetadata.offset).orElse(-1L))
                                     .setLastCaughtUpTimeMs(entry.getValue().lastCaughtUpTimeMs))
                   .collect(Collectors.toList());
    }

    private void clearInactiveObservers() {
        final long currentTimeMs = System.currentTimeMillis();
        observerReplicaStates.entrySet().removeIf(
            integerReplicaStateEntry ->
                currentTimeMs - integerReplicaStateEntry.getValue().lastFetchTimestamp.orElse(-1)
                    >= OBSERVER_SESSION_TIMEOUT_MS);
    }

    private boolean isVoter(int remoteNodeId) {
        return voterReplicaStates.containsKey(remoteNodeId);
    }

    /**
     * Update the local end offset after a log append to the leader. Return true if this
     * update results in a bump to the high watermark.
     *
     * @param endOffsetMetadata The new log end offset
     * @return true if the high watermark increased, false otherwise
     */
    public boolean updateLocalEndOffset(LogOffsetMetadata endOffsetMetadata) {
        return updateEndOffset(localId, endOffsetMetadata);
    }

    private static class ReplicaState implements Comparable<ReplicaState> {
        final int nodeId;

        Optional<LogOffsetMetadata> endOffset;

        OptionalLong lastFetchTimestamp;
        long lastCaughtUpTimeMs = 0L;
        long lastFetchLeaderLogEndOffset;

        public ReplicaState(int nodeId,
                            long lastLeaderLogEndOffset) {
            this.nodeId = nodeId;
            this.endOffset = Optional.empty();

            this.lastFetchTimestamp = OptionalLong.empty();
            this.lastFetchLeaderLogEndOffset = lastLeaderLogEndOffset;
        }

        void updateFetchState(long currentFetchOffset,
                              long currentLeaderEndOffset,
                              long currentFetchTimeMs) {
            if (currentFetchOffset >= currentLeaderEndOffset) {
                lastCaughtUpTimeMs = currentFetchTimeMs;
            } else if (currentFetchOffset >= lastFetchLeaderLogEndOffset) {
                lastCaughtUpTimeMs = lastFetchTimestamp.orElse(0L);
            }

            // To be resilient to system time shifts we do not strictly
            // require the timestamp be monotonically increasing.
            lastFetchTimestamp = OptionalLong.of(Math.max(lastFetchTimestamp.orElse(-1L), currentFetchTimeMs));
            lastFetchLeaderLogEndOffset = currentLeaderEndOffset;
        }

        @Override
        public int compareTo(ReplicaState that) {
            if (this.endOffset.equals(that.endOffset))
                return Integer.compare(this.nodeId, that.nodeId);
            else if (!this.endOffset.isPresent())
                return 1;
            else if (!that.endOffset.isPresent())
                return -1;
            else
                return Long.compare(that.endOffset.get().offset, this.endOffset.get().offset);
        }
    }

    private static class VoterState extends ReplicaState {
        boolean hasEndorsedLeader;

        public VoterState(int nodeId,
                          long lastLeaderLogEndOffset,
                          boolean hasEndorsedLeader) {
            super(nodeId, lastLeaderLogEndOffset);
            this.hasEndorsedLeader = hasEndorsedLeader;
        }
    }

    private static final Comparator<ReplicaState> FETCH_TIMESTAMP_COMPARATOR = (state, that) -> {
        if (state.lastFetchTimestamp.equals(that.lastFetchTimestamp))
            return Integer.compare(state.nodeId, that.nodeId);
        else if (!state.lastFetchTimestamp.isPresent())
            return 1;
        else if (!that.lastFetchTimestamp.isPresent())
            return -1;
        else
            return Long.compare(that.lastFetchTimestamp.getAsLong(), state.lastFetchTimestamp.getAsLong());
    };
}
