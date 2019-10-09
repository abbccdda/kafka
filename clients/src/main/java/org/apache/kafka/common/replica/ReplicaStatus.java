/*
 * Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.common.replica;

import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Represents the status of a replica in relation to the partition leader.
 */
public class ReplicaStatus {
    private final int brokerId;
    private final Mode mode;
    private final boolean isCaughtUp;
    private final boolean isInSync;
    private final long logStartOffset;
    private final long logEndOffset;
    private final long lastCaughtUpTimeMs;
    private final long lastFetchTimeMs;

    /**
     * The replication mode for a replica.
     */
    public enum Mode {
        LEADER((byte) 0),
        FOLLOWER((byte) 1),
        OBSERVER((byte) 2);

        private final byte value;

        Mode(byte value) {
            this.value = value;
        }

        public byte value() {
          return value;
        }

        public static Mode fromValue(byte value) {
            for (Mode mode : values()) {
                if (mode.value == value) {
                    return mode;
                }
            }
            throw new NoSuchElementException("Invalid replica mode " + value);
        }
    }

    public ReplicaStatus(int brokerId,
                         Mode mode,
                         boolean isCaughtUp,
                         boolean isInSync,
                         long logStartOffset,
                         long logEndOffset,
                         long lastCaughtUpTimeMs,
                         long lastFetchTimeMs) {
        this.brokerId = brokerId;
        this.mode = mode;
        this.isCaughtUp = isCaughtUp;
        this.isInSync = isInSync;
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
        this.lastCaughtUpTimeMs = lastCaughtUpTimeMs;
        this.lastFetchTimeMs = lastFetchTimeMs;
    }

    /**
     * The broker ID the replica exists on.
     */
    public int brokerId() {
        return brokerId;
    }

    /**
     * The current mode of the replica.
     */
    public Mode mode() {
        return mode;
    }

    /**
     * Whether the replica's log is sufficiently caught up to the leader.
     *
     * Note being caught up doesn't necessarily mean the replica is in the ISR set. For example,
     * the replica may be an observer, or a follower that cannot be included in the ISR due to
     * topic placement constraints.
     */
    public boolean isCaughtUp() {
        return isCaughtUp;
    }

    /**
     * Whether the replica is in the ISR set.
     */
    public boolean isInSync() {
        return isInSync;
    }

    /**
     * The replica's starting log offset, or {@link kafka.log.Log#UnknownOffset} if unknown.
     */
    public long logStartOffset() {
        return logStartOffset;
    }

    /**
     * The replica's ending log offset, or {@link kafka.log.Log#UnknownOffset} if unknown.
     */
    public long logEndOffset() {
        return logEndOffset;
    }

    /**
     * The time when the replica was caught up to the leader.
     *
     * If this replica is the leader, then it's the time at which the status was retrieved.
     */
    public long lastCaughtUpTimeMs() {
        return lastCaughtUpTimeMs;
    }

    /**
     * The time when the leader processed the last fetch request from the replica.
     *
     * If this replica is the leader, then it's the time at which the status was retrieved.
     */
    public long lastFetchTimeMs() {
        return lastFetchTimeMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaStatus that = (ReplicaStatus) o;
        return brokerId == that.brokerId &&
                mode == that.mode &&
                isCaughtUp == that.isCaughtUp &&
                isInSync == that.isInSync &&
                logStartOffset == that.logStartOffset &&
                logEndOffset == that.logEndOffset &&
                lastCaughtUpTimeMs == that.lastCaughtUpTimeMs &&
                lastFetchTimeMs == that.lastFetchTimeMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, mode, isCaughtUp, isInSync, logStartOffset, logEndOffset, lastCaughtUpTimeMs, lastFetchTimeMs);
    }

    @Override
    public String toString() {
        return "ReplicaStatus{" +
                "brokerId=" + brokerId +
                ", mode=" + mode +
                ", isCaughtUp=" + isCaughtUp +
                ", isInSync=" + isInSync +
                ", logStartOffset=" + logStartOffset +
                ", logEndOffset=" + logEndOffset +
                ", lastCaughtUpTimeMs=" + lastCaughtUpTimeMs +
                ", lastFetchTimeMs=" + lastFetchTimeMs +
                '}';
    }
}
