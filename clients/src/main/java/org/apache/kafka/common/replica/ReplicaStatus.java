/*
 * Copyright 2019 Confluent Inc.
 */

package org.apache.kafka.common.replica;

import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.kafka.common.Confluent;

/**
 * Represents the status of a replica in relation to the partition leader.
 */
@Confluent
public class ReplicaStatus {
    private final int brokerId;
    private final boolean isLeader;
    private final boolean isObserver;
    private final boolean isIsrEligible;
    private final boolean isInIsr;
    private final boolean isCaughtUp;
    private final long logStartOffset;
    private final long logEndOffset;
    private final long lastCaughtUpTimeMs;
    private final long lastFetchTimeMs;

    public ReplicaStatus(int brokerId,
                         boolean isLeader,
                         boolean isObserver,
                         boolean isIsrEligible,
                         boolean isInIsr,
                         boolean isCaughtUp,
                         long logStartOffset,
                         long logEndOffset,
                         long lastCaughtUpTimeMs,
                         long lastFetchTimeMs) {
        this.brokerId = brokerId;
        this.isLeader = isLeader;
        this.isObserver = isObserver;
        this.isIsrEligible = isIsrEligible;
        this.isInIsr = isInIsr;
        this.isCaughtUp = isCaughtUp;
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
     * Whether the replica is the ISR leader.
     */
    public boolean isLeader() {
        return isLeader;
    }

    /**
     * Whether the replica is an observer, otherwise a sync replica.
     */
    public boolean isObserver() {
        return isObserver;
    }

    /**
     * Whether the replica is a candidate for the ISR set.
     */
    public boolean isIsrEligible() {
        return isIsrEligible;
    }

    /**
     * Whether the replica is in the ISR set.
     */
    public boolean isInIsr() {
        return isInIsr;
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
                isLeader == that.isLeader &&
                isObserver == that.isObserver &&
                isIsrEligible == that.isIsrEligible &&
                isInIsr == that.isInIsr &&
                isCaughtUp == that.isCaughtUp &&
                logStartOffset == that.logStartOffset &&
                logEndOffset == that.logEndOffset &&
                lastCaughtUpTimeMs == that.lastCaughtUpTimeMs &&
                lastFetchTimeMs == that.lastFetchTimeMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, isLeader, isObserver, isIsrEligible, isInIsr, isCaughtUp,
                            logStartOffset, logEndOffset, lastCaughtUpTimeMs, lastFetchTimeMs);
    }

    @Override
    public String toString() {
        return "ReplicaStatus{" +
                "brokerId=" + brokerId +
                ", isLeader=" + isLeader +
                ", isObserver=" + isObserver +
                ", isIsrEligible=" + isIsrEligible +
                ", isInIsr=" + isInIsr +
                ", isCaughtUp=" + isCaughtUp +
                ", logStartOffset=" + logStartOffset +
                ", logEndOffset=" + logEndOffset +
                ", lastCaughtUpTimeMs=" + lastCaughtUpTimeMs +
                ", lastFetchTimeMs=" + lastFetchTimeMs +
                '}';
    }
}
