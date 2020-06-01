/*
 * Copyright (C) 2020 Confluent Inc.
 */
package kafka.common;

import org.apache.kafka.clients.admin.BrokerRemovalDescription;

/**
 * Class to store status of broker removal progress. This can be then queried by
 * user to monitor the progress of the broker removal operation.
 */
public class BrokerRemovalStatus {

    private final int brokerId; // id of the broker getting removed
    private final Exception exception;
    private final BrokerRemovalDescription.BrokerShutdownStatus brokerShutdownStatus;
    private final BrokerRemovalDescription.PartitionReassignmentsStatus partitionReassignmentsStatus;

    // Time at which remove broker api request was acknowledged. After this time, client can use
    // DescribeBrokersRemoval api to enquire about status of broker removal.
    private long startTime;
    // Time at which status was last updated
    private long lastUpdateTime;

    public BrokerRemovalStatus(int brokerId,
                               BrokerRemovalDescription.BrokerShutdownStatus brokerShutdownStatus,
                               BrokerRemovalDescription.PartitionReassignmentsStatus partitionReassignmentsStatus,
                               Exception e) {
        this.brokerId = brokerId;
        this.brokerShutdownStatus = brokerShutdownStatus;
        this.partitionReassignmentsStatus = partitionReassignmentsStatus;
        this.exception = e;
    }

    public int brokerId() {
        return brokerId;
    }

    /**
     * The nullable #{@link Exception} that this broker removal operation experienced
     */
    public Exception exception() {
        return exception;
    }

    public BrokerRemovalDescription.PartitionReassignmentsStatus partitionReassignmentsStatus() {
        return partitionReassignmentsStatus;
    }

    public BrokerRemovalDescription.BrokerShutdownStatus brokerShutdownStatus() {
        return brokerShutdownStatus;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public String toString() {
        return "BrokerRemovalStatus{" +
            "brokerId=" + brokerId +
            ", brokerShutdownStatus=" + brokerShutdownStatus.name() +
            ", partitionReassignmentsStatus=" + partitionReassignmentsStatus.name() +
            ", exception=" + exception +
            ", startTime=" + startTime +
            ", lastUpdateTime=" + lastUpdateTime +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerRemovalStatus that = (BrokerRemovalStatus) o;
        return brokerId == that.brokerId;
    }

    @Override
    public int hashCode() {
        return brokerId;
    }
}
