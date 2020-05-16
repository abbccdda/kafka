/**
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.model;

import java.util.Objects;
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

    public BrokerRemovalStatus(int brokerId,
                               BrokerRemovalDescription.BrokerShutdownStatus brokerShutdownStatus,
                               BrokerRemovalDescription.PartitionReassignmentsStatus partitionReassignmentsStatus,
                               Exception e) {
        this.brokerId = brokerId;
        this.brokerShutdownStatus = brokerShutdownStatus;
        this.partitionReassignmentsStatus = partitionReassignmentsStatus;
        this.exception = e;
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

    @Override
    public String toString() {
        return "BrokerRemovalStatus{" +
            "brokerId=" + brokerId +
            ", brokerShutdownStatus=" + brokerShutdownStatus.name() +
            ", partitionReassignmentsStatus=" + partitionReassignmentsStatus.name() +
            ", exception=" + exception +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerRemovalStatus that = (BrokerRemovalStatus) o;
        return brokerId == that.brokerId &&
            brokerShutdownStatus == that.brokerShutdownStatus &&
            partitionReassignmentsStatus == that.partitionReassignmentsStatus &&
            Objects.equals(exception, that.exception);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, brokerShutdownStatus, partitionReassignmentsStatus, exception);
    }
}
