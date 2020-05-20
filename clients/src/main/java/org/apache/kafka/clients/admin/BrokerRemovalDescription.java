/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import java.util.Optional;

/**
 * A description of a broker removal, which has been listed via {@link ConfluentAdmin#describeBrokerRemovals(DescribeBrokerRemovalsOptions)}.
 * The status of the removal is tracked by two separate status fields:
 *  1. #{@link BrokerRemovalDescription#brokerShutdownStatus()}, a #{@link org.apache.kafka.clients.admin.BrokerRemovalDescription.BrokerShutdownStatus}
 *      denoting the status of the shutdown operation
 *  2. #{@link BrokerRemovalDescription#partitionReassignmentsStatus()}, a #{@link org.apache.kafka.clients.admin.BrokerRemovalDescription.PartitionReassignmentsStatus}
 *      denoting the status of the partition reassignments operation
 *
 * When at least one of the two has a failed status, the broker removal operation is considered failed. The user is expected to retry the removal via #{@link ConfluentAdmin#removeBrokers(List)}.
 *
 * When both have a completed status, the broker removal operation is considered a success.
 */
public class BrokerRemovalDescription {

  /**
   * The status of the broker shutdown.
   *
   * Broker shutdown can be in one of four states:
   * 1. {@link BrokerRemovalDescription.BrokerShutdownStatus#FAILED} - when the broker removal operation failed midway, unable to initiate a shutdown on the broker.
   * The user is expected to either retry the removal or cancel it.
   * 2. {@link BrokerRemovalDescription.BrokerShutdownStatus#PENDING} - when the broker is yet to be shut down.
   * 3. {@link BrokerRemovalDescription.BrokerShutdownStatus#CANCELED} - when the shutdown operation is canceled (e.g due to the initial reassignment plan computation failing)
   * 3. {@link BrokerRemovalDescription.BrokerShutdownStatus#COMPLETE} - when the broker has successfully been shut down.
   * If this status is returned, the broker to be removed is no longer part of the Kafka cluster.
   */
  public enum BrokerShutdownStatus {
    FAILED,
    CANCELED,
    PENDING,
    COMPLETE
  }

  /**
   * The status of the partition reassignments, draining replicas out of the to be removed broker.
   *
   * Partition reassignments can be in one of five states:
   * 1. {@link BrokerRemovalDescription.PartitionReassignmentsStatus#FAILED} - when the broker removal operation failed midway.
   * It is unknown whether any replicas were moved away or not.
   * The user is expected to either retry the removal or cancel it.
   * 2. {@link BrokerRemovalDescription.PartitionReassignmentsStatus#PENDING} - when the partition reassignments plan is being computed. Reassignments are yet to happen.
   * 3. {@link BrokerRemovalDescription.PartitionReassignmentsStatus#IN_PROGRESS} - when the partitions are actively being reassigned.
   * 4. {@link BrokerRemovalDescription.PartitionReassignmentsStatus#CANCELED} - when the partition drain was cancelled because of unrelated factors, like the broker being restarted or the shutdown failing
   * 5. {@link BrokerRemovalDescription.PartitionReassignmentsStatus#COMPLETE} - when all the replicas of the to be removed broker were reassigned away from it.
   * If this status is returned, the broker to be removed has no replicas on it.
   */
  public enum PartitionReassignmentsStatus {
    CANCELED,
    FAILED,
    PENDING,
    IN_PROGRESS,
    COMPLETE
  }

  private final Integer brokerId;
  private final BrokerShutdownStatus brokerShutdownStatus;
  private final PartitionReassignmentsStatus partitionReassignmentsStatus;
  private final Optional<BrokerRemovalError> removalError;

  public BrokerRemovalDescription(Integer brokerId, BrokerShutdownStatus brokerShutdownStatus,
                                  PartitionReassignmentsStatus partitionReassignmentsStatus,
                                  Optional<BrokerRemovalError> removalError) {
    this.brokerId = brokerId;
    this.brokerShutdownStatus = brokerShutdownStatus;
    this.partitionReassignmentsStatus = partitionReassignmentsStatus;
    this.removalError = removalError;
  }

  public Integer brokerId() {
    return brokerId;
  }

  public BrokerShutdownStatus brokerShutdownStatus() {
    return brokerShutdownStatus;
  }

  public PartitionReassignmentsStatus partitionReassignmentsStatus() {
    return partitionReassignmentsStatus;
  }

  /**
   * The error that failed the broker removal operation
   */
  public Optional<BrokerRemovalError> removalError() {
    return removalError;
  }
}
