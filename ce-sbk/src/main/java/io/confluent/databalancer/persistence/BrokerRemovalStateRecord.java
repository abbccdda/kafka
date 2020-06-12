/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import io.confluent.databalancer.operation.BrokerRemovalStateMachine;
import kafka.common.BrokerRemovalDescriptionInternal;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;

/**
 * An internal, persisted representation of a broker removal operation's state
 */
public class BrokerRemovalStateRecord {
  private final int brokerId; // id of the broker getting removed
  private final BrokerRemovalStateMachine.BrokerRemovalState state;
  private final Exception exception;

  // Time at which remove broker api request was acknowledged. After this time, client can use
  // DescribeBrokersRemoval api to enquire about status of broker removal.
  private long startTime;
  // Time at which status was last updated
  private long lastUpdateTime;

  /**
   * @param brokerId - the ID of the broker that is being removed
   * @param state - the latest state of the broker removal operation
   * @param exception - nullable. the exception that caused the operation to enter this state
   */
  public BrokerRemovalStateRecord(int brokerId,
                                  BrokerRemovalStateMachine.BrokerRemovalState state,
                                  Exception exception) {
    this.brokerId = brokerId;
    this.state = state;
    this.exception = exception;
  }

  public long startTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long lastUpdateTime() {
    return lastUpdateTime;
  }

  public void setLastUpdateTime(long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  public int brokerId() {
    return brokerId;
  }

  public BrokerRemovalStateMachine.BrokerRemovalState state() {
    return state;
  }

  public Exception exception() {
    return exception;
  }

  public BrokerRemovalDescription.PartitionReassignmentsStatus partitionReassignmentsStatus() {
    return state.partitionReassignmentsStatus();
  }

  public BrokerRemovalDescription.BrokerShutdownStatus brokerShutdownStatus() {
    return state.brokerShutdownStatus();
  }

  public BrokerRemovalDescriptionInternal toRemovalDescription() {
    return new BrokerRemovalDescriptionInternal(brokerId, state.brokerShutdownStatus(), state.partitionReassignmentsStatus(), exception);
  }

  @Override
  public String toString() {
    return "BrokerRemovalStateRecord{" +
        "brokerId=" + brokerId +
        ", state=" + state +
        ", brokerShutdownStatus=" + brokerShutdownStatus().name() +
        ", partitionReassignmentsStatus=" + partitionReassignmentsStatus().name() +
        ", exception=" + exception +
        ", startTime=" + startTime +
        ", lastUpdateTime=" + lastUpdateTime +
        '}';
  }

  @Override
  public int hashCode() {
    return brokerId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BrokerRemovalStateRecord that = (BrokerRemovalStateRecord) o;
    return brokerId == that.brokerId;
  }

}
