/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import com.linkedin.kafka.cruisecontrol.PlanComputationOptions;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mutable set of options for broker removal to be passed around
 * each broker removal phase in #{@link BrokerRemovalPhaseExecutor}.
 *
 * Callers are expected to set the #{@code proposals} variable when the removal plan is computed.
 */
public class BrokerRemovalOptions {
  public final Set<Integer> brokersToRemove;
  public final Optional<Long> brokerEpoch;
  public final BrokerRemovalCallback progressCallback;
  public final String uid;
  public final PlanComputationOptions planComputationOptions;
  public final Long replicationThrottle;
  public final OperationProgress operationProgress;
  public final AtomicReference<Executor.ReservationHandle> reservationHandle;
  public Set<ExecutionProposal> proposals;

  /**
   * @param brokersToRemove - the IDs of the brokers to remove. This is currently limited to one
   * @param brokerEpoch - the epoch of the broker to remove, needed for the shutdown request
   * @param progressCallback - the callback to invoke when there is progress made on the removal
   * @param uid - a unique identifier of this specific operation
   * @param planComputationOptions - the options to use for the removal plan computation
   * @param replicationThrottle - the throttle to use for replication traffic, in bytes/sec
   */
  public BrokerRemovalOptions(Set<Integer> brokersToRemove, Optional<Long> brokerEpoch,
                              BrokerRemovalCallback progressCallback, String uid,
                              PlanComputationOptions planComputationOptions, Long replicationThrottle,
                              OperationProgress operationProgress) {
    this.brokersToRemove = brokersToRemove;
    this.brokerEpoch = brokerEpoch;
    this.progressCallback = progressCallback;
    this.uid = uid;
    this.planComputationOptions = planComputationOptions;
    this.replicationThrottle = replicationThrottle;
    this.operationProgress = operationProgress;
    this.reservationHandle = new AtomicReference<>();
  }

  public void setProposals(Set<ExecutionProposal> proposals) {
    this.proposals = proposals;
  }
}
