/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import org.apache.kafka.common.errors.BalancerOperationFailedException;
import org.apache.kafka.common.errors.PlanComputationException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A class that helps orchestrate all the necessary steps for achieving a broker removal.
 *
 * A broker removal consists of 4 steps:
 * 0. (pre-requisite) Acquire a reservation on the Executor, aborting running tasks
 * 1. Pre-shutdown plan computation - validate that a plan can be computed successfully
 * 2. Broker shutdown - shutdown the broker to be removed and wait for it to leave the cluster
 * 3. Actual plan computation - compute the plan which we'll execute to drain the broker
 * 4. Plan execution - execute the partition reassignments to move replicas away from the broker (drain)
 *
 * This builder class helps achieve the abstraction of handling success and failure conditions in
 * each of the 4 phases via 4 separate #{@link BrokerRemovalPhaseExecutor}s
 * that execute and handle failures for each phase.
 */
public class BrokerRemovalPhaseBuilder {
  private BrokerRemovalPhaseExecutor.Builder<Void> executorReservationPhaseBuilder;
  private BrokerRemovalPhaseExecutor.Builder<Void> initialPlanComputationPhaseBuilder;
  private BrokerRemovalPhaseExecutor.Builder<Void> brokerShutdownPhaseBuilder;
  private BrokerRemovalPhaseExecutor.Builder<Void> planComputationPhaseBuilder;
  private BrokerRemovalPhaseExecutor.Builder<Future<?>> planExecutionPhaseBuilder;

  /**
   * Build the necessary phase executors with the appropriate removal events on success/failure.
   * For more detail on the state machine that should be obeyed here,
   * see #{@link io.confluent.databalancer.operation.BrokerRemovalStateMachine}
   */
  public BrokerRemovalPhaseBuilder() {
    executorReservationPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder<>(
        null, // success here is a pre-requisite
        BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE,
        brokerIds ->
            String.format("Error while acquiring a reservation on the executor and aborting ongoing executions prior to beginning the broker removal operation for brokers %s.", brokerIds),
        BalancerOperationFailedException.class
    );
    initialPlanComputationPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder<>(
        BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS,
        BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE,
        brokerIds ->
            String.format("Error while computing the initial remove broker plan for brokers %s prior to shutdown.", brokerIds),
        PlanComputationException.class
    );
    brokerShutdownPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder<>(
        BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS,
        BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_FAILURE,
        brokerIds -> String.format("Error while executing broker shutdown for brokers %s.", brokerIds),
        BalancerOperationFailedException.class
    );
    planComputationPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder<>(
        BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS,
        BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE,
        brokerIds -> String.format("Error while computing broker removal plan for broker %s.", brokerIds),
        PlanComputationException.class
    );
    // the actual completion is registered in the Executor once all the proposals finish executing
    planExecutionPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder<>(
        null, BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE,
        brokerIds -> String.format("Unexpected exception while submitting the broker removal plan for broker %s", brokerIds),
        BalancerOperationFailedException.class
    );
  }

  /**
   * Returns all the phases chained together in a #{@link CompletableFuture} object
   */
  public BrokerRemovalFuture composeRemoval(
      BrokerRemovalOptions removalOpts,
      BrokerRemovalCallback progressCallback,
      BrokerRemovalPhase<Void> executorReservationPhase,
      BrokerRemovalPhase<Void> initialPlanComputationPhase,
      BrokerRemovalPhase<Void> brokerShutdownPhase,
      BrokerRemovalPhase<Void> planComputationPhase,
      BrokerRemovalPhase<Future<?>> planExecutionPhase) {
    BrokerRemovalPhaseExecutor<Void> executorReservationPhaseExecutor = executorReservationPhaseBuilder.build(progressCallback, removalOpts);
    BrokerRemovalPhaseExecutor<Void> initialPlanComputationPhaseExecutor = initialPlanComputationPhaseBuilder.build(progressCallback, removalOpts);
    BrokerRemovalPhaseExecutor<Void> brokerShutdownPhaseExecutor = brokerShutdownPhaseBuilder.build(progressCallback, removalOpts);
    BrokerRemovalPhaseExecutor<Void> planComputationPhaseExecutor = planComputationPhaseBuilder.build(progressCallback, removalOpts);
    BrokerRemovalPhaseExecutor<Future<?>>  planExecutionPhaseExecutor = planExecutionPhaseBuilder.build(progressCallback, removalOpts);

    CompletableFuture<Void> initialFuture = new CompletableFuture<>();
    return new BrokerRemovalFuture(removalOpts.reservationHandle, initialFuture,
        initialFuture
            .thenCompose(aVoid -> executorReservationPhaseExecutor.execute(executorReservationPhase))
            .thenCompose(aVoid -> initialPlanComputationPhaseExecutor.execute(initialPlanComputationPhase))
            .thenCompose(aVoid -> brokerShutdownPhaseExecutor.execute(brokerShutdownPhase))
            .thenCompose(aVoid -> planComputationPhaseExecutor.execute(planComputationPhase))
            .thenCompose(aVoid -> planExecutionPhaseExecutor.execute(planExecutionPhase)));
  }
}
