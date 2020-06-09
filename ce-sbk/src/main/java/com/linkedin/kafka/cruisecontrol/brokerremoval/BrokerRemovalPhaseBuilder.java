/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import org.apache.kafka.common.errors.PlanComputationException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
  private BrokerRemovalPhaseExecutor.Builder executorReservationPhaseBuilder;
  private BrokerRemovalPhaseExecutor.Builder initialPlanComputationPhaseBuilder;
  private BrokerRemovalPhaseExecutor.Builder brokerShutdownPhaseBuilder;
  private BrokerRemovalPhaseExecutor.Builder planComputationPhaseBuilder;
  private BrokerRemovalPhaseExecutor.Builder planExecutionPhaseBuilder;

  /**
   * Build the necessary phase executors with the appropriate removal events on success/failure.
   * For more detail on the state machine that should be obeyed here,
   * see #{@link io.confluent.databalancer.operation.BrokerRemovalStateMachine}
   */
  public BrokerRemovalPhaseBuilder() {
    executorReservationPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder(
        null, // success here is a pre-requisite
        BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE,
        brokerIds ->
            String.format("Error while acquiring a reservation on the executor and aborting ongoing executions prior to beginning the broker removal operation for brokers %s.", brokerIds),
        KafkaCruiseControlException.class
    );
    initialPlanComputationPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder(
        BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS,
        BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE,
        brokerIds ->
            String.format("Error while computing the initial remove broker plan for brokers %s prior to shutdown.", brokerIds),
        PlanComputationException.class
    );
    brokerShutdownPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder(
        BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS,
        BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_FAILURE,
        brokerIds -> String.format("Error while executing broker shutdown for brokers %s.", brokerIds),
        KafkaCruiseControlException.class
    );
    planComputationPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder(
        BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_SUCCESS,
        BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE,
        brokerIds -> String.format("Error while computing broker removal plan for broker %s.", brokerIds),
        PlanComputationException.class
    );
    // the actual completion is registered in the Executor once all the proposals finish executing
    planExecutionPhaseBuilder = new BrokerRemovalPhaseExecutor.Builder(
        null, BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE,
        brokerIds -> String.format("Unexpected exception while submitting the broker removal plan for broker %s", brokerIds),
        KafkaCruiseControlException.class
    );
  }

  /**
   * Returns all the phases chained together in a #{@link CompletableFuture} object
   */
  public BrokerRemovalExecution composeRemoval(
      BrokerRemovalOptions removalOpts,
      BrokerRemovalCallback progressCallback,
      BrokerRemovalPhase executorReservationPhase,
      BrokerRemovalPhase initialPlanComputationPhase,
      BrokerRemovalPhase brokerShutdownPhase,
      BrokerRemovalPhase planComputationPhase,
      BrokerRemovalPhase planExecutionPhase) {
    BrokerRemovalPhaseExecutor executorReservationPhaseExecutor = executorReservationPhaseBuilder.build(progressCallback, removalOpts);
    BrokerRemovalPhaseExecutor initialPlanComputationPhaseExecutor = initialPlanComputationPhaseBuilder.build(progressCallback, removalOpts);
    BrokerRemovalPhaseExecutor brokerShutdownPhaseExecutor = brokerShutdownPhaseBuilder.build(progressCallback, removalOpts);
    BrokerRemovalPhaseExecutor planComputationPhaseExecutor = planComputationPhaseBuilder.build(progressCallback, removalOpts);
    BrokerRemovalPhaseExecutor planExecutionPhaseExecutor = planExecutionPhaseBuilder.build(progressCallback, removalOpts);

    CompletableFuture<Void> initialFuture = new CompletableFuture<>();
    return new BrokerRemovalExecution(removalOpts.reservationHandle, initialFuture,
        initialFuture
            .thenCompose(aVoid -> executorReservationPhaseExecutor.execute(executorReservationPhase))
            .thenCompose(aVoid -> initialPlanComputationPhaseExecutor.execute(initialPlanComputationPhase))
            .thenCompose(aVoid -> brokerShutdownPhaseExecutor.execute(brokerShutdownPhase))
            .thenCompose(aVoid -> planComputationPhaseExecutor.execute(planComputationPhase))
            .thenCompose(aVoid -> planExecutionPhaseExecutor.execute(planExecutionPhase)));
  }

  public static class BrokerRemovalExecution {
    private AtomicReference<Executor.ReservationHandle> reservationHandle;
    private CompletableFuture<Void> initialFuture;
    public final CompletableFuture<Void> chainedFutures;

    public BrokerRemovalExecution(AtomicReference<Executor.ReservationHandle> reservationHandle,
                                  CompletableFuture<Void> initialFuture,
                                  CompletableFuture<Void> chainedFutures) {
      this.reservationHandle = reservationHandle;
      this.initialFuture = initialFuture;
      this.chainedFutures = chainedFutures;
    }

    /**
     * Executes the broker removal
     */
    public void execute(Duration duration) throws Throwable {
      try {
        initialFuture.complete(null);
        chainedFutures.get(duration.toMillis(), TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        throw e.getCause();
      } finally {
        Executor.ReservationHandle handle = reservationHandle.get();
        if (handle != null) {
          handle.close();
        }
      }
    }
  }
}
