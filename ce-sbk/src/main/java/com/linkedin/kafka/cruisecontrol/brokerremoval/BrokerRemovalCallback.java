/*
 * Copyright (C) 2020 Confluent, Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import io.confluent.databalancer.operation.BrokerRemovalStateMachine;

/**
 * This class helps track the progress of a single broker removal operation.
 * This callback should be invoked whenever any #{@link BrokerRemovalEvent}
 * occurs as part of removing the broker.
 *
 * The state machine is described thoroughly in #{@link BrokerRemovalStateMachine}.
 */
public interface BrokerRemovalCallback {
  enum BrokerRemovalEvent {
    INITIAL_PLAN_COMPUTATION_SUCCESS,
    INITIAL_PLAN_COMPUTATION_FAILURE,
    BROKER_SHUTDOWN_SUCCESS,
    BROKER_SHUTDOWN_FAILURE,
    PLAN_COMPUTATION_SUCCESS,
    PLAN_COMPUTATION_FAILURE,
    PLAN_EXECUTION_SUCCESS,
    PLAN_EXECUTION_FAILURE,
    BROKER_RESTARTED
  }

  /**
   * Registers a new errorful #{@link BrokerRemovalEvent} as part of the progress changing
   */
  public void registerEvent(BrokerRemovalEvent pe, Exception e);

  /**
   * Registers a new #{@link BrokerRemovalEvent} as part of the progress changing
   */
  public void registerEvent(BrokerRemovalEvent pe);
}
