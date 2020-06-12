/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import io.confluent.databalancer.operation.BrokerRemovalStateMachine;
import io.confluent.databalancer.record.RemoveBroker;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_CANCELED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_FAILED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.INITIAL_PLAN_COMPUTATION_FAILED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.INITIAL_PLAN_COMPUTATION_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_CANCELED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_FAILED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_CANCELED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_FAILED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_SUCCEEDED;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.broker_shutdown_canceled;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.broker_shutdown_failed;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.broker_shutdown_initiated;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.initial_plan_computation_failed;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.initial_plan_computation_initiated;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.plan_computation_canceled;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.plan_computation_failed;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.plan_computation_initiated;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.plan_execution_canceled;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.plan_execution_failed;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.plan_execution_initiated;
import static io.confluent.databalancer.record.RemoveBroker.RemovalState.plan_execution_succeeded;


/**
 * A simple helper to serialize #{@link io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState}
 */
public class BrokerRemovalStateSerializer {
  private static List<Mapping> stateMappings = Arrays.asList(
      new Mapping(INITIAL_PLAN_COMPUTATION_INITIATED, initial_plan_computation_initiated),
      new Mapping(INITIAL_PLAN_COMPUTATION_FAILED, initial_plan_computation_failed),

      new Mapping(BROKER_SHUTDOWN_FAILED, broker_shutdown_failed),
      new Mapping(BROKER_SHUTDOWN_INITIATED, broker_shutdown_initiated),
      new Mapping(BROKER_SHUTDOWN_CANCELED, broker_shutdown_canceled),

      new Mapping(PLAN_COMPUTATION_FAILED, plan_computation_failed),
      new Mapping(PLAN_COMPUTATION_CANCELED, plan_computation_canceled),
      new Mapping(PLAN_COMPUTATION_INITIATED, plan_computation_initiated),

      new Mapping(PLAN_EXECUTION_CANCELED, plan_execution_canceled),
      new Mapping(PLAN_EXECUTION_FAILED, plan_execution_failed),
      new Mapping(PLAN_EXECUTION_SUCCEEDED, plan_execution_succeeded),
      new Mapping(PLAN_EXECUTION_INITIATED, plan_execution_initiated)
  );

  private static Map<BrokerRemovalStateMachine.BrokerRemovalState, Mapping> stateToMappings = new HashMap<>();
  private static Map<RemoveBroker.RemovalState, Mapping> persistedStateToMappings = new HashMap<>();

  static {
    for (Mapping mapping : stateMappings) {
      stateToMappings.put(mapping.state, mapping);
      persistedStateToMappings.put(mapping.persistedState, mapping);
    }
    persistedStateToMappings = Collections.unmodifiableMap(persistedStateToMappings);
    stateToMappings = Collections.unmodifiableMap(stateToMappings);
  }

  public static RemoveBroker.RemovalState serialize(BrokerRemovalStateMachine.BrokerRemovalState state) {
    if (!stateToMappings.containsKey(state)) {
      throw new IllegalArgumentException(String.format("Cannot serialize state %s as it is not recognized", state));
    }
    return stateToMappings.get(state).persistedState;
  }

  public static BrokerRemovalStateMachine.BrokerRemovalState deserialize(RemoveBroker.RemovalState persistedState) {
    if (!persistedStateToMappings.containsKey(persistedState)) {
      throw new IllegalArgumentException(String.format("Cannot deserialize persisted state %s as it is not recognized", persistedState));
    }
    return persistedStateToMappings.get(persistedState).state;
  }

  private static class Mapping {
    private final BrokerRemovalStateMachine.BrokerRemovalState state;
    private final RemoveBroker.RemovalState persistedState;

    private Mapping(BrokerRemovalStateMachine.BrokerRemovalState state, RemoveBroker.RemovalState persistedState) {
      this.state = state;
      this.persistedState = persistedState;
    }
  }
}
