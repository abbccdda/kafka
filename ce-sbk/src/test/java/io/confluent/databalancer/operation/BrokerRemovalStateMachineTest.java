/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.operation;

import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.BROKER_RESTARTED;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS;
import static com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_INITIATED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_CANCELED;
import static io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_INITIATED;
import static org.junit.Assert.assertEquals;

public class BrokerRemovalStateMachineTest {

    int brokerId = 1;

    /**
     * Test if state machine is created w/o any initial state, it get initialized with start state.
     */
    @Test
    public void testInitialState() {
        BrokerRemovalStateMachine stateMachine = new BrokerRemovalStateMachine(brokerId);

        stateMachine.advanceState(INITIAL_PLAN_COMPUTATION_SUCCESS);
        assertEquals(BROKER_SHUTDOWN_INITIATED, stateMachine.currentState);
    }

    /**
     * Check if an invalid event is delivered to the state machine, it throws IllegalStateException.
     */
    @Test(expected = IllegalStateException.class)
    public void testInvalidTransition() {
        BrokerRemovalStateMachine stateMachine = new BrokerRemovalStateMachine(brokerId);

        stateMachine.advanceState(BROKER_SHUTDOWN_SUCCESS);
    }

    /**
     * Test if state machine can be crated from an intermediate state and it works properly.
     */
    @Test
    public void testStateMachineFromIntermediateState() {
        BrokerRemovalStateMachine stateMachine = new BrokerRemovalStateMachine(brokerId, PLAN_EXECUTION_INITIATED);

        stateMachine.advanceState(BROKER_RESTARTED);
        assertEquals(PLAN_EXECUTION_CANCELED, stateMachine.currentState);
    }
}