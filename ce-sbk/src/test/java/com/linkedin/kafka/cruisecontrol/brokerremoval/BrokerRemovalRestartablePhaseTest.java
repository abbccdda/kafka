/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import io.confluent.databalancer.operation.BrokerRemovalStateMachine.BrokerRemovalState;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class BrokerRemovalRestartablePhaseTest {

    private BrokerRemovalPhase mockPhase = mock(BrokerRemovalPhase.class);
    private BrokerRemovalCallback mockCb = mock(BrokerRemovalCallback.class);

    @Test
    public void testExecute_DoesntExecuteIfStatesAreDifferent() throws Exception {
        BrokerRemovalRestartablePhase restartablePhase = new BrokerRemovalRestartablePhase.BrokerRemovalRestartablePhaseBuilder()
            .setAlwaysExecute(false)
            .setBrokerRemovalStateTracker(mockCb)
            .setPhase(mockPhase).build();
        when(mockPhase.startState()).thenReturn(BrokerRemovalState.BROKER_SHUTDOWN_CANCELED);
        when(mockCb.currentState()).thenReturn(BrokerRemovalState.PLAN_COMPUTATION_CANCELED);

        BrokerRemovalOptions opts = mock(BrokerRemovalOptions.class);
        restartablePhase.execute(opts);

        verify(mockPhase, never()).execute(opts);
        assertTrue("Expected to have skipped execution", restartablePhase.hasSkippedExecution());
    }

    @Test
    public void testAlwaysExecutes_AlwaysCallsPhaseExecute() throws Exception {
        BrokerRemovalRestartablePhase restartablePhase = new BrokerRemovalRestartablePhase.BrokerRemovalRestartablePhaseBuilder()
            .setAlwaysExecute(true)
            .setBrokerRemovalStateTracker(mockCb)
            .setPhase(mockPhase).build();
        // ensure states are different - we wouldn't execute if alwaysExecute was false
        when(mockPhase.startState()).thenReturn(BrokerRemovalState.BROKER_SHUTDOWN_CANCELED);
        when(mockCb.currentState()).thenReturn(BrokerRemovalState.PLAN_COMPUTATION_CANCELED);

        BrokerRemovalOptions opts = mock(BrokerRemovalOptions.class);
        restartablePhase.execute(opts);

        verify(mockPhase).execute(opts);
        assertFalse("Expected to NOT have skipped execution", restartablePhase.hasSkippedExecution());
    }
}