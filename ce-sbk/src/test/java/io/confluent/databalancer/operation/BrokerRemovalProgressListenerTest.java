/*
 * Copyright (C) 2020 Confluent, Inc.
 */
package io.confluent.databalancer.operation;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BrokerRemovalProgressListenerTest {

  private BrokerRemovalStateMachine mockStateMachine;
  private BrokerRemovalProgressListener listener;

  @Before
  public void setUp() {
    mockStateMachine = mock(BrokerRemovalStateMachine.class);
    listener = new BrokerRemovalProgressListener(mockStateMachine);
  }

  @Test
  public void registerEvent() {
    listener.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
    verify(mockStateMachine, times(1)).advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, null);
  }

  @Test
  public void testRegisterEvent() {
    IOException exc = new IOException("exc!");
    listener.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, exc);
    verify(mockStateMachine, times(1)).advanceState(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS, exc);
  }
}