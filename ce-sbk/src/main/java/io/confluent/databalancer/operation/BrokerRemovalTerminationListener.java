/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.operation;

/**
 * A listener that gets called when the broker removal operation reaches a terminal state
 */
public interface BrokerRemovalTerminationListener {
  /**
   * Called when the state of the removal operation reaches a terminal point
   *
   * @param brokerId - the ID of the broker, whose removal operation reached a terminal state
   * @param state - the terminal state
   * @param e - nullable, the exception that caused the terminal state
   */
  void onTerminalState(int brokerId, BrokerRemovalStateMachine.BrokerRemovalState state, Exception e);
}
