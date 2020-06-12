/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.operation;

/**
 * A listener for the progress of a broker removal operation
 */
public interface BrokerRemovalProgressListener {
  /**
   * Called whenever the state of the removal operation changes.
   *
   * @param brokerId Id of the broker getting removed.
   * @param state the new broker removal state the operation is in
   * @param e - nullable, an exception that occurred during the broker removal op
   */
  void onProgressChanged(int brokerId,
                         BrokerRemovalStateMachine.BrokerRemovalState state,
                         Exception e);
}
