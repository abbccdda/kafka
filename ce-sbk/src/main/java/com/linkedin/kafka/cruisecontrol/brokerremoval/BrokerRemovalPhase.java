/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.brokerremoval;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import java.util.Optional;

/**
 * A functional interface to implement a phase of the broker removal operation.
 *
 * For more information regarding the broker removal phases, see #{@link KafkaCruiseControl#removeBroker(int, Optional, BrokerRemovalCallback, String)}
 * and #{@link BrokerRemovalPhaseExecutor}
 */
public interface BrokerRemovalPhase<T> {
  /**
   * Execute the broker removal phase
   * @return The result of the phase
   * @throws Exception - if anything goes wrong during phase execution
   */
  T execute(BrokerRemovalOptions args) throws Exception;
}
