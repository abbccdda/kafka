/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * The result of {@link ConfluentAdmin#describeBrokerRemovals(DescribeBrokerRemovalsOptions)}.
 *
 * The API of this class is evolving. See {@link ConfluentAdmin} for details.
 */
@InterfaceStability.Evolving
public class DescribeBrokerRemovalsResult {
  private final KafkaFuture<Map<Integer, BrokerRemovalDescription>> futures;

  DescribeBrokerRemovalsResult(KafkaFuture<Map<Integer, BrokerRemovalDescription>> brokerRemovals) {
    this.futures = brokerRemovals;
  }

  /**
   * Return a map of broker ids to futures which can be used to check the status of individual broker removals.
   */
  public KafkaFuture<Map<Integer, BrokerRemovalDescription>> descriptions() {
    return futures;
  }
}
