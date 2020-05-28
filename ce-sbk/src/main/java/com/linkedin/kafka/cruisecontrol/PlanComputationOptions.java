/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol;

import javax.annotation.concurrent.Immutable;

@Immutable
public class PlanComputationOptions {
  private final boolean allowCapacityEstimation;
  private final boolean excludeRecentlyDemotedBrokers;
  private final boolean excludeRecentlyRemovedBrokers;

  /**
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   */
  public PlanComputationOptions(boolean allowCapacityEstimation,
                                boolean excludeRecentlyDemotedBrokers,
                                boolean excludeRecentlyRemovedBrokers) {
    this.allowCapacityEstimation = allowCapacityEstimation;
    this.excludeRecentlyDemotedBrokers = excludeRecentlyDemotedBrokers;
    this.excludeRecentlyRemovedBrokers = excludeRecentlyRemovedBrokers;
  }

  public boolean toExcludeRecentlyDemotedBrokers() {
    return excludeRecentlyDemotedBrokers;
  }

  public boolean toExcludeRecentlyRemovedBrokers() {
    return excludeRecentlyRemovedBrokers;
  }

  public boolean toAllowCapacityEstimation() {
    return allowCapacityEstimation;
  }
}
