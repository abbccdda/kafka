/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.operation;

/**
 * An enumeration of the possible modes of cancelling a broker removal operation.
 * There are currently two modes:
 *
 * 1. #{@link BrokerRemovalCancellationMode#PERSISTENT_CANCELLATION} - the cancellation is long-lived and persisted.
 * This is typically done when a broker comes back up while being removed, therefore invalidating the removal operation
 *
 * 2. #{@link BrokerRemovalCancellationMode#TRANSIENT_CANCELLATION} - the cancellation is short-lived and not persisted.
 * This is typically done when Confluent Balancer fails over to another broker. Any removal operations are therefore
 * temporarily cancelled and restarted on the new Balancer.
 */
public enum BrokerRemovalCancellationMode {
  PERSISTENT_CANCELLATION,
  TRANSIENT_CANCELLATION
}
