/*
 * Copyright (C) 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;


/**
 * Thrown if a broker removal request for the specific broker was canceled.
 * Right now, this can happen due to the broker being restarted while the removal operation is ongoing.
 * In the future, we may support explicit user cancellations.
 */
public class BrokerRemovalCanceledException extends ApiException {

  public BrokerRemovalCanceledException(String message) {
    super(message);
  }

  public BrokerRemovalCanceledException(String message, Throwable cause) {
    super(message, cause);
  }
}
