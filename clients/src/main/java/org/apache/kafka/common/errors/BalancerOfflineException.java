/*
 * Copyright (C) 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * Thrown when the Confluent Balancer component is not ready to process requests, either because it is not enabled
 * or because it has not managed to start yet.
 */
public class BalancerOfflineException extends ApiException {

  public BalancerOfflineException(String message) {
    super(message);
  }

  public BalancerOfflineException(String message, Throwable cause) {
    super(message, cause);
  }
}
