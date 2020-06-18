/*
 * Copyright (C) 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * A general exception for any failure related to a user-initiated Confluent Balancer operation
 */
public class BalancerOperationFailedException extends ApiException {
  public BalancerOperationFailedException(String message) {
    super(message);
  }

  public BalancerOperationFailedException(String message, Throwable cause) {
    super(message, cause);
  }
}
