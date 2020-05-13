/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * An error occurred while computing the reassignment plan for removing a broker.
 */
public class PlanComputationException extends ApiException {

  public PlanComputationException(String message, Throwable cause) {
    super(message, cause);
  }

  public PlanComputationException(String message) {
    super(message);
  }
}
