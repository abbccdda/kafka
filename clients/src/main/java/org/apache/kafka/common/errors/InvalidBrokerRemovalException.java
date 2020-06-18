/*
 * Copyright (C) 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * An exception thrown when the requested broker removal operation is invalid.
 * Possible reasons are a non-existent broker ID, some partitions (RF=1) becoming unavailable as a result of the removal, etc.
 */
public class InvalidBrokerRemovalException extends ApiException {
  public InvalidBrokerRemovalException(String message) {
    super(message);
  }

  public InvalidBrokerRemovalException(String message, Throwable cause) {
    super(message, cause);
  }
}
