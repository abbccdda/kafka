/**
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.client;

import java.io.IOException;

/**
 * Thrown when a connection could not be established
 */
public class ConnectionException extends IOException {
  public ConnectionException(String message, Throwable cause) {
    super(message, cause);
  }
}
