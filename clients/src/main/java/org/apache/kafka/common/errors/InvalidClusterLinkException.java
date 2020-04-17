/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * Indicates that an invalid cluster link name was provided.
 */
public class InvalidClusterLinkException extends ApiException {

  private static final long serialVersionUID = 1L;

    public InvalidClusterLinkException(String message) {
        super(message);
    }

    public InvalidClusterLinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
