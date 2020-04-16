/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * Indicates that the cluster link for the given link name was not found.
 */
public class ClusterLinkNotFoundException extends ApiException {

    private static final long serialVersionUID = 1L;

    public ClusterLinkNotFoundException(String message) {
        super(message);
    }

    public ClusterLinkNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
