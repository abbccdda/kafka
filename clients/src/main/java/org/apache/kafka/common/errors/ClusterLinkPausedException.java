/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * Indicates that the cluster link is paused and therefore should not be operated on.
 */
public class ClusterLinkPausedException extends ApiException {

    private static final long serialVersionUID = 1L;

    public ClusterLinkPausedException(String message) {
        super(message);
    }

    public ClusterLinkPausedException(String message, Throwable cause) {
        super(message, cause);
    }
}
