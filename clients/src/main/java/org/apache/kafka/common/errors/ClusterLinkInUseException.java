/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * Indicates that the cluster link could not be deleted because it is currently
 * in use by one or more services.
 */
public class ClusterLinkInUseException extends ApiException {

    private static final long serialVersionUID = 1L;

    public ClusterLinkInUseException(String message) {
        super(message);
    }

    public ClusterLinkInUseException(String message, Throwable cause) {
        super(message, cause);
    }
}
