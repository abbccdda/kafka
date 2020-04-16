/*
 * Copyright 2020 Confluent Inc.
 */
package org.apache.kafka.common.errors;

/**
 * Indicates that the link name for the cluster link creation already exists.
 */
public class ClusterLinkExistsException extends ApiException {

    private static final long serialVersionUID = 1L;

    public ClusterLinkExistsException(String message) {
        super(message);
    }

    public ClusterLinkExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
