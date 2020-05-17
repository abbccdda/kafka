/**
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

/**
 * Exception thrown when Cruise Control startup is aborted midway by shutdown, caused
 * either by controller resignation or broker process shutdown.
 */
public class StartupCheckInterruptedException extends RuntimeException {

    public StartupCheckInterruptedException() {}

    public StartupCheckInterruptedException(InterruptedException ie) {
        super(ie);
    }
}
