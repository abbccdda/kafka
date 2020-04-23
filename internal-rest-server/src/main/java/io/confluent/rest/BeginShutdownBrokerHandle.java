/*
 Copyright 2020 Confluent Inc.
 */

package io.confluent.rest;

import org.apache.kafka.common.errors.StaleBrokerEpochException;

/**
 * This interface is to be used by the InternalRestServer to allow the operator to communicate with
 * the broker and initiate a controlled shutdown dependent on under replicated partitions and the
 * current controller.
 */
public interface BeginShutdownBrokerHandle {

    /**
     * The brokerId corresponding to this broker.
     */
    long brokerId();


    /**
     * The brokerEpoch corresponding to this broker.
     */
    long brokerEpoch();

    /**
     * The brokerId of the current controller.
     *
     * This can be null if there is no current controller known.
     */
    Integer controllerId();

    /**
     * The number of under replicated partitions on this broker.
     */
    long underReplicatedPartitions();

    /**
     * Start the shutdown process for this broker on the provided broker epoch.
     */
    void beginShutdown(long brokerEpoch) throws StaleBrokerEpochException;
}
