/*
 * Copyright 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import io.confluent.databalancer.operation.BalanceOpExecutionCompletionCallback;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * The NoOpDataBalancer is what's used for handling DataBalanceEngine requests
 * when nodes that aren't eligible to run the DataBalanceEngine (i.e. that
 * aren't the cluster controller).
 */
public class NoOpDataBalanceEngine implements DataBalanceEngine {

    private static final Logger LOG = LoggerFactory.getLogger(NoOpDataBalanceEngine.class);

    @Override
    public DataBalanceEngineContext getDataBalanceEngineContext() {
        return null;
    }

    @Override
    public void onActivation(EngineInitializationContext initializationContext) { }

    @Override
    public void onDeactivation() { }

    @Override
    public void shutdown() { }

    @Override
    public void updateThrottle(Long newThrottle) {  }

    @Override
    public void setAutoHealMode(boolean shouldAutoHeal) { }

    @Override
    public boolean isActive() {
        return false;
    }

    /**
     * Request is invalid if its get handled by SBK while its not controller.
     */
    @Override
    public void removeBroker(int brokerToRemove,
                             Optional<Long> brokerToRemoveEpoch,
                             String uid) {
        String msg = String.format("Received request to remove broker %d (uid %s) while SBK is not started.",
            brokerToRemove, uid);
        LOG.error(msg);
        throw new InvalidRequestException(msg);
    }

    @Override
    public void addBrokers(Set<Integer> brokersToAdd, BalanceOpExecutionCompletionCallback onExecutionCompletion, String uid) {
        String msg = String.format("Ignoring request to add brokers %s while SBK is not started.", brokersToAdd);
        LOG.warn(msg);
    }

    @Override
    public boolean cancelBrokerRemoval(int brokerId) {
        String msg = String.format("Ignoring request to cancel brokers removals for brokers %d while SBK is not started.", brokerId);
        LOG.warn(msg);
        return false;
    }
}
