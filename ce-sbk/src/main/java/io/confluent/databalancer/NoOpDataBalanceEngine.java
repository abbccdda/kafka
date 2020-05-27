/**
 * Copyright 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import kafka.server.KafkaConfig;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * The NoOpDataBalancer is what's used for handling DataBalanceEngine requests
 * when nodes that aren't eligible to run the DataBalanceEngine (i.e. that
 * aren't the cluster controller).
 */
public class NoOpDataBalanceEngine implements DataBalanceEngine {

    private static final Logger LOG = LoggerFactory.getLogger(NoOpDataBalanceEngine.class);

    @Override
    public void onActivation(KafkaConfig kafkaConfig) { }

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
    public void removeBroker(int brokerToRemove, Optional<Long> brokerToRemoveEpoch) {
        String msg = String.format("Received request to remove broker %d while SBK is not started.", brokerToRemove);
        LOG.error(msg);
        throw new InvalidRequestException(msg);
    }
}
