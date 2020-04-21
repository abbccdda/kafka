/**
 * Copyright 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import kafka.server.KafkaConfig;

/**
 * The NoOpDataBalancer is what's used for handling DataBalanceEngine requests
 * when nodes that aren't eligible to run the DataBalanceEngine (i.e. that
 * aren't the cluster controller).
 */
public class NoOpDataBalanceEngine implements DataBalanceEngine {
    @Override
    public void onActivation(KafkaConfig kafkaConfig) { }

    @Override
    public void onDeactivation() { }

    @Override
    public void shutdown() { }

    @Override
    public void updateThrottle(Long newThrottle) {  }

    @Override
    public boolean isActive() {
        return false;
    }
}
