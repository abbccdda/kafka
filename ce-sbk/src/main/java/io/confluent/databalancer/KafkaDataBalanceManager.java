/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import kafka.controller.DataBalanceManager;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class KafkaDataBalanceManager implements DataBalanceManager {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataBalanceManager.class);

    private KafkaConfig kafkaConfig;
    private DataBalanceEngineFactory dbeFactory;
    private DataBalanceEngine balanceEngine;

    static class DataBalanceEngineFactory {
        DataBalanceEngine makeActiveDataBalanceEngine() {
            return new ConfluentDataBalanceEngine();
        }

        DataBalanceEngine makeInactiveDataBalanceEngine() {
            return new NoOpDataBalanceEngine();
        }
    }

    /**
     * Create a KafkaDataBalanceManager. The DataBalanceManager is expected to be long-lived (broker lifetime).
     * @param kafkaConfig
     */
    public KafkaDataBalanceManager(KafkaConfig kafkaConfig) {
        this(kafkaConfig, new DataBalanceEngineFactory());
    }

    /**
     * Visible for testing. cruiseControl expected to be a mock testing object
     */
    KafkaDataBalanceManager(KafkaConfig kafkaConfig, DataBalanceEngineFactory dbeFactory) {
        Objects.requireNonNull(kafkaConfig, "KafkaConfig must not be null");
        this.kafkaConfig = kafkaConfig;
        this.dbeFactory = dbeFactory;
        this.balanceEngine = dbeFactory.makeInactiveDataBalanceEngine();
    }

    /**
     * Start-up the DataBalanceManager. Once this is executed, the broker is eligible to
     * be running the DataBalanceEngine but that's subject to broker configuration.
     */
    @Override
    public synchronized void startUp() {
        // This node is now eligible to execute
        balanceEngine = dbeFactory.makeActiveDataBalanceEngine();
        if (!kafkaConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG)) {
            LOG.info("DataBalancer: Skipping DataBalancer Startup as its not enabled.");
            return;
        }

        balanceEngine.startUp(kafkaConfig);
    }

    /**
     * Shut down any services. This renders the broker ineligible for executing any DataBalanceEngine operations.
     */
    @Override
    public synchronized void shutdown() {
        balanceEngine.shutdown();
        balanceEngine = dbeFactory.makeInactiveDataBalanceEngine();
    }

    /**
     * Updates the internal cruiseControl configuration based on dynamic property updates in the broker's KafkaConfig
     */
    @Override
    public synchronized void updateConfig(KafkaConfig newConfig) {
        // Commit all changes first, but keep the original for deciding what to do
        KafkaConfig oldConfig = kafkaConfig;
        kafkaConfig = newConfig;

        // The most important change is enable<->disable
        if (!kafkaConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG).equals(oldConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG))) {
            // This node is eligible to be running the data balancer AND the enabled config changed.
            if (kafkaConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG)) {
                balanceEngine.startUp(kafkaConfig);
            } else {
                balanceEngine.shutdown();
            }
            // All other changes are effectively applied by the startup/shutdown (CC has been started with the new config, or it's been shut down),
            // so config updates are done now. Finish.
            return;
        }

        if (!kafkaConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG).equals(oldConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG))) {
            balanceEngine.updateThrottle(kafkaConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG));
        }
    }
}
