/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import kafka.controller.DataBalancer;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;

public class KafkaDataBalancer implements DataBalancer {
    private KafkaConfig kafkaConfig;
    private KafkaCruiseControl cruiseControl = null;

    public KafkaDataBalancer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    /**
     * Visible for testing. cruiseControl expected to be a mock testing object
     */
    KafkaDataBalancer(KafkaConfig kafkaConfig, KafkaCruiseControl cruiseControl) {
        this.kafkaConfig = kafkaConfig;
        this.cruiseControl = cruiseControl;
    }

    @Override
    public void startUp() {

    }

    @Override
    public void shutdown() {

    }

    /**
     * Updates the internal cruiseControl configuration based on dynamic property updates in the broker's KafkaConfig
     */
    @Override
    public synchronized void updateConfig(KafkaConfig newConfig) {
        if (cruiseControl == null) {
            // cruise control isn't currently running, updated config values will be loaded in once cruise control starts.
            // at this point the singleton kafkaConfig object has already been updated, if cruise control starts at any point
            // after updateConfig has been called, it will read from the updated kafkaConfig
            kafkaConfig = newConfig;
            return;
        }
        if (kafkaConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG) != newConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG)) {
            cruiseControl.setSelfHealingFor(AnomalyType.GOAL_VIOLATION, newConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG));
        }
        if (kafkaConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG) != newConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG)) {
            cruiseControl.updateThrottle(newConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG));
        }
        kafkaConfig = newConfig;
    }
}

