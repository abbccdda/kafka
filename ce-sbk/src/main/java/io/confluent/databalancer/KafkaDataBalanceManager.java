/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.yammer.metrics.core.MetricName;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import kafka.controller.DataBalanceManager;
import kafka.metrics.KafkaYammerMetrics;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

public class KafkaDataBalanceManager implements DataBalanceManager {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataBalanceManager.class);
    public static final String ACTIVE_BALANCER_COUNT_METRIC_NAME = "ActiveBalancerCount";

    private KafkaConfig kafkaConfig;
    private DataBalanceEngine balanceEngine;

    private final DataBalanceEngineFactory dbeFactory;
    private final DataBalancerMetricsRegistry dataBalancerMetricsRegistry;

    /**
     * Used to encapsulate which DataBalanceEngine is currently relevant for this broker
     * (State Pattern). Brokers which can be running the DataBalancer have an ActiveDataBalanceEngine,
     * while brokers which are not eligible to be running the DataBalancer have an InactiveDataBalanceEngine.
     * Because the active DataBalanceEngine has actual resources (threads, etc) associated with it, we create
     * one at startup rather than constantly setting-up and tearing-down these objects.
     */
    static class DataBalanceEngineFactory {
        private final DataBalanceEngine activeDataBalanceEngine;
        private final DataBalanceEngine inactiveDataBalanceEngine;

            DataBalanceEngineFactory(DataBalancerMetricsRegistry dataBalancerMetricsRegistry) {
            this(new ConfluentDataBalanceEngine(dataBalancerMetricsRegistry),
                 new NoOpDataBalanceEngine());
        }

        DataBalanceEngineFactory(DataBalanceEngine activeBalanceEngine, DataBalanceEngine inactiveBalanceEngine) {
            activeDataBalanceEngine = Objects.requireNonNull(activeBalanceEngine);
            inactiveDataBalanceEngine = Objects.requireNonNull(inactiveBalanceEngine);
        }

        DataBalanceEngine getActiveDataBalanceEngine() {
            return activeDataBalanceEngine;
        }

        DataBalanceEngine getInactiveDataBalanceEngine() {
            return inactiveDataBalanceEngine;
        }

        void shutdown() {
            activeDataBalanceEngine.shutdown();
            inactiveDataBalanceEngine.shutdown();
        }
    }

    // Visible for testing
    static Set<MetricName> getMetricsWhiteList() {
        DataBalancerMetricsRegistry.MetricsWhitelistBuilder metricsWhitelistBuilder =
                new DataBalancerMetricsRegistry.MetricsWhitelistBuilder();
        metricsWhitelistBuilder.addMetric(KafkaDataBalanceManager.class, ACTIVE_BALANCER_COUNT_METRIC_NAME);
        return metricsWhitelistBuilder.buildWhitelist();
    }

    /**
     * Create a KafkaDataBalanceManager. The DataBalanceManager is expected to be long-lived (broker lifetime).
     * @param kafkaConfig
     */
    public KafkaDataBalanceManager(KafkaConfig kafkaConfig) {
        this(kafkaConfig, new DataBalancerMetricsRegistry(KafkaYammerMetrics.defaultRegistry(), getMetricsWhiteList()));
    }

    private KafkaDataBalanceManager(KafkaConfig kafkaConfig,
                                   DataBalancerMetricsRegistry dbMetricsRegistry) {
        this(kafkaConfig, new DataBalanceEngineFactory(dbMetricsRegistry), dbMetricsRegistry);
    }

    /**
     * Visible for testing. cruiseControl expected to be a mock testing object
     */
    KafkaDataBalanceManager(KafkaConfig kafkaConfig, DataBalanceEngineFactory dbeFactory,
                            DataBalancerMetricsRegistry metricsRegistry) {
        this.kafkaConfig = Objects.requireNonNull(kafkaConfig, "KafkaConfig must be non-null");
        this.dbeFactory = Objects.requireNonNull(dbeFactory, "DataBalanceEngineFactory must be non-null");
        this.dataBalancerMetricsRegistry = Objects.requireNonNull(metricsRegistry, "MetricsRegistry must be non-null");

        this.balanceEngine = dbeFactory.getInactiveDataBalanceEngine();

        this.dataBalancerMetricsRegistry.newGauge(KafkaDataBalanceManager.class, "ActiveBalancerCount",
                () -> balanceEngine.isActive() ? 1 : 0, false);
    }

    /**
     * Start-up the DataBalanceManager. Once this is executed, the broker is eligible to
     * be running the DataBalanceEngine but that's subject to broker configuration.
     */
    @Override
    public synchronized void onElection() {
        // This node is now eligible to execute
        balanceEngine = dbeFactory.getActiveDataBalanceEngine();
        if (!kafkaConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG)) {
            LOG.info("DataBalancer: Skipping DataBalancer Startup as its not enabled.");
            return;
        }

        balanceEngine.onActivation(kafkaConfig);
    }

    /**
     * When the broker ceases to be the primary DataBalancer in the cluster. This renders the broker ineligible for
     * executing any DataBalanceEngine operations. Shut down all running services.
     */
    @Override
    public synchronized void onResignation() {
        balanceEngine.onDeactivation();
        balanceEngine = dbeFactory.getInactiveDataBalanceEngine();
    }

    /**
     * To be called when the KafkaDataBalanceManager is being fully shut down, rather
     * than temporarily disabled for later startup. Expected to be called on broker shutdown only.
     */
    @Override
    public synchronized void shutdown() {
        dbeFactory.shutdown();
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
                balanceEngine.onActivation(kafkaConfig);
            } else {
                balanceEngine.onDeactivation();
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
