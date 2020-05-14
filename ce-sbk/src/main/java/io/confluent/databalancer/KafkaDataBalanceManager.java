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
import scala.Option;

import java.util.Objects;
import java.util.Optional;
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

        /**
         * Instantiate the DataBalanceEngine via the normal path.
         * This creates instances of all DataBalanceEngine objects so that they can be reused as
         * needed throughout the Factory's lifetime.
         */
        DataBalanceEngineFactory(DataBalancerMetricsRegistry dataBalancerMetricsRegistry) {
            this(new ConfluentDataBalanceEngine(dataBalancerMetricsRegistry),
                 new NoOpDataBalanceEngine());
        }

        // Visible for testing
        DataBalanceEngineFactory(DataBalanceEngine activeBalanceEngine, DataBalanceEngine inactiveBalanceEngine) {
            activeDataBalanceEngine = Objects.requireNonNull(activeBalanceEngine);
            inactiveDataBalanceEngine = Objects.requireNonNull(inactiveBalanceEngine);
        }

        /**
         * Get the instance of the ActiveDataBalanceEngine.
         */
        DataBalanceEngine getActiveDataBalanceEngine() {
            return activeDataBalanceEngine;
        }

        /**
         * Get the instance of the inactive DataBalanceEngine.
         */
        DataBalanceEngine getInactiveDataBalanceEngine() {
            return inactiveDataBalanceEngine;
        }

        /**
         * Shutdown the Factory.
         * This is expected to only be called when the Factory needs to go away (i.e. on broker shutdown).
         */
        void shutdown() throws InterruptedException {
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
     * IT IS EXPECTED THAT onResignation IS CALLED BEFORE THIS. (KafkaController::shutdown() does exactly that.)
     */
    @Override
    public synchronized void shutdown() {
        try {
            // Shutdown all engines
            dbeFactory.shutdown();
        } catch (InterruptedException e) {
            // Interruption during shutdown is not that big a deal. Warn but continue on.
            LOG.warn("DataBalanceManager interrupted during shutdown.");
        }
    }

    /**
     * Updates the internal cruiseControl configuration based on dynamic property updates in the broker's KafkaConfig
     */
    @Override
    public synchronized void updateConfig(KafkaConfig oldConfig, KafkaConfig newConfig) {
        // Commit all changes first, but keep the original for deciding what to do
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

    @Override
    public synchronized void removeBroker(int brokerToRemove, Option<Long> brokerToRemoveEpoch) {
        if (!balanceEngine.isActive()) {
            String msg = String.format("Received request to remove broker %d while SBK is not started.", brokerToRemove);
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        Optional<Long> brokerEpoch = brokerToRemoveEpoch.isEmpty() ? Optional.empty()
                : Optional.of(brokerToRemoveEpoch.get());
        balanceEngine.removeBroker(brokerToRemove, brokerEpoch);
    }
}
