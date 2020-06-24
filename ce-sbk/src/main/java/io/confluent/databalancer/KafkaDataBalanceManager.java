/*
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.yammer.metrics.core.MetricName;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import io.confluent.databalancer.operation.BalanceOpExecutionCompletionCallback;
import io.confluent.databalancer.operation.BrokerRemovalCancellationMode;
import io.confluent.databalancer.operation.BrokerRemovalStateTracker;
import io.confluent.databalancer.persistence.ApiStatePersistenceStore;
import io.confluent.databalancer.persistence.BrokerRemovalStateRecord;
import kafka.common.BrokerRemovalDescriptionInternal;
import kafka.controller.DataBalanceManager;
import kafka.metrics.KafkaYammerMetrics;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.BalancerOfflineException;
import org.apache.kafka.common.errors.BrokerRemovalCanceledException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import scala.Option;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class KafkaDataBalanceManager implements DataBalanceManager {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataBalanceManager.class);
    public static final String ACTIVE_BALANCER_COUNT_METRIC_NAME = "ActiveBalancerCount";
    public static final String BROKER_REMOVAL_STATE_METRIC_NAME = "BrokerRemovalOperationState";
    public static final String BROKER_ADD_COUNT_METRIC_NAME = "BrokerAddCount";

    // package private for testing
    // the whole set is used for brokerAdd operations so the whole set must be synchronized
    Set<Integer> brokersToAdd;
    private KafkaConfig kafkaConfig;
    // Visible for testing
    DataBalanceEngine balanceEngine;

    private final DataBalanceEngineFactory dbeFactory;
    private final DataBalancerMetricsRegistry dataBalancerMetricsRegistry;
    private Time time;

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
        DataBalanceEngineFactory(DataBalancerMetricsRegistry dataBalancerMetricsRegistry, KafkaConfig config) {
            this(new ConfluentDataBalanceEngine(dataBalancerMetricsRegistry, config),
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
     */
    public KafkaDataBalanceManager(KafkaConfig kafkaConfig) {
        this(kafkaConfig, new DataBalancerMetricsRegistry(KafkaYammerMetrics.defaultRegistry(), getMetricsWhiteList()));
    }

    private KafkaDataBalanceManager(KafkaConfig kafkaConfig,
                                    DataBalancerMetricsRegistry dbMetricsRegistry) {
        this(kafkaConfig,
             new DataBalanceEngineFactory(dbMetricsRegistry, kafkaConfig),
             dbMetricsRegistry,
             new SystemTime());
    }

    /**
     * Visible for testing. cruiseControl expected to be a mock testing object
     */
    KafkaDataBalanceManager(KafkaConfig kafkaConfig, DataBalanceEngineFactory dbeFactory,
                            DataBalancerMetricsRegistry metricsRegistry, Time time) {
        this.kafkaConfig = Objects.requireNonNull(kafkaConfig, "KafkaConfig must be non-null");
        this.dbeFactory = Objects.requireNonNull(dbeFactory, "DataBalanceEngineFactory must be non-null");
        this.dataBalancerMetricsRegistry = Objects.requireNonNull(metricsRegistry, "MetricsRegistry must be non-null");
        this.time = time;
        this.balanceEngine = dbeFactory.getInactiveDataBalanceEngine();
        this.dataBalancerMetricsRegistry.newGauge(KafkaDataBalanceManager.class, "ActiveBalancerCount",
                () -> balanceEngine.isActive() ? 1 : 0, false);
        this.brokersToAdd = ConcurrentHashMap.newKeySet();
        enableBrokerIdLogging(kafkaConfig);
    }

    public static Integer getBrokerId(KafkaConfig config) {
        return config.getInt(KafkaConfig$.MODULE$.BrokerIdProp());
    }

    private static void enableBrokerIdLogging(KafkaConfig kafkaConfig) {
        MDC.put("brokerId", getBrokerId(kafkaConfig).toString());
    }

    /**
     * Start-up the DataBalanceManager. Once this is executed, the broker is eligible to
     * be running the DataBalanceEngine but that's subject to broker configuration.
     */
    @Override
    public synchronized void onElection(Map<Integer, Long> brokerEpochs) {
        enableBrokerIdLogging(kafkaConfig);

        // This node is now eligible to execute
        balanceEngine = dbeFactory.getActiveDataBalanceEngine();
        if (!kafkaConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG)) {
            LOG.info("DataBalancer: Skipping DataBalancer Startup as its not enabled.");
            return;
        }

        activateEngine(brokerEpochs);
    }

    public boolean isActive() {
        return balanceEngine.isActive();
    }

    /**
     * When the broker ceases to be the primary DataBalancer in the cluster. This renders the broker ineligible for
     * executing any DataBalanceEngine operations. Shut down all running services.
     */
    @Override
    public synchronized void onResignation() {
        enableBrokerIdLogging(kafkaConfig);
        cancelAllExistingBrokerRemovals(BrokerRemovalCancellationMode.TRANSIENT_CANCELLATION);
        deactivateEngine();
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
                activateEngine(Collections.emptyMap());
            } else {
                cancelAllExistingBrokerRemovals(BrokerRemovalCancellationMode.PERSISTENT_CANCELLATION);
                deactivateEngine();
            }
            // All other changes are effectively applied by the startup/shutdown (CC has been started with the new config, or it's been shut down),
            // so config updates are done now. Finish.
            return;
        }

        if (!kafkaConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG).equals(oldConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG))) {
            balanceEngine.updateThrottle(kafkaConfig.getLong(ConfluentConfigs.BALANCER_THROTTLE_CONFIG));
        }

        if (!kafkaConfig.getString(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG).equals(oldConfig.getString(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG))) {
            // At least initially, goal-violation auto-healing is enabled with ANY_UNEVEN_LOAD and disabled with EMPTY_BROKERS.
            // KafkaConfig has already ensured that these are the only two values right now.
            boolean shouldEnableImbalanceAutoHeal = kafkaConfig.getString(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG).equals(ConfluentConfigs.BalancerSelfHealMode.ANY_UNEVEN_LOAD.toString());
            balanceEngine.setAutoHealMode(shouldEnableImbalanceAutoHeal);
        }

    }

    @Override
    public void onBrokersStartup(Set<Integer> emptyBrokers, Set<Integer> newBrokers) {
        // No new brokers at all
        if (newBrokers.isEmpty()) {
            return;
        }
        if (!balanceEngine.isActive()) {
            // Return nothing; this is completely ok
            LOG.warn("Notified of broker additions (empty broker ids {}, new brokers {}) but SBK is disabled -- ignoring for now",
                emptyBrokers, newBrokers);
            return;
        }

        // If any removals are in progress for ANY new brokers, those should be canceled.
        cancelExistingBrokerRemovals(newBrokers);

        if (emptyBrokers.isEmpty()) {
            // Nothing more to do!
            return;
        }

        // Only empty brokers get added.
        // TODO: when operation arbitration logic is added in the DataBalanceEngine, that can decide what should be executed next, instead of doing it here. (CNKAF-757)
        Set<Integer> addingBrokers;
        brokersToAdd.addAll(emptyBrokers);
        addingBrokers = new HashSet<>(brokersToAdd);
        String operationUid = String.format("addBroker-%d", time.milliseconds());

        // On completion, clear set of brokers being added
        BalanceOpExecutionCompletionCallback onAddComplete = (opSuccess, ex) -> {
            // A successful completion should clear the added brokers. An exceptional completion should, as well.
            LOG.info("Add Operation completed with success value {}", opSuccess);
            if (opSuccess || ex != null) {
                brokersToAdd.removeAll(addingBrokers);
                LOG.info("Broker Add op (of brokers {}) completion, remaining brokers to add: {}",
                        addingBrokers, brokersToAdd);
            }
        };

        balanceEngine.addBrokers(addingBrokers, onAddComplete, operationUid);
    }

    /**
     * Cancels all the existing broker removals, conditionally persisting the cancellation state
     * depending on the #{@code cancellationMode}.
     */
    private synchronized void cancelAllExistingBrokerRemovals(BrokerRemovalCancellationMode cancellationMode) {
        if (!balanceEngine.isActive()) return;

        Map<Integer, BrokerRemovalStateTracker> stateTrackers = balanceEngine.getDataBalanceEngineContext()
                .getBrokerRemovalsStateTrackers();
        cancelExistingBrokerRemovals(stateTrackers.values(), cancellationMode);
    }

    /**
     * Cancels the existing broker removal operation for #{@code newBrokers} and persists the cancellation state.
     */
    private synchronized void cancelExistingBrokerRemovals(Set<Integer> newBrokers) {
        Map<Integer, BrokerRemovalStateTracker> stateTrackers = balanceEngine.getDataBalanceEngineContext()
                .getBrokerRemovalsStateTrackers();
        Set<BrokerRemovalStateTracker> validStateTrackers = newBrokers
                .stream()
                .map(stateTrackers::get)
                .filter(Objects::nonNull).collect(Collectors.toSet());
        cancelExistingBrokerRemovals(validStateTrackers, BrokerRemovalCancellationMode.PERSISTENT_CANCELLATION);
    }

    private synchronized void cancelExistingBrokerRemovals(Collection<BrokerRemovalStateTracker> stateTrackers,
                                                           BrokerRemovalCancellationMode cancellationMode) {
        Map<Integer, BrokerRemovalStateTracker> allStateTrackers = balanceEngine.getDataBalanceEngineContext()
                .getBrokerRemovalsStateTrackers();
        Set<Integer> allBrokerIds = allStateTrackers.keySet();

        List<Integer> cancelledRemovalOperations = stateTrackers.stream()
            .map(stateTracker -> {
                Integer brokerId = null;
                if (tryCancelBrokerRemoval(stateTracker, cancellationMode)) {
                    brokerId = stateTracker.brokerId();
                }
                allStateTrackers.remove(stateTracker.brokerId());
                return brokerId;
            }).filter(Objects::nonNull).collect(Collectors.toList());

        if (cancelledRemovalOperations.isEmpty()) {
            LOG.debug("No broker removal operations were canceled for {}, either due to none being present or a failure in cancellation", allBrokerIds);
        } else {
            LOG.info("Cancelled the broker removal operations for brokers {}. (new brokers {})", cancelledRemovalOperations, allBrokerIds);
        }
    }

    /**
     * Attempts to cancel the broker removal operation by first registering the cancellation state and then cancelling the future.
     * The ordering is important. For the reasoning,
     * @see <a href="https://confluentinc.atlassian.net/wiki/spaces/~518048762/pages/1325369874/Cancellation+and+Persistence+for+Broker+Removal">this page</a>
     */
    private boolean tryCancelBrokerRemoval(BrokerRemovalStateTracker stateTracker,
                                           BrokerRemovalCancellationMode cancellationMode) {
        int brokerId = stateTracker.brokerId();
        LOG.info("Setting cancelled state on broker removal operation {}", brokerId);
        String errMsg = String.format("The broker removal operation for broker %d was canceled, " +
                "likely due to the broker starting back up while it was being removed or as part of shutdown.", brokerId);
        BrokerRemovalCanceledException cancelException = new BrokerRemovalCanceledException(errMsg);

        boolean isInCanceledState = stateTracker.cancel(cancelException, cancellationMode);

        if (isInCanceledState) {
            LOG.info("Successfully set canceled status on broker removal task for broker {}. Proceeding with cancellation of the operation", brokerId);
            boolean wasCanceled = balanceEngine.cancelBrokerRemoval(brokerId);
            if (wasCanceled) {
                LOG.info("Successfully canceled the broker removal operation for broker {}.", brokerId);
            } else {
                LOG.error("Did not succeed in canceling the broker removal operation for broker {}", brokerId);
            }
            return wasCanceled;
        } else {
            LOG.info("Will not cancel broker removal operation for broker {} because it is in state {}",
                brokerId, stateTracker.currentState());
            return false;
        }
    }

    @Override
    public List<BrokerRemovalDescriptionInternal> brokerRemovals() {
        if (!balanceEngine.isActive()) {
            // Return nothing; this is completely ok
            LOG.warn("SBK is disabled. Returning empty broker removal status.");
            return Collections.emptyList();
        }

        DataBalanceEngineContext dataBalanceEngineContext = balanceEngine.getDataBalanceEngineContext();
        ApiStatePersistenceStore persistenceStore = dataBalanceEngineContext.getPersistenceStore();
        return persistenceStore == null ? Collections.emptyList() :
                persistenceStore.getAllBrokerRemovalStateRecords()
                        .values()
                        .stream()
                        .map(BrokerRemovalStateRecord::toRemovalDescription)
                        .collect(Collectors.toList());
    }

    @Override
    public synchronized void scheduleBrokerRemoval(int brokerToRemove, Option<Long> brokerToRemoveEpoch) {
        if (!balanceEngine.isActive()) {
            String msg = String.format("Received request to remove broker %d while SBK is not started.", brokerToRemove);
            LOG.error(msg);
            throw new BalancerOfflineException(msg);
        }

        Optional<Long> brokerEpochOpt = brokerToRemoveEpoch.isEmpty() ? Optional.empty()
            : Optional.of(brokerToRemoveEpoch.get());
        String uid = String.format("remove-broker-%d-%d", brokerToRemove, time.milliseconds());

        LOG.info("Submitting broker removal operation with UUID {} for broker {} (epoch {})", uid, brokerToRemove, brokerToRemoveEpoch);
        balanceEngine.removeBroker(brokerToRemove, brokerEpochOpt, uid);
    }

    /**
     * Register a Gauge metric to denote the current state of the broker removal operation
     */
    private AtomicReference<String> registerBrokerRemovalMetric(int brokerId) {
        AtomicReference<String> stateReference = new AtomicReference<>("NOT_STARTED");

        dataBalancerMetricsRegistry.newGauge(ConfluentDataBalanceEngine.class,
            BROKER_REMOVAL_STATE_METRIC_NAME,
            stateReference::get, true,
            brokerIdMetricTag(brokerId));

        return stateReference;
    }

    // package-private for testing
    Map<String, String> brokerIdMetricTag(int brokerId) {
        Map<String, String> brokerIdTag = new HashMap<>();
        brokerIdTag.put("broker", String.valueOf(brokerId));
        return brokerIdTag;
    }

    private void activateEngine(Map<Integer, Long> brokerEpochs) {
        balanceEngine.onActivation(
                new EngineInitializationContext(kafkaConfig, brokerEpochs, this::registerBrokerRemovalMetric)
        );
        dataBalancerMetricsRegistry.newGauge(ConfluentDataBalanceEngine.class,
                BROKER_ADD_COUNT_METRIC_NAME,
                brokersToAdd::size,
                true);
    }

    private void deactivateEngine() {
        balanceEngine.onDeactivation();
    }
}
