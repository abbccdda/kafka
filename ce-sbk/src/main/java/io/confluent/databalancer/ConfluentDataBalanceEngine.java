/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalFuture;
import com.linkedin.kafka.cruisecontrol.client.BlockingSendClient;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsSamplerBase;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import io.confluent.databalancer.operation.BalanceOpExecutionCompletionCallback;
import io.confluent.databalancer.operation.BrokerRemovalProgressListener;
import io.confluent.databalancer.operation.BrokerRemovalStateMachine;
import io.confluent.databalancer.operation.BrokerRemovalStateTracker;
import io.confluent.databalancer.operation.BrokerRemovalTerminationListener;
import io.confluent.databalancer.operation.PersistRemoveApiStateListener;
import io.confluent.databalancer.persistence.ApiStatePersistenceStore;
import io.confluent.databalancer.persistence.BrokerRemovalStateRecord;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.BalancerOfflineException;
import org.apache.kafka.common.errors.BrokerRemovalInProgressException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A simple class for:
 * - providing an API mapping from DataBalanceManager<->CruiseControl
 * - managing synchronization and computation resources for CruiseControl and the methods exposed
 */
public class ConfluentDataBalanceEngine implements DataBalanceEngine {
    private static final Logger LOG = LoggerFactory.getLogger(ConfluentDataBalanceEngine.class);

    // A list of classes that need to be checked to make sure that conditions are met for
    // successful startup.
    // Visible for testing
    static final List<StartupComponent> STARTUP_COMPONENTS = new LinkedList<>();

    static class StartupComponent {
        private final BiConsumer<KafkaCruiseControlConfig, Semaphore> startUpLambda;
        private final String componentName;

        public StartupComponent(String componentName, BiConsumer<KafkaCruiseControlConfig, Semaphore> startUpLambda) {
            this.componentName = componentName;
            this.startUpLambda = startUpLambda;
        }

        public void start(KafkaCruiseControlConfig config, Semaphore semaphore) {
            startUpLambda.accept(config, semaphore);
        }

        @Override
        public String toString() {
            return String.format("StartupComponent %s", this.componentName);
        }
    }

    static {
        STARTUP_COMPONENTS.add(new StartupComponent(KafkaSampleStore.class.getSimpleName(), KafkaSampleStore::checkStartupCondition));
        STARTUP_COMPONENTS.add(new StartupComponent(ApiStatePersistenceStore.class.getSimpleName(), ApiStatePersistenceStore::checkStartupCondition));
    }

    private static final int SHUTDOWN_TIMEOUT_MS = 15000;
    private static final String START_ANCHOR = "^";
    private static final String END_ANCHOR = "$";
    private static final String WILDCARD_SUFFIX = ".*";
    // potentially removable goals
    private static final String NETWORK_IN_CAPACITY_GOAL = NetworkInboundCapacityGoal.class.getName();
    private static final String NETWORK_OUT_CAPACITY_GOAL = NetworkOutboundCapacityGoal.class.getName();
    private static final String SHUTDOWN_MANAGER_CLIENT_ID = "SBK-broker-shutdown-manager";

    // ccRunner is used to control all access to the underlying CruiseControl object;
    // access is serialized through here (in particular for startup/shutdown).
    private final ExecutorService ccRunner;

    // a function that takes in a broker ID as a parameter and registers a metric that tracks the broker removal state
    private Function<Integer, AtomicReference<String>> brokerRemovalStateMetricRegistrationHandler;

    // package-private for testing
    final BrokerRemovalTerminationListener removalTerminationListener;

    // Visible for testing
    final ConfluentDataBalanceEngineContext context;
    private Semaphore abortStartupCheck  = new Semaphore(0);

    /**
     * If we have following timeline:
     *
     * ----+------+-------+------+------
     *     |Sa    |Se     |Ta    |Te
     *
     * where,
     *
     * Sa = start arrives (submitted to executor). Events processed after this are, by the SingleThreadExecutor,
     *      guaranteed to be handled after this event completes.
     * Se = start executes. After this, it's running or failed.
     * Ta = sTop arrives (submitted to executor). Events processed after this are guaranteed to be handled
     *      after this instance shuts down.
     * Te = stop executes.
     *
     * In the absence of errors, (Sa,Ta) is the safe time to submit requests. (Requests will execute in the time (Se, Ta)
     *
     * In the presence of errors, requests submitted between (Sa, Se) will fail as CruiseControl will not be ready
     * to serve request. The error will get reported asynchronously.
     */
    // Visible for testing
    volatile boolean canAcceptRequests = false;

    public ConfluentDataBalanceEngine(DataBalancerMetricsRegistry dataBalancerMetricsRegistry, KafkaConfig config) {
        this(Executors.newSingleThreadExecutor(createThreadFactory(config)),
             createContext(dataBalancerMetricsRegistry));
    }

    private static ConfluentDataBalanceEngineContext createContext(DataBalancerMetricsRegistry dataBalancerMetricsRegistry) {
        return new ConfluentDataBalanceEngineContext(dataBalancerMetricsRegistry, null, new SystemTime());
    }

    private static KafkaCruiseControlThreadFactory createThreadFactory(KafkaConfig config) {
        return new KafkaCruiseControlThreadFactory("DataBalanceEngine",
                true,
                LOG,
                Optional.of(KafkaDataBalanceManager.getBrokerId(config)));
    }

    ConfluentDataBalanceEngine(ExecutorService executor,
                               ConfluentDataBalanceEngineContext context) {
        this.ccRunner = Objects.requireNonNull(executor, "ExecutorService must be non-null");
        this.context = context;
        this.removalTerminationListener = (brokerId1, state, e) -> {
            context.brokerRemovalsStateTrackers.remove(brokerId1);
            LOG.info("Removal for broker {} reached terminal state {}", brokerId1, state);
        };
    }

    @Override
    public DataBalanceEngineContext getDataBalanceEngineContext() {
        return context;
    }

    @Override
    public synchronized void onActivation(EngineInitializationContext initializationContext)  {
        brokerRemovalStateMetricRegistrationHandler = initializationContext.brokerRemovalStateMetricRegistrationHandler;

        LOG.info("DataBalancer: Scheduling DataBalanceEngine Startup");

        abortStartupCheck.drainPermits();
        canAcceptRequests = true;
        ccRunner.submit(() -> startCruiseControl(initializationContext, this::createKafkaCruiseControl));
    }

    @Override
    public synchronized void onDeactivation() {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine Shutdown.");

        // If startup is in progress, abort it
        abortStartupCheck.release();
        canAcceptRequests = false;
        submitToCcRunner(this::stopCruiseControl, "DataBalancer is already stopped.");
    }

    /**
     * Called when the object is going away for good (end of broker lifetime). May stall for a while for cleanup.
     * IT IS EXPECTED THAT onDeactivation WILL HAVE ALREADY BEEN CALLED WHEN THIS IS INVOKED.
     */
    @Override
    public void shutdown() throws InterruptedException {
        ccRunner.shutdown();
        ccRunner.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void updateThrottle(Long newThrottle) {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine throttle update to {}", newThrottle);
        submitToCcRunner(() -> updateThrottleHelper(newThrottle),
                "Cannot update throttle when no DataBalancer is active.");
    }

    @Override
    public void setAutoHealMode(boolean shouldAutoHeal) {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine auto-heal update (setting to {})", shouldAutoHeal);
        submitToCcRunner(() -> updateAutoHealHelper(shouldAutoHeal), "Attempt to update auto-heal mode (" +
                shouldAutoHeal + ") when no DataBalancer is active.");
    }

    @Override
    public void removeBroker(int brokerToRemove,
                             Optional<Long> brokerToRemoveEpoch,
                             String uid) {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine broker removal: {} (uid: {})", brokerToRemove, uid);
        if (!canAcceptRequests) {
            String msg = String.format("Received request to remove broker %d (uid %s) while SBK is not started.",
                    brokerToRemove, uid);
            LOG.error(msg);
            throw new BalancerOfflineException(msg);
        }

        // Validate that the broker is not already getting removed
        ApiStatePersistenceStore persistenceStore = context.getPersistenceStore();
        BrokerRemovalStateRecord existingBrokerRemovalStateRecord = persistenceStore.getBrokerRemovalStateRecord(brokerToRemove);
        if (existingBrokerRemovalStateRecord != null && !existingBrokerRemovalStateRecord.state().isTerminal()) {
            String msg = String.format("Received request to remove broker %d (uid %s) while another request " +
                    "to remove the broker is already in progress", brokerToRemove, uid);
            LOG.error(msg);
            throw new BrokerRemovalInProgressException(msg);
        }

        // Validate that no other broker is already getting removed
        Set<BrokerRemovalStateRecord> ongoingRemovals = persistenceStore.getAllBrokerRemovalStateRecords().values().stream()
                .filter(status -> !status.state().isTerminal())
                .collect(Collectors.toSet());
        if (!ongoingRemovals.isEmpty()) {
            String brokerIds = ongoingRemovals.stream()
                    .map(status -> String.valueOf(status.brokerId()))
                    .collect(Collectors.joining(","));
            String msg = "Cannot remove broker " + brokerToRemove +
                    " as broker removals already in progress: " + brokerIds;
            LOG.error(msg);
            throw new BrokerRemovalInProgressException(msg);
        }

        BrokerRemovalProgressListener listener = new PersistRemoveApiStateListener(
            context.getPersistenceStore());
        BrokerRemovalStateTracker stateTracker = new BrokerRemovalStateTracker(brokerToRemove,
            listener,
            removalTerminationListener,
            brokerRemovalStateMetricRegistrationHandler.apply(brokerToRemove));

        submitRemoveBroker(brokerToRemove, brokerToRemoveEpoch, stateTracker, uid);
    }

    private void submitRemoveBroker(int brokerToRemove,
                                    Optional<Long> brokerToRemoveEpoch,
                                    BrokerRemovalStateTracker stateTracker,
                                    String uid) {
        context.brokerRemovalsStateTrackers.put(brokerToRemove, stateTracker);
        // This call will create initial remove broker api state. We should not fail synchronously after this line.
        stateTracker.initialize();

        submitToCcRunner(() -> doRemoveBroker(brokerToRemove, brokerToRemoveEpoch, stateTracker, uid),
                "Broker removal operation with UID " + uid + " was not initiated" +
                        " due to the data balance engine not being initialized");
    }

    /**
     * Submit a task to {@link #ccRunner} to run.
     */
    private void submitToCcRunner(Runnable task, String notActiveErrorMessage) {
        ccRunner.submit(() -> {
            if (context.isCruiseControlInitialized()) {
                task.run();
            } else {
                LOG.info(notActiveErrorMessage);
            }
        });
    }

    @Override
    public void addBrokers(Set<Integer> brokersToAdd, @Nonnull BalanceOpExecutionCompletionCallback onExecutionCompletion, String uid) {
        if (!canAcceptRequests) {
            String msg = String.format("Received request to add brokers %s while SBK is not started.", brokersToAdd);
            LOG.error(msg);
            throw new BalancerOfflineException(msg);
        }

        // Check to make sure this can proceed (no ongoing removals)
        ApiStatePersistenceStore persistenceStore = context.getPersistenceStore();
        Map<Integer, BrokerRemovalStateRecord> existingBrokerRemovalStatus = persistenceStore.getAllBrokerRemovalStateRecords();
        if (existingBrokerRemovalStatus.values().stream().anyMatch(s -> !s.state().isTerminal())) {
            LOG.warn("Broker removals ongoing, will not add new brokers {}", brokersToAdd);
            return;
        }

        LOG.info("DataBalancer: Scheduling DataBalanceEngine broker addition: {}", brokersToAdd);
        submitToCcRunner(() -> doAddBrokers(brokersToAdd, onExecutionCompletion, uid),
                "Broker addition operation with UID " + uid + " was not initiated" +
                " due to the data balance engine not being initialized");
    }

    /**
     * Cancels the on-going broker removal operations for the given #{@code brokerIds}
     * @return - the successfully canceled broker removal operations and their associated exceptions
     */
    @Override
    public boolean cancelBrokerRemoval(int brokerId) {
        BrokerRemovalFuture removalFuture = context.brokerRemovalFuture(brokerId);
        if (removalFuture == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Will not cancel broker {} removal as it is not being removed.", brokerId);
            }
            return false;
        }

        LOG.info("Canceling broker removal task for broker {}", brokerId);
        boolean wasCanceled = removalFuture.cancel();
        LOG.info("Canceled broker removal task for broker {} (future canceled {})", brokerId, wasCanceled);
        return wasCanceled;
    }

    /**
     * Returns if CruiseControl is active and can work on balancing cluster. Its different to
     * {@code #canAcceptRequests} in the sense we can accept requests even though CruiseControl hasn't
     * been started (say start event arrived but hasn't been processed yet).
     */
    @Override
    public boolean isActive() {
        return context.isCruiseControlInitialized();
    }

    // Package-private for testing
    void updateThrottleHelper(Long newThrottle) {
        LOG.info("Updating balancer throttle to {}", newThrottle);
        context.getCruiseControl().updateThrottle(newThrottle);
    }

    /*
     * Helper to actually set the auto-heal mode.
     * Package-private for testing
     */
     void updateAutoHealHelper(boolean shouldAutoHeal) {
        LOG.info("Changing GOAL_VIOLATION anomaly self-healing actions to {}", shouldAutoHeal);
        context.getCruiseControl().setGoalViolationSelfHealing(shouldAutoHeal);
    }

    private void doRemoveBroker(int brokerToRemove, Optional<Long> brokerToRemoveEpoch,
                                BrokerRemovalStateTracker stateTracker, String uid) {
        LOG.info("Initiating broker removal operation with UID {} for broker {} (epoch {})",
                uid, brokerToRemove, brokerToRemoveEpoch);
        try {
            BrokerRemovalFuture brokerRemovalFuture = context.getCruiseControl().removeBroker(
                    brokerToRemove, brokerToRemoveEpoch,
                    (success, ex) -> {
                        context.removeBrokerRemovalFuture(brokerToRemove);
                    },
                    stateTracker, uid);
            context.putBrokerRemovalFuture(brokerToRemove, brokerRemovalFuture);
            brokerRemovalFuture.execute(Duration.ofMinutes(60));
        } catch (InterruptedException ex) {
            LOG.error("Interrupted when removing broker: {}", brokerToRemove, ex);
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        } catch (Throwable e) {
            // BrokerRemovalStateTracker already handles all business logic related errors and
            // persists them in Api State store topic. If we reach here, its some generic error not
            // related to remove broker. So we should log and add catch for other known errors
            // (like InterruptedException)
            LOG.error("Broker removal operation with UID {} failed due to ", uid, e);
        }
    }

    /**
     * Launch CruiseControl. Expected to run in a thread-safe context such as a SingleThreadExecutor.
     * This should be appropriately serialized with shutdown requests.
     */
    // Visible for testing
    void startCruiseControl(EngineInitializationContext initializationContext,
                            Function<KafkaConfig, KafkaCruiseControl> kafkaCruiseControlCreator) {
        if (context.isCruiseControlInitialized()) {
            LOG.warn("DataBalanceEngine already running when startUp requested.");
            return;
        }

        LOG.info("DataBalancer: Instantiating DataBalanceEngine");
        try {
            KafkaCruiseControl newCruiseControl = kafkaCruiseControlCreator.apply(initializationContext.kafkaConfig);
            newCruiseControl.startUp();
            context.setPersistenceStore(new ApiStatePersistenceStore(initializationContext.kafkaConfig, context.getTime(), generateClientConfigs(initializationContext.kafkaConfig)));

            // Now we have cruise control started, resubmit pending operations
            resubmitPendingOperations(initializationContext);

            // This should be last line in this method. Setting cruise control object makes
            // `isActive`/`context.isCruiseControlInitialized()` methods to return true, which allows
            // databalance engine to accept new requests.
            context.setCruiseControl(newCruiseControl);
            LOG.info("DataBalancer: DataBalanceEngine started");
        } catch (StartupCheckInterruptedException e) {
            LOG.warn("DataBalanceEngine startup aborted by shutdown.", e);
            context.closeAndClearState();
        } catch (Exception e) {
            LOG.warn("Unable to start up DataBalanceEngine", e);
            context.closeAndClearState();
        }
    }

    /**
     * Resubmit any ongoing operations that couldn't complete as controller went down. SBK gets restarted
     * along with that.
     *
     * Currently this only restarts remove operations, add operations need to be tackled too.
     */
    private void resubmitPendingOperations(EngineInitializationContext initializationContext) {
        ApiStatePersistenceStore persistenceStore = context.getPersistenceStore();
        Set<BrokerRemovalStateRecord> pendingRemovals = persistenceStore.getAllBrokerRemovalStateRecords().values().stream()
                .filter(status -> !status.state().isTerminal())
                .collect(Collectors.toSet());
        if (pendingRemovals.isEmpty()) {
            LOG.info("No pending SBK operations found at startup.");
        }

        BrokerRemovalProgressListener listener = new PersistRemoveApiStateListener(context.getPersistenceStore());

        for (BrokerRemovalStateRecord removalStateRecord : pendingRemovals) {
            int brokerId = removalStateRecord.brokerId();
            BrokerRemovalStateMachine.BrokerRemovalState pendingState = removalStateRecord.state();
            BrokerRemovalStateTracker stateTracker = new BrokerRemovalStateTracker(brokerId,
                    pendingState,
                    listener,
                    removalTerminationListener,
                    brokerRemovalStateMetricRegistrationHandler.apply(brokerId));

            String uid = String.format("remove-broker-%d-%d", brokerId, context.getTime().milliseconds());
            submitRemoveBroker(brokerId,
                               Optional.ofNullable(initializationContext.brokerEpochs.get(brokerId)),
                               stateTracker,
                               uid);
            LOG.info("Submitted pending operation to remove broker id {} with state {}.", brokerId, pendingState);
        }
    }

    // This method abstracts "new" call so that unit test can mock this part out
    // and test startCruiseControl method
    private KafkaCruiseControl createKafkaCruiseControl(KafkaConfig kafkaConfig) {
        BlockingSendClient.Builder blockingSendClientBuilder = new BlockingSendClient.Builder(kafkaConfig,
                new Metrics(),
                context.getTime(),
                SHUTDOWN_MANAGER_CLIENT_ID,
                new LogContext());

        KafkaCruiseControlConfig config = generateCruiseControlConfig(kafkaConfig);
        Class<?> sampler = config.getClass(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG);
        // All metric sampler classes derived from the ConfluentMetricsSampler have a startup component
        if (ConfluentMetricsSamplerBase.class.isAssignableFrom(sampler)) {
            STARTUP_COMPONENTS.add(new StartupComponent(sampler.getSimpleName(), ConfluentMetricsSamplerBase::checkStartupCondition));
        }

        checkStartupComponentsReady(config);

        return new KafkaCruiseControl(config, context.getDataBalancerMetricsRegistry(), blockingSendClientBuilder);
    }

    /**
     * Check if all components needed for successful startup are ready.
     */
    // Visible for testing
    void checkStartupComponentsReady(KafkaCruiseControlConfig config) {
        for (StartupComponent startupComponent : STARTUP_COMPONENTS) {
            LOG.info("DataBalancer: Checking {}", startupComponent);
            startupComponent.start(config, abortStartupCheck);
            LOG.info("DataBalancer: {} ready to proceed", startupComponent);
        }
        LOG.info("DataBalancer: Startup checking succeeded, proceeding to full validation.");
    }

    /**
     * Shutdown the running CruiseControl services. Expected to run in a thread-safe context such as a SingleThread
     * Executor. (In particular avoid conflict with startup requests.)
     */
    // Visible for testing
    void stopCruiseControl() {
        LOG.info("DataBalancer: Starting DataBalanceEngine Shutdown");

        try {
            context.getCruiseControl().userTriggeredStopExecution();
            context.closeAndClearState();
        } catch (Exception ex) {
            LOG.warn("Unable to stop DataBalanceEngine", ex);
        }
        LOG.info("DataBalancer: DataBalanceEngine shutdown completed.");
    }

    void doAddBrokers(Set<Integer> brokersToAdd, BalanceOpExecutionCompletionCallback executionCompletionCallback, String operationUid) {
        if (brokersToAdd.isEmpty()) {
            return;
        }

        LOG.info("DataBalancer: Starting addBrokers call");
        try {
            context.getCruiseControl().addBrokers(brokersToAdd, executionCompletionCallback, operationUid);
        } catch (Exception ex) {
            // Report error up?
            LOG.warn("Broker addition of {} failed", brokersToAdd, ex);
        }
    }

    /**
     * The function forms a regex expression by performing OR operation on the topic names and topic prefixes
     * considering each of these as string literals.
     * For example,
     * confluent.balancer.exclude.topic.names = [topic1, topic2],
     * confluent.balancer.exclude.topic.prefixes = [prefix1, prefix2]
     * The regex computed would be = "^\\Qtopic1\\E$|^\\Qtopic2\\E$|^\\Qprefix1\\E.*|^\\Qprefix2\\E.*"
     * Visible for testing
     */
    static String generateCcTopicExclusionRegex(KafkaConfig config) {
        List<String> topicNames = config.getList(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_NAMES_CONFIG);
        List<String> topicPrefixes = config.getList(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_PREFIXES_CONFIG);

        Stream<String> topicNameRegexStream = topicNames.stream().map(topic -> START_ANCHOR + Pattern.quote(topic) + END_ANCHOR);
        Stream<String> topicPrefixRegexStream = topicPrefixes.stream().map(prefix -> START_ANCHOR + Pattern.quote(prefix) + WILDCARD_SUFFIX);

        return Stream.concat(topicNameRegexStream, topicPrefixRegexStream).collect(Collectors.joining("|"));
    }

    /**
     * Given a KafkaConfig, generate an appropriate KafkaCruiseControlConfig to bring up CruiseControl internally.
     * Visible for testing
     */
    static KafkaCruiseControlConfig generateCruiseControlConfig(KafkaConfig config) {
        // Extract all confluent.databalancer.X properties from the KafkaConfig, so we
        // can create a CruiseControlConfig from it.
        Map<String, Object> ccConfigProps = new HashMap<>(config.originalsWithPrefix(ConfluentConfigs.CONFLUENT_BALANCER_PREFIX));
        // Special overrides: zookeeper.connect, etc.
        ccConfigProps.putIfAbsent(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, config.get(KafkaConfig.ZkConnectProp()));
        ccConfigProps.put(BrokerCapacityResolver.LOG_DIRS_CONFIG, getConfiguredLogDirs(config));

        // Adjust our goals list as needed -- if network capacities are not provided, remove them from the list
        List<String> goals = new LinkedList<>(KafkaCruiseControlConfig.DEFAULT_GOALS_LIST);
        List<String> hardGoals = new LinkedList<>(KafkaCruiseControlConfig.DEFAULT_HARD_GOALS_LIST);
        List<String> anomalyDetectionGoals = new LinkedList<>(KafkaCruiseControlConfig.DEFAULT_ANOMALY_DETECTION_GOALS_LIST);
        // if network in/out are zero, we don't want to enforce network capacity goals
        long networkInCapacity = config.getLong(ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_CONFIG);
        if (networkInCapacity <= 0) {
            removeGoalFromLists(NETWORK_IN_CAPACITY_GOAL, goals, hardGoals, anomalyDetectionGoals);
        }
        long networkOutCapacity = config.getLong(ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_CONFIG);
        if (networkOutCapacity <= 0) {
            removeGoalFromLists(NETWORK_OUT_CAPACITY_GOAL, goals, hardGoals, anomalyDetectionGoals);
        }

        ccConfigProps.putIfAbsent(KafkaCruiseControlConfig.GOALS_CONFIG, String.join(",", goals));
        ccConfigProps.putIfAbsent(KafkaCruiseControlConfig.HARD_GOALS_CONFIG, String.join(",", hardGoals));
        ccConfigProps.putIfAbsent(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG, String.join(",", anomalyDetectionGoals));
        configureCruiseControlSelfHealing(config, ccConfigProps);
        // Derive bootstrap.servers from the provided KafkaConfig, instead of requiring
        // users to specify it.
        if (ccConfigProps.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            populateClientConfigs(config, ccConfigProps);
        }
        LOG.info("DataBalancer: BOOTSTRAP_SERVERS determined to be {}", ccConfigProps.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG));

        // Some CruiseControl properties can be interpreted from existing properties,
        // but those properties aren't defined in KafkaConfig because they're in external modules,
        // e.g. the MetricsReporter (needed by SBK for getting data about the cluster). These may
        // be available in the "originals."
        // Specifically:
        // Metrics Reporter topic -- needed to read metrics
        // Metrics Reporter replication factor -- use this for the SampleStore
        Map<String, Object> kccProps = config.originals();

        // Our metrics reporter sampler pulls from the Metrics Reporter Sampler. Copy that over if needed.
        String metricsReporterTopic = (String) kccProps.get(ConfluentMetricsReporterConfig.TOPIC_CONFIG);
        if (metricsReporterTopic != null && metricsReporterTopic.length() > 0) {
            ccConfigProps.putIfAbsent(ConfluentMetricsSamplerBase.METRIC_REPORTER_TOPIC_PATTERN, metricsReporterTopic);
        }

        String metricsReporterReplFactor = (String) kccProps.get(ConfluentMetricsReporterConfig.TOPIC_REPLICAS_CONFIG);
        // The metrics reporter replication factor is the same RF we should use for all databalancer topics like:
        // 1. sample store topic.
        // 2. api state persistence topic
        if (metricsReporterReplFactor != null && metricsReporterReplFactor.length() > 0) {
            ccConfigProps.putIfAbsent(ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_CONFIG,
                    metricsReporterReplFactor);
        }

        ccConfigProps.put(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG,
                generateCcTopicExclusionRegex(config));

        return new KafkaCruiseControlConfig(ccConfigProps);
    }

    /**
     * Derives the bootstrap.servers from the provided #{@link KafkaConfig}
     * and populates the passed in #{@code props} with it, along with more default client properties
     */
    static void populateClientConfigs(KafkaConfig config, Map<String, Object> props) {
        Map<String, Object> clientConfigs = generateClientConfigs(config);
        LOG.info("Adding configs {} to config", clientConfigs);

        props.putAll(clientConfigs);
    }

    private static Map<String, Object> generateClientConfigs(KafkaConfig config) {
        Endpoint interBrokerEndpoint = config.listeners()
            .find(ep -> ep.listenerName().equals(config.interBrokerListenerName()))
            .get().toJava();

        if (interBrokerEndpoint.host() == null || interBrokerEndpoint.host().isEmpty()) {
            // If listeners list doesn't have host port for bootstrap server, then check advertised listeners
            // As advertised listeners are used by clients to connect to kafka brokers, they should contain
            // hostname. Also Inter broker communication endpoint is guaranteed to be part of advertised listeners.
            // NOTE: `listeners` list is used by kafka to bind to host/port, where as `advertised.listeners` is
            // used by clients to connect to kafka cluster
            Endpoint advertisedInterBrokerEndpoint = config.advertisedListeners()
                    .find(ep -> ep.listenerName().equals(config.interBrokerListenerName()))
                    .get().toJava();

            if (advertisedInterBrokerEndpoint.host() == null || advertisedInterBrokerEndpoint.host().isEmpty()) {
                LOG.warn(String.format("Could not find a host in both the normal (%s) and advertised (%s) internal broker listener. This will default to localhost",
                    interBrokerEndpoint, advertisedInterBrokerEndpoint));
            }

            interBrokerEndpoint = advertisedInterBrokerEndpoint;
        }

        LOG.info("DataBalancer: Listener endpoint is {}", interBrokerEndpoint);
        return ConfluentConfigs.interBrokerClientConfigs(config, interBrokerEndpoint);
    }

    private static void configureCruiseControlSelfHealing(KafkaConfig config, Map<String, Object> cruiseControlProps) {
        Long brokerFailureSelfHealingThreshold = config.getLong(ConfluentConfigs.BALANCER_BROKER_FAILURE_THRESHOLD_CONFIG);
        boolean brokerFailureSelfHealingEnabled = !brokerFailureSelfHealingThreshold.equals(ConfluentConfigs.BALANCER_BROKER_FAILURE_THRESHOLD_DISABLED);
        cruiseControlProps.putIfAbsent(KafkaCruiseControlConfig.SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG, brokerFailureSelfHealingEnabled);
        if (brokerFailureSelfHealingEnabled) {
            cruiseControlProps.putIfAbsent(KafkaCruiseControlConfig.BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG,
                    brokerFailureSelfHealingThreshold);
        }

        if (config.getString(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG).equals(ConfluentConfigs.BalancerSelfHealMode.ANY_UNEVEN_LOAD.toString())) {
            cruiseControlProps.putIfAbsent(KafkaCruiseControlConfig.SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG, true);
        } else {
            // Explicitly set this to false if not in ANY_UNEVEN_LOAD case.
            cruiseControlProps.putIfAbsent(KafkaCruiseControlConfig.SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG, false);
        }

    }

    /**
     * Get the log directories from the Kafka Config. For SBK, there can be only one.
     */
    @SuppressWarnings("deprecation")
    private static String getConfiguredLogDirs(KafkaConfig config) {
        List<String> logDirs = JavaConverters.seqAsJavaList(config.logDirs());
        if (logDirs == null || logDirs.size() == 0) {
            throw new ConfigException("Broker configured with null or empty log directory");
        }
        if (logDirs.size() > 1) {
            throw new ConfigException("SBK configured with multiple log directories");
        }
        return logDirs.get(0);
    }

    @SafeVarargs
    private static void removeGoalFromLists(String goal, List<String>... lists) {
        for (List<String> goalList : lists) {
            goalList.remove(goal);
        }
    }
}
