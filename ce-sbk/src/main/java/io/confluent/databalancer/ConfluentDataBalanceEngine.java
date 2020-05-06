/**
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfluentDataBalanceEngine implements DataBalanceEngine {
    private static final Logger LOG = LoggerFactory.getLogger(ConfluentDataBalanceEngine.class);

    // A list of classes that need to be checked to make sure that conditions are met for
    // successful startup.
    // Visible for testing
    static final List<String> STARTUP_COMPONENTS = new LinkedList<>();
    private static final String CHECK_STARTUP_CONDITION_METHOD_NAME = "checkStartupCondition";

    static {
        STARTUP_COMPONENTS.add("io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler");
        STARTUP_COMPONENTS.add("com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore");
    }

    private static final String START_ANCHOR = "^";
    private static final String END_ANCHOR = "$";
    private static final String WILDCARD_SUFFIX = ".*";
    // potentially removable goals
    private static final String NETWORK_IN_CAPACITY_GOAL = NetworkInboundCapacityGoal.class.getName();
    private static final String NETWORK_OUT_CAPACITY_GOAL = NetworkOutboundCapacityGoal.class.getName();

    // ccRunner is used to control all access to the underlying CruiseControl object;
    // access is serialized through here (in particular for startup/shutdown).
    private final ExecutorService ccRunner;
    private final DataBalancerMetricsRegistry dataBalancerMetricsRegistry;
    private volatile KafkaCruiseControl cruiseControl;
    private Semaphore abortStartupCheck  = new Semaphore(0);

    public ConfluentDataBalanceEngine(DataBalancerMetricsRegistry dataBalancerMetricsRegistry) {
        this(dataBalancerMetricsRegistry,
             null,
             Executors.newSingleThreadExecutor(
                     new KafkaCruiseControlThreadFactory("DataBalanceEngine", true, LOG)));
    }

    // Visible for testing
    ConfluentDataBalanceEngine(DataBalancerMetricsRegistry dataBalancerMetricsRegistry, KafkaCruiseControl cc, ExecutorService executor) {
        this.dataBalancerMetricsRegistry = Objects.requireNonNull(dataBalancerMetricsRegistry, "DataBalancerMetricsRegistry must be non-null");
        ccRunner = Objects.requireNonNull(executor, "ExecutorService must be non-null");
        cruiseControl = cc;
    }

    @Override
    public synchronized void onActivation(KafkaConfig kafkaConfig)  {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine Startup");
        abortStartupCheck.drainPermits();
        ccRunner.submit(() -> startCruiseControl(kafkaConfig));
    }

    @Override
    public synchronized void onDeactivation() {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine Shutdown");

        // If startup is in progress, abort it
        abortStartupCheck.release();

        ccRunner.submit(this::stopCruiseControl, null);
    }

    @Override
    public void shutdown() {
        ccRunner.shutdown();
    }

    @Override
    public void updateThrottle(Long newThrottle) {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine throttle update");
        ccRunner.submit(() -> updateThrottleHelper(newThrottle));
    }

    @Override
    public boolean isActive() {
        return cruiseControl != null;
    }

    private void updateThrottleHelper(Long newThrottle) {
        if (cruiseControl != null) {
            LOG.info("Updating balancer throttle to {}", newThrottle);
            cruiseControl.updateThrottle(newThrottle);
        }
    }

    /**
     * Launch CruiseControl. Expected to run in a thread-safe context such as a SingleThreadExecutor.
     * This should be appropriately serialized with shutdown requests.
     *
     * @param kafkaConfig -- the broker configuration, from which the CruiseControl config will be derived
     */
    private void startCruiseControl(KafkaConfig kafkaConfig) {
        if (cruiseControl != null) {
            LOG.warn("DataBalanceEngine already running when startUp requested.");
            return;
        }

        LOG.info("DataBalancer: Instantiating DataBalanceEngine");
        try {
            KafkaCruiseControlConfig config = generateCruiseControlConfig(kafkaConfig);
            checkStartupComponentsReady(config);
            KafkaCruiseControl newCruiseControl = new KafkaCruiseControl(config, dataBalancerMetricsRegistry);
            newCruiseControl.startUp();
            this.cruiseControl = newCruiseControl;
            LOG.info("DataBalancer: DataBalanceEngine started");
        } catch (StartupCheckInterruptedException e) {
            LOG.warn("DataBalanceEngine startup aborted by shutdown.", e);
            this.cruiseControl = null;
        } catch (Exception e) {
            LOG.warn("Unable to start up DataBalanceEngine", e);
            this.cruiseControl = null;
        }
    }

    /**
     * Check if all components needed for successful startup are ready.
     */
    // Visible for testing
    void checkStartupComponentsReady(KafkaCruiseControlConfig config) throws Exception {
        for (String startupComponent : STARTUP_COMPONENTS) {
            LOG.info("DataBalancer: Checking startup component {}", startupComponent);
            // Get the method object to validate
            Class<?> startupComponentClass = Class.forName(startupComponent);
            LOG.info("DataBalancer: Checking startup readiness for {}", startupComponent);
            Method method = startupComponentClass.getMethod(
                    CHECK_STARTUP_CONDITION_METHOD_NAME, KafkaCruiseControlConfig.class, Semaphore.class);
            method.invoke(null, config, abortStartupCheck);
            LOG.info("DataBalancer: Startup component {} ready to proceed", startupComponent);
        }
        LOG.info("DataBalancer: Startup checking succeeded, proceeding to full validation.");
    }

    /**
     * Shutdown the running CruiseControl services. Expected to run in a thread-safe context such as a SingleThread
     * Executor. (In particular avoid conflict with startup requests.)
     */
    // Visible for testing
    void stopCruiseControl() {
        if (cruiseControl != null) {
            LOG.info("DataBalancer: Starting DataBalanceEngine Shutdown");
            try {
                cruiseControl.userTriggeredStopExecution();
                cruiseControl.shutdown();
            } finally {
                cruiseControl = null;
                dataBalancerMetricsRegistry.clearShortLivedMetrics();
                LOG.info("DataBalancer: DataBalanceEngine shutdown completed.");
            }
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
        List<String> logDirs = JavaConverters.seqAsJavaList(config.logDirs());
        if (logDirs == null || logDirs.size() == 0) {
            throw new ConfigException("Broker configured with null or empty log directory");
        }
        if (logDirs.size() > 1) {
            throw new ConfigException("SBK configured with multiple log directories");
        }
        ccConfigProps.put(BrokerCapacityResolver.LOG_DIRS_CONFIG, logDirs.get(0));

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
        // The defaults for the various self-healing properties are annoyingly difficult to set in the SelfHealingNotifier
        ccConfigProps.putIfAbsent(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, String.valueOf(false));
        ccConfigProps.putIfAbsent(SelfHealingNotifier.SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG, String.valueOf(true));

        // Derive bootstrap.servers from the provided KafkaConfig, instead of requiring
        // users to specify it.
        if (ccConfigProps.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            Endpoint interBrokerEp = config.listeners().toStream()
                .find(ep -> ep.listenerName().equals(config.interBrokerListenerName()))
                .get().toJava();
            LOG.info("DataBalancer: Listener endpoint is {}", interBrokerEp);
            Map<String, Object> clientConfigs = ConfluentConfigs.interBrokerClientConfigs(config, interBrokerEp);

            LOG.info("Adding configs {} to config", clientConfigs);

            ccConfigProps.putAll(clientConfigs);
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
            ccConfigProps.putIfAbsent(ConfluentMetricsReporterSampler.METRIC_REPORTER_TOPIC_PATTERN, metricsReporterTopic);
        }

        String metricsReporterReplFactor = (String) kccProps.get(ConfluentMetricsReporterConfig.TOPIC_REPLICAS_CONFIG);
        // The metrics reporter replication factor is the same RF we should use for the sample store topic.
        if (metricsReporterReplFactor != null && metricsReporterReplFactor.length() > 0) {
            ccConfigProps.putIfAbsent(KafkaSampleStore.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG,
                    metricsReporterReplFactor);
        }

        ccConfigProps.put(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG,
                generateCcTopicExclusionRegex(config));

        return new KafkaCruiseControlConfig(ccConfigProps);
    }

    private static void removeGoalFromLists(String goal, List<String>... lists) {
        for (List<String> goalList : lists) {
            goalList.remove(goal);
        }
    }
}
