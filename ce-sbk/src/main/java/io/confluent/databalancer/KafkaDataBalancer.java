/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.yammer.metrics.Metrics;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import kafka.controller.DataBalancer;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.Map;

public class KafkaDataBalancer implements DataBalancer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataBalancer.class);

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

    private KafkaConfig kafkaConfig;
    private ExecutorService ccRunner;
    private KafkaCruiseControl cruiseControl;
    private Semaphore abortStartupCheck  = new Semaphore(0);

    public KafkaDataBalancer(KafkaConfig kafkaConfig) {
        Objects.requireNonNull(kafkaConfig, "KafkaConfig must not be null");
        this.kafkaConfig = kafkaConfig;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("CruiseControl-StartStop-%d")
                .build();
        ccRunner = Executors.newSingleThreadExecutor(threadFactory);
    }

    /**
     * Visible for testing. cruiseControl expected to be a mock testing object
     */
    KafkaDataBalancer(KafkaConfig kafkaConfig, KafkaCruiseControl cruiseControl) {
        this(kafkaConfig);
        this.cruiseControl = cruiseControl;
    }

    @Override
    public synchronized void startUp()  {
        LOG.info("DataBalancer: Scheduling Cruise Control Startup");
        int numStopRequests = abortStartupCheck.availablePermits();
        ccRunner.submit(() -> startCruiseControl(numStopRequests));
    }

    @Override
    public synchronized void shutdown() {
        LOG.info("DataBalancer: Scheduling Cruise Control Shutdown");

        // If startup is in progress, abort it
        abortStartupCheck.release();

        ccRunner.submit(this::stopCruiseControl);
    }

    private void startCruiseControl(int numStopRequests) {
        if (cruiseControl != null) {
            LOG.warn("CruiseControl already running when startUp requested.");
            return;
        }

        if (!kafkaConfig.getBoolean(ConfluentConfigs.BALANCER_ENABLE_CONFIG)) {
            LOG.info("DataBalancer: Skipping Cruise Control Startup as its not enabled.");
            return;
        }

        LOG.info("DataBalancer: Instantiating Cruise Control");
        try {
            KafkaCruiseControlConfig config = generateCruiseControlConfig(kafkaConfig);
            checkStartupComponentsReady(config, numStopRequests);
            KafkaCruiseControl cruiseControl = new KafkaCruiseControl(config, Metrics.defaultRegistry());
            cruiseControl.startUp();
            this.cruiseControl = cruiseControl;
            LOG.info("DataBalancer: Cruise Control started");
        } catch (StartupCheckInterruptedException e) {
            LOG.warn("CruiseControl startup aborted by shutdown.", e);
            this.cruiseControl = null;
        } catch (Exception e) {
            LOG.warn("Unable to start up CruiseControl", e);
            this.cruiseControl = null;
        }
    }

    /**
     * Check if all components needed for successful startup are ready.
     */
    // Visible for testing
    void checkStartupComponentsReady(KafkaCruiseControlConfig config, int numStopRequests) throws Exception {
        synchronized (this) {
            if (abortStartupCheck.availablePermits() > numStopRequests) {
                // A stop request arrived since start was called. Do nothing
                return;
            }
            // else drain the permits, so that a future stop request can abort a hanged startup check
            abortStartupCheck.drainPermits();
        }
        for (String startupComponent : STARTUP_COMPONENTS) {
            // Get the method object to validate
            Class<?> startupComponentClass = Class.forName(startupComponent);
            Method method = startupComponentClass.getMethod(
                    CHECK_STARTUP_CONDITION_METHOD_NAME, KafkaCruiseControlConfig.class, Semaphore.class);
            method.invoke(null, config, abortStartupCheck);
        }
    }

    // Visible for testing
    void stopCruiseControl() {
        if (cruiseControl != null) {
            LOG.info("DataBalancer: Starting Cruise Control Shutdown");
            try {
                cruiseControl.shutdown();
            } finally {
                cruiseControl = null;
                LOG.info("DataBalancer: Cruise Control shutdown completed.");
            }
        }
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

    /**
     * The function forms a regex expression by performing OR operation on the topic names and topic prefixes
     * considering each of these as string literals.
     * For example,
     * confluent.balancer.exclude.topic.names = [topic1, topic2],
     * confluent.balancer.exclude.topic.prefixes = [prefix1, prefix2]
     * The regex computed would be = "^\\Qtopic1\\E$|^\\Qtopic2\\E$|^\\Qprefix1\\E.*|^\\Qprefix2\\E.*"
     * Visible for testing
     */
    String generateRegex(KafkaConfig config) {
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
        Map<String, Object> ccConfigProps = new java.util.HashMap<>(config.originalsWithPrefix(ConfluentConfigs.CONFLUENT_BALANCER_PREFIX));

        // Special overrides: zookeeper.connect, etc.
        ccConfigProps.put(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, config.get(KafkaConfig.ZkConnectProp()));

        // CNKAF-528: Derive bootstrap.servers from the provided KafkaConfig, instead of requiring
        // users to specify it.

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

        return new KafkaCruiseControlConfig(ccConfigProps);
    }
}
