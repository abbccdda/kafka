/**
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import com.yammer.metrics.Metrics;
import io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

    // ccRunner is used to control all access to the underlying CruiseControl object;
    // access is serialized through here (in particular for startup/shutdown).
    private ExecutorService ccRunner;
    private KafkaCruiseControl cruiseControl;
    private Semaphore abortStartupCheck  = new Semaphore(0);

    public ConfluentDataBalanceEngine() {
        this(null,
             Executors.newSingleThreadExecutor(
                 new ThreadFactoryBuilder()
                     .setNameFormat("DataBalanceEngine-%d")
                     .build()));
    }

    // Visible for testing
    ConfluentDataBalanceEngine(KafkaCruiseControl cc, ExecutorService executor) {
        ccRunner = executor;
        cruiseControl = cc;
    }

    @Override
    public synchronized void startUp(KafkaConfig kafkaConfig)  {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine Startup");
        abortStartupCheck.drainPermits();
        ccRunner.submit(() -> startCruiseControl(kafkaConfig));
    }

    @Override
    public synchronized void shutdown() {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine Shutdown");

        // If startup is in progress, abort it
        abortStartupCheck.release();

        ccRunner.submit(this::stopCruiseControl, null);
    }

    @Override
    public void updateThrottle(Long newThrottle) {
        LOG.info("DataBalancer: Scheduling DataBalanceEngine throttle update");
        ccRunner.submit(() -> updateThrottleHelper(newThrottle));
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
            KafkaCruiseControl cruiseControl = new KafkaCruiseControl(config, Metrics.defaultRegistry());
            cruiseControl.startUp();
            this.cruiseControl = cruiseControl;
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
            // Get the method object to validate
            Class<?> startupComponentClass = Class.forName(startupComponent);
            Method method = startupComponentClass.getMethod(
                    CHECK_STARTUP_CONDITION_METHOD_NAME, KafkaCruiseControlConfig.class, Semaphore.class);
            method.invoke(null, config, abortStartupCheck);
        }
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
                cruiseControl.shutdown();
            } finally {
                cruiseControl = null;
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

        // Derive bootstrap.servers from the provided KafkaConfig, instead of requiring
        // users to specify it.
        String bootstrapServers = config.listeners().toStream()
                .find(ep -> ep.listenerName().equals(config.interBrokerListenerName()))
                .map(ep -> ep.connectionString())
                .getOrElse(() -> "");

        if (!bootstrapServers.equals("")) {
            ccConfigProps.putIfAbsent(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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

}
