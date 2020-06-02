/**
 * Copyright (C) 2020 Confluent, Inc.
 */

package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalPhaseBuilder;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import io.confluent.cruisecontrol.analyzer.goals.CrossRackMovementGoal;
import io.confluent.cruisecontrol.analyzer.goals.SequentialReplicaMovementGoal;
import io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;
import io.confluent.databalancer.operation.BrokerRemovalStateTracker;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import java.time.Duration;
import java.util.Optional;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.utils.MockTime;
import kafka.utils.TestUtils$;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class ConfluentDataBalanceEngineTest  {
    private Properties brokerProps;
    private KafkaConfig initConfig;

    private Time mockTime = new MockTime();

    @Mock
    private KafkaCruiseControl mockCruiseControl;

    @Mock
    private DataBalancerMetricsRegistry mockMetricsRegistry;

    // Spy over executor service returned by currentThreadExecutorService
    private ExecutorService executorService;

    /**
     * An executor service class that we can mock and also runs the task in the thread that
     * submits the task by using {@code java.util.concurrent.RejectedExecutionHandler} passed to it.
     *
     * @see #currentThreadExecutorService()
     */
    public static class RejectedExecutorService extends ThreadPoolExecutor {
        public RejectedExecutorService(int corePoolSize,
                                       int maximumPoolSize,
                                       long keepAliveTime,
                                       TimeUnit unit,
                                       BlockingQueue<Runnable> workQueue,
                                       RejectedExecutionHandler handler) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
        }

        @Override
        public void execute(Runnable command) {
            // `rejectedExecution` will throw RejectedExectionException that will be handled
            // by RejectedExecutionHandler. We use `CallersRunsPolicy` which responds by running the
            // task in calling thread, making the execution synchronous from caller perspective.
            getRejectedExecutionHandler().rejectedExecution(command, this);
        }
    }

    // Kind of a mock to replace the SingleThreadExecutorService with something that just runs in the current
    // thread, guaranteeing completion. (Only usable with the CruiseControl mock, above.)
    private static ExecutorService currentThreadExecutorService() {
        ThreadPoolExecutor.CallerRunsPolicy callerRunsPolicy = new ThreadPoolExecutor.CallerRunsPolicy();
        return new RejectedExecutorService(0, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<>(), callerRunsPolicy);
    }

    @Before
    public void setUp() {
        brokerProps = new Properties();
        brokerProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), TestUtils$.MODULE$.MockZkConnect());
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 200L);

        initConfig = new KafkaConfig(brokerProps);
        MockitoAnnotations.initMocks(this);
        Mockito.doNothing().when(mockCruiseControl).userTriggeredStopExecution();
    }

    private ConfluentDataBalanceEngine getTestDataBalanceEngine() {
        executorService = Mockito.spy(currentThreadExecutorService());
        return new ConfluentDataBalanceEngine(mockMetricsRegistry, mockCruiseControl, executorService, mockTime);
    }

    @Test
    public void testGenerateRegexNoTopicsOrPrefixes() {
        // test without TOPIC_PREFIXES or TOPIC_NAMES set
        String regex = ConfluentDataBalanceEngine.generateCcTopicExclusionRegex(initConfig);
        assertEquals("Unexpected regex generated", "", regex);
    }

    @Test
    public void testGenerateRegexOnlyTopics() {
        String topicName = "topic1";
        String topicNamePrefixed = topicName + "2";
        brokerProps.put(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_NAMES_CONFIG, topicName);

        KafkaConfig config = new KafkaConfig(brokerProps);
        String regex = ConfluentDataBalanceEngine.generateCcTopicExclusionRegex(config);

        assertEquals("Unexpected regex generated", "^\\Qtopic1\\E$", regex);
        assertTrue("Expected exact topic name to match", topicName.matches(regex));
        assertFalse("Expected topic with exact topic name as prefix not to match", topicNamePrefixed.matches(regex));
        assertFalse("Expected non-matching topic name not to match", "a".matches(regex));
    }

    @Test
    public void testGenerateRegexOnlyPrefixes() {
        String topicPrefix = "prefix1";
        String topicFull = topicPrefix + "hello";
        String topicSuffix = "not" + topicPrefix;
        brokerProps.put(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_PREFIXES_CONFIG, topicPrefix);

        KafkaConfig config = new KafkaConfig(brokerProps);
        String regex = ConfluentDataBalanceEngine.generateCcTopicExclusionRegex(config);

        assertEquals("Unexpected regex generated", "^\\Qprefix1\\E.*", regex);
        assertTrue("Expected exact topic prefix to match", topicPrefix.matches(regex));
        assertTrue("Expected topic with prefix to match", topicFull.matches(regex));
        assertFalse("Expected suffix not to match", topicSuffix.matches(regex));
    }

    @Test
    public void testGenerateRegex() {
        String topicNames = "topic1, top.c2, test-topic";
        String topicPrefixes = "prefix1, pref*x2";
        brokerProps.put(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_NAMES_CONFIG, topicNames);
        brokerProps.put(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_PREFIXES_CONFIG, topicPrefixes);

        KafkaConfig config = new KafkaConfig(brokerProps);
        String regex = ConfluentDataBalanceEngine.generateCcTopicExclusionRegex(config);

        // topic names/prefixes are wrapped in \\Q \\E as a result of Pattern.quote to ignore metacharacters present in the topic
        assertEquals("Unexpected regex generated", "^\\Qtopic1\\E$|^\\Qtop.c2\\E$|^\\Qtest-topic\\E$|" +
                "^\\Qprefix1\\E.*|^\\Qpref*x2\\E.*", regex);

        assertTrue("Expected exact topic name to match", "topic1".matches(regex));
        assertTrue("Expected exact topic name to match", "test-topic".matches(regex));
        assertTrue("Expected exact topic name with metadata characters to match", "top.c2".matches(regex));
        assertTrue("Expected prefix to match topic name", "prefix1-xyz".matches(regex));
        assertTrue("Expected prefix to match exact topic name", "prefix1".matches(regex));

        assertFalse("Expected partial topic name not to match", "topic1-name".matches(regex));
        assertFalse("Expected topicPrefix value present in middle of topic name not to match", "abc-prefix1-xyz".matches(regex));
        assertFalse("Expected topicPrefix value as suffix not to match", "abc-prefix1".matches(regex));
        assertFalse("Expected topicName value as suffix in topic name not to match", "abc-topic1".matches(regex));
        assertFalse("Expected topicName with regex metacharacters to be treated as a literal", "topic2".matches(regex));
        assertFalse("Expected topicPrefix with regex metacharacters to be treated as a literal", "prefix2".matches(regex));
    }

    @Test
    public void testGenerateCruiseControlConfig() {
        // Add required properties
        final String sampleZkString = "zookeeper-1-internal.pzkc-ldqwz.svc.cluster.local:2181,zookeeper-2-internal.pzkc-ldqwz.svc.cluster.local:2181/testKafkaCluster";

        // Goals Config should be this
        List<String> expectedGoalsConfig = new ArrayList<>(Arrays.asList(
                CrossRackMovementGoal.class.getName(),
                SequentialReplicaMovementGoal.class.getName(),
                ReplicaCapacityGoal.class.getName(),
                DiskCapacityGoal.class.getName(),
                NetworkInboundCapacityGoal.class.getName(),
                NetworkOutboundCapacityGoal.class.getName(),
                ReplicaDistributionGoal.class.getName(),
                DiskUsageDistributionGoal.class.getName(),
                NetworkInboundUsageDistributionGoal.class.getName(),
                NetworkOutboundUsageDistributionGoal.class.getName(),
                CpuUsageDistributionGoal.class.getName(),
                TopicReplicaDistributionGoal.class.getName(),
                LeaderReplicaDistributionGoal.class.getName(),
                LeaderBytesInDistributionGoal.class.getName()
        ));
        // Not a valid ZK connect URL but to validate what gets copied over.
        brokerProps.put(KafkaConfig.ZkConnectProp(), sampleZkString);

        // Add required properties to test -- network capacity is necessary
        String nwInCapacity = ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_CONFIG;
        String nwOutCapacity = ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_CONFIG;

        String metricSamplerClass = ConfluentConfigs.CONFLUENT_BALANCER_PREFIX +
                KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG;
        String nonBalancerPropertyKey = "confluent.non_balancer_property_key";

        // Just some arbitrary values for the network capacity -- needs to be non-zero
        brokerProps.put(nwInCapacity, "1200");
        brokerProps.put(nwOutCapacity, "780");
        brokerProps.put(metricSamplerClass, "io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler");
        brokerProps.put(nonBalancerPropertyKey, "nonBalancerPropertyValue");

        KafkaConfig config = new KafkaConfig(brokerProps);
        // We expect only one listener in a bare-bones config.
        assertEquals("More than one listeners found: " + config.listeners(),
                1, config.listeners().length());
        Endpoint interBpEp = config.listeners().head().toJava();
        String expectedBootstrapServers = (interBpEp.host() == null ? "" : interBpEp.host()) + ":" + interBpEp.port();
        //String expectedBootstrapServers = config.listeners().head().connectionString();
        KafkaCruiseControlConfig ccConfig = ConfluentDataBalanceEngine.generateCruiseControlConfig(config);

        assertTrue("expected bootstrap servers " + expectedBootstrapServers + " not set, got " +
                ccConfig.getList(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG),
                ccConfig.getList(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG).contains(expectedBootstrapServers));
        String actualZkString = ccConfig.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
        assertEquals(actualZkString + " not same as expected " + sampleZkString,
                sampleZkString, actualZkString);
        assertNotNull("balancer n/w input capacity property not present",
                ccConfig.getDouble(KafkaCruiseControlConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG));
        assertNotNull("balancer metrics sampler class property not present",
                ccConfig.getClass(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG));
        assertThrows("nonBalancerPropertyValue present", ConfigException.class,
                () -> ccConfig.getString(nonBalancerPropertyKey));
        List<String> actualGoalsConfig = ccConfig.getList(KafkaCruiseControlConfig.GOALS_CONFIG);
        assertEquals(actualGoalsConfig + " not same as expected " + expectedGoalsConfig,
                expectedGoalsConfig, actualGoalsConfig);
        List<String> actualDefaultGoalsConfig = ccConfig.getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG);
        assertEquals("Goal config is not empty: " + actualGoalsConfig, Collections.emptyList(), actualDefaultGoalsConfig);

        // Default is EMPTY_BROKERS -> no automatic self-healing
        assertEquals("Expected goal-violation self-healing to be disabled",
                     ccConfig.getBoolean(KafkaCruiseControlConfig.SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG),
                     false);

        // Not all required properties go into the KafkaCruiseControlConfig. Extract everything for validation.
        // Expect nothing to be present as no overrides were present in this config
        Map<String, Object> ccOriginals = ccConfig.originals();
        assertFalse("ConfluentMetricsReporterSampler.METRIC_REPORTER_TOPIC_PATTERN found in config",
                ccOriginals.containsKey(ConfluentMetricsReporterSampler.METRIC_REPORTER_TOPIC_PATTERN));
        assertFalse("ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_CONFIG found in config",
                ccOriginals.containsKey(ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_CONFIG));
    }

    @Test
    public void testGeneratedConfigWithOverrides() {
        // Add required properties
        final String sampleZkString = "zookeeper-1-internal.pzkc-ldqwz.svc.cluster.local:2181,zookeeper-2-internal.pzkc-ldqwz.svc.cluster.local:2181/testKafkaCluster";
        // Set a different bootstrap server
        final String expectedBootstrapServers = "localhost:9095";
        final String listenerString = "PLAINTEXT://" + expectedBootstrapServers;

        // Goals Config should be this -- not overridden
        List<String> expectedGoalsConfig = new ArrayList<>(
                Arrays.asList(
                        CrossRackMovementGoal.class.getName(),
                        SequentialReplicaMovementGoal.class.getName(),
                        ReplicaCapacityGoal.class.getName(),
                        DiskCapacityGoal.class.getName(),
                        NetworkInboundCapacityGoal.class.getName(),
                        NetworkOutboundCapacityGoal.class.getName(),
                        ReplicaDistributionGoal.class.getName(),
                        DiskUsageDistributionGoal.class.getName(),
                        NetworkInboundUsageDistributionGoal.class.getName(),
                        NetworkOutboundUsageDistributionGoal.class.getName(),
                        CpuUsageDistributionGoal.class.getName(),
                        TopicReplicaDistributionGoal.class.getName(),
                        LeaderReplicaDistributionGoal.class.getName(),
                        LeaderBytesInDistributionGoal.class.getName()
                ));

        // Test a limited subset of default goals.
        List<String> testDefaultGoalsConfig = new ArrayList<>(
                Arrays.asList(
                        CrossRackMovementGoal.class.getName(),
                        ReplicaCapacityGoal.class.getName(),
                        ReplicaDistributionGoal.class.getName(),
                        DiskCapacityGoal.class.getName()
                ));

        // Anomaly Goals must be a subset of self-healing goals (or default goals if no self-healing goals set).
        // Commit ta that requirement.
        List<String> testAnomalyGoalsConfig = new ArrayList<>(
                Arrays.asList(
                        ReplicaCapacityGoal.class.getName(),
                        ReplicaDistributionGoal.class.getName(),
                        DiskCapacityGoal.class.getName()
                ));

        // Set Default Goals to this
        String defaultGoalsOverride = String.join(",", testDefaultGoalsConfig);
        String anomalyGoalsOverride = String.join(",", testAnomalyGoalsConfig);

        // Not a valid ZK connect URL but to validate what gets copied over.
        brokerProps.put(KafkaConfig.ZkConnectProp(), sampleZkString);
        brokerProps.put(KafkaConfig.ListenersProp(), listenerString);

        // Add required properties to test
        String nwInCapacity = ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_CONFIG;
        String nwOutCapacity = ConfluentConfigs.BALANCER_NETWORK_OUT_CAPACITY_CONFIG;

        String  metricsTopicConfig = ConfluentMetricsReporterConfig.TOPIC_CONFIG;
        String testMetricsTopic = "testMetricsTopic";
        String metricsRfConfig = ConfluentMetricsReporterConfig.TOPIC_REPLICAS_CONFIG;
        String testMetricsRfValue = "2";

        brokerProps.put(nwInCapacity, "1200");
        brokerProps.put(nwOutCapacity, "780");
        brokerProps.put(metricsTopicConfig, testMetricsTopic);
        brokerProps.put(metricsRfConfig, testMetricsRfValue);

        brokerProps.put(ConfluentConfigs.CONFLUENT_BALANCER_PREFIX + KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG,
                defaultGoalsOverride);
        brokerProps.put(ConfluentConfigs.CONFLUENT_BALANCER_PREFIX + KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG,
                anomalyGoalsOverride);

        // Disable generalized auto-healing
        brokerProps.put(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG, ConfluentConfigs.BalancerSelfHealMode.EMPTY_BROKER.toString());

        KafkaConfig config = new KafkaConfig(brokerProps);
        KafkaCruiseControlConfig ccConfig = ConfluentDataBalanceEngine.generateCruiseControlConfig(config);
        // Validate the non-default listener
        assertTrue("KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG doesn't contain " + expectedBootstrapServers,
                ccConfig.getList(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG).contains(expectedBootstrapServers));

        assertEquals("Expected goal-violation self-healing to be disabled",
                     ccConfig.getBoolean(KafkaCruiseControlConfig.SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG),
                     false);

        // Not all properties go into the KafkaCruiseControlConfig. Extract everything for validation.
        Map<String, Object> ccOriginals = ccConfig.originals();

        Object actualMetricsTopic = ccOriginals.get(ConfluentMetricsReporterSampler.METRIC_REPORTER_TOPIC_PATTERN);
        assertEquals(actualMetricsTopic + " is not same as expected " + testMetricsTopic,
                testMetricsTopic, actualMetricsTopic);
        Object actualTopicRf = ccOriginals.get(ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_CONFIG);
        assertEquals(actualTopicRf + " is not same as expected " + testMetricsRfValue,
                testMetricsRfValue, actualTopicRf);

        List<String> actualGoalsConfig = ccConfig.getList(KafkaCruiseControlConfig.GOALS_CONFIG);
        assertEquals(actualGoalsConfig + " is not same as expected " + expectedGoalsConfig,
                expectedGoalsConfig, actualGoalsConfig);
        List<String> actualDefaultGoalsConfig = ccConfig.getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG);
        assertEquals(actualDefaultGoalsConfig + " is not same as expected " + testDefaultGoalsConfig,
                testDefaultGoalsConfig, actualDefaultGoalsConfig);
    }

    @Test
    public void testGenerateCruiseControlConfigWithZeroNetworkCapacity() {
        // Add required properties
        final String sampleZkString = "zookeeper-1-internal.pzkc-ldqwz.svc.cluster.local:2181,zookeeper-2-internal.pzkc-ldqwz.svc.cluster.local:2181/testKafkaCluster";

        // Goals Config should be lacking the outbound network goals initially
        List<String> expectedGoalsConfig = new ArrayList<>(KafkaCruiseControlConfig.DEFAULT_GOALS_LIST);
        // If this fails, NetworkOutboundCapacityGoal was not present
        assertTrue("NetworkOutboundCapacityGoal was expected to be in DEFAULT_GOALS_LIST",
                expectedGoalsConfig.remove(NetworkOutboundCapacityGoal.class.getName()));

        List<String> expectedHardGoalsConfig = new ArrayList<>(KafkaCruiseControlConfig.DEFAULT_HARD_GOALS_LIST);
        assertTrue("NetworkOutboundCapacityGoal expected to be in DEFAULT_HARD_GOALS_LIST",
                expectedHardGoalsConfig.remove(NetworkOutboundCapacityGoal.class.getName()));

        List<String> expectedAnomalyDetectionGoalsConfig = new ArrayList<>(KafkaCruiseControlConfig.DEFAULT_ANOMALY_DETECTION_GOALS_LIST);
        assertTrue("NetworkOutboundCapacityGoal expected to be in DEFAULT_ANOMALY_DETECTION_GOALS",
                expectedAnomalyDetectionGoalsConfig.remove(NetworkOutboundCapacityGoal.class.getName()));

        // Not a valid ZK connect URL but to validate what gets copied over.
        brokerProps.put(KafkaConfig.ZkConnectProp(), sampleZkString);

        // Add required properties to test -- network capacity is necessary
        // Intentionally leave out network-outbound -- this should remove the outbound goal from capacity config
        String nwInCapacity = ConfluentConfigs.BALANCER_NETWORK_IN_CAPACITY_CONFIG;

        brokerProps.put(nwInCapacity, "1200");

        // Outbound capacity was not set -- this should result in no NetworkOutbound goal, and
        // all goals should be properly updated
        KafkaConfig config = new KafkaConfig(brokerProps);
        KafkaCruiseControlConfig ccConfig = ConfluentDataBalanceEngine.generateCruiseControlConfig(config);

        assertEquals(expectedGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.GOALS_CONFIG));
        assertEquals(Collections.emptyList(), ccConfig.getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG));
        assertEquals(expectedHardGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.HARD_GOALS_CONFIG));
        assertEquals(expectedAnomalyDetectionGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG));

        // Now take out the NW-IN capacity. Both inbound and outbound capacity goals should be gone.
        brokerProps.remove(nwInCapacity);
        expectedGoalsConfig.remove(NetworkInboundCapacityGoal.class.getName());
        expectedHardGoalsConfig.remove(NetworkInboundCapacityGoal.class.getName());
        expectedAnomalyDetectionGoalsConfig.remove(NetworkInboundCapacityGoal.class.getName());

        config = new KafkaConfig(brokerProps);
        ccConfig = ConfluentDataBalanceEngine.generateCruiseControlConfig(config);

        assertEquals(expectedGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.GOALS_CONFIG));
        assertEquals(Collections.emptyList(), ccConfig.getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG));
        assertEquals(expectedHardGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.HARD_GOALS_CONFIG));
        assertEquals(expectedAnomalyDetectionGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG));
    }

    @Test
    public void testInvalidSelfHealingConfig() {
        // Add required properties
        final String selfHealingDisabled = "disabled";
        brokerProps.put(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG, selfHealingDisabled);

        assertThrows("Expected invalid self-healing config to throw ConfigException", ConfigException.class,
                () -> new KafkaConfig(brokerProps));
    }

    @Test
    public void testGenerateCruiseControlExclusionConfig() {
        // Add required properties
        // Set topic exclusions (same as above tests)
        String topicNames = "topic1, top.c2, test-topic";
        String topicPrefixes = "prefix1, pref*x2";
        brokerProps.put(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_NAMES_CONFIG, topicNames);
        brokerProps.put(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_PREFIXES_CONFIG, topicPrefixes);

        KafkaConfig config = new KafkaConfig(brokerProps);
        KafkaCruiseControlConfig ccConfig = ConfluentDataBalanceEngine.generateCruiseControlConfig(config);

        // Validate that the CruiseControl regex behaves as we would expect
        String configRegex = ccConfig.getString(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG);

        assertTrue("Expected exact topic name to match", "topic1".matches(configRegex));
        assertTrue("Expected exact topic name to match", "test-topic".matches(configRegex));
        assertTrue("Expected exact topic name with metadata characters to match", "top.c2".matches(configRegex));
        assertTrue("Expected prefix to match topic name", "prefix1-xyz".matches(configRegex));
        assertTrue("Expected prefix to match exact topic name", "prefix1".matches(configRegex));

        assertFalse("Expected partial topic name not to match", "topic1-name".matches(configRegex));
        assertFalse("Expected topicPrefix value present in middle of topic name not to match", "abc-prefix1-xyz".matches(configRegex));
        assertFalse("Expected topicPrefix value as suffix not to match", "abc-prefix1".matches(configRegex));
        assertFalse("Expected topicName value as suffix in topic name not to match", "abc-topic1".matches(configRegex));
        assertFalse("Expected topicName with regex metacharacters to be treated as a literal", "topic2".matches(configRegex));
        assertFalse("Expected topicPrefix with regex metacharacters to be treated as a literal", "prefix2".matches(configRegex));
    }

    @Test
    public void testGeneratedEncryptedInterBrokerConfig() {
        Properties props = new Properties();
        final String localListener = "localhost:9075";
        props.put(KafkaConfig$.MODULE$.ZkConnectProp(), "localhost:9095");
        props.put(KafkaConfig$.MODULE$.ListenersProp(), "INTERNAL://" + localListener);
        props.put(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(), "INTERNAL:SSL");
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "test.truststore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "test.keystore.jks");
        props.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, Collections.singletonList("TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA"));
        props.put(SslConfigs.SSL_PROVIDER_CONFIG, "JVM");
        props.put("listener.name.internal.ssl.keystore.location", "listener.keystore.jks");
        props.put("inter.broker.listener.name", "INTERNAL");

        KafkaCruiseControlConfig ccConfig = ConfluentDataBalanceEngine.generateCruiseControlConfig(new KafkaConfig(props));
        assertTrue("AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG doesn't contain " + localListener,
                ccConfig.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG).contains(localListener));

        // Security operations may not be present in default KafkaCruiseControlConfig
        Map<String, Object> clientConfigs = KafkaCruiseControlUtils.filterAdminClientConfigs(ccConfig.values());
        assertEquals("SSL", clientConfigs.get(AdminClientConfig.SECURITY_PROTOCOL_CONFIG));
        assertEquals("test.truststore.jks", clientConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("listener.keystore.jks", clientConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("TLSv1.2", clientConfigs.get(SslConfigs.SSL_PROTOCOL_CONFIG));
        assertEquals(Collections.singletonList("TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA"), clientConfigs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));
        assertEquals("JVM", clientConfigs.get(SslConfigs.SSL_PROVIDER_CONFIG));
    }

    @Test
    public void testCruiseControlSelfHealingConfig() {
        KafkaConfig brokerFailureHealingEnabled = initConfig;
        KafkaCruiseControlConfig ccConfigBrokerFailureHealingEnabled = ConfluentDataBalanceEngine.generateCruiseControlConfig(brokerFailureHealingEnabled);
        assertTrue("expected self healing for broker failure to be enabled",
                ccConfigBrokerFailureHealingEnabled.getBoolean(KafkaCruiseControlConfig.SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG));
        assertEquals("expected broker failure threshold to be passed through", ConfluentConfigs.BALANCER_BROKER_FAILURE_THRESHOLD_DEFAULT,
                ccConfigBrokerFailureHealingEnabled.getLong(KafkaCruiseControlConfig.BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG));

        brokerProps.put(ConfluentConfigs.BALANCER_BROKER_FAILURE_THRESHOLD_CONFIG, ConfluentConfigs.BALANCER_BROKER_FAILURE_THRESHOLD_DISABLED);
        KafkaConfig brokerFailureHealingDisabled = new KafkaConfig(brokerProps);
        KafkaCruiseControlConfig ccConfigBrokerFailureHealingDisabled = ConfluentDataBalanceEngine.generateCruiseControlConfig(brokerFailureHealingDisabled);
        assertFalse("expected self healing for broker failure to be disabled",
                ccConfigBrokerFailureHealingDisabled.getBoolean(KafkaCruiseControlConfig.SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG));
        assertEquals("cannot pass through negative self healing threshold to SelfHealingNotifier",
                KafkaCruiseControlConfig.DEFAULT_BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS,
                ccConfigBrokerFailureHealingDisabled.getLong(KafkaCruiseControlConfig.BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG));
    }

    @Test
    public void testStartupComponentsReadySuccessful() {
        List<ConfluentDataBalanceEngine.StartupComponent> startupComponents = ConfluentDataBalanceEngine.STARTUP_COMPONENTS;
        try {
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.add(new ConfluentDataBalanceEngine.StartupComponent(
                ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.class.getName(),
                ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent::checkStartupCondition)
            );

            KafkaCruiseControlConfig ccConfig = mock(KafkaCruiseControlConfig.class);

            ConfluentDataBalanceEngine cc = getTestDataBalanceEngine();
            cc.checkStartupComponentsReady(ccConfig);
            assertTrue("Check startup method was not called.",
                    ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.checkupMethodCalled);
        } finally {
            // Restore components
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.addAll(startupComponents);
            ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.checkupMethodCalled = false;
        }
    }

    @Test
    public void testStartupComponentsReadyAbort() throws Exception {
        List<ConfluentDataBalanceEngine.StartupComponent> startupComponents = ConfluentDataBalanceEngine.STARTUP_COMPONENTS;
        try {
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.add(new ConfluentDataBalanceEngine.StartupComponent(
                ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.class.getName(),
                ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent::checkStartupCondition)
            );

            KafkaCruiseControlConfig ccConfig = mock(KafkaCruiseControlConfig.class);

            ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.block = true;
            ConfluentDataBalanceEngine dataBalancer = getTestDataBalanceEngine();

            AtomicBoolean abortCalled = new AtomicBoolean(false);
            Thread testThread = new Thread(() -> {
                try {
                    dataBalancer.checkStartupComponentsReady(ccConfig);
                } catch (StartupCheckInterruptedException e) {
                    abortCalled.set(true);
                }
            });
            testThread.start();

            // Wait until checkStartupCondition method is called.
            ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.TEST_SYNC_SEMAPHORE.acquire();
            // This should unblock MockDatabalancerStartupComponent
            dataBalancer.onDeactivation();
            testThread.join();

            assertTrue("Check Startup method was not called.",
                    ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.checkupMethodCalled);
            assertTrue("Startup method was not aborted.", abortCalled.get());
        } finally {
            // Restore components
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.addAll(startupComponents);
            ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.checkupMethodCalled = false;
            ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.block = false;
        }
    }

    private static class MockDatabalancerStartupComponent {

        public static boolean block = false;
        public static boolean checkupMethodCalled = false;
        public static final Semaphore TEST_SYNC_SEMAPHORE = new Semaphore(0);

        public static void checkStartupCondition(KafkaCruiseControlConfig config,
                                                 Semaphore abortStartupCheck) {
            checkupMethodCalled = true;
            if (block) {
                TEST_SYNC_SEMAPHORE.release();

                // This will be unblocked when databalancer.shutdown is called
                try {
                    abortStartupCheck.acquire();
                    throw new StartupCheckInterruptedException();
                } catch (InterruptedException e) {
                    throw new StartupCheckInterruptedException(e);
                }
            }
        }
    }

    @Test
    public void testStopCruiseControlNotInitialized() {
        // Don't use the regular getTestDataBalanceEngine as that has a defined CruiseControl, which we don't want.
        ConfluentDataBalanceEngine dbe = new ConfluentDataBalanceEngine(mockMetricsRegistry, null, currentThreadExecutorService(), mockTime);
        dbe.stopCruiseControl(); // should be a no-op
        verify(mockCruiseControl, never()).shutdown();
        verify(mockMetricsRegistry, never()).clearShortLivedMetrics();
    }

    @Test
    public void testStopCruiseControlAfterShutdownd() {
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        dbe.stopCruiseControl(); // This shuts down the mock
        verify(mockCruiseControl, times(1)).shutdown();
        verify(mockMetricsRegistry, times(1)).clearShortLivedMetrics();
        dbe.stopCruiseControl();
        // Shutdown should not be called again
        verify(mockCruiseControl, times(1)).shutdown();
    }

    @Test
    public void testStopCruiseControl() {
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        dbe.stopCruiseControl();
        verify(mockCruiseControl).shutdown();  // Shutdown should have been called
        verify(mockCruiseControl).userTriggeredStopExecution();
        verify(mockMetricsRegistry).clearShortLivedMetrics();
    }

    @Test
    public void testDeactivation() {
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        dbe.canAcceptRequests = true;
        dbe.onDeactivation();
        verify(mockCruiseControl).shutdown();  // Shutdown should have been called
        verify(mockCruiseControl).userTriggeredStopExecution();
        verify(mockMetricsRegistry).clearShortLivedMetrics();

        assertFalse("DatabalanceEngine is not stopped", dbe.canAcceptRequests);
    }

    /**
     * Test starting cruise control when its already there is a no-op.
     */
    @Test
    public void testStartCruiseControlNoOp() {
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        dbe.startCruiseControl(null, null);

        assertSame(dbe.cruiseControl, mockCruiseControl);
    }

    @Test
    @SuppressWarnings("deprecation") // JavaConverters is deprecated in scala 2.13
    public void testStartCruiseControlSuccess() {
        List<ConfluentDataBalanceEngine.StartupComponent> startupComponents = ConfluentDataBalanceEngine.STARTUP_COMPONENTS;
        try {
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            KafkaConfig config = mock(KafkaConfig.class);
            List<String> logDirs = Collections.singletonList("/log_dir");
            when(config.logDirs()).thenReturn(JavaConverters.asScalaBuffer(logDirs));
            when(config.getString(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG)).thenReturn(ConfluentConfigs.BalancerSelfHealMode.ANY_UNEVEN_LOAD.toString());
            when(config.originalsWithPrefix(Mockito.anyString())).thenReturn(
                    Collections.singletonMap(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap_server"));

            ConfluentDataBalanceEngine dbe = new ConfluentDataBalanceEngine(
                    mockMetricsRegistry, null, currentThreadExecutorService(), mockTime);

            dbe.startCruiseControl(config, kafkaconfig -> mockCruiseControl);
            verify(mockCruiseControl).startUp();
        } finally {
            // Restore components
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.addAll(startupComponents);
        }
    }

    @Test
    public void testStartCruiseControlFailed() {
        List<ConfluentDataBalanceEngine.StartupComponent> startupComponents = ConfluentDataBalanceEngine.STARTUP_COMPONENTS;
        try {
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            KafkaConfig config = mock(KafkaConfig.class);

            ConfluentDataBalanceEngine dbe = new ConfluentDataBalanceEngine(
                    mockMetricsRegistry, null, currentThreadExecutorService(), mockTime);

            dbe.startCruiseControl(config, kafkaconfig -> {
                throw new RuntimeException();
            });
            verify(mockCruiseControl, never()).startUp();
        } finally {
            // Restore components
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.addAll(startupComponents);
        }
    }

    @Test
    public void testOnActivation() {
        KafkaConfig config = mock(KafkaConfig.class);

        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        Mockito.doReturn(null).when(executorService).submit(any(Runnable.class));

        dbe.onActivation(config);
        verify(executorService).submit(any(Runnable.class));
        assertTrue("DatabalanceEngine is not started", dbe.canAcceptRequests);
    }

    @Test
    public void testRemoveBroker() throws Throwable {
        int brokerToRemove = 1;
        Optional<Long> brokerEpoch = Optional.of(1L);

        KafkaConfig config = mock(KafkaConfig.class);
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        dbe.onActivation(config);
        BrokerRemovalPhaseBuilder.BrokerRemovalExecution exec = mock(BrokerRemovalPhaseBuilder.BrokerRemovalExecution.class);
        when(mockCruiseControl.removeBroker(Mockito.eq(brokerToRemove), Mockito.eq(brokerEpoch),
            any(BrokerRemovalCallback.class), anyString())).thenReturn(exec);

        BrokerRemovalStateTracker mockTracker = mock(BrokerRemovalStateTracker.class);
        dbe.removeBroker(brokerToRemove, brokerEpoch, mockTracker, "uid");

        verify(executorService, times(2)).submit(any(Runnable.class));
        verify(mockCruiseControl).removeBroker(Mockito.eq(brokerToRemove), Mockito.eq(brokerEpoch),
            any(BrokerRemovalCallback.class), anyString());
        verify(exec, only()).execute(Duration.ofMinutes(60));

        assertTrue("DatabalanceEngine is not started", dbe.canAcceptRequests);
    }

    @Test(expected = InvalidRequestException.class)
    public void testRemoveBrokerThrowsInvalidRequestExceptionIfNoActiveDatabalancer() {
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        int brokerToRemove = 1;
        Optional<Long> brokerEpoch = Optional.of(1L);
        BrokerRemovalStateTracker mockTracker = mock(BrokerRemovalStateTracker.class);

        dbe.removeBroker(brokerToRemove, brokerEpoch, mockTracker, "uid");
    }

    @Test
    public void testUpdateThrottleWhileRunning() {
        ConfluentDataBalanceEngine realDbe = getTestDataBalanceEngine();
        ConfluentDataBalanceEngine dbe = spy(realDbe);
        dbe.updateThrottle(100L);
        verify(mockCruiseControl).updateThrottle(100L);
        verify(dbe).updateThrottleHelper(100L);
    }

    @Test
    public void testUpdateThrottleWhileStopped() {
        ConfluentDataBalanceEngine realDbe = getTestDataBalanceEngine();
        ConfluentDataBalanceEngine dbe = spy(realDbe);
        dbe.stopCruiseControl();
        dbe.updateThrottle(100L);
        verify(mockCruiseControl, never()).updateThrottle(anyLong());
        // helper should be called even though CC isn't
        verify(dbe).updateThrottleHelper(100L);
    }

    @Test
    public void testUpdateAutoHeal() {
        ConfluentDataBalanceEngine realDbe = getTestDataBalanceEngine();
        ConfluentDataBalanceEngine dbe = spy(realDbe);
        dbe.setAutoHealMode(true);
        verify(mockCruiseControl).setGoalViolationSelfHealing(true);
        verify(dbe).updateAutoHealHelper(true);

        dbe.setAutoHealMode(false);
        verify(mockCruiseControl).setGoalViolationSelfHealing(false);
        verify(dbe).updateAutoHealHelper(false);
    }

    @Test
    public void testUpdateAutoHealWhenStopped() {
        ConfluentDataBalanceEngine realDbe = getTestDataBalanceEngine();
        ConfluentDataBalanceEngine dbe = spy(realDbe);
        dbe.stopCruiseControl();
        dbe.setAutoHealMode(true);
        verify(mockCruiseControl, never()).setGoalViolationSelfHealing(true);
        verify(dbe).updateAutoHealHelper(true);
    }

}
