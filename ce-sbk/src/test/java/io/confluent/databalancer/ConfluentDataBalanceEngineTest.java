/**
 * Copyright (C) 2020 Confluent, Inc.
 */

package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
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
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import io.confluent.cruisecontrol.analyzer.goals.CrossRackMovementGoal;
import io.confluent.cruisecontrol.analyzer.goals.SequentialReplicaMovementGoal;
import io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import io.confluent.metrics.reporter.ConfluentMetricsReporterConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.utils.TestUtils$;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class ConfluentDataBalanceEngineTest  {
    private Properties brokerProps;
    private KafkaConfig initConfig;

    @Mock
    private KafkaCruiseControl mockCruiseControl;

    @Mock
    private DataBalancerMetricsRegistry mockMetricsRegistry;

    // Kind of a mock to replace the SingleThreadExecutorService with something that just runs in the current
    // thread, guaranteeing completion. (Only usable with the CruiseControl mock, above.)
    private static ExecutorService currentThreadExecutorService() {
        ThreadPoolExecutor.CallerRunsPolicy callerRunsPolicy = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor(0, 1, 0L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), callerRunsPolicy) {
            @Override
            public void execute(Runnable command) {
                callerRunsPolicy.rejectedExecution(command, this);
            }
        };
    }

    @Before
    public void setUp() {
        brokerProps = new Properties();
        brokerProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), TestUtils$.MODULE$.MockZkConnect());
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 200L);

        initConfig = new KafkaConfig(brokerProps);
        MockitoAnnotations.initMocks(this);
    }

    private ConfluentDataBalanceEngine getTestDataBalanceEngine() {
        return new ConfluentDataBalanceEngine(mockMetricsRegistry, mockCruiseControl, currentThreadExecutorService());
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

        // Add required properties to test
        String nwInCapacity = ConfluentConfigs.CONFLUENT_BALANCER_PREFIX +
                KafkaCruiseControlConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG;
        String metricSamplerClass = ConfluentConfigs.CONFLUENT_BALANCER_PREFIX +
                KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG;
        String nonBalancerPropertyKey = "confluent.non_balancer_property_key";

        brokerProps.put(nwInCapacity, "0.12");
        brokerProps.put(metricSamplerClass, "io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler");
        brokerProps.put(nonBalancerPropertyKey, "nonBalancerPropertyValue");

        KafkaConfig config = new KafkaConfig(brokerProps);
        // We expect only one listener in a bare-bones config.
        assertTrue(config.listeners().length() == 1);
        String expectedBootstrapServers = config.listeners().head().connectionString();
        KafkaCruiseControlConfig ccConfig = ConfluentDataBalanceEngine.generateCruiseControlConfig(config);

        assertTrue(ccConfig.getList(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG).contains(expectedBootstrapServers));
        assertEquals(sampleZkString, ccConfig.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG));
        assertNotNull("balancer n/w input capacity property not present",
                ccConfig.getDouble(KafkaCruiseControlConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG));
        assertNotNull("balancer metrics sampler class property not present",
                ccConfig.getClass(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG));
        assertThrows("nonBalancerPropertyValue present", ConfigException.class,
                () -> ccConfig.getString(nonBalancerPropertyKey));
        assertEquals(expectedGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.GOALS_CONFIG));
        assertEquals(Collections.emptyList(), ccConfig.getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG));

        // Not all properties go into the KafkaCruiseControlConfig. Extract everything for validation.
        // Expect nothing to be present as no overrides were present in this config
        Map<String, Object> ccOriginals = ccConfig.originals();
        assertFalse(ccOriginals.containsKey(ConfluentMetricsReporterSampler.METRIC_REPORTER_TOPIC_PATTERN));
        assertFalse(ccOriginals.containsKey(KafkaSampleStore.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG));

    }

    @Test
    public void testGeneratedConfigWithOverrides() {
        // Add required properties
        final String sampleZkString = "zookeeper-1-internal.pzkc-ldqwz.svc.cluster.local:2181,zookeeper-2-internal.pzkc-ldqwz.svc.cluster.local:2181/testKafkaCluster";

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

        // Add required properties to test
        String nwInCapacity = ConfluentConfigs.CONFLUENT_BALANCER_PREFIX +
                KafkaCruiseControlConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG;
        String  metricsTopicConfig = ConfluentMetricsReporterConfig.TOPIC_CONFIG;
        String testMetricsTopic = "testMetricsTopic";
        String metricsRfConfig = ConfluentMetricsReporterConfig.TOPIC_REPLICAS_CONFIG;
        String testMetricsRfValue = "2";

        brokerProps.put(nwInCapacity, "0.12");
        brokerProps.put(metricsTopicConfig, testMetricsTopic);
        brokerProps.put(metricsRfConfig, testMetricsRfValue);

        brokerProps.put(ConfluentConfigs.CONFLUENT_BALANCER_PREFIX + KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG,
                defaultGoalsOverride);
        brokerProps.put(ConfluentConfigs.CONFLUENT_BALANCER_PREFIX + KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG,
                anomalyGoalsOverride);

        KafkaConfig config = new KafkaConfig(brokerProps);
        KafkaCruiseControlConfig ccConfig = ConfluentDataBalanceEngine.generateCruiseControlConfig(config);
        // Not all properties go into the KafkaCruiseControlConfig. Extract everything for validation.
        Map<String, Object> ccOriginals = ccConfig.originals();

        assertEquals(testMetricsTopic, ccOriginals.get(ConfluentMetricsReporterSampler.METRIC_REPORTER_TOPIC_PATTERN));
        assertEquals(testMetricsRfValue, ccOriginals.get(KafkaSampleStore.SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG));

        assertEquals(expectedGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.GOALS_CONFIG));
        assertEquals(testDefaultGoalsConfig, ccConfig.getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG));
    }


    @Test
    public void testGenerateCruiseControlExclusionConfig() {
        // Add required properties
        final String sampleZkString = "zookeeper-1-internal.pzkc-ldqwz.svc.cluster.local:2181,zookeeper-2-internal.pzkc-ldqwz.svc.cluster.local:2181/testKafkaCluster";
        String bootstrapServersConfig = ConfluentConfigs.CONFLUENT_BALANCER_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        String bootstrapServers =  "localhost:9092";

        brokerProps.put(bootstrapServersConfig, bootstrapServers);

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
    public void testStartupComponentsReadySuccessful() throws Exception {
        List<String> startupComponents = ConfluentDataBalanceEngine.STARTUP_COMPONENTS;
        try {
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.add(ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.class.getName());

            KafkaConfig config = mock(KafkaConfig.class);
            KafkaCruiseControlConfig ccConfig = mock(KafkaCruiseControlConfig.class);

            ConfluentDataBalanceEngine cc = getTestDataBalanceEngine();
            cc.checkStartupComponentsReady(ccConfig);
            assertTrue(ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.checkupMethodCalled);
        } finally {
            // Restore components
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.addAll(startupComponents);
            ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.checkupMethodCalled = false;
        }
    }

    @Test
    public void testStartupComponentsReadyAbort() throws Exception {
        List<String> startupComponents = ConfluentDataBalanceEngine.STARTUP_COMPONENTS;
        try {
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.clear();
            ConfluentDataBalanceEngine.STARTUP_COMPONENTS.add(ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.class.getName());

            KafkaConfig config = mock(KafkaConfig.class);
            KafkaCruiseControlConfig ccConfig = mock(KafkaCruiseControlConfig.class);

            ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.block = true;
            ConfluentDataBalanceEngine dataBalancer = getTestDataBalanceEngine();

            AtomicBoolean abortCalled = new AtomicBoolean(false);
            Thread testThread = new Thread(() -> {
                try {
                    dataBalancer.checkStartupComponentsReady(ccConfig);
                } catch (InvocationTargetException e) {
                    if (e.getTargetException() instanceof StartupCheckInterruptedException) {
                        abortCalled.set(true);
                    }
                } catch (Exception e) {
                    // Should not come here and assert should fail
                }
            });
            testThread.start();

            // Wait until checkStartupCondition method is called.
            ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.TEST_SYNC_SEMAPHORE.acquire();
            // This should unblock MockDatabalancerStartupComponent
            dataBalancer.shutdown();
            testThread.join();

            assertTrue(ConfluentDataBalanceEngineTest.MockDatabalancerStartupComponent.checkupMethodCalled);
            assertTrue(abortCalled.get());
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
                                                 Semaphore abortStartupCheck) throws InterruptedException {
            checkupMethodCalled = true;
            if (block) {
                TEST_SYNC_SEMAPHORE.release();

                // This will be unblocked when databalancer.shutdown is called
                abortStartupCheck.acquire();
                throw new StartupCheckInterruptedException();
            }
        }
    }

    @Test
    public void testStopCruiseControlNotInitialized() {
        // Don't use the regular getTestDataBalanceEngine as that has a defined CruiseControl, which we don't want.
        ConfluentDataBalanceEngine dbe = new ConfluentDataBalanceEngine(mockMetricsRegistry, null, currentThreadExecutorService());
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
        verify(mockMetricsRegistry).clearShortLivedMetrics();
    }

    @Test
    public void testShutdown() {
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        dbe.shutdown();
        verify(mockCruiseControl).shutdown();  // Shutdown should have been called
        verify(mockMetricsRegistry).clearShortLivedMetrics();
    }

    @Test
    public void testUpdateThrottleWhileRunning() {
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        dbe.updateThrottle(100L);
        verify(mockCruiseControl).updateThrottle(100L);
    }

    @Test
    public void testUpdateThrottleWhileStopped() {
        ConfluentDataBalanceEngine dbe = getTestDataBalanceEngine();
        dbe.stopCruiseControl();
        dbe.updateThrottle(100L);
        verify(mockCruiseControl, never()).updateThrottle(anyLong());
    }
}

