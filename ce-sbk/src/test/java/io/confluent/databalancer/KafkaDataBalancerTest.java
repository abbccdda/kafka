/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import kafka.controller.DataBalancer;
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class KafkaDataBalancerTest {
    private Properties brokerProps;
    private KafkaConfig initConfig;
    private KafkaConfig updatedConfig;

    private DataBalancer dataBalancer;

    @Mock
    private KafkaCruiseControl mockCruiseControl;

    @Before
    public void setUp() {
        brokerProps = new Properties();
        brokerProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), TestUtils$.MODULE$.MockZkConnect());
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 200L);
        //brokerProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        initConfig = new KafkaConfig(brokerProps);
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testUpdateConfigBalancerEnable() {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalancer(initConfig, mockCruiseControl);

        dataBalancer.updateConfig(updatedConfig);
        verify(mockCruiseControl).setSelfHealingFor(AnomalyType.GOAL_VIOLATION, false);
        
        dataBalancer.updateConfig(initConfig);
        verify(mockCruiseControl).setSelfHealingFor(AnomalyType.GOAL_VIOLATION, true);

        dataBalancer.updateConfig(initConfig);
        verifyNoMoreInteractions(mockCruiseControl);
    }

    @Test
    public void testUpdateConfigBalancerThrottle() {
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 100L);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalancer(initConfig, mockCruiseControl);

        dataBalancer.updateConfig(updatedConfig);
        verify(mockCruiseControl).updateThrottle(100L);

        dataBalancer.updateConfig(initConfig);
        verify(mockCruiseControl).updateThrottle(200L);

        dataBalancer.updateConfig(initConfig);
        verifyNoMoreInteractions(mockCruiseControl);
    }

    @Test
    public void testUpdateConfigNoPropsUpdated() {
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalancer(initConfig, mockCruiseControl);

        // expect nothing to be updated
        dataBalancer.updateConfig(updatedConfig);
        verifyNoInteractions(mockCruiseControl);
    }
    
    @Test
    public void testGenerateRegexNoTopicsOrPrefixes() {
        // test without TOPIC_PREFIXES or TOPIC_NAMES set
        KafkaDataBalancer kafkaDataBalancer = new KafkaDataBalancer(initConfig);
        String regex = kafkaDataBalancer.generateRegex(initConfig);
        assertEquals("Unexpected regex generated", "", regex);
    }

    @Test
    public void testGenerateRegexOnlyTopics() {
        String topicName = "topic1";
        String topicNamePrefixed = topicName + "2";
        brokerProps.put(ConfluentConfigs.BALANCER_EXCLUDE_TOPIC_NAMES_CONFIG, topicName);

        KafkaConfig config = new KafkaConfig(brokerProps);
        KafkaDataBalancer kafkaDataBalancer = new KafkaDataBalancer(config);
        String regex = kafkaDataBalancer.generateRegex(config);

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
        KafkaDataBalancer kafkaDataBalancer = new KafkaDataBalancer(config);
        String regex = kafkaDataBalancer.generateRegex(config);

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
        KafkaDataBalancer kafkaDataBalancer = new KafkaDataBalancer(config);
        String regex = kafkaDataBalancer.generateRegex(config);

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
        String bootstrapServers = ConfluentConfigs.CONFLUENT_BALANCER_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
        brokerProps.put(bootstrapServers, "localhost:9092");
        brokerProps.put(ConfluentConfigs.CONFLUENT_BALANCER_PREFIX + KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG,
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal");

        // Add peroperties to test
        String nwInCapacity = ConfluentConfigs.CONFLUENT_BALANCER_PREFIX +
                KafkaCruiseControlConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG;
        String metricSamplerClass = ConfluentConfigs.CONFLUENT_BALANCER_PREFIX +
                KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG;
        String nonBalancerPropertyKey = "confluent.non_balancer_property_key";

        brokerProps.put(nwInCapacity, "0.12");
        brokerProps.put(metricSamplerClass, "io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler");
        brokerProps.put(nonBalancerPropertyKey, "nonBalancerPropertyValue");

        KafkaConfig config = new KafkaConfig(brokerProps);
        KafkaDataBalancer kafkaDataBalancer = new KafkaDataBalancer(config);
        KafkaCruiseControlConfig ccConfig = kafkaDataBalancer.generateCruiseControlConfig();

        assertNotNull("balancer n/w input capacity property not present",
                ccConfig.getDouble(KafkaCruiseControlConfig.NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG));
        assertNotNull("balancer metrics sampler class property not present",
                ccConfig.getClass(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG));
        assertThrows("nonBalancerPropertyValue present", ConfigException.class,
                () -> ccConfig.getString(nonBalancerPropertyKey));
    }

    @Test
    public void testStartupComponentsReadySuccessful() throws Exception {
        List<String> startupComponents = KafkaDataBalancer.STARTUP_COMPONENTS;
        try {
            KafkaDataBalancer.STARTUP_COMPONENTS.clear();
            KafkaDataBalancer.STARTUP_COMPONENTS.add(MockDatabalancerStartupComponent.class.getName());

            KafkaConfig config = mock(KafkaConfig.class);
            KafkaCruiseControlConfig ccConfig = mock(KafkaCruiseControlConfig.class);

            KafkaDataBalancer dataBalancer = new KafkaDataBalancer(config);
            dataBalancer.checkStartupComponentsReady(ccConfig, 0);
            assertTrue(MockDatabalancerStartupComponent.checkupMethodCalled);
        } finally {
            // Restore components
            KafkaDataBalancer.STARTUP_COMPONENTS.clear();
            KafkaDataBalancer.STARTUP_COMPONENTS.addAll(startupComponents);
            MockDatabalancerStartupComponent.checkupMethodCalled = false;
        }
    }

    @Test
    public void testStartupComponentsReadyAbort() throws Exception {
        List<String> startupComponents = KafkaDataBalancer.STARTUP_COMPONENTS;
        try {
            KafkaDataBalancer.STARTUP_COMPONENTS.clear();
            KafkaDataBalancer.STARTUP_COMPONENTS.add(MockDatabalancerStartupComponent.class.getName());

            KafkaConfig config = mock(KafkaConfig.class);
            KafkaCruiseControlConfig ccConfig = mock(KafkaCruiseControlConfig.class);

            MockDatabalancerStartupComponent.block = true;
            KafkaDataBalancer dataBalancer = new KafkaDataBalancer(config);

            AtomicBoolean abortCalled = new AtomicBoolean(false);
            Thread testThread = new Thread(() -> {
                try {
                    dataBalancer.checkStartupComponentsReady(ccConfig, 0);
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
            MockDatabalancerStartupComponent.TEST_SYNC_SEMAPHORE.acquire();
            // This should unblock MockDatabalancerStartupComponent
            dataBalancer.shutdown();
            testThread.join();

            assertTrue(MockDatabalancerStartupComponent.checkupMethodCalled);
            assertTrue(abortCalled.get());
        } finally {
            // Restore components
            KafkaDataBalancer.STARTUP_COMPONENTS.clear();
            KafkaDataBalancer.STARTUP_COMPONENTS.addAll(startupComponents);
            MockDatabalancerStartupComponent.checkupMethodCalled = false;
            MockDatabalancerStartupComponent.block = false;
        }
    }

    @Test
    public void testStopCruiseControlNotInitialized() {
        KafkaConfig config = mock(KafkaConfig.class);
        KafkaDataBalancer dataBalancer = new KafkaDataBalancer(config);
        dataBalancer.stopCruiseControl(); // should be a no-op
    }

    @Test
    public void testStopCruiseControl() {
        KafkaConfig config = mock(KafkaConfig.class);
        KafkaCruiseControl cc = mock(KafkaCruiseControl.class);

        KafkaDataBalancer dataBalancer = new KafkaDataBalancer(config, cc);
        dataBalancer.stopCruiseControl();
        verify(cc).shutdown();  // Shutdown should have been called
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
}
