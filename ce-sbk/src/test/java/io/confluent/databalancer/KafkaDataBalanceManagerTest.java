/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import kafka.controller.DataBalanceManager;
import kafka.metrics.KafkaYammerMetrics;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.utils.TestUtils;
import kafka.utils.TestUtils$;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import scala.Option;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class KafkaDataBalanceManagerTest {
    private Properties brokerProps;
    private KafkaConfig initConfig;
    private KafkaConfig updatedConfig;

    private DataBalanceManager dataBalancer;

    @Mock
    private KafkaDataBalanceManager.DataBalanceEngineFactory mockDataBalanceEngineFactory;
    @Mock
    private DataBalanceEngine mockActiveDataBalanceEngine;
    @Mock
    private DataBalanceEngine mockInactiveDataBalanceEngine;
    @Mock
    private DataBalancerMetricsRegistry mockDbMetrics;

    @Before
    public void setUp() {
        brokerProps = new Properties();
        brokerProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), TestUtils$.MODULE$.MockZkConnect());
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 200L);
        initConfig = new KafkaConfig(brokerProps);

        MockitoAnnotations.initMocks(this);
        when(mockDataBalanceEngineFactory.getActiveDataBalanceEngine()).thenReturn(mockActiveDataBalanceEngine);
        when(mockDataBalanceEngineFactory.getInactiveDataBalanceEngine()).thenReturn(mockInactiveDataBalanceEngine);
        when(mockActiveDataBalanceEngine.isActive()).thenReturn(true);
    }

    @Test
    public void testUpdateConfigBalancerEnable() throws InterruptedException {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics);
        // Instantiate the Active DBE
        dataBalancer.onElection();

        dataBalancer.updateConfig(initConfig, updatedConfig);
        verify(mockActiveDataBalanceEngine).onDeactivation();

        // Now check to see that it handles the state change properly
        Mockito.reset(mockActiveDataBalanceEngine);
        dataBalancer.updateConfig(updatedConfig, initConfig);
        verify(mockActiveDataBalanceEngine).onActivation(initConfig);

        dataBalancer.updateConfig(initConfig, initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
        // Since it's been active this whole time, the inactive DBE should never have been called
        verify(mockInactiveDataBalanceEngine, never()).onActivation(any(KafkaConfig.class));
        verify(mockInactiveDataBalanceEngine, never()).onDeactivation();

        verify(mockActiveDataBalanceEngine, never()).shutdown();
        verify(mockInactiveDataBalanceEngine, never()).shutdown();
    }

    @Test
    public void testUpdateConfigBalancerEnableOnNonEligibleNode() throws InterruptedException {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics);

        dataBalancer.updateConfig(initConfig, updatedConfig);
        verify(mockInactiveDataBalanceEngine).onDeactivation();

        // Now check to see that it handles the state change properly
        Mockito.reset(mockActiveDataBalanceEngine);
        dataBalancer.updateConfig(updatedConfig, initConfig);
        verify(mockInactiveDataBalanceEngine).onActivation(initConfig);

        dataBalancer.updateConfig(initConfig, initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
        
        // Since it's been inactive this whole time, the active DBE should never have been called
        verify(mockActiveDataBalanceEngine, never()).onActivation(any(KafkaConfig.class));
        verify(mockActiveDataBalanceEngine, never()).onDeactivation();

        verify(mockActiveDataBalanceEngine, never()).shutdown();
        verify(mockInactiveDataBalanceEngine, never()).shutdown();
    }

    @Test
    public void testUpdateConfigBalancerThrottle() {
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 100L);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics);
        dataBalancer.onElection();
        verify(mockActiveDataBalanceEngine).onActivation(initConfig);

        dataBalancer.updateConfig(initConfig, updatedConfig);
        verify(mockActiveDataBalanceEngine).updateThrottle(100L);

        dataBalancer.updateConfig(updatedConfig, initConfig);
        verify(mockActiveDataBalanceEngine).updateThrottle(200L);

        dataBalancer.updateConfig(initConfig, initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
    }

    @Test
    public void testUpdateConfigNoPropsUpdated() {
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics);
        dataBalancer.onElection();
        verify(mockActiveDataBalanceEngine).onActivation(initConfig);

        // expect nothing to be updated
        dataBalancer.updateConfig(initConfig, updatedConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
    }

    @Test
    public void testEnableFromOff() {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        KafkaConfig disabledConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(disabledConfig, mockDataBalanceEngineFactory, mockDbMetrics);
        dataBalancer.onElection();
        // We SHOULD NOT have attempted to launch CC
        verify(mockActiveDataBalanceEngine, never()).onActivation(any(KafkaConfig.class));

        // Now update and enable
        dataBalancer.updateConfig(disabledConfig, initConfig);
        verify(mockActiveDataBalanceEngine).onActivation(initConfig);
    }

    @Test
    public void testConfluentBalancerEnabledMetric() {
        MetricsRegistry metrics = KafkaYammerMetrics.defaultRegistry();
        DataBalancerMetricsRegistry dbMetricsRegistry = new DataBalancerMetricsRegistry(metrics,
                KafkaDataBalanceManager.getMetricsWhiteList());
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, dbMetricsRegistry);
        dataBalancer.onElection();
        verifyMetricValue(metrics, 1);
        dataBalancer.onResignation();
        verifyMetricValue(metrics, 0);

        cleanMetrics(metrics);
    }

    private void verifyMetricValue(MetricsRegistry metricsRegistry, Integer expectedValue) {
        Map<MetricName, Metric> metrics = metricsRegistry.allMetrics();
        assertEquals(1, metrics.size());
        MetricName metricName = new ArrayList<>(metrics.keySet()).get(0);
        assertEquals("ActiveBalancerCount", metricName.getName());
        assertEquals("kafka.databalancer", metricName.getGroup());
        assertEquals(expectedValue, ((Gauge<?>) metrics.get(metricName)).value());
    }

    public void cleanMetrics(MetricsRegistry metricsRegistry) {
        TestUtils.clearYammerMetrics();
        metricsRegistry.shutdown();
    }

    @Test
    public void testShutdownOnActive() throws InterruptedException {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics);
        // Instantiate the Active DBE
        dataBalancer.onElection();
        verify(mockActiveDataBalanceEngine).onActivation(initConfig);

        // Since it's been active this whole time, the inactive DBE should never have been called
        verify(mockInactiveDataBalanceEngine, never()).onActivation(any(KafkaConfig.class));
        verify(mockInactiveDataBalanceEngine, never()).onDeactivation();

        // The expected shutdown path in the Controller calls resignation
        dataBalancer.onResignation();

        dataBalancer.shutdown();

        verify(mockActiveDataBalanceEngine).shutdown();
        verify(mockInactiveDataBalanceEngine).shutdown();
    }

    @Test
    public void testShutdownOnInactive() throws InterruptedException {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics);

        verify(mockActiveDataBalanceEngine, never()).onActivation(initConfig);
        verify(mockInactiveDataBalanceEngine, never()).onDeactivation();

        dataBalancer.shutdown();
        verify(mockActiveDataBalanceEngine, never()).onDeactivation();
        verify(mockInactiveDataBalanceEngine, never()).onDeactivation();

        verify(mockActiveDataBalanceEngine).shutdown();
        verify(mockInactiveDataBalanceEngine).shutdown();
    }

    @Test(expected = IllegalStateException.class)
    public void testRemoveBrokerNotActive() {
        dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics);
        dataBalancer.removeBroker(2, Option.<Long>apply(25L));
    }

    /**
     * Confirm that remove broker api call is processed successfully
     */
    @Test
    public void testRemoveBrokerAccepted() {
        dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics);
        dataBalancer.onElection();

        dataBalancer.removeBroker(1, Option.<Long>apply(15L));
        verify(mockActiveDataBalanceEngine).removeBroker(anyInt(), any());
    }

    /**
     * Check that we can remove broker that isn't alive.
     */
    @Test
    public void testRemoveNotAliveBroker() {
        dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics);
        dataBalancer.onElection();

        dataBalancer.removeBroker(1, Option.<Long>apply(null));
        verify(mockActiveDataBalanceEngine).removeBroker(anyInt(), any());
    }
}
