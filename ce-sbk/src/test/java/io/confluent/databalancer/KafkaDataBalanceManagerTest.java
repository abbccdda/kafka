/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import java.util.Arrays;
import java.util.Collections;
import kafka.common.BrokerRemovalStatus;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;
import io.confluent.databalancer.operation.BrokerRemovalStateMachine;
import io.confluent.databalancer.operation.BrokerRemovalStateTracker;
import java.util.Optional;
import java.util.function.Supplier;
import kafka.controller.DataBalanceManager;
import kafka.metrics.KafkaYammerMetrics;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.utils.TestUtils;
import kafka.utils.TestUtils$;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import scala.Option;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;


import static io.confluent.databalancer.KafkaDataBalanceManager.BROKER_REMOVAL_STATE_METRIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
    @Mock
    private Time time;
    @Captor
    private ArgumentCaptor<Supplier<String>> stateMetricSupplierCaptor;

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
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
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
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);

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
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
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
    public void testUpdateConfigAutoHealMode() {
        brokerProps.put(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG, ConfluentConfigs.BalancerSelfHealMode.ANY_UNEVEN_LOAD.toString());
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
        dataBalancer.onElection();
        verify(mockActiveDataBalanceEngine).onActivation(initConfig);

        dataBalancer.updateConfig(initConfig, updatedConfig);
        verify(mockActiveDataBalanceEngine).setAutoHealMode(true);

        dataBalancer.updateConfig(updatedConfig, initConfig);
        verify(mockActiveDataBalanceEngine).setAutoHealMode(false);

        dataBalancer.updateConfig(initConfig, initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
    }

    @Test
    public void testUpdateConfigMultipleProperties() {
        // Leave enabled the same but change all other dynamic properties to ensure they get updated as expected
        brokerProps.put(ConfluentConfigs.BALANCER_AUTO_HEAL_MODE_CONFIG, ConfluentConfigs.BalancerSelfHealMode.ANY_UNEVEN_LOAD.toString());
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 100L);

        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
        dataBalancer.onElection();
        verify(mockActiveDataBalanceEngine).onActivation(initConfig);

        dataBalancer.updateConfig(initConfig, updatedConfig);
        verify(mockActiveDataBalanceEngine).setAutoHealMode(true);
        verify(mockActiveDataBalanceEngine).updateThrottle(100L);

        dataBalancer.updateConfig(updatedConfig, initConfig);
        verify(mockActiveDataBalanceEngine).setAutoHealMode(false);
        verify(mockActiveDataBalanceEngine).updateThrottle(200L);
        
        dataBalancer.updateConfig(initConfig, initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
    }

    @Test
    public void testUpdateConfigNoPropsUpdated() {
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
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
        dataBalancer = new KafkaDataBalanceManager(disabledConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
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
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, dbMetricsRegistry, time);
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
                mockDbMetrics, time);
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
                mockDbMetrics, time);

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
            mockDbMetrics, time);
        dataBalancer.scheduleBrokerRemoval(2, Option.apply(25L));
    }

    /**
     * Confirm that remove broker api call is processed successfully
     */
    @Test
    public void testRemoveBrokerAccepted() {
        dataBalancer = new KafkaDataBalanceManager(initConfig,
            new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
            mockDbMetrics, time);
        KafkaDataBalanceManager kafkaDataBalanceManager = (KafkaDataBalanceManager) dataBalancer;
        ArgumentCaptor<BrokerRemovalStateTracker> argument = ArgumentCaptor.forClass(BrokerRemovalStateTracker.class);
        int brokerId = 1;
        Exception expectedListenerException = new Exception("Listener exception!");

        assertRemoveBrokerCalled(brokerId, argument, stateMetricSupplierCaptor);

        // also test that the listener passed to BrokerRemovalStateTracker updates the state in KafkaDataBalanceManager
        argument.getValue().registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_FAILURE, expectedListenerException);

        BrokerRemovalStatus expectedStatus = new BrokerRemovalStatus(brokerId, BrokerRemovalDescription.BrokerShutdownStatus.CANCELED,
            BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED,
            expectedListenerException);
        assertEquals("Expected one removal status to be populated",
            1, kafkaDataBalanceManager.brokerRemovalsStatus.size());
        assertTrue("Expected the removed broker's removal status to be populated",
            kafkaDataBalanceManager.brokerRemovalsStatus.containsKey(brokerId));
        assertEquals(expectedStatus,
            kafkaDataBalanceManager.brokerRemovalsStatus.get(brokerId));
        assertEquals(BrokerRemovalStateMachine.BrokerRemovalState.INITIAL_PLAN_COMPUTATION_FAILED.toString(),
            stateMetricSupplierCaptor.getValue().get());
    }

    /**
     * Confirm that remove broker api call is processed successfully
     */
    @Test
    public void testRemoveBrokerListenerContinuouslyUpdatesStatus() {
        dataBalancer = new KafkaDataBalanceManager(initConfig,
            new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
            mockDbMetrics, time);
        KafkaDataBalanceManager kafkaDataBalanceManager = (KafkaDataBalanceManager) dataBalancer;
        ArgumentCaptor<BrokerRemovalStateTracker> argument = ArgumentCaptor.forClass(BrokerRemovalStateTracker.class);
        int brokerId = 1;

        assertRemoveBrokerCalled(brokerId, argument, stateMetricSupplierCaptor);

        // 1. Test that the listener passed to BrokerRemovalStateTracker updates the state in KafkaDataBalanceManager
        Exception expectedListenerException = new Exception("Listener exception!");

        argument.getValue().registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.INITIAL_PLAN_COMPUTATION_SUCCESS);
        argument.getValue().registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.BROKER_SHUTDOWN_SUCCESS);

        BrokerRemovalStatus expectedStatus = new BrokerRemovalStatus(brokerId, BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
            null);

        assertEquals("Expected one removal status to be populated",
            1, kafkaDataBalanceManager.brokerRemovalsStatus.size());
        assertTrue("Expected the removed broker's removal status to be populated",
            kafkaDataBalanceManager.brokerRemovalsStatus.containsKey(brokerId));
        assertEquals(expectedStatus,
            kafkaDataBalanceManager.brokerRemovalsStatus.get(brokerId));
        assertEquals(BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_INITIATED.toString(),
            stateMetricSupplierCaptor.getValue().get());

        // 2. Test the listener updates the state again
        argument.getValue().registerEvent(
            BrokerRemovalCallback.BrokerRemovalEvent.PLAN_COMPUTATION_FAILURE,
            expectedListenerException);

        BrokerRemovalStatus expectedStatus2 = new BrokerRemovalStatus(brokerId, BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED,
            expectedListenerException);

        assertEquals("Expected one removal status to be populated",
            1, kafkaDataBalanceManager.brokerRemovalsStatus.size());
        assertTrue("Expected the removed broker's removal status to be populated",
            kafkaDataBalanceManager.brokerRemovalsStatus.containsKey(brokerId));
        assertEquals(expectedStatus2,
            kafkaDataBalanceManager.brokerRemovalsStatus.get(brokerId));
        assertEquals(BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_FAILED.toString(),
            stateMetricSupplierCaptor.getValue().get());
    }

    private void assertRemoveBrokerCalled(int brokerId,
                                          ArgumentCaptor<BrokerRemovalStateTracker> argumentCaptor,
                                          ArgumentCaptor<Supplier<String>> stateMetricCaptor) {
        KafkaDataBalanceManager kafkaDataBalanceManager = (KafkaDataBalanceManager) dataBalancer;
        dataBalancer.onElection();
        long brokerEpoch = 15L;
        Optional<Long> expectedOpt = Optional.of(brokerEpoch);

        assertTrue("Expected no broker removal statuses to be populated",
            kafkaDataBalanceManager.brokerRemovalsStatus.isEmpty());

        dataBalancer.scheduleBrokerRemoval(brokerId, Option.apply(brokerEpoch));

        verify(mockActiveDataBalanceEngine).removeBroker(eq(brokerId), eq(expectedOpt),
            argumentCaptor.capture(), any(String.class));

        verify(mockDbMetrics).newGauge(Mockito.eq(ConfluentDataBalanceEngine.class),
            Mockito.eq(BROKER_REMOVAL_STATE_METRIC_NAME), stateMetricCaptor.capture(),
            Mockito.eq(true),
            Mockito.eq(kafkaDataBalanceManager.brokerIdMetricTag(brokerId)));
    }

    /**
     * Check that we can remove broker that isn't alive.
     */
    @Test
    public void testRemoveNotAliveBroker() {
        dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection();

        Optional<Long> expectedOpt = Optional.empty();
        dataBalancer.scheduleBrokerRemoval(1, Option.empty());
        verify(mockActiveDataBalanceEngine).removeBroker(eq(1), eq(expectedOpt),
            any(BrokerRemovalStateTracker.class), any(String.class));
    }

    @Test
    public void testBrokerRemovals() {
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
            new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
            mockDbMetrics, time);

        assertEquals(Collections.emptyList(), dataBalancer.brokerRemovals());

        BrokerRemovalStatus broker1Status = new BrokerRemovalStatus(1,
            BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
            BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED,
            new Exception("Partition reassignment failed!")
        );
        dataBalancer.brokerRemovalsStatus.put(1, broker1Status);
        BrokerRemovalStatus broker2Status = new BrokerRemovalStatus(2,
            BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
            BrokerRemovalDescription.PartitionReassignmentsStatus.COMPLETE,
            null
        );
        dataBalancer.brokerRemovalsStatus.put(2, broker2Status);

        assertEquals(Arrays.asList(broker1Status, broker2Status), dataBalancer.brokerRemovals());
    }
}
