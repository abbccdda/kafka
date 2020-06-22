/*
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import io.confluent.databalancer.operation.BalanceOpExecutionCompletionCallback;
import io.confluent.databalancer.operation.BrokerRemovalStateTracker;
import io.confluent.databalancer.persistence.ApiStatePersistenceStore;
import kafka.controller.DataBalanceManager;
import kafka.metrics.KafkaYammerMetrics;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.utils.TestUtils;
import kafka.utils.TestUtils$;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.BalancerOfflineException;
import org.apache.kafka.common.errors.BrokerRemovalCanceledException;
import org.apache.kafka.common.utils.Time;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import scala.Option;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
    @Mock
    private ApiStatePersistenceStore mockPersistenceStore;
    @Mock
    private DataBalanceEngineContext mockDbeContext;
    @Captor
    private ArgumentCaptor<BalanceOpExecutionCompletionCallback> execCbCaptor;

    @Captor
    private ArgumentCaptor<EngineInitializationContext> initializationContext;

    private Map<Integer, BrokerRemovalStateTracker> stateTrackers;

    @Before
    public void setUp() {
        brokerProps = new Properties();
        brokerProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), TestUtils$.MODULE$.MockZkConnect());
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 200L);
        initConfig = new KafkaConfig(brokerProps);
        stateTrackers = new HashMap<>();

        MockitoAnnotations.initMocks(this);
        when(mockDataBalanceEngineFactory.getActiveDataBalanceEngine()).thenReturn(mockActiveDataBalanceEngine);
        when(mockDataBalanceEngineFactory.getInactiveDataBalanceEngine()).thenReturn(mockInactiveDataBalanceEngine);
        setupMockDbe();
    }

    private void setupMockDbe() {
        when(mockActiveDataBalanceEngine.isActive()).thenReturn(true);
        when(mockActiveDataBalanceEngine.getDataBalanceEngineContext()).thenReturn(mockDbeContext);
        when(mockDbeContext.getPersistenceStore()).thenReturn(mockPersistenceStore);
        when(mockDbeContext.getBrokerRemovalsStateTrackers()).thenReturn(stateTrackers);
    }

    @Test
    public void testUpdateConfigBalancerEnable() throws InterruptedException {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
        // Instantiate the Active DBE
        dataBalancer.onElection(Collections.emptyMap());

        dataBalancer.updateConfig(initConfig, updatedConfig);
        verify(mockActiveDataBalanceEngine).onDeactivation();

        // Now check to see that it handles the state change properly
        Mockito.reset(mockActiveDataBalanceEngine);
        dataBalancer.updateConfig(updatedConfig, initConfig);
        verify(mockActiveDataBalanceEngine).onActivation(initializationContext.capture());
        KafkaConfig receivedKafkaConfig = initializationContext.getValue().kafkaConfig;
        assertEquals(receivedKafkaConfig.toString(), initConfig, receivedKafkaConfig);

        dataBalancer.updateConfig(initConfig, initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
        // Since it's been active this whole time, the inactive DBE should never have been called
        verify(mockInactiveDataBalanceEngine, never()).onActivation(any(EngineInitializationContext.class));
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
        verify(mockInactiveDataBalanceEngine).onActivation(initializationContext.capture());
        KafkaConfig receivedKafkaConfig = initializationContext.getValue().kafkaConfig;
        assertEquals(receivedKafkaConfig.toString(), initConfig, receivedKafkaConfig);

        dataBalancer.updateConfig(initConfig, initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);

        // Since it's been inactive this whole time, the active DBE should never have been called
        verify(mockActiveDataBalanceEngine, never()).onActivation(any(EngineInitializationContext.class));
        verify(mockActiveDataBalanceEngine, never()).onDeactivation();

        verify(mockActiveDataBalanceEngine, never()).shutdown();
        verify(mockInactiveDataBalanceEngine, never()).shutdown();
    }

    @Test
    public void testUpdateConfigBalancerThrottle() {
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 100L);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());
        verify(mockActiveDataBalanceEngine).onActivation(initializationContext.capture());
        KafkaConfig receivedKafkaConfig = initializationContext.getValue().kafkaConfig;
        assertEquals(receivedKafkaConfig.toString(), initConfig, receivedKafkaConfig);

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
        dataBalancer.onElection(Collections.emptyMap());
        verify(mockActiveDataBalanceEngine).onActivation(initializationContext.capture());
        KafkaConfig receivedKafkaConfig = initializationContext.getValue().kafkaConfig;
        assertEquals(receivedKafkaConfig.toString(), initConfig, receivedKafkaConfig);

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
        dataBalancer.onElection(Collections.emptyMap());
        verify(mockActiveDataBalanceEngine).onActivation(initializationContext.capture());
        KafkaConfig receivedKafkaConfig = initializationContext.getValue().kafkaConfig;
        assertEquals(receivedKafkaConfig.toString(), initConfig, receivedKafkaConfig);

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
        dataBalancer.onElection(Collections.emptyMap());
        verify(mockActiveDataBalanceEngine).onActivation(initializationContext.capture());
        KafkaConfig receivedKafkaConfig = initializationContext.getValue().kafkaConfig;
        assertEquals(receivedKafkaConfig.toString(), initConfig, receivedKafkaConfig);

        // expect nothing to be updated
        dataBalancer.updateConfig(initConfig, updatedConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
    }

    @Test
    public void testEnableFromOff() {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        KafkaConfig disabledConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(disabledConfig, mockDataBalanceEngineFactory, mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());
        // We SHOULD NOT have attempted to launch CC
        verify(mockActiveDataBalanceEngine, never()).onActivation(initializationContext.capture());

        // Now update and enable
        dataBalancer.updateConfig(disabledConfig, initConfig);
        verify(mockActiveDataBalanceEngine).onActivation(initializationContext.capture());
        KafkaConfig receivedKafkaConfig = initializationContext.getValue().kafkaConfig;
        assertEquals(receivedKafkaConfig.toString(), initConfig, receivedKafkaConfig);
    }

    @Test
    public void testConfluentBalancerEnabledMetric() {
        MetricsRegistry metrics = KafkaYammerMetrics.defaultRegistry();

        cleanMetrics(metrics);
        DataBalancerMetricsRegistry dbMetricsRegistry = new DataBalancerMetricsRegistry(metrics,
                KafkaDataBalanceManager.getMetricsWhiteList());
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory, dbMetricsRegistry, time);
        dataBalancer.onElection(Collections.emptyMap());
        verifyMetricValue(metrics, KafkaDataBalanceManager.ACTIVE_BALANCER_COUNT_METRIC_NAME, 1);
        dataBalancer.onResignation();
        verifyMetricValue(metrics, KafkaDataBalanceManager.ACTIVE_BALANCER_COUNT_METRIC_NAME, 0);

        cleanMetrics(metrics);
    }

    private void verifyMetricValue(MetricsRegistry metricsRegistry, String metricSimpleName, Integer expectedValue) {
        Map<MetricName, Metric> metrics = metricsRegistry.allMetrics();
        // Note may throw if metric not present
        MetricName metricName = metrics.keySet().stream().filter(m -> m.getName().equals(metricSimpleName)).findFirst().get();

        assertEquals(1, metrics.keySet().stream().filter(m -> m.getName().equals(metricSimpleName)).count());

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
        dataBalancer.onElection(Collections.emptyMap());
        verify(mockActiveDataBalanceEngine).onActivation(initializationContext.capture());
        KafkaConfig receivedKafkaConfig = initializationContext.getValue().kafkaConfig;
        assertEquals(receivedKafkaConfig.toString(), initConfig, receivedKafkaConfig);

        // Since it's been active this whole time, the inactive DBE should never have been called
        verify(mockInactiveDataBalanceEngine, never()).onActivation(any(EngineInitializationContext.class));
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

        verify(mockActiveDataBalanceEngine, never()).onActivation(initializationContext.capture());
        verify(mockInactiveDataBalanceEngine, never()).onDeactivation();

        dataBalancer.shutdown();
        verify(mockActiveDataBalanceEngine, never()).onDeactivation();
        verify(mockInactiveDataBalanceEngine, never()).onDeactivation();

        verify(mockActiveDataBalanceEngine).shutdown();
        verify(mockInactiveDataBalanceEngine).shutdown();
    }

    @Test(expected = BalancerOfflineException.class)
    public void testRemoveBroker_NotActiveThrowsBalancerOfflineException() {
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
        int brokerId = 1;

        actAndAssertRemoveBrokerCalled(brokerId);
    }

    private void actAndAssertRemoveBrokerCalled(int brokerId) {
        dataBalancer.onElection(Collections.emptyMap());
        long brokerEpoch = 15L;
        Optional<Long> expectedOpt = Optional.of(brokerEpoch);

        dataBalancer.scheduleBrokerRemoval(brokerId, Option.apply(brokerEpoch));

        verify(mockActiveDataBalanceEngine).removeBroker(eq(brokerId), eq(expectedOpt), any(String.class));
    }

    /**
     * Check that we can remove broker that isn't alive.
     */
    @Test
    public void testRemoveNotAliveBroker() {
        dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());

        Optional<Long> expectedOpt = Optional.empty();
        dataBalancer.scheduleBrokerRemoval(1, Option.empty());
        verify(mockActiveDataBalanceEngine).removeBroker(eq(1), eq(expectedOpt), any(String.class));
    }

    @Test
    public void testBrokerRemovals() throws Exception {
        KafkaDataBalanceManager.DataBalanceEngineFactory dbeFactory =
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine);

        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig, dbeFactory,
                mockDbMetrics, time);
        dataBalancer.balanceEngine = mockActiveDataBalanceEngine;
        assertEquals(Collections.emptyList(), dataBalancer.brokerRemovals());
    }

    /**
     * Confirm that add clears pending work after successful completion
     */
    @Test
    public void testOnBrokersStartup_AddBroker_AddOfOneSucceeds() {
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());
        Set<Integer> newBrokers = new HashSet<>();
        newBrokers.add(10);
        dataBalancer.onBrokersStartup(newBrokers, newBrokers);
        verify(mockActiveDataBalanceEngine).addBrokers(eq(newBrokers), execCbCaptor.capture(), anyString());

        assertEquals("New brokers not present in DataBalancer", newBrokers, dataBalancer.brokersToAdd);

        execCbCaptor.getValue().accept(true, null);

        assertTrue("Expected brokersToAdd to be cleared", dataBalancer.brokersToAdd.isEmpty());
    }

    /**
     * Confirm that add that stops but doesn't actually fail is not considered done yet.
     */
    @Test
    public void testOnBrokersStartup_AddBroker_AddOfOnePauses() {
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());
        Set<Integer> newBrokers = new HashSet<>();
        newBrokers.add(10);
        dataBalancer.onBrokersStartup(newBrokers, newBrokers);
        verify(mockActiveDataBalanceEngine).addBrokers(eq(newBrokers), execCbCaptor.capture(), anyString());

        assertEquals("New brokers not present in DataBalancer", newBrokers, dataBalancer.brokersToAdd);

        execCbCaptor.getValue().accept(false, null);

        assertEquals("Expected brokersToAdd to not get cleared after unsuccessful completion", newBrokers, dataBalancer.brokersToAdd);
    }

    /**
     * Confirm that add clears pending work after an exception occurs
     */
    @Test
    public void testOnBrokersStartup_AddBroker_AddOfOneThrowsException() {
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());
        Set<Integer> newBrokers = new HashSet<>();
        newBrokers.add(10);
        dataBalancer.onBrokersStartup(newBrokers, newBrokers);
        verify(mockActiveDataBalanceEngine).addBrokers(eq(newBrokers), execCbCaptor.capture(), anyString());

        assertEquals("New brokers not present in DataBalancer", newBrokers, dataBalancer.brokersToAdd);

        // Signal an exception. Op failure clears the list.
        execCbCaptor.getValue().accept(false, new KafkaCruiseControlException("boom"));

        assertTrue("Expected brokersToAdd to be cleared", dataBalancer.brokersToAdd.isEmpty());
    }

    /**
     * Confirm that add clears pending work after successful completion
     */
    @Test
    public void testOnBrokersStartup_AddBroker_AddOfMultipleSucceeds() {
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());
        Set<Integer> newBrokers1 = new HashSet<>();
        newBrokers1.add(10);
        dataBalancer.onBrokersStartup(newBrokers1, newBrokers1);
        assertEquals("New brokers not present in DataBalancer", newBrokers1, dataBalancer.brokersToAdd);
        verify(mockActiveDataBalanceEngine).addBrokers(eq(newBrokers1), any(BalanceOpExecutionCompletionCallback.class), anyString());

        // Now two more brokers show, hello! All should be merged together.
        Set<Integer> newBrokers2 = Stream.of(11, 12).collect(Collectors.toSet());
        dataBalancer.onBrokersStartup(newBrokers2, newBrokers2);

        Set<Integer> expectedBrokerList2 = new HashSet<>(newBrokers1);
        expectedBrokerList2.addAll(newBrokers2);

        verify(mockActiveDataBalanceEngine).addBrokers(eq(expectedBrokerList2), execCbCaptor.capture(), anyString());
        assertEquals("New brokers not present in DataBalancer", expectedBrokerList2, dataBalancer.brokersToAdd);

        // Success!
        execCbCaptor.getValue().accept(true, null);
        assertTrue("Expected brokersToAdd to be cleared", dataBalancer.brokersToAdd.isEmpty());
    }

    /**
     * Confirm that add clears pending work after successful completion
     */
    @Test
    public void testOnBrokersStartup_AddBroker_AddOfMultipleSucceedsWithRace() {
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());
        Set<Integer> newBrokers1 = new HashSet<>();
        newBrokers1.add(10);
        dataBalancer.onBrokersStartup(newBrokers1, newBrokers1);
        assertEquals("New brokers not present in DataBalancer", newBrokers1, dataBalancer.brokersToAdd);
        verify(mockActiveDataBalanceEngine).addBrokers(eq(newBrokers1), execCbCaptor.capture(), anyString());

        // Now two more brokers show, hello! All should be merged together.
        Set<Integer> newBrokers2 = Stream.of(11, 12).collect(Collectors.toSet());
        dataBalancer.onBrokersStartup(newBrokers2, newBrokers2);

        Set<Integer> expectedBrokerList2 = new HashSet<>(newBrokers1);
        expectedBrokerList2.addAll(newBrokers2);

        verify(mockActiveDataBalanceEngine).addBrokers(eq(expectedBrokerList2), execCbCaptor.capture(), anyString());
        assertEquals("New brokers not present in DataBalancer", expectedBrokerList2, dataBalancer.brokersToAdd);

        // Success for the first op. There was a race in submitting, so it completed.
        execCbCaptor.getAllValues().get(0).accept(true, null);
        assertEquals("Expected second brokers to still be present", newBrokers2, dataBalancer.brokersToAdd);
        // The second one completes
        execCbCaptor.getAllValues().get(1).accept(true, null);
        assertTrue("Expected brokersToAdd to be cleared", dataBalancer.brokersToAdd.isEmpty());
    }

    @Test
    public void testOnBrokersStartup_DoesntTakeActionIfEngineInactive() {
        Set<Integer> emptyBrokers = new HashSet<>();
        emptyBrokers.add(1);
        Set<Integer> allNewBrokers = new HashSet<>();
        allNewBrokers.add(1);
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
            new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
            mockDbMetrics, time);
        BrokerRemovalStateTracker stateTracker = mock(BrokerRemovalStateTracker.class);

        when(mockActiveDataBalanceEngine.isActive()).thenReturn(false); // ensure its inactive

        // act
        dataBalancer.onBrokersStartup(emptyBrokers, allNewBrokers);

        verifyNoInteractions(stateTracker);
    }

    /**
     * Assert that ongoing broker removals are canceled only if they have passed the shutdown state
     */
    @Test
    public void testOnBrokersStartup_CancelsRemovalWhenShutdownStatePassed() {
        Set<Integer> emptyBrokers = new HashSet<>();
        emptyBrokers.add(1);
        Set<Integer> allNewBrokers = new HashSet<>();
        allNewBrokers.add(1);
        allNewBrokers.add(2);
        allNewBrokers.add(3);

        BrokerRemovalStateTracker mockStateTracker1 = mock(BrokerRemovalStateTracker.class);
        when(mockStateTracker1.cancel(any(), eq(true))).thenReturn(true);
        when(mockStateTracker1.brokerId()).thenReturn(1);
        BrokerRemovalStateTracker mockStateTracker3 = mock(BrokerRemovalStateTracker.class);
        when(mockStateTracker3.brokerId()).thenReturn(3);
        when(mockStateTracker3.cancel(any(), eq(true))).thenReturn(false);

        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
            new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
            mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());

        Map<Integer, BrokerRemovalStateTracker> stateTrackers = dataBalancer.balanceEngine.getDataBalanceEngineContext().getBrokerRemovalsStateTrackers();
        stateTrackers.put(1, mockStateTracker1);
        // 2 doesn't have a broker removal ongoing
        stateTrackers.put(3, mockStateTracker3);

        // act
        dataBalancer.onBrokersStartup(emptyBrokers, allNewBrokers);

        verify(mockStateTracker1).cancel(any(BrokerRemovalCanceledException.class), eq(true));
        verify(mockStateTracker3).cancel(any(BrokerRemovalCanceledException.class), eq(true));
        verify(mockActiveDataBalanceEngine).cancelBrokerRemoval(1);
        verify(mockActiveDataBalanceEngine, never()).cancelBrokerRemoval(3); // broker 3 should not be cancelled
        verify(mockActiveDataBalanceEngine).addBrokers(eq(emptyBrokers), any(BalanceOpExecutionCompletionCallback.class), anyString());
        assertEquals(0, stateTrackers.size()); // all state trackers should have been cleaned up
    }

    /**
     * Assert that new brokers arriving when none are empty results in a no-op addition.
     */
    @Test
    public void testOnBrokersStartup_DoesntAddNonEmptyBrokers() {
        Set<Integer> emptyBrokers = new HashSet<>();
        Set<Integer> allNewBrokers = new HashSet<>();
        allNewBrokers.add(1);
        allNewBrokers.add(2);
        allNewBrokers.add(3);

        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());

        // act
        dataBalancer.onBrokersStartup(emptyBrokers, allNewBrokers);

        verify(mockActiveDataBalanceEngine, never()).addBrokers(any(), any(BalanceOpExecutionCompletionCallback.class), anyString());
    }

    @Test
    public void testOnBrokersStartup_AddBroker_AddOfMultipleOnlyMergesEmpty() {
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                mockDbMetrics, time);
        dataBalancer.onElection(Collections.emptyMap());
        Set<Integer> newBrokers1 = new HashSet<>();
        newBrokers1.add(10);
        dataBalancer.onBrokersStartup(newBrokers1, newBrokers1);
        assertEquals("New brokers not present in DataBalancer", newBrokers1, dataBalancer.brokersToAdd);
        verify(mockActiveDataBalanceEngine).addBrokers(eq(newBrokers1), any(BalanceOpExecutionCompletionCallback.class), anyString());

        // Now two more brokers show, but non-empty. No further calls expected to the ActiveDataBalanceEngine.
        reset(mockActiveDataBalanceEngine);
        setupMockDbe();

        Set<Integer> newBrokers2 = Stream.of(11, 12).collect(Collectors.toSet());
        Set<Integer> emptyBrokers2 = new HashSet<>();
        dataBalancer.onBrokersStartup(emptyBrokers2, newBrokers2);
        verify(mockActiveDataBalanceEngine, never()).addBrokers(any(), any(BalanceOpExecutionCompletionCallback.class), anyString());

        // And some more brokers arrive, one of which is empty. We should merge.
        Set<Integer> newBrokers3 = Stream.of(13, 14).collect(Collectors.toSet());
        Set<Integer> emptyBrokers3 = new HashSet<>();
        emptyBrokers3.add(14);
        dataBalancer.onBrokersStartup(emptyBrokers3, newBrokers3);

        Set<Integer> expectedBrokerList3 = new HashSet<>(newBrokers1);
        expectedBrokerList3.addAll(emptyBrokers3);

        verify(mockActiveDataBalanceEngine).addBrokers(eq(expectedBrokerList3), any(BalanceOpExecutionCompletionCallback.class), anyString());
        assertEquals("New brokers not present in DataBalancer", expectedBrokerList3, dataBalancer.brokersToAdd);
    }

    @Test
    public void testAddMetric() {
        MetricsRegistry metrics = KafkaYammerMetrics.defaultRegistry();

        cleanMetrics(metrics);
        DataBalancerMetricsRegistry dbMetricsRegistry = new DataBalancerMetricsRegistry(metrics,
                KafkaDataBalanceManager.getMetricsWhiteList());
        KafkaDataBalanceManager dataBalancer = new KafkaDataBalanceManager(initConfig,
                new KafkaDataBalanceManager.DataBalanceEngineFactory(mockActiveDataBalanceEngine, mockInactiveDataBalanceEngine),
                dbMetricsRegistry, time);

        // No brokers adding on startup
        dataBalancer.onElection(Collections.emptyMap());
        verifyMetricValue(metrics, KafkaDataBalanceManager.BROKER_ADD_COUNT_METRIC_NAME, 0);

        // Submit an add, count should go up
        Set<Integer> newBrokers = new HashSet<>();
        newBrokers.add(10);
        dataBalancer.onBrokersStartup(newBrokers, newBrokers);

        verifyMetricValue(metrics, KafkaDataBalanceManager.BROKER_ADD_COUNT_METRIC_NAME, 1);

        verify(mockActiveDataBalanceEngine).addBrokers(eq(newBrokers), execCbCaptor.capture(), anyString());

        // Success! Count should go back to 0
        execCbCaptor.getValue().accept(true, null);
        verifyMetricValue(metrics, KafkaDataBalanceManager.BROKER_ADD_COUNT_METRIC_NAME, 0);
    }
}