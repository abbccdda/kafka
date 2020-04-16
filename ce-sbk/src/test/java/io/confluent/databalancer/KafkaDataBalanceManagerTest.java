/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import kafka.controller.DataBalanceManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.utils.TestUtils$;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
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

    @Before
    public void setUp() {
        brokerProps = new Properties();
        brokerProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), TestUtils$.MODULE$.MockZkConnect());
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 200L);
        initConfig = new KafkaConfig(brokerProps);
        MockitoAnnotations.initMocks(this);
        when(mockDataBalanceEngineFactory.makeActiveDataBalanceEngine()).thenReturn(mockActiveDataBalanceEngine);
        when(mockDataBalanceEngineFactory.makeInactiveDataBalanceEngine()).thenReturn(mockInactiveDataBalanceEngine);
    }

    @Test
    public void testUpdateConfigBalancerEnable() {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory);
        // Instantiate the Active DBE
        dataBalancer.startUp();

        dataBalancer.updateConfig(updatedConfig);
        verify(mockActiveDataBalanceEngine).shutdown();

        // Now check to see that it handles the state change properly
        Mockito.reset(mockActiveDataBalanceEngine);
        dataBalancer.updateConfig(initConfig);
        verify(mockActiveDataBalanceEngine).startUp(initConfig);

        dataBalancer.updateConfig(initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
        // Since it's been active this whole time, the inactive DBE should never have been called
        verify(mockInactiveDataBalanceEngine, never()).startUp(any(KafkaConfig.class));
        verify(mockInactiveDataBalanceEngine, never()).shutdown();
    }

    @Test
    public void testUpdateConfigBalancerEnableOnNonEligibleNode() {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory);

        dataBalancer.updateConfig(updatedConfig);
        verify(mockInactiveDataBalanceEngine).shutdown();

        // Now check to see that it handles the state change properly
        Mockito.reset(mockActiveDataBalanceEngine);
        dataBalancer.updateConfig(initConfig);
        verify(mockInactiveDataBalanceEngine).startUp(initConfig);

        dataBalancer.updateConfig(initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
        
        // Since it's been inactive this whole time, the active DBE should never have been called
        verify(mockActiveDataBalanceEngine, never()).startUp(any(KafkaConfig.class));
        verify(mockActiveDataBalanceEngine, never()).shutdown();
    }

    @Test
    public void testUpdateConfigBalancerThrottle() {
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 100L);
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory);
        dataBalancer.startUp();
        verify(mockActiveDataBalanceEngine).startUp(initConfig);

        dataBalancer.updateConfig(updatedConfig);
        verify(mockActiveDataBalanceEngine).updateThrottle(100L);

        dataBalancer.updateConfig(initConfig);
        verify(mockActiveDataBalanceEngine).updateThrottle(200L);

        dataBalancer.updateConfig(initConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
    }

    @Test
    public void testUpdateConfigNoPropsUpdated() {
        updatedConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(initConfig, mockDataBalanceEngineFactory);
        dataBalancer.startUp();
        verify(mockActiveDataBalanceEngine).startUp(initConfig);

        // expect nothing to be updated
        dataBalancer.updateConfig(updatedConfig);
        verifyNoMoreInteractions(mockActiveDataBalanceEngine);
    }

    @Test
    public void testEnableFromOff() {
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, false);
        KafkaConfig disabledConfig = new KafkaConfig(brokerProps);
        dataBalancer = new KafkaDataBalanceManager(disabledConfig, mockDataBalanceEngineFactory);
        dataBalancer.startUp();
        // We SHOULD NOT have attempted to launch CC
        verify(mockActiveDataBalanceEngine, never()).startUp(any(KafkaConfig.class));

        // Now update and enable
        dataBalancer.updateConfig(initConfig);
        verify(mockActiveDataBalanceEngine).startUp(initConfig);
    }

}
