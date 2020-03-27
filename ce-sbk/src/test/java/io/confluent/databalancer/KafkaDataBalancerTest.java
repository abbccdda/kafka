/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import kafka.controller.DataBalancer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Properties;

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
        brokerProps.put(KafkaConfig$.MODULE$.ZkConnectProp(), kafka.utils.TestUtils.MockZkConnect());
        brokerProps.put(ConfluentConfigs.BALANCER_ENABLE_CONFIG, true);
        brokerProps.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 200L);
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
}
