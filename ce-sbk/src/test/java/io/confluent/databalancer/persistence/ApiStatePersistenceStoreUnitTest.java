/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import io.confluent.databalancer.record.ApiStatus;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit test to test ApiPersistenceStore methods. As opposed to {@link ApiStatePersistenceStoreTest}
 * this class uses mocks to abstract away external components.
 */
@RunWith(MockitoJUnitRunner.class)
public class ApiStatePersistenceStoreUnitTest {

    private static final String API_STATE_PERSISTENCE_STORE = "_ApiStatePersistenceTestStore";
    private static final String BOOTSTRAP_SERVERS = "broker1:9092,broker2:9093";
    private static final String ZK_CONNECTION_PROPERTY = "localhost:2181";

    @Mock
    private ApiStatePersistenceStore persistenceStore;
    @Mock
    private KafkaBasedLog<ApiStatus.ApiStatusKey, ApiStatus.ApiStatusMessage> persistenceStoreLog;

    private MockTime time = new MockTime();

    /**
     * Test if no topic name is specified we use the default topic name.
     */
    @Test
    public void testDefaultTopic() {
        Map<String, String> config = getConfig();
        config.remove(ConfluentConfigs.BALANCER_API_STATE_TOPIC_CONFIG);

        String expectedTopic = ConfluentConfigs.BALANCER_API_STATE_TOPIC_DEFAULT;
        String topic = ApiStatePersistenceStore.getApiStatePersistenceStoreTopicName(config);
        Assert.assertEquals("Topic name " + topic + " does not match expected topic: " + expectedTopic,
                expectedTopic, topic);

        KafkaConfig kafkaConfig = new KafkaConfig(config);
        topic = ApiStatePersistenceStore.getApiStatePersistenceStoreTopicName(kafkaConfig);
        Assert.assertEquals("Topic name " + topic + " does not match expected topic: " + expectedTopic,
                expectedTopic, topic);
    }

    /**
     * Test we get the topic name that is set in the config.
     */
    @Test
    public void testTopicFromConfig() {
        Map<String, String> config = getConfig();

        String expectedTopic = API_STATE_PERSISTENCE_STORE;
        String topic = ApiStatePersistenceStore.getApiStatePersistenceStoreTopicName(config);
        Assert.assertEquals("Topic name " + topic + " does not match expected topic: " + expectedTopic,
                expectedTopic, topic);

        KafkaConfig kafkaConfig = getKafkaConfig();
        topic = ApiStatePersistenceStore.getApiStatePersistenceStoreTopicName(kafkaConfig);
        Assert.assertEquals("Topic name " + topic + " does not match expected topic: " + expectedTopic,
                expectedTopic, topic);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProducerConfig() {
        // Setup mocks
        Mockito.doCallRealMethod().when(persistenceStore)
                .init(Mockito.any(KafkaConfig.class), Mockito.any(Time.class), Mockito.any(Map.class));
        Mockito.doCallRealMethod().when(persistenceStore)
                .setupAndCreateKafkaBasedLog(Mockito.any(KafkaConfig.class), Mockito.any(Time.class));

        ArgumentCaptor<Map<String, Object>> producerProperties = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, Object>> consumerProperties = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Time> timeCaptor = ArgumentCaptor.forClass(Time.class);

        Mockito.doReturn(persistenceStoreLog).when(persistenceStore).createKafkaBasedLog(producerProperties.capture(),
                consumerProperties.capture(),
                timeCaptor.capture());
        Mockito.doNothing().when(persistenceStoreLog).start();

        Map<String, Object> baseClientConfigs = new HashMap<>();
        baseClientConfigs.put("baseKey", "baseValue");

        // Call method to test
        KafkaConfig kafkaConfig = getKafkaConfig();
        persistenceStore.init(kafkaConfig, time, baseClientConfigs);

        // Validate if producer config is created correctly
        Assert.assertEquals(time, timeCaptor.getValue());
        Map<String, Object> producerConfigs = producerProperties.getValue();

        Assert.assertEquals(BOOTSTRAP_SERVERS, producerConfigs.get("bootstrap.servers"));
        Assert.assertEquals(ApiStatePersistenceStore.SbkApiStatusKeySerde.class.getName(),
                producerConfigs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        Assert.assertEquals(ApiStatePersistenceStore.SbkApiStatusMessageSerde.class.getName(),
                producerConfigs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        Assert.assertEquals(API_STATE_PERSISTENCE_STORE + "-producer--1",
                producerConfigs.get(CommonClientConfigs.CLIENT_ID_CONFIG));
        Assert.assertEquals("baseValue",
                producerConfigs.get("baseKey"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConsumerConfig() {
        // Setup mocks
        Mockito.doCallRealMethod().when(persistenceStore)
                .init(Mockito.any(KafkaConfig.class), Mockito.any(Time.class), Mockito.any(Map.class));
        Mockito.doCallRealMethod().when(persistenceStore)
                .setupAndCreateKafkaBasedLog(Mockito.any(KafkaConfig.class), Mockito.any(Time.class));

        ArgumentCaptor<Map<String, Object>> producerProperties = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, Object>> consumerProperties = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Time> timeCaptor = ArgumentCaptor.forClass(Time.class);

        Mockito.doReturn(persistenceStoreLog).when(persistenceStore).createKafkaBasedLog(producerProperties.capture(),
                consumerProperties.capture(),
                timeCaptor.capture());
        Mockito.doNothing().when(persistenceStoreLog).start();

        Map<String, Object> baseClientConfigs = new HashMap<>();
        baseClientConfigs.put("baseKey", "baseValue");

        // Call method to test
        KafkaConfig kafkaConfig = getKafkaConfig();
        persistenceStore.init(kafkaConfig, time, baseClientConfigs);

        // Validate if producer config is created correctly
        Assert.assertEquals(time, timeCaptor.getValue());
        Map<String, Object> consumerConfigs = consumerProperties.getValue();

        Assert.assertEquals(BOOTSTRAP_SERVERS, consumerConfigs.get("bootstrap.servers"));
        Assert.assertEquals(ApiStatePersistenceStore.SbkApiStatusKeySerde.class.getName(),
                consumerConfigs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        Assert.assertEquals(ApiStatePersistenceStore.SbkApiStatusMessageSerde.class.getName(),
                consumerConfigs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        Assert.assertEquals(API_STATE_PERSISTENCE_STORE + "-consumer--1",
                consumerConfigs.get(CommonClientConfigs.CLIENT_ID_CONFIG));
        Assert.assertEquals("baseValue",
                consumerConfigs.get("baseKey"));
    }

    @Test
    public void testExceptionSerialization() {
        String msg = "A window of opportunity.";
        NotEnoughValidWindowsException nevwe = new NotEnoughValidWindowsException(msg);

        NotEnoughValidWindowsException deserializedEx = (NotEnoughValidWindowsException) ApiStatePersistenceStore.deserializeException(
                ApiStatePersistenceStore.serializeException(nevwe));
        Assert.assertNotNull("Deserialized exception is null.", deserializedEx);
        Assert.assertEquals(msg, deserializedEx.getMessage());
    }

    private KafkaConfig getKafkaConfig() {
        return new KafkaConfig(getConfig());
    }

    private Map<String, String> getConfig() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(ConfluentConfigs.BALANCER_API_STATE_TOPIC_CONFIG, API_STATE_PERSISTENCE_STORE);
        configMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, ZK_CONNECTION_PROPERTY);
        configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG, "false");

        return configMap;
    }
}
