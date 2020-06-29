/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.SbkTopicUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import io.confluent.databalancer.StartupCheckInterruptedException;
import io.confluent.databalancer.record.ApiStatus;
import io.confluent.databalancer.record.ApiStatus.ApiStatusKey;
import io.confluent.databalancer.record.ApiStatus.ApiStatusMessage;
import io.confluent.databalancer.record.RemoveBroker.BrokerRemovalStateRecordProto;
import io.confluent.serializers.ProtoSerde;
import kafka.common.BrokerAddStatus;
import kafka.log.LogConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class stores state of an SBK API (remove broker, add broker) as it executes
 * its state machine and transitions from one state to another. This is also used in
 * case of restart where the new Databalancer node picks up the task from its
 * unfinished state and completes it.
 */
public class ApiStatePersistenceStore implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ApiStatePersistenceStore.class);

    private static final int API_STATE_TOPIC_PARTITION_COUNT = 1;
    public static final String API_STATE_TOPIC_CLEANUP_POLICY = LogConfig.Compact();

    private static final long READ_TO_END_TIMEOUT_MS = 30_000;

    // -1 as retention time means log will not be deleted. Its ignored anyway for a compact topic
    private static final int MIN_RETENTION_TIME_MS = -1;

    private KafkaBasedLog<ApiStatusKey, ApiStatusMessage> apiStatePersistenceLog;
    private String topic;
    private Map<Integer, BrokerRemovalStateRecord> brokerRemovalStateRecordMap = new ConcurrentHashMap<>();
    private Map<Integer, BrokerAddStatus> brokerAddStatusMap = new ConcurrentHashMap<>();
    private Map<String, Object> baseClientProperties;

    public ApiStatePersistenceStore(KafkaConfig config, Time time, Map<String, Object> clientProperties) {
        init(config, time, clientProperties);
    }

    public void init(KafkaConfig config, Time time, Map<String, Object> clientProperties) {
        this.baseClientProperties = clientProperties;
        this.topic = getApiStatePersistenceStoreTopicName(config);
        this.apiStatePersistenceLog = setupAndCreateKafkaBasedLog(config, time);

        // Start the log that reads log from the very beginning and invokes the read callback
        // This is used to catch up to latest state and read all stored api status records.
        apiStatePersistenceLog.start();
        LOG.info("Started Datablancer Api State Persistence Store");
    }

    // Visible For Testing
    KafkaBasedLog<ApiStatusKey, ApiStatusMessage> setupAndCreateKafkaBasedLog(KafkaConfig config, Time time) {
        Map<String, Object> producerProps = getProducerConfig(config);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SbkApiStatusKeySerde.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SbkApiStatusMessageSerde.class.getName());

        Map<String, Object> consumerProps = getConsumerConfig(config);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, SbkApiStatusKeySerde.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SbkApiStatusMessageSerde.class.getName());

        return createKafkaBasedLog(producerProps, consumerProps, time);
    }

    KafkaBasedLog<ApiStatusKey, ApiStatusMessage> createKafkaBasedLog(Map<String, Object> producerProps,
            Map<String, Object> consumerProps,
            Time time) {
        return new KafkaBasedLog<>(
                topic,
                producerProps,
                consumerProps,
                new ConsumeCallback(),
                time,
                null // Runnable to create topic, we have already taken care of this
        );
    }

    @Override
    public void close() {
        apiStatePersistenceLog.stop();
    }

    /**
     * Save api status passed in as argument {@code removalStatus} to persistence store. If {@code isNew}
     * is set to true, the "startTime" field will be set to current time. This is the case when we acknowledge
     * the api request to process. Any update to api status will then onward will have this field set and the
     * {@code isNew} flag should be false.
     *
     * After adding the {@code removalStatus} to topic, the method flushes the producer and then reads it back,
     * which may block upto {@link #READ_TO_END_TIMEOUT_MS}.
     */
    public void save(BrokerRemovalStateRecord removalStateRecord, boolean isNew) throws InterruptedException {
        ApiStatusKey key = ApiStatusKey.newBuilder()
                .setBrokerId(removalStateRecord.brokerId())
                .setConfigType(ApiStatus.ApiType.REMOVE_BROKER)
                .build();

        String error = "";
        if (removalStateRecord.exception() != null) {
            error = serializeException(removalStateRecord.exception());
        }

        long now = System.currentTimeMillis();
        BrokerRemovalStateRecordProto.Builder removeBrokerStatus = BrokerRemovalStateRecordProto.newBuilder()
                .setVersion(1)
                .setBrokerId(removalStateRecord.brokerId())
                .setError(error)
                .setRemovalState(BrokerRemovalStateSerializer.serialize(removalStateRecord.state()))
                .setLastUpdateTime(now);
        if (isNew) {
            if (removalStateRecord.startTime() > 0) {
                LOG.error("Start time already set for a new Broker removal status: {}",
                        removalStateRecord.startTime(),
                        new RuntimeException() // This is to print the stack that caused this bug
                );
            }
            removeBrokerStatus.setStartTime(now);
        } else {
            if (removalStateRecord.startTime() == 0) {
                LOG.error("Start time should be set for an existing Broker removal status. Broker id: {}",
                        removalStateRecord.brokerId(),
                        new RuntimeException() // This is to print the stack that caused this bug
                );
            }
            removeBrokerStatus.setStartTime(removalStateRecord.startTime());
        }

        ApiStatusMessage message = ApiStatusMessage.newBuilder()
                .setRemoveBrokerStatus(removeBrokerStatus.build())
                .build();
        apiStatePersistenceLog.send(key, message);

        try {
            // This will flush producer and read recently written data
            apiStatePersistenceLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // Reflect timestamp in passed in parameter broker status object
            removalStateRecord.setStartTime(removeBrokerStatus.getStartTime());
            removalStateRecord.setLastUpdateTime(now);
        } catch (ExecutionException | TimeoutException e) {
            LOG.error("Error when writing api status to Kafka.", e);
            throw new RuntimeException("Error when writing api status to Kafka.", e);
        }
    }

    static String serializeException(Exception ex) {
        String error;
        try {
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
                objectOutputStream.writeObject(ex);
                error = Base64.getEncoder().encodeToString(outputStream.toByteArray());
            }
        } catch (IOException e) {
            LOG.error("Unable to serialize exception.", ex);
            throw new RuntimeException("Error while serializing exception: " + ex, e);
        }
        return error;
    }

    static Exception deserializeException(String serializedException) {
        byte[] decodedSerializedException = Base64.getDecoder().decode(serializedException);
        try {
            try (ObjectInputStream ois = new ObjectInputStream(
                    new ByteArrayInputStream(decodedSerializedException))) {
                return (Exception) ois.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("Unable to deserialize exception: " + serializedException, e);
        }
        return null;
    }

    public BrokerRemovalStateRecord getBrokerRemovalStateRecord(int brokerId) {
        return brokerRemovalStateRecordMap.get(brokerId);
    }

    public Map<Integer, BrokerRemovalStateRecord> getAllBrokerRemovalStateRecords() {
        return Collections.unmodifiableMap(brokerRemovalStateRecordMap);
    }

    public void addBrokerRemovalStateRecord(BrokerRemovalStateRecord stateRecord) {
        brokerRemovalStateRecordMap.put(stateRecord.brokerId(), stateRecord);
    }

    public BrokerAddStatus getBrokerAddStatus(int brokerId) {
        return brokerAddStatusMap.get(brokerId);
    }

    public Map<Integer, BrokerAddStatus> getAllBrokerAddStatus() {
        return Collections.unmodifiableMap(brokerAddStatusMap);
    }

    public void addBrokerAddStatus(BrokerAddStatus status) {
        brokerAddStatusMap.put(status.brokerId(), status);
    }

    // Visible for testing
    public static class SbkApiStatusKeySerde extends ProtoSerde<ApiStatusKey> {
        public SbkApiStatusKeySerde() {
            super(ApiStatusKey.getDefaultInstance());
        }
    }

    // Visible for testing
    public static class SbkApiStatusMessageSerde extends ProtoSerde<ApiStatusMessage> {
        public SbkApiStatusMessageSerde() {
            super(ApiStatusMessage.getDefaultInstance());
        }
    }

    private class ConsumeCallback implements
            Callback<ConsumerRecord<ApiStatusKey, ApiStatusMessage>> {

        @Override
        public void onCompletion(Throwable error, ConsumerRecord<ApiStatusKey, ApiStatusMessage> record) {

            if (error != null) {
                LOG.error("Error when saving record. Broker Id: {}, API type: {} ",
                        record.key().getBrokerId(),
                        record.key().getConfigType());
                if (record.key().getConfigType() == ApiStatus.ApiType.REMOVE_BROKER) {
                    LOG.error("state: {}, start time: {}, last update time: {}",
                            record.value().getRemoveBrokerStatus().getRemovalState(),
                            record.value().getRemoveBrokerStatus().getStartTime(),
                            record.value().getRemoveBrokerStatus().getLastUpdateTime());
                } else if (record.key().getConfigType() == ApiStatus.ApiType.ADD_BROKER) {
                    LOG.error("start time: {}, last update time: {}",
                            record.value().getAddBrokerStatus().getStartTime(),
                            record.value().getAddBrokerStatus().getLastUpdateTime());
                }
                LOG.error("Unexpected error in consumer callback for ApiStatePersistenceStore: ", error);
                return;
            }

            if (record.key().getConfigType() == ApiStatus.ApiType.REMOVE_BROKER) {
                BrokerRemovalStateRecordProto removeBrokerStatus = record.value().getRemoveBrokerStatus();

                Exception ex = null;
                String serializedException = removeBrokerStatus.getError();
                if (!serializedException.isEmpty()) {
                    ex = deserializeException(serializedException);
                }
                BrokerRemovalStateRecord status = new BrokerRemovalStateRecord(removeBrokerStatus.getBrokerId(),
                        BrokerRemovalStateSerializer.deserialize(removeBrokerStatus.getRemovalState()),
                        ex);
                status.setStartTime(removeBrokerStatus.getStartTime());
                status.setLastUpdateTime(removeBrokerStatus.getLastUpdateTime());

                addBrokerRemovalStateRecord(status);
            } else {
                LOG.error("Invalid ApiType: {}", record.key().getConfigType());
            }
        }
    }

    /**
     * Make sure any condition needed to start this {@code CruiseControlComponent} is satisfied.
     */
    public static void checkStartupCondition(KafkaCruiseControlConfig config,
                                             Semaphore abortStartupCheck) {
        Map<String, Object> configPairs = config.mergedConfigValues();

        String topic = getApiStatePersistenceStoreTopicName(configPairs);
        SbkTopicUtils.SbkTopicConfig topicConfig = getTopicConfig(topic, configPairs);

        long maxTimeoutSec = 60;
        long currentTimeoutInSec = 1;
        while (!checkTopicCreated(configPairs, topicConfig)) {
            LOG.info("Waiting for {} seconds to ensure that api persistent store topic is created/exists.",
                    currentTimeoutInSec);
            try {
                if (abortStartupCheck.tryAcquire(currentTimeoutInSec, TimeUnit.SECONDS)) {
                    throw new StartupCheckInterruptedException();
                }
            } catch (InterruptedException e) {
                throw new StartupCheckInterruptedException(e);
            }
            currentTimeoutInSec = Math.min(2 * currentTimeoutInSec, maxTimeoutSec);
        }

        LOG.info("Confirmed that topic {} exists.", topic);
    }

    /**
     * Check if api state topic is already present, if not create it.
     *
     * @return "true" if topic exists, "false" otherwise.
     */
    static boolean checkTopicCreated(Map<String, ?> config, SbkTopicUtils.SbkTopicConfig topicConfig) {
        try {
            return SbkTopicUtils.checkTopicPropertiesMaybeCreate(topicConfig, config);
        } catch (Exception ex) {
            LOG.error("Error when checking for api state topics.", ex);
            return false;
        }
    }

    static SbkTopicUtils.SbkTopicConfig getTopicConfig(String topic, Map<String, ?> config) {
        return new SbkTopicUtils.SbkTopicConfigBuilder()
            .setTopic(topic)
            .setReplicationFactor(config, ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_SBK_CONFIG,
                    ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_DEFAULT)
            .setCleanupPolicy(API_STATE_TOPIC_CLEANUP_POLICY)
            .setPartitionCount(API_STATE_TOPIC_PARTITION_COUNT)
            .setMinRetentionTimeMs(MIN_RETENTION_TIME_MS)
            .build();
    }

    private Map<String, Object> getProducerConfig(KafkaConfig config) {
        Map<String, Object> producerConfigs = getClientConfig(config, ConfluentConfigs.ClientType.PRODUCER);
        return KafkaCruiseControlUtils.filterProducerConfigs(producerConfigs);
    }

    private Map<String, Object> getConsumerConfig(KafkaConfig config) {
        Map<String, Object> consumerConfigs = getClientConfig(config, ConfluentConfigs.ClientType.CONSUMER);
        return KafkaCruiseControlUtils.filterConsumerConfigs(consumerConfigs);
    }

    private Map<String, Object> getClientConfig(KafkaConfig config, ConfluentConfigs.ClientType clientType) {
        Map<String, Object> configs = new HashMap<>(baseClientProperties);

        Map<String, Object> clientConfigs = ConfluentConfigs.clientConfigs(config,
            ConfluentConfigs.CONFLUENT_BALANCER_PREFIX,
            clientType,
            topic,
            String.valueOf(config.brokerId()));

        configs.putAll(clientConfigs);

        return configs;
    }

    // VisibleForTesting
    static String getApiStatePersistenceStoreTopicName(Map<String, ?> config) {
        String topicFromConfig = (String) config.get(ConfluentConfigs.BALANCER_API_STATE_TOPIC_CONFIG);
        return getApiStatePersistenceStoreTopicName(topicFromConfig);
    }

    // VisibleForTesting
    static String getApiStatePersistenceStoreTopicName(KafkaConfig config) {
        String topicFromConfig = (String) config.get(ConfluentConfigs.BALANCER_API_STATE_TOPIC_CONFIG);
        return getApiStatePersistenceStoreTopicName(topicFromConfig);
    }

    private static String getApiStatePersistenceStoreTopicName(String topicFromConfig) {
        return topicFromConfig == null || topicFromConfig.isEmpty()
                ? ConfluentConfigs.BALANCER_API_STATE_TOPIC_DEFAULT : topicFromConfig;
    }
}
