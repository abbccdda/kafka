/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import com.linkedin.kafka.cruisecontrol.SbkTopicUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import io.confluent.databalancer.StartupCheckInterruptedException;
import kafka.log.LogConfig;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * This class stores state of an SBK api (remove broker, add broker) as it executes
 * its state machine and transitions from one state to another. This is also used in
 * case of restart where the new Databalancer node picks up the task from its
 * unfinished state and completes it.
 */
public class ApiStatePersistenceStore {

    private static final int API_STATE_TOPIC_PARTITION_COUNT = 1;

    public static final String API_STATE_TOPIC_CLEANUP_POLICY = LogConfig.Compact();

    private static final Logger LOG = LoggerFactory.getLogger(ApiStatePersistenceStore.class);

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
            LOG.error("Error when checking for api state topic: {}", ex.getMessage());
            LOG.debug("Error: ", ex);
            return false;
        }
    }

    static SbkTopicUtils.SbkTopicConfig getTopicConfig(String topic, Map<String, ?> config) {
        return new SbkTopicUtils.SbkTopicConfigBuilder()
            .setTopic(topic)
            .setReplicationFactor(config, ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_CONFIG,
                    ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_DEFAULT)
            .setCleanupPolicy(API_STATE_TOPIC_CLEANUP_POLICY)
            .setPartitionCount(API_STATE_TOPIC_PARTITION_COUNT)
            .setMinRetentionTimeMs(-1) // -1 means log will not be deleted. Its ignored anyway for a compact topic
            .build();
    }

    static String getApiStatePersistenceStoreTopicName(Map<String, ?> config) {
        String apiStatePersistenceStoreTopic = (String) config.get(ConfluentConfigs.BALANCER_API_STATE_TOPIC_CONFIG);
        return apiStatePersistenceStoreTopic == null || apiStatePersistenceStoreTopic.isEmpty()
                ? ConfluentConfigs.BALANCER_API_STATE_TOPIC_DEFAULT
                : apiStatePersistenceStoreTopic;
    }
}
