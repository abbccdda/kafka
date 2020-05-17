/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test class for Topic creation/consumer/producer/checking in TestUtils class.
 */
public class SbkTopicUtilsTest {

    private static final String PARTITION_COUNT_CONFIG = "PartitionCount";
    private static final String REPLICATION_FACTOR_CONFIG = "ReplicationFactor";

    @Test
    public void testTopicConfigBuilderWithDefaultConfigs() {
        String expectedTopic = "test-topic";
        long expectedMinRetentionTimeMs = 20_0000;
        String expectedCleanupPolicy = "test";
        int defaultReplicationFactor = 5;
        int defaultPartitionCount = 50;

        Map<String, Object> config = new HashMap<>();

        SbkTopicUtils.SbkTopicConfigBuilder builder = new SbkTopicUtils.SbkTopicConfigBuilder();
        builder.setCleanupPolicy(expectedCleanupPolicy);
        builder.setMinRetentionTimeMs(expectedMinRetentionTimeMs);
        builder.setTopic(expectedTopic);

        builder.setPartitionCount(config, PARTITION_COUNT_CONFIG, defaultPartitionCount);
        builder.setReplicationFactor(config, REPLICATION_FACTOR_CONFIG, defaultReplicationFactor);

        SbkTopicUtils.SbkTopicConfig topicConfig = builder.build();

        Assert.assertEquals(expectedCleanupPolicy, topicConfig.cleanupPolicy);
        Assert.assertEquals(expectedMinRetentionTimeMs, topicConfig.minRetentionTimeMs);
        Assert.assertEquals(expectedTopic, topicConfig.topic);
        Assert.assertEquals(defaultPartitionCount, topicConfig.partitionCount);
        Assert.assertEquals(defaultReplicationFactor, topicConfig.replicationFactor);
    }

    @Test
    public void testTopicConfigBuilderWithNotDefaultConfigs() {
        String expectedTopic = "test-topic";
        long expectedMinRetentionTimeMs = 20_0000;
        String expectedCleanupPolicy = "test";
        int defaultReplicationFactor = 5;
        int expectedReplicationFactor = 3;
        int defaultPartitionCount = 50;
        int expectedPartitionCount = 30;

        Map<String, Object> config = new HashMap<>();
        config.put(PARTITION_COUNT_CONFIG, Integer.toString(expectedPartitionCount));
        config.put(REPLICATION_FACTOR_CONFIG, Integer.toString(expectedReplicationFactor));

        SbkTopicUtils.SbkTopicConfigBuilder builder = new SbkTopicUtils.SbkTopicConfigBuilder();
        builder.setCleanupPolicy(expectedCleanupPolicy);
        builder.setMinRetentionTimeMs(expectedMinRetentionTimeMs);
        builder.setTopic(expectedTopic);

        builder.setPartitionCount(config, PARTITION_COUNT_CONFIG, defaultPartitionCount);
        builder.setReplicationFactor(config, REPLICATION_FACTOR_CONFIG, defaultReplicationFactor);

        SbkTopicUtils.SbkTopicConfig topicConfig = builder.build();

        Assert.assertEquals(expectedCleanupPolicy, topicConfig.cleanupPolicy);
        Assert.assertEquals(expectedMinRetentionTimeMs, topicConfig.minRetentionTimeMs);
        Assert.assertEquals(expectedTopic, topicConfig.topic);
        Assert.assertEquals(expectedPartitionCount, topicConfig.partitionCount);
        Assert.assertEquals(expectedReplicationFactor, topicConfig.replicationFactor);
    }
}
