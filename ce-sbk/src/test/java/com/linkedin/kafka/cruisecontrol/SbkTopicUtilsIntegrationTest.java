/*
 * Copyright (C) 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import kafka.log.LogConfig;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SbkTopicUtilsIntegrationTest extends CCKafkaIntegrationTestHarness {

    private static final String TEST_TOPIC = "test-topic";

    @Override
    public int clusterSize() {
        return 3;
    }

    @Before
    public void setUp() {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCheckTopicPropertiesMaybeCreate() throws Exception {
        Map<String, Object> configMap = new KafkaCruiseControlConfig(getTestConfig()).mergedConfigValues();
        KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                "TestUtilsGroup",
                "TestUtilsMetric",
                false);
        ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(KafkaCruiseControlUtils.filterAdminClientConfigs(configMap));
        try {
            SbkTopicUtils.SbkTopicConfig topicConfig = new SbkTopicUtils.SbkTopicConfigBuilder()
                    .setTopic(TEST_TOPIC)
                    .setCleanupPolicy(LogConfig.Delete())
                    .setMinRetentionTimeMs(60_000)
                    // These configs don't exist, so defaults will get used.
                    .setReplicationFactor(configMap, "ReplicationFactorConfig", (short) 3)
                    .setPartitionCount(configMap, "PartitionCountConfig", 5)
                    .build();

            // At this point, no topics should exist. First pass should fail, and instantiate the topics
            Assert.assertFalse(SbkTopicUtils.checkTopicPropertiesMaybeCreate(topicConfig, configMap));
            Set<String> topics = JavaConverters.setAsJavaSet(kafkaZkClient.getAllTopicsInCluster(false));
            Assert.assertTrue("All topics list does not contain recently added topic " + TEST_TOPIC,
                    topics.contains(TEST_TOPIC));
            // Topic creation is not instantaneous; wait for completion.
            TestUtils.waitForCondition(() -> adminClient.listTopics().names().get().contains(TEST_TOPIC),
                    30_000,
                    "Check " + TEST_TOPIC + " exists.");
        } finally {
            KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    @Test
    public void testIncreasePartitionCount() throws Exception {
        Map<String, Object> configMap = new KafkaCruiseControlConfig(getTestConfig()).mergedConfigValues();
        KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                "TestUtilsGroup",
                "TestUtilsMetric",
                false);
        AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
        ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(KafkaCruiseControlUtils.filterAdminClientConfigs(configMap));
        try {
            int startPartitionCount = 3;
            int newPartitionCount = 5;
            KafkaTestUtils.createTopic(adminClient, TEST_TOPIC, startPartitionCount, 3);

            Set<String> topicList = Collections.singleton(TEST_TOPIC);
            TopicDescription topicDescription = adminClient.describeTopics(topicList).values().get(TEST_TOPIC).get();
            SbkTopicUtils.maybeIncreaseTopicPartitionCount(
                    kafkaZkClient, adminZkClient, TEST_TOPIC, topicDescription, newPartitionCount);

            // Check partition count again, we should now have
            // Topic creation is not instantaneous; wait for completion.
            TestUtils.waitForCondition(() -> {
                        TopicDescription td = adminClient.describeTopics(topicList).values().get(TEST_TOPIC).get();
                        return td.partitions().size() == newPartitionCount;
                    },
                    30_000,
                    "Check " + TEST_TOPIC + " exists.");
        } finally {
            KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    private Map<String, String> getTestConfig() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().connectionString());
        configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG, "false");

        return configMap;
    }
}
