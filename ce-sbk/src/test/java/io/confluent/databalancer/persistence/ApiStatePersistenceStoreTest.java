/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.SbkTopicUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

@Category(IntegrationTest.class)
public class ApiStatePersistenceStoreTest extends CCKafkaIntegrationTestHarness {

    private static final String API_STATE_TOPIC = "_ApiStatePersistenceTestStore";

    @Rule
    final public Timeout globalTimeout = Timeout.millis(60_000);

    @Override
    public int clusterSize() {
        return 3;
    }

    @Before
    public void initialize() {
        super.setUp();
    }

    @After
    public void cleanup() {
        super.tearDown();
    }

    @Test
    public void testCheckStartupCondition() throws Exception {

        ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(
                KafkaCruiseControlUtils.filterAdminClientConfigs(getConfig()));

        try {
            KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getConfig());
            Semaphore startupAbortSemaphore = new Semaphore(0);
            ApiStatePersistenceStore.checkStartupCondition(config, startupAbortSemaphore);
            Set<String> topics = adminClient.listTopics().names().get();
            Assert.assertTrue(topics.contains(API_STATE_TOPIC));
        } finally {
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    @Test
    public void testCheckTopicCreated() throws Exception {
        ConfluentAdmin adminClient = KafkaCruiseControlUtils.createAdmin(
                KafkaCruiseControlUtils.filterAdminClientConfigs(getConfig()));
        try {
            Map<String, Object> configMap = new KafkaCruiseControlConfig(getConfig()).mergedConfigValues();
            String topic = ApiStatePersistenceStore.getApiStatePersistenceStoreTopicName(configMap);
            SbkTopicUtils.SbkTopicConfig topicConfig = ApiStatePersistenceStore.getTopicConfig(topic, configMap);

            // At this point, no topics should exist
            // First pass should fail, and instantiate the topics
            Assert.assertFalse(ApiStatePersistenceStore.checkTopicCreated(configMap, topicConfig));
            Set<String> topics = adminClient.listTopics().names().get();
            Assert.assertTrue(topics.contains(API_STATE_TOPIC));
            // Topic creation is not instantaneous; wait for completion.
            // This is rather yucky but the test doesn't control the topic creation components.
            TestUtils.waitForCondition(() -> ApiStatePersistenceStore.checkTopicCreated(configMap, topicConfig),
                    30_000,
                    "Check " + API_STATE_TOPIC + " exists.");
        } finally {
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    private Map<String, String> getConfig() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(ConfluentConfigs.BALANCER_API_STATE_TOPIC_CONFIG, API_STATE_TOPIC);
        configMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeper().connectionString());
        configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG, "false");

        return configMap;
    }
}
