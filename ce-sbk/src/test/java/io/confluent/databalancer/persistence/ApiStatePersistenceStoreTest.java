/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import com.linkedin.cruisecontrol.exception.CruiseControlException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.SbkTopicUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import kafka.common.BrokerRemovalStatus;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Semaphore;

/**
 * Integration test for ApiPersistenceStore. Instantiates embedded ZK and embedded
 * kafka server and uses them to test.
 */
@Category(IntegrationTest.class)
public class ApiStatePersistenceStoreTest {

    private static final String API_STATE_TOPIC = "_ApiStatePersistenceTestStore";
    private static final long TEST_TIMEOUT = 60_000;

    private EmbeddedKafkaCluster kafkaCluster;

    @Rule
    final public Timeout globalTimeout = Timeout.millis(TEST_TIMEOUT);

    private MockTime time = new MockTime();

    protected int numBrokers() {
        return 3;
    }

    @Before
    public void setUp() {
        kafkaCluster = new EmbeddedKafkaCluster();
        kafkaCluster.startZooKeeper();
        kafkaCluster.startBrokers(numBrokers(), new Properties());
    }

    @After
    public void tearDown() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
    }

    /**
     * Produce one remove broker api status and read it back.
     */
    @Test
    public void testProduceConsumeOneRecord() throws Exception {
        KafkaConfig config = getKafkaConfig();
        try (ApiStatePersistenceStore store = new ApiStatePersistenceStore(config, time, Collections.emptyMap())) {
            int brokerId = 1;
            BrokerRemovalDescription.BrokerShutdownStatus bssStatus = BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE;
            BrokerRemovalDescription.PartitionReassignmentsStatus parStatus = BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS;
            String errorMessage = "test message";
            CruiseControlException error = new CruiseControlException(errorMessage);
            BrokerRemovalStatus status = new BrokerRemovalStatus(brokerId, bssStatus, parStatus, error);
            store.save(status, true);

            BrokerRemovalStatus brokerRemovalStatus = store.getBrokerRemovalStatus(brokerId);
            Assert.assertEquals(status, brokerRemovalStatus);

            CruiseControlException ex = (CruiseControlException) brokerRemovalStatus.exception();
            Assert.assertEquals(errorMessage, ex.getMessage());

            Assert.assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE, brokerRemovalStatus.brokerShutdownStatus());
            Assert.assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS, brokerRemovalStatus.partitionReassignmentsStatus());

            Assert.assertTrue("Start time should be set.", brokerRemovalStatus.getStartTime() > 0);
            Assert.assertTrue("Last update time should be set", brokerRemovalStatus.getLastUpdateTime() > 0);
            Assert.assertEquals(brokerRemovalStatus.getStartTime(), brokerRemovalStatus.getLastUpdateTime());
        }
    }

    @Test
    public void testProduceConsumeMultipleRecords() throws Exception {
        KafkaConfig config = getKafkaConfig();
        try (ApiStatePersistenceStore store = new ApiStatePersistenceStore(config, time, Collections.emptyMap())) {

            int firstBrokerId = 1;
            BrokerRemovalDescription.BrokerShutdownStatus bssStatus = BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE;
            BrokerRemovalDescription.PartitionReassignmentsStatus parStatus = BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS;
            String errorMessage = "test message";
            CruiseControlException error = new CruiseControlException(errorMessage);
            BrokerRemovalStatus firstBrokerStatus = new BrokerRemovalStatus(firstBrokerId, bssStatus, parStatus, error);
            store.save(firstBrokerStatus, true);

            int secondBrokerId = 2;
            BrokerRemovalStatus secondBrokerStatus = new BrokerRemovalStatus(secondBrokerId,
                    BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
                    BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
                    error);
            store.save(secondBrokerStatus, true);

            Assert.assertEquals(firstBrokerStatus, store.getBrokerRemovalStatus(firstBrokerId));
            Assert.assertEquals(secondBrokerStatus, store.getBrokerRemovalStatus(secondBrokerId));
            Assert.assertEquals(2, store.getAllBrokerRemovalStatus().size());
        }
    }

    @Test
    public void testProduceConsumeBrokerStatusMultipleTime() throws Exception {
        KafkaConfig config = getKafkaConfig();
        try (ApiStatePersistenceStore store = new ApiStatePersistenceStore(config, time, Collections.emptyMap())) {
            int brokerId = 1;
            BrokerRemovalDescription.BrokerShutdownStatus bssStatus = BrokerRemovalDescription.BrokerShutdownStatus.PENDING;
            BrokerRemovalDescription.PartitionReassignmentsStatus parStatus = BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS;
            CruiseControlException error = new CruiseControlException("test message");

            BrokerRemovalStatus brokerStatus = new BrokerRemovalStatus(brokerId, bssStatus, parStatus, error);
            store.save(brokerStatus, true);
            BrokerRemovalStatus brokerRemovalStatus = store.getBrokerRemovalStatus(brokerId);
            Assert.assertEquals(brokerStatus, brokerRemovalStatus);
            long startTime = brokerRemovalStatus.getStartTime();

            bssStatus = BrokerRemovalDescription.BrokerShutdownStatus.FAILED;
            parStatus = BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED;
            String updatedMessage = "updated message";
            CruiseControlException newError = new CruiseControlException(updatedMessage);

            BrokerRemovalStatus newBrokerStatus = new BrokerRemovalStatus(brokerId, bssStatus, parStatus, newError);
            newBrokerStatus.setStartTime(brokerRemovalStatus.getStartTime());
            store.save(newBrokerStatus, false);

            brokerRemovalStatus = store.getBrokerRemovalStatus(brokerId);
            Assert.assertEquals(newBrokerStatus, brokerRemovalStatus);
            Assert.assertTrue("Start time should be set.", brokerRemovalStatus.getStartTime() > 0);
            Assert.assertEquals(newBrokerStatus.getStartTime(), brokerRemovalStatus.getStartTime());

            Assert.assertTrue("Last update time should be set", brokerRemovalStatus.getLastUpdateTime() > 0);
            Assert.assertNotEquals(brokerRemovalStatus.getStartTime(), brokerRemovalStatus.getLastUpdateTime());
            Assert.assertEquals(brokerRemovalStatus.getStartTime(), startTime);
        }
    }

    @Test
    public void testApiStatusExistAfterRestart() throws Exception {
        KafkaConfig config = getKafkaConfig();
        ApiStatePersistenceStore store = new ApiStatePersistenceStore(config, time, Collections.emptyMap());

        int firstBrokerId = 1;
        BrokerRemovalStatus firstBrokerStatus = new BrokerRemovalStatus(firstBrokerId,
                BrokerRemovalDescription.BrokerShutdownStatus.PENDING,
                BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
                null);
        store.save(firstBrokerStatus, true);
        long startTime = firstBrokerStatus.getStartTime();
        long lastUpdateTime = firstBrokerStatus.getLastUpdateTime();

        int secondBrokerId = 2;
        BrokerRemovalStatus secondBrokerStatus = new BrokerRemovalStatus(secondBrokerId,
                BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
                BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED,
                null);
        store.save(secondBrokerStatus, true);

        // Update first broker state again, so we have two insertion, one update
        firstBrokerStatus = new BrokerRemovalStatus(firstBrokerId,
                BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE,
                BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS,
                null);
        firstBrokerStatus.setStartTime(startTime);
        store.save(firstBrokerStatus, false);

        firstBrokerStatus = store.getBrokerRemovalStatus(firstBrokerId);
        Assert.assertEquals(startTime, firstBrokerStatus.getStartTime());
        Assert.assertNotEquals(lastUpdateTime, firstBrokerStatus.getLastUpdateTime());
        lastUpdateTime = firstBrokerStatus.getLastUpdateTime();


        // Close the Api Persistence store and create a new one to read record. This simulates restart
        store.close();

        store = new ApiStatePersistenceStore(config, time, Collections.emptyMap());

        Assert.assertEquals("There should only be two api states.",
                2, store.getAllBrokerRemovalStatus().size());

        // Validate first broker status persistence
        BrokerRemovalStatus brokerRemovalStatus = store.getBrokerRemovalStatus(firstBrokerId);
        Assert.assertEquals(firstBrokerStatus, brokerRemovalStatus);

        Assert.assertTrue("Start time should be set.", brokerRemovalStatus.getStartTime() > 0);
        Assert.assertEquals(brokerRemovalStatus.getStartTime(), startTime);
        Assert.assertTrue("Last update time should be set", brokerRemovalStatus.getLastUpdateTime() > 0);
        Assert.assertNotEquals(brokerRemovalStatus.getStartTime(), brokerRemovalStatus.getLastUpdateTime());
        Assert.assertEquals(lastUpdateTime, brokerRemovalStatus.getLastUpdateTime());

        // Validate second broker status persistence
        brokerRemovalStatus = store.getBrokerRemovalStatus(secondBrokerId);
        Assert.assertNull("Exception is not null: " + brokerRemovalStatus.exception(), brokerRemovalStatus.exception());
        Assert.assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE, brokerRemovalStatus.brokerShutdownStatus());
        Assert.assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED, brokerRemovalStatus.partitionReassignmentsStatus());
        Assert.assertEquals(brokerRemovalStatus.getStartTime(), brokerRemovalStatus.getLastUpdateTime());

        store.close();
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
            TestUtils.waitForCondition(() -> {
                Set<String> topics = adminClient.listTopics().names().get();
                return topics.contains(API_STATE_TOPIC);
            }, TEST_TIMEOUT / 2, API_STATE_TOPIC + " can't be created.");

            // Topic creation is not instantaneous; wait for completion.
            // This is rather yucky but the test doesn't control the topic creation components.
            TestUtils.waitForCondition(() -> ApiStatePersistenceStore.checkTopicCreated(configMap, topicConfig),
                    30_000,
                    "Check " + API_STATE_TOPIC + " exists.");
        } finally {
            KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
        }
    }

    private KafkaConfig getKafkaConfig() {
        return new KafkaConfig(getConfig());
    }

    private Map<String, String> getConfig() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(ConfluentConfigs.BALANCER_API_STATE_TOPIC_CONFIG, API_STATE_TOPIC);
        configMap.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
        configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, kafkaCluster.zkConnect());
        configMap.put(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG, "false");

        return configMap;
    }
}
