/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.persistence;

import com.linkedin.cruisecontrol.exception.CruiseControlException;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.SbkTopicUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import io.confluent.databalancer.operation.BrokerRemovalStateMachine;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
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
            BrokerRemovalStateMachine.BrokerRemovalState state = BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_FAILED;
            String errorMessage = "test message";
            CruiseControlException error = new CruiseControlException(errorMessage);
            BrokerRemovalStateRecord status = new BrokerRemovalStateRecord(brokerId, state, error);
            store.save(status, true);

            BrokerRemovalStateRecord brokerRemovalStateRecord = store.getBrokerRemovalStateRecord(brokerId);
            Assert.assertEquals(status, brokerRemovalStateRecord);

            CruiseControlException ex = (CruiseControlException) brokerRemovalStateRecord.exception();
            Assert.assertEquals(errorMessage, ex.getMessage());

            Assert.assertEquals(BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_FAILED.brokerShutdownStatus(), brokerRemovalStateRecord.brokerShutdownStatus());
            Assert.assertEquals(BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_FAILED.partitionReassignmentsStatus(), brokerRemovalStateRecord.partitionReassignmentsStatus());

            Assert.assertTrue("Start time should be set.", brokerRemovalStateRecord.startTime() > 0);
            Assert.assertTrue("Last update time should be set", brokerRemovalStateRecord.lastUpdateTime() > 0);
            Assert.assertEquals(brokerRemovalStateRecord.startTime(), brokerRemovalStateRecord.lastUpdateTime());
        }
    }

    @Test
    public void testProduceConsumeMultipleRecords() throws Exception {
        KafkaConfig config = getKafkaConfig();
        try (ApiStatePersistenceStore store = new ApiStatePersistenceStore(config, time, Collections.emptyMap())) {

            int firstBrokerId = 1;
            String errorMessage = "test message";
            CruiseControlException error = new CruiseControlException(errorMessage);
            BrokerRemovalStateRecord firstBrokerStateRecord = new BrokerRemovalStateRecord(firstBrokerId, BrokerRemovalStateMachine.BrokerRemovalState.INITIAL_PLAN_COMPUTATION_INITIATED, error);
            store.save(firstBrokerStateRecord, true);

            int secondBrokerId = 2;
            BrokerRemovalStateRecord secondBrokerStatus = new BrokerRemovalStateRecord(secondBrokerId,
                BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_FAILED,
                error);
            store.save(secondBrokerStatus, true);

            Assert.assertEquals(firstBrokerStateRecord, store.getBrokerRemovalStateRecord(firstBrokerId));
            Assert.assertEquals(secondBrokerStatus, store.getBrokerRemovalStateRecord(secondBrokerId));
            Assert.assertEquals(2, store.getAllBrokerRemovalStateRecords().size());
        }
    }

    @Test
    public void testProduceConsumeBrokerStateRecordMultipleTime() throws Exception {
        KafkaConfig config = getKafkaConfig();
        try (ApiStatePersistenceStore store = new ApiStatePersistenceStore(config, time, Collections.emptyMap())) {
            int brokerId = 1;
            CruiseControlException error = new CruiseControlException("test message");

            BrokerRemovalStateRecord brokerStateRecord = new BrokerRemovalStateRecord(brokerId, BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_INITIATED, error);
            store.save(brokerStateRecord, true);
            BrokerRemovalStateRecord receivedBrokerRemovalStateRecord = store.getBrokerRemovalStateRecord(brokerId);
            Assert.assertEquals(brokerStateRecord, receivedBrokerRemovalStateRecord);
            long startTime = receivedBrokerRemovalStateRecord.startTime();

            String updatedMessage = "updated message";
            CruiseControlException newError = new CruiseControlException(updatedMessage);

            BrokerRemovalStateRecord newBrokerStatus = new BrokerRemovalStateRecord(brokerId, BrokerRemovalStateMachine.BrokerRemovalState.BROKER_SHUTDOWN_FAILED, newError);
            newBrokerStatus.setStartTime(receivedBrokerRemovalStateRecord.startTime());
            store.save(newBrokerStatus, false);

            receivedBrokerRemovalStateRecord = store.getBrokerRemovalStateRecord(brokerId);
            Assert.assertEquals(newBrokerStatus, receivedBrokerRemovalStateRecord);
            Assert.assertTrue("Start time should be set.", receivedBrokerRemovalStateRecord.startTime() > 0);
            Assert.assertEquals(newBrokerStatus.startTime(), receivedBrokerRemovalStateRecord.startTime());

            Assert.assertTrue("Last update time should be set", receivedBrokerRemovalStateRecord.lastUpdateTime() > 0);
            Assert.assertNotEquals(receivedBrokerRemovalStateRecord.startTime(), receivedBrokerRemovalStateRecord.lastUpdateTime());
            Assert.assertEquals(receivedBrokerRemovalStateRecord.startTime(), startTime);
        }
    }

    @Test
    public void testApiStatusExistAfterRestart() throws Exception {
        KafkaConfig config = getKafkaConfig();
        ApiStatePersistenceStore store = new ApiStatePersistenceStore(config, time, Collections.emptyMap());

        int firstBrokerId = 1;
        BrokerRemovalStateRecord firstBrokerStateRecord = new BrokerRemovalStateRecord(firstBrokerId,
                BrokerRemovalStateMachine.BrokerRemovalState.PLAN_COMPUTATION_INITIATED,
                null);
        store.save(firstBrokerStateRecord, true);
        long startTime = firstBrokerStateRecord.startTime();
        long lastUpdateTime = firstBrokerStateRecord.lastUpdateTime();

        int secondBrokerId = 2;
        BrokerRemovalStateRecord secondBrokerStatus = new BrokerRemovalStateRecord(secondBrokerId,
            BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_INITIATED,
                null);
        store.save(secondBrokerStatus, true);

        // Update first broker state again, so we have two insertion, one update
        firstBrokerStateRecord = new BrokerRemovalStateRecord(firstBrokerId,
            BrokerRemovalStateMachine.BrokerRemovalState.PLAN_EXECUTION_SUCCEEDED,
                null);
        firstBrokerStateRecord.setStartTime(startTime);
        store.save(firstBrokerStateRecord, false);

        firstBrokerStateRecord = store.getBrokerRemovalStateRecord(firstBrokerId);
        Assert.assertEquals(startTime, firstBrokerStateRecord.startTime());
        Assert.assertNotEquals(lastUpdateTime, firstBrokerStateRecord.lastUpdateTime());
        lastUpdateTime = firstBrokerStateRecord.lastUpdateTime();


        // Close the Api Persistence store and create a new one to read record. This simulates restart
        store.close();

        store = new ApiStatePersistenceStore(config, time, Collections.emptyMap());

        Assert.assertEquals("There should only be two api states.",
                2, store.getAllBrokerRemovalStateRecords().size());

        // Validate first broker status persistence
        BrokerRemovalStateRecord brokerRemovalStateRecord = store.getBrokerRemovalStateRecord(firstBrokerId);
        Assert.assertEquals(firstBrokerStateRecord, brokerRemovalStateRecord);

        Assert.assertTrue("Start time should be set.", brokerRemovalStateRecord.startTime() > 0);
        Assert.assertEquals(brokerRemovalStateRecord.startTime(), startTime);
        Assert.assertTrue("Last update time should be set", brokerRemovalStateRecord.lastUpdateTime() > 0);
        Assert.assertNotEquals(brokerRemovalStateRecord.startTime(), brokerRemovalStateRecord.lastUpdateTime());
        Assert.assertEquals(lastUpdateTime, brokerRemovalStateRecord.lastUpdateTime());

        // Validate second broker status persistence
        brokerRemovalStateRecord = store.getBrokerRemovalStateRecord(secondBrokerId);
        Assert.assertNull("Exception is not null: " + brokerRemovalStateRecord.exception(), brokerRemovalStateRecord.exception());
        Assert.assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE, brokerRemovalStateRecord.brokerShutdownStatus());
        Assert.assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.IN_PROGRESS, brokerRemovalStateRecord.partitionReassignmentsStatus());
        Assert.assertEquals(brokerRemovalStateRecord.startTime(), brokerRemovalStateRecord.lastUpdateTime());

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
