/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import kafka.log.LogConfig;
import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.configResourcesForBrokers;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.AUTO_THROTTLE;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ReplicationThrottleHelperTest extends CCKafkaIntegrationTestHarness {

  @Override
  public int clusterSize() {
    return 4;
  }

  @Before
  public void setUp() {
    super.setUp();
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  private void createTopics() {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections.singletonMap(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Arrays.asList(
          new NewTopic(TOPIC0, 2, (short) 2),
          new NewTopic(TOPIC1, 2, (short) 2)
      ));
    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }

  private ExecutionTask inProgressTaskForProposal(long id, ExecutionProposal proposal) {
    ExecutionTask task = new ExecutionTask(id, proposal, ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
    task.inProgress(0);
    return task;
  }

  private ExecutionTask completedTaskForProposal(long id, ExecutionProposal proposal) {
    ExecutionTask task = inProgressTaskForProposal(id, proposal);
    task.completed(1);
    return task;
  }

  @Test
  public void isNoOpWhenThrottleIsNull() throws InterruptedException, ExecutionException {
    KafkaZkClient mockKafkaZkClient = EasyMock.strictMock(KafkaZkClient.class);
    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient, Collections.emptyList(), Collections.emptyMap());
    EasyMock.replay(mockKafkaZkClient, adminClient);

    // Test would fail on any unexpected interactions with the kafkaZkClient
    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(mockKafkaZkClient, adminClient, null);
    ExecutionProposal proposal = new ExecutionProposal(
        new TopicPartition("topic", 0),
        100,
        new ReplicaPlacementInfo(0),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
        Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal), null);
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
  }

  private void assertExpectedThrottledRateForBroker(KafkaZkClient kafkaZkClient, int broker, Long expectedRate) {
    Properties brokerConfig = kafkaZkClient.getEntityConfigs(ConfigType.Broker(), String.valueOf(broker));
    String expectedString = expectedRate == null ? null : String.valueOf(expectedRate);
    assertEquals(expectedString, brokerConfig.getProperty(ReplicationThrottleHelper.LEADER_THROTTLED_RATE));
    assertEquals(expectedString, brokerConfig.getProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_RATE));
  }

  private void assertExpectedThrottledReplicas(KafkaZkClient kafkaZkClient, String topic, String expectedReplicas) {
    Properties topicConfig = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    assertEquals(expectedReplicas, topicConfig.getProperty(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS));
    assertEquals(expectedReplicas, topicConfig.getProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS));
  }

  @Test
  public void addingThrottlesWithNoPreExistingThrottles() throws InterruptedException, ExecutionException {
    createTopics();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
        "ReplicationThrottleHelperTestMetricGroup",
        "AddingThrottlesWithNoPreExistingThrottles",
            false);

    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedThrottledBrokers = Arrays.asList(0, 1, 2);
    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient, configResourcesForBrokers(expectedThrottledBrokers), Collections.emptyMap());
    EasyMock.replay(adminClient);
    final long throttleRate = 100L;
    try {
      ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate);
      ExecutionProposal proposal = new ExecutionProposal(
              new TopicPartition(TOPIC0, 0),
              100,
              new ReplicaPlacementInfo(0),
              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

      ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal), null);

    for (Integer brokerId : expectedThrottledBrokers) {
      assertExpectedThrottledRateForBroker(kafkaZkClient, brokerId, throttleRate);
    }
      // No throttle on broker 3 because it's not involved in any of the execution proposals:
      assertExpectedThrottledRateForBroker(kafkaZkClient, 3, null);
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "0:0,0:1,0:2");

      // We expect all throttles to be cleaned up
      throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

      Arrays.asList(0, 1, 2, 3).forEach(i -> assertExpectedThrottledRateForBroker(kafkaZkClient, i, null));
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, null);
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testExceptionInAdminClientDoesntStopThrottling() throws InterruptedException {
    createTopics();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
        "ReplicationThrottleHelperTestMetricGroup",
        "AddingThrottlesWithNoPreExistingThrottles",
        false);

    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedThrottledBrokers = Arrays.asList(0, 1, 2);
    EasyMock.expect(
            adminClient.describeConfigs(KafkaCruiseControlUnitTestUtils.configResourcesForBrokers(expectedThrottledBrokers))
    ).andThrow(new KafkaException("!!!")).times(1);
    EasyMock.replay(adminClient);

    final long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(
            new TopicPartition(TOPIC0, 0),
            100,
            new ReplicaPlacementInfo(0),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal), null);

    for (Integer brokerId : expectedThrottledBrokers) {
      assertExpectedThrottledRateForBroker(kafkaZkClient, brokerId, throttleRate);
    }
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(kafkaZkClient, 3, null);
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "0:0,0:1,0:2");

    // We expect all throttles to be cleaned up
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    Arrays.asList(0, 1, 2, 3).forEach(i -> assertExpectedThrottledRateForBroker(kafkaZkClient, i, null));
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, null);
  }

  @Test
  public void testNoThrottlesAreSetIfAllBrokersHaveStaticThrottles() throws InterruptedException, ExecutionException {
    createTopics();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "ReplicationThrottleHelperTestMetricGroup",
            "AddingThrottlesWithNoPreExistingThrottles",
            false);
    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedThrottledBrokers = Arrays.asList(0, 1, 2);
    List<Integer> expectedEmptyStaticThrottledBrokers = Collections.singletonList(3);
    Map<String, List<ConfigEntry>> staticThrottleConfigs = new HashMap<>();
    for (Integer brokerId : expectedThrottledBrokers) {
      staticThrottleConfigs.put(brokerId.toString(), Arrays.asList(
              new ConfigEntry(LogConfig.FollowerReplicationThrottledReplicasProp(), "*"),
              new ConfigEntry(LogConfig.LeaderReplicationThrottledReplicasProp(), "*")
      ));
    }
    // Kafka can return null values for sensitive configs or configs that are not set
    for (Integer brokerId: expectedEmptyStaticThrottledBrokers) {
      staticThrottleConfigs.put(brokerId.toString(), Arrays.asList(
              new ConfigEntry(LogConfig.FollowerReplicationThrottledReplicasProp(), null),
              new ConfigEntry(LogConfig.LeaderReplicationThrottledReplicasProp(), null)
      ));
    }
    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient, configResourcesForBrokers(expectedThrottledBrokers), staticThrottleConfigs);
    EasyMock.replay(adminClient);

    long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(
            new TopicPartition(TOPIC0, 0),
            100,
            new ReplicaPlacementInfo(0),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal), null);

    for (Integer brokerId : expectedThrottledBrokers) {
      assertExpectedThrottledRateForBroker(kafkaZkClient, brokerId, throttleRate);
    }
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(kafkaZkClient, 3, null);
    // Topic throttled replicas should not be placed because brokers have static throttles
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, null);

    // We expect all throttles to be cleaned up
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    Arrays.asList(0, 1, 2, 3).forEach(i -> assertExpectedThrottledRateForBroker(kafkaZkClient, i, null));
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, null);
  }

  /**
   * if 2/3 brokers have static throttles, we need to set the topic-level throttle for all 3 brokers
   */
  @Test
  public void testTopicThrottlesAreSetIfNotAllBrokersHaveStaticThrottles() throws InterruptedException, ExecutionException {
    createTopics();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "ReplicationThrottleHelperTestMetricGroup",
            "TopicThrottlesAreSetIfNotAllBrokersHaveStaticThrottles",
            false);
    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedStaticThrottledBrokers = Arrays.asList(0, 1);
    List<Integer> expectedEmptyStaticThrottledBrokers = Arrays.asList(2, 3);
    Map<String, List<ConfigEntry>> staticThrottleConfigs = new HashMap<>();
    for (Integer brokerId : expectedStaticThrottledBrokers) {
      staticThrottleConfigs.put(brokerId.toString(), Arrays.asList(
              new ConfigEntry(LogConfig.FollowerReplicationThrottledReplicasProp(), "*"),
              new ConfigEntry(LogConfig.LeaderReplicationThrottledReplicasProp(), "*")
      ));
    }
    // Kafka can return null values for sensitive configs or configs that are not set
    for (Integer brokerId: expectedEmptyStaticThrottledBrokers) {
      staticThrottleConfigs.put(brokerId.toString(), Arrays.asList(
              new ConfigEntry(LogConfig.FollowerReplicationThrottledReplicasProp(), null),
              new ConfigEntry(LogConfig.LeaderReplicationThrottledReplicasProp(), null)
      ));
    }
    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient, configResourcesForBrokers(Arrays.asList(0, 1, 2)), staticThrottleConfigs);
    EasyMock.replay(adminClient);

    long throttleRate = 100L;

    ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate);
    ExecutionProposal proposal = new ExecutionProposal(
            new TopicPartition(TOPIC0, 0),
            100,
            new ReplicaPlacementInfo(0),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionTask task = completedTaskForProposal(0, proposal);

    throttleHelper.setThrottles(Collections.singletonList(proposal), null);

    for (Integer brokerId : expectedStaticThrottledBrokers) {
      assertExpectedThrottledRateForBroker(kafkaZkClient, brokerId, throttleRate);
    }
    assertExpectedThrottledRateForBroker(kafkaZkClient, 2, throttleRate);
    // No throttle on broker 3 because it's not involved in any of the execution proposals:
    assertExpectedThrottledRateForBroker(kafkaZkClient, 3, null);
    // All topic throttled replicas should be placed because not all brokers have static throttles
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "0:0,0:1,0:2");

    // We expect all throttles to be cleaned up
    throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

    Arrays.asList(0, 1, 2, 3).forEach(i -> assertExpectedThrottledRateForBroker(kafkaZkClient, i, null));
    assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, null);
  }

  @Test
  public void addingThrottlesWithPreExistingThrottles() throws InterruptedException, ExecutionException {
    createTopics();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "ReplicationThrottleHelperTestMetricGroup",
            "addingThrottlesWithPreExistingThrottles",
            false);
    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedThrottledBrokers = Arrays.asList(0, 1, 2);
    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient,
            configResourcesForBrokers(expectedThrottledBrokers), Collections.emptyMap());
    EasyMock.replay(adminClient);

    final long throttleRate = 100L;
    try {
      ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate);
      ExecutionProposal proposal = new ExecutionProposal(
              new TopicPartition(TOPIC0, 0),
              100,
              new ReplicaPlacementInfo(0),
              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

      ExecutionTask task = completedTaskForProposal(0, proposal);

      // Broker 0 has an existing leader and follower throttle; we expect these to be preserved.
      Properties broker0Config = new Properties();
      long preExistingBroker0ThrottleRate = 200L;
      broker0Config.setProperty(ReplicationThrottleHelper.LEADER_THROTTLED_RATE, String.valueOf(preExistingBroker0ThrottleRate));
      broker0Config.setProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_RATE, String.valueOf(preExistingBroker0ThrottleRate));
      ExecutorUtils.changeBrokerConfig(new AdminZkClient(kafkaZkClient), 0, broker0Config);

      // Partition 1 (which is not involved in any execution proposal) has pre-existing throttled
      // replicas (on both leaders and followers); we expect these configurations to be merged
      // with our new throttled replicas.
      Properties topic0Config = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), TOPIC0);
      topic0Config.setProperty(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS, "1:0,1:1");
      topic0Config.setProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS, "1:0,1:1");
      ExecutorUtils.changeTopicConfig(new AdminZkClient(kafkaZkClient), TOPIC0, topic0Config);

      // Topic 1 is not involved in any execution proposal. It has pre-existing throttled replicas.
      Properties topic1Config = kafkaZkClient.getEntityConfigs(ConfigType.Topic(), TOPIC1);
      topic1Config.setProperty(ReplicationThrottleHelper.LEADER_THROTTLED_REPLICAS, "1:1");
      topic1Config.setProperty(ReplicationThrottleHelper.FOLLOWER_THROTTLED_REPLICAS, "1:1");
      ExecutorUtils.changeTopicConfig(new AdminZkClient(kafkaZkClient), TOPIC1, topic1Config);

    throttleHelper.setThrottles(Collections.singletonList(proposal), null);

      assertExpectedThrottledRateForBroker(kafkaZkClient, 0, preExistingBroker0ThrottleRate);
      assertExpectedThrottledRateForBroker(kafkaZkClient, 1, throttleRate);
      assertExpectedThrottledRateForBroker(kafkaZkClient, 2, throttleRate);
      // No throttle on broker 3 because it's not involved in any of the execution proposals:
      assertExpectedThrottledRateForBroker(kafkaZkClient, 3, null);
      // Existing throttled replicas are merged with new throttled replicas for topic 0:
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "0:0,0:1,0:2,1:0,1:1");
      // Existing throttled replicas are unchanged for topic 1:
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC1, "1:1");

      throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());

      // We expect all throttles related to replica movement to be removed. Specifically,
      // any throttles related to partitions which were not moved will remain.
      // However, we do expect the broker throttles to be removed.
      throttleHelper.clearThrottles(Collections.singletonList(task), Collections.emptyList());
      Arrays.asList(0, 1, 2, 3).forEach(i -> assertExpectedThrottledRateForBroker(kafkaZkClient, i, null));
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "1:0,1:1");
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC1, "1:1");
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void doNoteRemoveThrottlesForInProgressTasks() throws InterruptedException, ExecutionException {
    createTopics();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
        "ReplicationThrottleHelperTestMetricGroup",
        "doNoteRemoveThrottlesForInProgressTasks",
        false);
    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedThrottledBrokers = Arrays.asList(0, 1, 2, 3);
    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient,
            configResourcesForBrokers(expectedThrottledBrokers), Collections.emptyMap());
    EasyMock.replay(adminClient);

    final long throttleRate = 100L;

    try {
      ReplicationThrottleHelper throttleHelper = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate);
      ExecutionProposal proposal = new ExecutionProposal(
              new TopicPartition(TOPIC0, 0),
              100,
              new ReplicaPlacementInfo(0),
              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

      ExecutionProposal proposal2 = new ExecutionProposal(
              new TopicPartition(TOPIC0, 1),
              100,
              new ReplicaPlacementInfo(0),
              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(3)),
              Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    throttleHelper.setThrottles(Arrays.asList(proposal, proposal2), null);

      ExecutionTask completedTask = completedTaskForProposal(0, proposal);
      ExecutionTask inProgressTask = inProgressTaskForProposal(1, proposal2);


    for (Integer brokerId : expectedThrottledBrokers) {
      assertExpectedThrottledRateForBroker(kafkaZkClient, brokerId, throttleRate);
    }
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "0:0,0:1,0:2,1:0,1:2,1:3");

      throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
      assertExpectedThrottledRateForBroker(kafkaZkClient, 0, throttleRate);
      // we expect broker 1 to be null since all replica movement related to it has completed.
      assertExpectedThrottledRateForBroker(kafkaZkClient, 1, null);
      assertExpectedThrottledRateForBroker(kafkaZkClient, 2, throttleRate);
      // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
      assertExpectedThrottledRateForBroker(kafkaZkClient, 3, throttleRate);
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "1:0,1:2,1:3");

      // passing an inProgress task that is not complete should have no effect.
      throttleHelper.clearThrottles(Collections.singletonList(completedTask), Collections.singletonList(inProgressTask));
      assertExpectedThrottledRateForBroker(kafkaZkClient, 0, throttleRate);
      // we expect broker 1 to be null since all replica movement related to it has completed.
      assertExpectedThrottledRateForBroker(kafkaZkClient, 1, null);
      assertExpectedThrottledRateForBroker(kafkaZkClient, 2, throttleRate);
      // We expect broker 3 to have a throttle on it because there is an in-progress replica being moved
      assertExpectedThrottledRateForBroker(kafkaZkClient, 3, throttleRate);
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, "1:0,1:2,1:3");

      // Completing the in-progress task and clearing the throttles should clean everything up.
      inProgressTask.completed(3);
      throttleHelper.clearThrottles(Arrays.asList(completedTask, inProgressTask), Collections.emptyList());

      Arrays.asList(0, 1, 2, 3).forEach(i -> assertExpectedThrottledRateForBroker(kafkaZkClient, i, null));
      assertExpectedThrottledReplicas(kafkaZkClient, TOPIC0, null);
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void removeReplicasFromConfigTest() {
    Set<String> replicas = new LinkedHashSet<>();
    replicas.add("foo");
    replicas.add("bar");
    replicas.add("baz");
    String throttleConfig = "foo,bar,qux,qaz,baz";
    String result = ReplicationThrottleHelper.removeReplicasFromConfig(throttleConfig, replicas);
    assertEquals(result, "qux,qaz");
  }

  @Test
  public void testDoesNotSetThrottleRate() throws ExecutionException, InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
        "ReplicationThrottleHelperTestMetricGroup",
        "DoesNotSetThrottleRate",
        false);
    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedThrottledBrokers = Arrays.asList(0, 1, 2, 3);
    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient,
            configResourcesForBrokers(expectedThrottledBrokers), Collections.emptyMap());
    EasyMock.replay(adminClient);

    final long throttleRate = 100L;

    try {
      ReplicationThrottleHelper helper = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate);
      boolean updatedThrottle = helper.setThrottleRate(throttleRate);
      assertFalse("Setting the throttle to the same rate should not update it", updatedThrottle);

      ReplicationThrottleHelper helper2 = new ReplicationThrottleHelper(kafkaZkClient, adminClient, null);
      updatedThrottle = helper2.setThrottleRate(null);
      assertFalse("Setting the throttle to the same rate should not update it", updatedThrottle);

      ReplicationThrottleHelper helper3 = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate + 1);
      updatedThrottle = helper3.setThrottleRate(throttleRate);
      assertTrue("Setting the throttle to a different rate should update it", updatedThrottle);

      ReplicationThrottleHelper helper4 = new ReplicationThrottleHelper(kafkaZkClient, adminClient, throttleRate);
      updatedThrottle = helper4.setThrottleRate(null);
      assertTrue("Setting the throttle to a different rate should update it", updatedThrottle);
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    }
  }

  @Test
  public void testAutomaticThrottleComputation() throws ExecutionException, InterruptedException {
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
            "ReplicationThrottleHelperTestMetricGroup",
            "testAutomaticThrottleComputation",
            false);
    KafkaAdminClient adminClient = EasyMock.mock(KafkaAdminClient.class);
    List<Integer> expectedThrottledBrokers = Arrays.asList(0, 1, 2, 3);
    KafkaCruiseControlUnitTestUtils.mockDescribeConfigs(adminClient,
            configResourcesForBrokers(expectedThrottledBrokers), Collections.emptyMap());
    EasyMock.replay(adminClient);

    Long throttleRate1 = 50000000L;
    Long throttleRate2 = 30000000L;
    LoadMonitor mockLoadMonitor = EasyMock.createMock(LoadMonitor.class);
    EasyMock.expect(mockLoadMonitor.computeThrottle()).andReturn(throttleRate1).once();
    EasyMock.expect(mockLoadMonitor.computeThrottle()).andReturn(throttleRate2).once();
    EasyMock.replay(mockLoadMonitor);

    ReplicationThrottleHelper helper = new ReplicationThrottleHelper(kafkaZkClient, adminClient, AUTO_THROTTLE);
    ExecutionProposal proposal = new ExecutionProposal(
            new TopicPartition(TOPIC0, 0),
            100,
            new ReplicaPlacementInfo(0),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(1)),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    ExecutionProposal proposal2 = new ExecutionProposal(
            new TopicPartition(TOPIC0, 1),
            100,
            new ReplicaPlacementInfo(0),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(3)),
            Arrays.asList(new ReplicaPlacementInfo(0), new ReplicaPlacementInfo(2)));

    // Throttle should be computed by the load monitor
    helper.setThrottles(Arrays.asList(proposal, proposal2), mockLoadMonitor);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 0, throttleRate1);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 1, throttleRate1);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 2, throttleRate1);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 3, throttleRate1);
    helper.removeAllThrottles();

    // Simulate the end of an execution by resetting state, so that the throttle is recomputed
    helper.resetThrottleAfterExecution();
    helper.setThrottles(Arrays.asList(proposal, proposal2), mockLoadMonitor);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 0, throttleRate2);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 1, throttleRate2);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 2, throttleRate2);
    assertExpectedThrottledRateForBroker(kafkaZkClient, 3, throttleRate2);

    EasyMock.verify(mockLoadMonitor);
  }
}
