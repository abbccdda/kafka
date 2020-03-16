/*
 Copyright 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;

public class LeaderReplicaDistributionGoalTest {

  private static final String LEADER_BROKER_HOST = "leader-host";
  private static final Integer LEADER_BROKER_ID = 1;
  private static final String FOLLOWER_BROKER_HOST = "follower-host";
  private static final Integer FOLLOWER_BROKER_ID = 2;
  private static final String TOPIC = "test-topic";

  private ClusterModel _model;
  private Broker _b1;
  private Broker _b2;
  private LeaderReplicaDistributionGoal _goal;

  @Before
  public void setUp() {
    _model = new ClusterModel(new ModelGeneration(1, 1), 100.0);
    _model.createRack("rack");
    _b1 = _model.createBroker("rack", LEADER_BROKER_HOST, LEADER_BROKER_ID, new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY), false);
    _b2 = _model.createBroker("rack", FOLLOWER_BROKER_HOST, FOLLOWER_BROKER_ID, new BrokerCapacityInfo(TestConstants.BROKER_CAPACITY), false);
    _goal = new LeaderReplicaDistributionGoal();
    List<Long> windows = Collections.singletonList(1L);

    for (int i = 0; i < 10; i++) {
      TopicPartition tp = new TopicPartition(TOPIC, i);
      _model.createReplica("rack", LEADER_BROKER_ID, tp, 0, true);
      _model.createReplica("rack", FOLLOWER_BROKER_ID, tp, 1, false);

      _model.setReplicaLoad("rack", _b1.id(), tp,
          getAggregatedMetricValues(
              40.0,
              TestConstants.LARGE_BROKER_CAPACITY / 3,
              TestConstants.MEDIUM_BROKER_CAPACITY / 2,
              TestConstants.LARGE_BROKER_CAPACITY / 3),
          windows);
      _model.setReplicaLoad("rack", _b2.id(), tp,
          getAggregatedMetricValues(
              40.0,
              TestConstants.LARGE_BROKER_CAPACITY / 3,
              TestConstants.MEDIUM_BROKER_CAPACITY / 2,
              TestConstants.LARGE_BROKER_CAPACITY / 3),
          windows);
    }
  }

  @Test
  public void testRebalanceForBrokerBalancesLeaderReplicas() {
    Set<Goal> goals = new HashSet<>();
    goals.add(_goal);

    // should not be balanced
    for (Map.Entry<TopicPartition, ReplicaPlacementInfo> entry : _model.getLeaderDistribution().entrySet()) {
      assertEquals(LEADER_BROKER_ID, entry.getValue().brokerId());
    }

    OptimizationOptions opts = new OptimizationOptions(new HashSet<>());

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(5L));
    _goal.configure(new KafkaCruiseControlConfig(props).mergedConfigValues());
    _goal.initGoalState(_model, opts);
    _goal.rebalanceForBroker(_b1, _model, goals, opts);

    // should be more balanced
    int b1LeaderCount = 0;
    int b2LeaderCount = 0;
    for (Map.Entry<TopicPartition, ReplicaPlacementInfo> entry : _model.getLeaderDistribution().entrySet()) {
      if (LEADER_BROKER_ID.equals(entry.getValue().brokerId())) {
        b1LeaderCount++;
      } else {
        b2LeaderCount++;
      }
    }
    assertEquals(6, b1LeaderCount);
    assertEquals(4, b2LeaderCount);
  }

  @Test
  public void testRebalanceForBrokerIgnoresExcludedTopics() {
    Set<Goal> goals = new HashSet<>();
    goals.add(_goal);

    // should not be balanced
    for (Map.Entry<TopicPartition, ReplicaPlacementInfo> entry : _model.getLeaderDistribution().entrySet()) {
      assertEquals(LEADER_BROKER_ID, entry.getValue().brokerId());
    }

    Set<String> excludedTopics = new HashSet<>();
    excludedTopics.add(TOPIC);
    OptimizationOptions opts = new OptimizationOptions(excludedTopics);

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(5L));
    _goal.configure(new KafkaCruiseControlConfig(props).mergedConfigValues());
    _goal.initGoalState(_model, opts);
    _goal.rebalanceForBroker(_b1, _model, goals, opts);

    // should not be balanced
    for (Map.Entry<TopicPartition, ReplicaPlacementInfo> entry : _model.getLeaderDistribution().entrySet()) {
      assertEquals(LEADER_BROKER_ID, entry.getValue().brokerId());
    }
  }
}
