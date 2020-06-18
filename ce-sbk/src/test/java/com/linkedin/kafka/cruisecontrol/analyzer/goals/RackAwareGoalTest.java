/*
 Copyright 2019 Confluent Inc.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import kafka.common.TopicPlacement;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RackAwareGoalTest {
    private RackAwareGoal _rackAwareGoal = new RackAwareGoal();
    private ClusterModel _clusterModel;

    @Before
    public void setUp() {
        _clusterModel = DeterministicCluster.rackAwareModel(TestConstants.BROKER_CAPACITY);
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        _rackAwareGoal.configure(new KafkaCruiseControlConfig(props).mergedConfigValues());
    }

    @Test
    public void testAcceptMoveWithinRack() {
        _rackAwareGoal.initGoalState(_clusterModel, new OptimizationOptions(Collections.emptySet()));
        assertSame(ActionAcceptance.ACCEPT, _rackAwareGoal.actionAcceptance(new BalancingAction(
                        new TopicPartition("A", 0),
                        1,
                        0,
                        ActionType.INTER_BROKER_REPLICA_MOVEMENT),
                _clusterModel));

        assertSame(ActionAcceptance.ACCEPT, _rackAwareGoal.actionAcceptance(new BalancingAction(
                        new TopicPartition("A", 0),
                        4,
                        5,
                        ActionType.INTER_BROKER_REPLICA_SWAP,
                        new TopicPartition("B", 0)),
                _clusterModel));
    }

    @Test
    public void testAcceptMoveToRackWithFewerReplicas() {
        _rackAwareGoal.initGoalState(_clusterModel, new OptimizationOptions(Collections.emptySet()));
        assertSame(ActionAcceptance.ACCEPT, _rackAwareGoal.actionAcceptance(new BalancingAction(
                        new TopicPartition("A", 0),
                        3,
                        6,
                        ActionType.INTER_BROKER_REPLICA_MOVEMENT),
                _clusterModel));
    }

    @Test
    public void testRejectMoveToRackWithMoreReplicas() {
        _rackAwareGoal.initGoalState(_clusterModel, new OptimizationOptions(Collections.emptySet()));
        assertSame(ActionAcceptance.BROKER_REJECT, _rackAwareGoal.actionAcceptance(new BalancingAction(
                        new TopicPartition("A", 0),
                        4,
                        2,
                        ActionType.INTER_BROKER_REPLICA_MOVEMENT),
                _clusterModel));
    }

    @Test
    public void testRebalanceOverloadedRack() throws OptimizationFailureException {
        OptimizationOptions options = new OptimizationOptions(Collections.emptySet());
        _rackAwareGoal.optimize(_clusterModel, Collections.emptySet(), options);
        verifyRackAwarenessSatisfied();
    }

    @Test
    public void testUnbalancedClusterThrowsException() {
        _rackAwareGoal.initGoalState(_clusterModel, new OptimizationOptions(Collections.emptySet()));
        assertThrows(OptimizationFailureException.class, () -> _rackAwareGoal.updateGoalState(_clusterModel, Collections.emptySet()));
    }

    @Test
    public void testUnbalancedClusterWithPlacementConstraintsDoesNotThrowException() throws OptimizationFailureException {
        List<Integer> topicAReplicas = _clusterModel.partition(new TopicPartition("A", 0)).replicas()
                .stream().map(r -> r.broker().id()).collect(Collectors.toList());
        List<Integer> topicBReplicas = _clusterModel.partition(new TopicPartition("B", 0)).replicas()
                .stream().map(r -> r.broker().id()).collect(Collectors.toList());
        _clusterModel.setTopicPlacements(Collections.singletonMap(
                "B", TopicPlacement.parse("{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"1\"}}]," +
                        " \"observers\": [{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}]}").get()));
        _rackAwareGoal.optimize(_clusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet()));

        List<Integer> newTopicAReplicas = _clusterModel.partition(new TopicPartition("A", 0)).replicas()
                .stream().map(r -> r.broker().id()).collect(Collectors.toList());
        List<Integer> newTopicBReplicas = _clusterModel.partition(new TopicPartition("B", 0)).replicas()
                .stream().map(r -> r.broker().id()).collect(Collectors.toList());
        assertEquals(topicAReplicas, newTopicAReplicas);
        assertEquals(topicBReplicas, newTopicBReplicas);
    }

    private void verifyRackAwarenessSatisfied() {
        Map<String, Integer> replicaCounts = new HashMap<>();
        replicaCounts.put("0", 0);
        replicaCounts.put("1", 0);
        for (Replica r : _clusterModel.leaderReplicas()) {
            replicaCounts.replaceAll((i, v) -> 0);
            for (Broker b : _clusterModel.partition(r.topicPartition()).partitionBrokers()) {
                replicaCounts.compute(b.rack().id(), (k, v) -> v + 1);
            }

            assertTrue(replicaCounts.get("0") == 1 && replicaCounts.get("1") == 2 ||
                    replicaCounts.get("0") == 2 && replicaCounts.get("1") == 1);
        }
    }
}
