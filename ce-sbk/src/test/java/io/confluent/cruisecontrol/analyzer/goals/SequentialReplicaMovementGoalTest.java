/*
 Copyright 2019 Confluent Inc.
 */

package io.confluent.cruisecontrol.analyzer.goals;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.yammer.metrics.core.MetricsRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Test;

public class SequentialReplicaMovementGoalTest {
    private SequentialReplicaMovementGoal _goal = new SequentialReplicaMovementGoal();

    // Topics (single partition) -> Replicas: T0 -> 0, 1, 2; T1 -> 1; T2 -> 2
    private ClusterModel _clusterModel = unbalancedCluster();

    @Test
    public void testAcceptMovementOfUnrelatedReplica() {
        // Move TopicPartition(T0, 0) from broker 0 to broker 5
        BalancingAction action = new BalancingAction(
            new TopicPartition("T0", 0),
            0,
            5,
            ActionType.INTER_BROKER_REPLICA_MOVEMENT);
        assertEquals(ActionAcceptance.ACCEPT, _goal.actionAcceptance(action, _clusterModel));

        // Swap TopicPartition(T0, 0) on broker 0 with TopicPartition(T1, 0) on broker 1
        action = new BalancingAction(
            new TopicPartition("T0", 0),
            0,
            1,
            ActionType.INTER_BROKER_REPLICA_SWAP,
            new TopicPartition("T1", 0));
        assertEquals(ActionAcceptance.ACCEPT, _goal.actionAcceptance(action, _clusterModel));
    }

    @Test
    public void testRejectMovementOfRelatedReplica() {
        // Relocate TopicPartition(T0, 0) from broker 1 to broker 4
        _clusterModel.relocateReplica(
            new TopicPartition("T0", 0),
            1,
            4
        );

        // Move TopicPartition(T0, 0) from broker 0 to broker 5
        BalancingAction action = new BalancingAction(
            new TopicPartition("T0", 0),
            0,
            5,
            ActionType.INTER_BROKER_REPLICA_MOVEMENT);
        assertEquals(ActionAcceptance.BROKER_REJECT, _goal.actionAcceptance(action, _clusterModel));

        // Swap TopicPartition(T0, 0) on broker 0 with TopicPartition(T1, 0) on broker 1
        action = new BalancingAction(
            new TopicPartition("T0", 0),
            0,
            1,
            ActionType.INTER_BROKER_REPLICA_SWAP,
            new TopicPartition("T1", 0));
        assertEquals(ActionAcceptance.BROKER_REJECT, _goal.actionAcceptance(action, _clusterModel));
    }

    @Test
    public void testAcceptLeaderMovement() {
        // Relocate TopicPartition(T0, 0) from broker 1 to broker 4
        _clusterModel.relocateReplica(
            new TopicPartition("T0", 0),
            1,
            4
        );

        // Move leadership of TopicPartition(T0, 0) from broker 0 to broker 2
        BalancingAction action = new BalancingAction(
            new TopicPartition("T0", 0),
            0,
            2,
            ActionType.LEADERSHIP_MOVEMENT);
        assertEquals(ActionAcceptance.ACCEPT, _goal.actionAcceptance(action, _clusterModel));
    }

    /**
     * Test that SequentialReplicaMovementGoal blocks parallel replica movement by attempting to balance a cluster that
     * could satisfy the NetworkInboundUsageDistributionGoal. This test should move only one replica of T0.
     */
    @Test
    public void testGoalExecution() throws KafkaCruiseControlException {
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(6L));
        props.setProperty(KafkaCruiseControlConfig.HARD_GOALS_CONFIG,
            "io.confluent.cruisecontrol.analyzer.goals.SequentialReplicaMovementGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal");
        props.setProperty(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG,
            "io.confluent.cruisecontrol.analyzer.goals.SequentialReplicaMovementGoal," +
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal");
        props.setProperty(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG,
            "com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal");

        KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
        List<Goal> goalNameByPriority = config.getConfiguredInstances(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG, Goal.class);

        GoalOptimizer goalOptimizer = new GoalOptimizer(
            config,
            null,
            new SystemTime(),
            new MetricsRegistry(),
            EasyMock.mock(Executor.class));

        OptimizerResult result = goalOptimizer.optimizations(
            unbalancedCluster(),
            goalNameByPriority,
            new OperationProgress());

        // Verify that the optimizer has yield as proposal for TopicPartition(T0, 0)
        assertEquals(1, result.goalProposals().size());
        ExecutionProposal proposal = result.goalProposals().iterator().next();
        assertEquals(new TopicPartition("T0", 0), proposal.topicPartition());

        // Verify that the old replicas and the new replicas have same size
        Set<Integer> oldReplicas = proposal.oldReplicas().stream()
            .map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet());
        Set<Integer> newReplicas = proposal.newReplicas().stream()
            .map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet());
        assertEquals(oldReplicas.size(), newReplicas.size());

        // Verify that new replicas has only one new broker id and that it is one of the
        // empty ones
        newReplicas.removeAll(oldReplicas);
        assertEquals(1, newReplicas.size());
        assertTrue(newReplicas.contains(3) || newReplicas.contains(4) || newReplicas.contains(5));
    }

    // One racks, six brokers, three partition, one with three replicas, two with 1 replica
    public static ClusterModel unbalancedCluster() {
        Map<Integer, Integer> rackByBroker = new HashMap<>();
        rackByBroker.put(0, 0);
        rackByBroker.put(1, 0);
        rackByBroker.put(2, 0);
        rackByBroker.put(3, 0);
        rackByBroker.put(4, 0);
        rackByBroker.put(5, 0);

        ClusterModel cluster = DeterministicCluster.getHomogeneousCluster(rackByBroker, TestConstants.BROKER_CAPACITY);

        // Create topic partition.
        TopicPartition pInfoT00 = new TopicPartition("T0", 0);
        TopicPartition pInfoT10 = new TopicPartition("T1", 0);
        TopicPartition pInfoT20 = new TopicPartition("T2", 0);

        // Create replicas for topic: T0
        cluster.createReplica(rackByBroker.get(0).toString(), 0, pInfoT00, 0, true);
        cluster.createReplica(rackByBroker.get(1).toString(), 1, pInfoT00, 1, false);
        cluster.createReplica(rackByBroker.get(2).toString(), 2, pInfoT00, 2, false);

        // Create replicas for topic: T1
        cluster.createReplica(rackByBroker.get(1).toString(), 1, pInfoT10, 0, true);

        // Create replicas for topic: T2
        cluster.createReplica(rackByBroker.get(2).toString(), 2, pInfoT20, 0, true);

        // Create snapshots and push them to the cluster.
        List<Long> windows = Collections.singletonList(1L);

        // Broker 0
        cluster.setReplicaLoad(rackByBroker.get(0).toString(), 0, pInfoT00,
            getAggregatedMetricValues(
                40.0,
                TestConstants.LARGE_BROKER_CAPACITY / 3,
                TestConstants.MEDIUM_BROKER_CAPACITY / 2,
                TestConstants.LARGE_BROKER_CAPACITY / 3),
            windows);

        // Broker 1
        cluster.setReplicaLoad(rackByBroker.get(1).toString(), 1, pInfoT00,
            getAggregatedMetricValues(
                5.0,
                TestConstants.LARGE_BROKER_CAPACITY / 3,
                0.0,
                TestConstants.LARGE_BROKER_CAPACITY / 3),
            windows);
        cluster.setReplicaLoad(rackByBroker.get(1).toString(), 1, pInfoT10,
            getAggregatedMetricValues(
                40.0,
                TestConstants.LARGE_BROKER_CAPACITY / 2,
                TestConstants.MEDIUM_BROKER_CAPACITY / 2,
                TestConstants.LARGE_BROKER_CAPACITY / 3),
            windows);

        // Broker 2
        cluster.setReplicaLoad(rackByBroker.get(2).toString(), 2, pInfoT00,
            getAggregatedMetricValues(
                5.0,
                TestConstants.LARGE_BROKER_CAPACITY / 3,
                0.0,
                TestConstants.LARGE_BROKER_CAPACITY / 3),
            windows);
        cluster.setReplicaLoad(rackByBroker.get(2).toString(), 2, pInfoT20,
            getAggregatedMetricValues(
                40.0,
                TestConstants.LARGE_BROKER_CAPACITY / 2,
                TestConstants.MEDIUM_BROKER_CAPACITY / 2,
                TestConstants.LARGE_BROKER_CAPACITY / 3),
            windows);

        return cluster;
    }
}
