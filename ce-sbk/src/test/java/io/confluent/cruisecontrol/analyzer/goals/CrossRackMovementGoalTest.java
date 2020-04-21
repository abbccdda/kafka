/*
 Copyright 2019 Confluent Inc.
 */

package io.confluent.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

public class CrossRackMovementGoalTest {
    private CrossRackMovementGoal _goal = new CrossRackMovementGoal();
    // Topics -> Partitions: T1 -> 0, 1; T2 -> 0, 1, 2
    private ClusterModel _clusterModel = DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY);

    @Test
    public void testSelfSatisfied() {
        // Satisfied by movement within a rack and between racks
        BalancingAction action = new BalancingAction(
                new TopicPartition("T1", 0),
                0,
                1,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT);
        Assert.assertTrue(_goal.selfSatisfied(_clusterModel, action));

        action = new BalancingAction(
                new TopicPartition("T1", 0),
                2,
                1,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT);
        Assert.assertTrue(_goal.selfSatisfied(_clusterModel, action));

        // Satisfied by leadership movement within a rack and between racks
        action = new BalancingAction(
                new TopicPartition("T1", 1),
                1,
                0,
                ActionType.LEADERSHIP_MOVEMENT);
        Assert.assertTrue(_goal.selfSatisfied(_clusterModel, action));

        action = new BalancingAction(
                new TopicPartition("T1", 0),
                0,
                2,
                ActionType.LEADERSHIP_MOVEMENT);
        Assert.assertTrue(_goal.selfSatisfied(_clusterModel, action));
    }

    @Test
    public void testAcceptMovementWithinRack() {
        // Move TopicPartition(T1, 0) from broker 0 (on rack 0) to broker 1 (on rack 0)
        BalancingAction action = new BalancingAction(
                new TopicPartition("T1", 0),
                0,
                1,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT);
        Assert.assertEquals(ActionAcceptance.ACCEPT, _goal.actionAcceptance(action, _clusterModel));

        // Swap TopicPartition(T1, 0) on broker 0 (on rack 0) with TopicPartition(T1, 1) on broker 1 (on rack 0)
        action = new BalancingAction(
                new TopicPartition("T1", 0),
                0,
                1,
                ActionType.INTER_BROKER_REPLICA_SWAP,
                new TopicPartition("T1", 1));
        Assert.assertEquals(ActionAcceptance.ACCEPT, _goal.actionAcceptance(action, _clusterModel));
    }

    @Test
    public void testRejectMovementBetweenRacks() {
        // Move TopicPartition(T1, 0) from broker 2 (on rack 1) to broker 1 (on rack 0)
        BalancingAction action = new BalancingAction(
                new TopicPartition("T1", 0),
                2,
                1,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT);
        Assert.assertEquals(ActionAcceptance.BROKER_REJECT, _goal.actionAcceptance(action, _clusterModel));

        // Swap TopicPartition(T1, 0) on broker 0 (on rack 0) with TopicPartition(T1, 1) on broker 2 (on rack 1)
        action = new BalancingAction(
                new TopicPartition("T1", 0),
                0,
                2,
                ActionType.INTER_BROKER_REPLICA_SWAP,
                new TopicPartition("T1", 1));
        Assert.assertEquals(ActionAcceptance.BROKER_REJECT, _goal.actionAcceptance(action, _clusterModel));
    }

    @Test
    public void testAcceptLeaderMovement() {
        // Move leadership of TopicPartition(T1, 1) from broker 1 (on rack 1) to broker 0 (on rack 0)
        BalancingAction action = new BalancingAction(
                new TopicPartition("T1", 1),
                1,
                0,
                ActionType.LEADERSHIP_MOVEMENT);
        Assert.assertEquals(ActionAcceptance.ACCEPT, _goal.actionAcceptance(action, _clusterModel));

        // Move leadership of TopicPartition(T1, 0) on broker 0 (on rack 0) to broker 2 (on rack 1)
        action = new BalancingAction(
                new TopicPartition("T1", 0),
                0,
                2,
                ActionType.LEADERSHIP_MOVEMENT);
        Assert.assertEquals(ActionAcceptance.ACCEPT, _goal.actionAcceptance(action, _clusterModel));
    }

    @Test
    public void testNoBrokersToBalance() {
        Assert.assertTrue(_goal.brokersToBalance(_clusterModel).isEmpty());
    }

    /**
     * Test that CrossRackMovementGoal blocks cross-rack movement by attempting to balance a cluster that
     * could satisfy the RackAwareGoal. This test should throw an OptimizationFailureException because,
     * despite capacity to balance across racks, the CrossRackMovementGoal will block it,
     *
     * @throws KafkaCruiseControlException
     */
    @Test
    public void testGoalExecution() throws KafkaCruiseControlException {
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(6L));
        props.setProperty(KafkaCruiseControlConfig.HARD_GOALS_CONFIG,
                "io.confluent.cruisecontrol.analyzer.goals.CrossRackMovementGoal," +
                "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
        props.setProperty(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG,
                "io.confluent.cruisecontrol.analyzer.goals.CrossRackMovementGoal," +
                "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");
        props.setProperty(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG,
                "com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal");

        KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
        List<Goal> goalNameByPriority = config.getConfiguredInstances(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG, Goal.class);
        MetricsRegistry metricsRegistry = new MetricsRegistry();
        GoalOptimizer goalOptimizer = new GoalOptimizer(
                config,
                null,
                new SystemTime(),
                KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry),
                EasyMock.mock(Executor.class));

        // Attempt to balance a cluster that could satisfy the RackAwareGoal, which should fail
        Assert.assertThrows(OptimizationFailureException.class,
                () -> goalOptimizer.optimizations(
                        DeterministicCluster.rackAwareSatisfiable(),
                        goalNameByPriority,
                        new OperationProgress()));
        metricsRegistry.shutdown();
    }
}
