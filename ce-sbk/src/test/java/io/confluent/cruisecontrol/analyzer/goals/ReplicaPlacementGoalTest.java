/**
 Copyright 2020 Confluent Inc.
 */

package io.confluent.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Rack;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import kafka.common.TopicPlacement;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class ReplicaPlacementGoalTest {
    private ReplicaPlacementGoal goal = new ReplicaPlacementGoal();
    private ClusterModel clusterModel;
    private ClusterModel observerClusterModel;
    private TopicPlacement topic1Placement;
    private TopicPlacement topic2UnsatisfiablePlacement;
    private TopicPlacement topic2SatisfiablePlacement;
    private TopicPlacement topic2MultipleMovesPlacement;
    private TopicPlacement topic1ObserverPlacement;

    @Before
    public void setUp() {
        /**
         * Cluster model with the following configuration, used to test action acceptance:
         *
         * Rack   | Brokers
         * R0     | B0, B1
         * R1     | B2
         *
         * Broker | Replicas
         * B0     | T1_P0_leader, T2_P2_leader, T2_P1_leader
         * B1     | T1_P1_leader, T2_P0_leader, T2_P2_follower
         * B2     | T2_P1_follower, T1_P0_follower, T2_P0_follower, T1_P1_follower
         *
         * Topic1 already satisfies placement constraints s.t. one replica is on r0, one replica is on r1
         * Topic2 can satisfy a placement constraint s.t. one replica is on r0, one replica is on r1 by moving T2_P2_follower
         * from B1 to B2
         */
        clusterModel = DeterministicCluster.topicPlacementClusterModel(TestConstants.BROKER_CAPACITY);
        String topic1PlacementJson = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}," +
                " {\"count\": 1, \"constraints\":{\"rack\":\"1\"}}]}";
        topic1Placement = TopicPlacement.parse(topic1PlacementJson).get();

        String topic2UnsatisfiablePlacementJson = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"1\"}}]}";
        topic2UnsatisfiablePlacement = TopicPlacement.parse(topic2UnsatisfiablePlacementJson).get();

        String topic2SatisfiablePlacementJson = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}," +
                " {\"count\": 1, \"constraints\":{\"rack\":\"1\"}}]}";
        topic2SatisfiablePlacement = TopicPlacement.parse(topic2SatisfiablePlacementJson).get();

        String topic2MultipleMovesJson = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}]}";
        topic2MultipleMovesPlacement = TopicPlacement.parse(topic2MultipleMovesJson).get();
        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        goal.configure(new KafkaCruiseControlConfig(props).mergedConfigValues());

        /**
         * Creates a cluster model with the following configuration:
         * Rack   | Brokers
         * R0     | B0, B1
         * R1     | B2, B3
         * R2     | B4
         *
         * Broker | Replicas
         * B0     | T1_P0_leader
         * B1     | T1_P0_follower
         * B2     | T1_P0_observer
         * B3     | T1_P0_observer
         * B4     | Empty
         */
        observerClusterModel = DeterministicCluster.observerClusterModel(TestConstants.BROKER_CAPACITY);
        // create topic placement with all sync replicas on rack 1, observer replicas on rack 0
        // with the observer cluster model, we will have to swap the placement of observers and replicas
        String topic1ObserverPlacementJson = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"1\"}}]," +
                " \"observers\": [{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}]}";
        topic1ObserverPlacement = TopicPlacement.parse(topic1ObserverPlacementJson).get();
    }

    @Test
    public void testLeadershipMovementAcceptance() {
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, topic1Placement));
        // adds observer T1P0 B1R0
        clusterModel.createReplica(DeterministicCluster.RACK_BY_BROKER.get(1).toString(), 1,
                new TopicPartition(DeterministicCluster.T1, 0), 2, false, false, null, false, true);
        // reject leadership change to observer
        BalancingAction action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 0),
                0,
                1,
                ActionType.LEADERSHIP_MOVEMENT
        );
        assertEquals(ActionAcceptance.REPLICA_REJECT, goal.actionAcceptance(action, clusterModel));

        // accept other leadership changes
        action = new BalancingAction(
            new TopicPartition(DeterministicCluster.T1, 0),
                0,
                2,
                ActionType.LEADERSHIP_MOVEMENT
        );
        assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(action, clusterModel));
    }

    @Test
    public void testReplicaMovementAcceptance() {
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, topic1Placement));
        // T1P1 B2R1 -> B0R0 rejected
        BalancingAction action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 1),
                2,
                0,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT
        );
        assertEquals(ActionAcceptance.BROKER_REJECT, goal.actionAcceptance(action, clusterModel));
        // T1P0 B0R0 -> B1R0 accepted
        action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 0),
                0,
                1,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT);
        assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(action, clusterModel));

        // T1P1 B1R0 -> B0R0 accepted
        action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 1),
                1,
                0,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT);
        assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(action, clusterModel));

        // T2 has no topic placement, it should be able to move across racks
        action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T2, 0),
                2,
                0,
                ActionType.INTER_BROKER_REPLICA_MOVEMENT
        );
        assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(action, clusterModel));
    }

    @Test
    public void testReplicaSwapAcceptance() {
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, topic1Placement));
        // T1P0 B2R1 <-> T2P2 B1R0 should be broker reject, T1P0 cant move to this broker
        BalancingAction action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 0),
                2,
                1,
                ActionType.INTER_BROKER_REPLICA_SWAP,
                new TopicPartition(DeterministicCluster.T2, 2)
        );
        assertEquals(ActionAcceptance.BROKER_REJECT, goal.actionAcceptance(action, clusterModel));

        // T2P2 B1R0 <-> T1P0 B2R1 replica reject, T2P2 can move to this broker
        action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T2, 2),
                1,
                2,
                ActionType.INTER_BROKER_REPLICA_SWAP,
                new TopicPartition(DeterministicCluster.T1, 0)
        );
        assertEquals(ActionAcceptance.REPLICA_REJECT, goal.actionAcceptance(action, clusterModel));

        // T1P1 B1R0 <-> T1P0 B0R0 movement within rack should be accepted
        action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 1),
                1,
                0,
                ActionType.INTER_BROKER_REPLICA_SWAP,
                new TopicPartition(DeterministicCluster.T1, 0)
        );
        assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(action, clusterModel));

        // T1P0 B0R0 <-> T1P1 B1R0 order shouldn't matter
        action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 0),
                0,
                1,
                ActionType.INTER_BROKER_REPLICA_SWAP,
                new TopicPartition(DeterministicCluster.T1, 1)
        );
        assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(action, clusterModel));

        // T1P0 B0R0 <-> T2P0 B1R0 movement within rack should be accepted
        action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 0),
                0,
                1,
                ActionType.INTER_BROKER_REPLICA_SWAP,
                new TopicPartition(DeterministicCluster.T2, 0)
        );
        assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(action, clusterModel));

        // replicas without topic placements set should be able to be swapped across rack
        clusterModel.setTopicPlacements(Collections.emptyMap());
        action = new BalancingAction(
                new TopicPartition(DeterministicCluster.T1, 0),
                2,
                1,
                ActionType.INTER_BROKER_REPLICA_SWAP,
                new TopicPartition(DeterministicCluster.T2, 2)
        );
        assertEquals(ActionAcceptance.ACCEPT, goal.actionAcceptance(action, clusterModel));
    }

    @Test
    public void testGoalDetectsPlacementSatisfiable() throws OptimizationFailureException {
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T2, topic2UnsatisfiablePlacement));
        assertThrows("Expected goal state initialization to detect unsatisfiable topic placement", OptimizationFailureException.class,
                () -> goal.initGoalState(clusterModel, new OptimizationOptions(Collections.emptySet())));

        // should not throw exception if unsatisfiable placement is for an ignored topic
        goal.initGoalState(clusterModel, new OptimizationOptions(Collections.singleton(DeterministicCluster.T2)));

        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T2, topic2SatisfiablePlacement));
        // should not throw exception if placement is satisfiable
        goal.initGoalState(clusterModel, new OptimizationOptions(Collections.emptySet()));
    }

    @Test
    public void testRebalanceForBroker() throws OptimizationFailureException {
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T2, topic2SatisfiablePlacement));
        OptimizationOptions options = new OptimizationOptions(Collections.emptySet());
        goal.initGoalState(clusterModel, options);
        assertThrows("Expect verification to detect failed optimization before rebalancing", OptimizationFailureException.class,
                () -> goal.updateGoalState(clusterModel, Collections.emptySet()));
        // we want to move a replica off of broker 1 to broker 2
        Broker broker = clusterModel.broker(1);
        goal.rebalanceForBroker(broker, clusterModel, Collections.emptySet(), options);
        Map<String, List<Partition>> partitionsByTopic = clusterModel.getPartitionsByTopic();
        List<Partition> topic2Partitions = partitionsByTopic.get(DeterministicCluster.T2);
        Rack rack0 = clusterModel.rack("0");
        Rack rack1 = clusterModel.rack("1");
        assertTrue("Expected partitions to span rack 0", topic2Partitions.stream().allMatch(partition -> partition.containsRack(rack0)));
        assertTrue("Expected partitions to span rack 1", topic2Partitions.stream().allMatch(partition -> partition.containsRack(rack1)));
        // expect validation not to throw an exception
        goal.updateGoalState(clusterModel, Collections.emptySet());
    }

    @Test
    public void testRebalanceForBrokerRoleSwap() throws OptimizationFailureException {
        observerClusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, topic1ObserverPlacement));
        OptimizationOptions options = new OptimizationOptions(Collections.emptySet());
        goal.initGoalState(observerClusterModel, options);
        assertThrows("Expect verification to detect failed optimization before rebalancing", OptimizationFailureException.class,
                () -> goal.updateGoalState(observerClusterModel, Collections.emptySet()));
        // we expect the replica on broker 1 to change role in-place
        Broker brokerToBalance = observerClusterModel.broker(1);
        goal.rebalanceForBroker(brokerToBalance, observerClusterModel, Collections.emptySet(), options);
        Map<String, List<Partition>> partitionsByTopic = observerClusterModel.getPartitionsByTopic();
        Partition topic1Partition = partitionsByTopic.get(DeterministicCluster.T1).get(0);
        assertTrue("Expected replica on broker 1 to become an observer",
                topic1Partition.partitionObserverBrokers().contains(brokerToBalance));
    }

    @Test
    public void testRebalanceForBrokerIgnoresExcludedTopics() throws OptimizationFailureException {
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T2, topic2SatisfiablePlacement));
        Set<String> excludedTopics = Collections.singleton(DeterministicCluster.T2);
        OptimizationOptions options = new OptimizationOptions(excludedTopics);
        goal.initGoalState(clusterModel, options);
        // we should not move any replicas
        Broker broker = clusterModel.broker(1);
        goal.rebalanceForBroker(broker, clusterModel, Collections.emptySet(), options);
        Map<String, List<Partition>> partitionsByTopic = clusterModel.getPartitionsByTopic();
        List<Partition> topic2Partitions = partitionsByTopic.get(DeterministicCluster.T2);
        Rack rack0 = clusterModel.rack("0");
        Rack rack1 = clusterModel.rack("1");
        // expect replica distribution to be unchanged, T2 will have replicas in rack 0 but not rack 1
        assertTrue("Expected partitions to span rack 0", topic2Partitions.stream().allMatch(partition -> partition.containsRack(rack0)));
        assertFalse("Expected partitions not to span rack 1", topic2Partitions.stream().allMatch(partition -> partition.containsRack(rack1)));
        // expect validation not to throw an exception
        goal.updateGoalState(clusterModel, excludedTopics);
    }

    @Test
    public void testReplicaPlacementGoalOptimize() throws OptimizationFailureException {
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T2, topic2MultipleMovesPlacement));
        OptimizationOptions options = new OptimizationOptions(Collections.emptySet());
        validateGoalOptimization(clusterModel, topic2MultipleMovesPlacement, options);
    }

    @Test
    public void testObserverMovementGoalOptimize() throws OptimizationFailureException {
        observerClusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, topic1ObserverPlacement));
        OptimizationOptions options = new OptimizationOptions(Collections.emptySet());
        validateGoalOptimization(observerClusterModel, topic1ObserverPlacement, options);
    }

    @Test
    public void testReplicasMovedOffDeadBroker() throws OptimizationFailureException, ClusterModel.NonExistentBrokerException {
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, topic1Placement));
        clusterModel.setBrokerState(0, Broker.State.DEAD);
        OptimizationOptions options = new OptimizationOptions(Collections.emptySet());
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedReplicaDistribution = clusterModel.getReplicaDistribution();
        Map<TopicPartition, ReplicaPlacementInfo> preOptimizedLeaderDistribution = clusterModel.getLeaderDistribution();
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedObserverDistribution = clusterModel.getObserverDistribution();
        boolean success = goal.optimize(clusterModel, Collections.emptySet(), options);
        assertTrue("Expected goal optimization to succeed", success);
        Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(preOptimizedReplicaDistribution, preOptimizedLeaderDistribution,
                preOptimizedObserverDistribution, clusterModel);
        TopicPartition tp = new TopicPartition(DeterministicCluster.T1, 0);
        for (ExecutionProposal proposal : proposals) {
            Set<ReplicaPlacementInfo> replicasToRemove = proposal.replicasToRemove();
            Set<ReplicaPlacementInfo> replicasToAdd = proposal.replicasToAdd();

            // ensure replicas moved off of broker 0
            assertTrue("Replicas should only be removed from dead broker 0",
                    replicasToRemove.stream().allMatch(placement -> placement.brokerId() == 0));
            assertTrue("Replicas should not be added to dead broker 0",
                    replicasToAdd.stream().noneMatch(placement -> placement.brokerId() == 0));

            // topic partition for T1 can only be moved to broker 1 to keep satisfying replica placement goal
            if (proposal.topicPartition().equals(tp)) {
                assertTrue(replicasToAdd.stream().allMatch(placement -> placement.brokerId() == 1));
            }
        }
    }

    @Test
    public void testReplicaMovementEligibleBrokers() {
        TopicPartition tp = new TopicPartition(DeterministicCluster.T1, 0);
        // use observer cluster model to test rejection of movement to racks not part of constraints
        observerClusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, topic1ObserverPlacement));
        Replica replicaToMove = observerClusterModel.broker(1).replica(tp);
        Set<Broker> eligibleBrokers = new HashSet<>(goal.replicaMovementEligibleBrokers(replicaToMove, observerClusterModel));
        // observer cluster model should be movement constrained for observers and replicas
        assertFalse("Replica should be a sync replica", replicaToMove.isObserver());
        assertEquals("Replica should have no movement candidates", Collections.emptySet(), eligibleBrokers);
        replicaToMove = observerClusterModel.broker(2).replica(tp);
        assertTrue("Replica should be an observer", replicaToMove.isObserver());
        eligibleBrokers = new HashSet<>(goal.replicaMovementEligibleBrokers(replicaToMove, observerClusterModel));
        assertEquals("Replica should have no movement candidates", Collections.emptySet(), eligibleBrokers);

        // use other cluster model to test normal replica movement within constraints
        clusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T2, topic2MultipleMovesPlacement));
        tp = new TopicPartition(DeterministicCluster.T2, 1);
        replicaToMove = clusterModel.broker(2).replica(tp);
        eligibleBrokers = new HashSet<>(goal.replicaMovementEligibleBrokers(replicaToMove, clusterModel));
        Set<Broker> expectedBrokers = new HashSet<>();
        expectedBrokers.add(clusterModel.broker(1));
        assertEquals("Expected partition to be able to move to broker on rack with unsatisfied constraint", expectedBrokers, eligibleBrokers);
    }

    @Test
    public void testGoalOptimizeObserverDistributionChange() throws OptimizationFailureException {
        String topic1ChangeDistributionJson = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"1\"}}," +
                "{\"count\": 1, \"constraints\":{\"rack\":\"2\"}}]," +
                " \"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}]}";
        TopicPlacement changeDistributionPlacement = TopicPlacement.parse(topic1ChangeDistributionJson).get();
        observerClusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, changeDistributionPlacement));
        validateGoalOptimization(observerClusterModel, changeDistributionPlacement, new OptimizationOptions(
                Collections.emptySet()));
    }

    @Test
    public void testObserverMoveToNewBroker() throws OptimizationFailureException, ClusterModel.NonExistentBrokerException {
        String topic1ChangeLeaderToObserver = "{\"version\":1,\"replicas\":[{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}]," +
                " \"observers\": [{\"count\": 2, \"constraints\":{\"rack\":\"1\"}}," +
                " {\"count\": 1, \"constraints\":{\"rack\":\"2\"}}]}";
        TopicPlacement tpLeaderToObserver = TopicPlacement.parse(topic1ChangeLeaderToObserver).get();
        observerClusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, tpLeaderToObserver));
        observerClusterModel.setBrokerState(4, Broker.State.NEW);
        validateGoalOptimization(observerClusterModel, tpLeaderToObserver, new OptimizationOptions(Collections.emptySet()));
    }

    @Test
    public void testGoalOptimizeReplicationFactorChange() throws OptimizationFailureException {
        String topic1DecreaseObserversJson = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"1\"}}]," +
                " \"observers\": [{\"count\": 1, \"constraints\":{\"rack\":\"0\"}}]}";
        TopicPlacement changeRfPlacement = TopicPlacement.parse(topic1DecreaseObserversJson).get();
        observerClusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, changeRfPlacement));
        OptimizationOptions options = new OptimizationOptions(Collections.emptySet());
        validateGoalOptimization(observerClusterModel, changeRfPlacement, options);

        String topic1IncreaseReplicasJson = "{\"version\":1,\"replicas\":[{\"count\": 2, \"constraints\":{\"rack\":\"1\"}}," +
                "{\"count\": 1, \"constraints\":{\"rack\":\"2\"}}]," +
                " \"observers\": [{\"count\": 2, \"constraints\":{\"rack\":\"0\"}}]}";
        changeRfPlacement = TopicPlacement.parse(topic1IncreaseReplicasJson).get();
        observerClusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, changeRfPlacement));
        validateGoalOptimization(observerClusterModel, changeRfPlacement, options);
    }

    private void validateGoalOptimization(ClusterModel cluster, TopicPlacement placement,
                                          OptimizationOptions options) throws OptimizationFailureException {
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedReplicaDistribution = cluster.getReplicaDistribution();
        Map<TopicPartition, ReplicaPlacementInfo> preOptimizedLeaderDistribution = cluster.getLeaderDistribution();
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedObserverDistribution = cluster.getObserverDistribution();

        int expectedObservers = placement.observers().stream().mapToInt(constraint -> constraint.count()).sum();
        int expectedSyncReplicas = placement.replicas().stream().mapToInt(constraint -> constraint.count()).sum();

        Set<Integer> expectedSyncReplicaBrokers = new HashSet<>();
        Set<Integer> expectedObserverBrokers = new HashSet<>();
        for (TopicPlacement.ConstraintCount constraint : placement.replicas()) {
            List<Integer> brokerIds = cluster.aliveBrokersMatchingAttributes(constraint.constraints()).stream().
                    mapToInt(broker -> broker.id()).boxed().collect(Collectors.toList());
            expectedSyncReplicaBrokers.addAll(brokerIds);
        }
        for (TopicPlacement.ConstraintCount constraint : placement.observers()) {
            List<Integer> brokerIds = cluster.aliveBrokersMatchingAttributes(constraint.constraints()).stream().
                    mapToInt(broker -> broker.id()).boxed().collect(Collectors.toList());
            expectedObserverBrokers.addAll(brokerIds);
        }

        boolean success = goal.optimize(cluster, Collections.emptySet(), options);
        assertTrue("Expected goal optimization to succeed", success);

        Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(preOptimizedReplicaDistribution, preOptimizedLeaderDistribution,
                preOptimizedObserverDistribution, cluster, true);
        assertTrue("Expected goal to generate proposals", !proposals.isEmpty());
        for (ExecutionProposal proposal : proposals) {
            List<ReplicaPlacementInfo> syncReplicas = syncReplicas(proposal.newReplicas(), proposal.newObservers());
            assertEquals("Number of observers does not match expected", proposal.newObservers().size(),
                    expectedObservers);
            assertEquals("Number of total replicas does not match expected", proposal.newReplicas().size(),
                    expectedObservers + expectedSyncReplicas);
            List<Integer> actualSyncReplicaBrokers = syncReplicas.stream().mapToInt(
                    ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toList());
            List<Integer> actualObserverBrokers = proposal.newObservers().stream().mapToInt(
                    ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toList());

            // if there are more potential brokers than replicas, validate that all brokers receiving sync replicas
            // are valid sync replica brokers otherwise, validate that all expected brokers receive a sync replica
            if (expectedSyncReplicas < expectedSyncReplicaBrokers.size()) {
                assertTrue("Sync replicas did not move to expected brokers",
                        expectedSyncReplicaBrokers.containsAll(actualSyncReplicaBrokers));
            } else {
                assertEquals("Sync replicas should have been added to all expected sync replica brokers",
                        expectedSyncReplicaBrokers, new HashSet<>(actualSyncReplicaBrokers));
            }
            // same for observers
            if (expectedObservers < expectedObserverBrokers.size()) {
                assertTrue("Observers did not move to expected brokers",
                        expectedObserverBrokers.containsAll(actualObserverBrokers));
            } else {
                assertEquals("Observers should have been added to all expected brokers",
                        expectedObserverBrokers, new HashSet<>(actualObserverBrokers));
            }
        }
    }

    private List<ReplicaPlacementInfo> syncReplicas(List<ReplicaPlacementInfo> allReplicas, List<ReplicaPlacementInfo> observers) {
        List<ReplicaPlacementInfo> syncReplicas = new ArrayList<>(allReplicas);
        syncReplicas.removeAll(observers);
        return syncReplicas;
    }
}
