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
import java.util.Arrays;
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
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedReplicaDistribution = clusterModel.getReplicaDistribution();
        Map<TopicPartition, ReplicaPlacementInfo> preOptimizedLeaderDistribution = clusterModel.getLeaderDistribution();
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedObserverDistribution = clusterModel.getObserverDistribution();
        boolean success = goal.optimize(clusterModel, Collections.emptySet(), options);
        assertTrue("Expected goal optimization to succeed", success);
        Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(preOptimizedReplicaDistribution, preOptimizedLeaderDistribution,
                preOptimizedObserverDistribution, clusterModel);
        for (ExecutionProposal proposal : proposals) {
            Set<ReplicaPlacementInfo> replicasToRemove = proposal.replicasToRemove();
            Set<ReplicaPlacementInfo> replicasToAdd = proposal.replicasToAdd();

            assertTrue("Replicas should only be removed from broker 2",
                    replicasToRemove.stream().allMatch(placement -> placement.brokerId() == 2));
            assertTrue("Replicas should not be added to broker 2",
                    replicasToAdd.stream().noneMatch(placement -> placement.brokerId() == 2));
        }
    }

    @Test
    public void testObserverMovementGoalOptimize() throws OptimizationFailureException {
        observerClusterModel.setTopicPlacements(Collections.singletonMap(DeterministicCluster.T1, topic1ObserverPlacement));
        TopicPartition t1p0 = new TopicPartition(DeterministicCluster.T1, 0);
        OptimizationOptions options = new OptimizationOptions(Collections.emptySet());
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedReplicaDistribution = observerClusterModel.getReplicaDistribution();
        Map<TopicPartition, ReplicaPlacementInfo> preOptimizedLeaderDistribution = observerClusterModel.getLeaderDistribution();
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedObserverDistribution = observerClusterModel.getObserverDistribution();
        List<ReplicaPlacementInfo> initialObserverPlacement = preOptimizedObserverDistribution.get(t1p0);
        List<ReplicaPlacementInfo> initialSyncReplicas = syncReplicas(preOptimizedReplicaDistribution.get(t1p0), initialObserverPlacement);
        assertTrue("Sync replicas should not be on brokers 2 or 3 before optimization",
                initialSyncReplicas.stream().noneMatch(placement -> placement.brokerId() >= 2));
        assertTrue("Observers should not be on broker 0 or 1 before optimization",
                initialObserverPlacement.stream().noneMatch(placement -> placement.brokerId() < 2));
        boolean success = goal.optimize(observerClusterModel, Collections.emptySet(), options);
        assertTrue("Expected goal optimization to succeed", success);
        Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(preOptimizedReplicaDistribution, preOptimizedLeaderDistribution,
                preOptimizedObserverDistribution, observerClusterModel);
        for (ExecutionProposal proposal : proposals) {
            List<ReplicaPlacementInfo> syncReplicas = syncReplicas(proposal.newReplicas(), proposal.newObservers());
            assertEquals("Number of observers should be unchanged", proposal.newObservers().size(), 2);
            List<Integer> expectedSyncBrokers = Arrays.asList(2, 3);
            assertTrue("Sync replicas should have moved to brokers 2/3", syncReplicas.stream().allMatch(
                    placement -> expectedSyncBrokers.contains(placement.brokerId())));
            List<Integer> expectedObserverBrokers = Arrays.asList(0, 1);
            assertTrue("Observers should have moved to broker 0/1", proposal.newObservers().stream().allMatch(
                    placement -> expectedObserverBrokers.contains(placement.brokerId())));
        }
    }

    @Test
    public void testReplicasMovedOffDeadBroker() throws OptimizationFailureException {
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
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedReplicaDistribution = observerClusterModel.getReplicaDistribution();
        Map<TopicPartition, ReplicaPlacementInfo> preOptimizedLeaderDistribution = observerClusterModel.getLeaderDistribution();
        Map<TopicPartition, List<ReplicaPlacementInfo>> preOptimizedObserverDistribution = observerClusterModel.getObserverDistribution();
        boolean success = goal.optimize(observerClusterModel, Collections.emptySet(), new OptimizationOptions(Collections.emptySet()));
        assertTrue("Expected goal optimization to succeed", success);
        Set<ExecutionProposal> proposals = AnalyzerUtils.getDiff(preOptimizedReplicaDistribution, preOptimizedLeaderDistribution,
                preOptimizedObserverDistribution, observerClusterModel);
        for (ExecutionProposal proposal : proposals) {
            List<ReplicaPlacementInfo> syncReplicas = syncReplicas(proposal.newReplicas(), proposal.newObservers());
            assertEquals("Number of observers should be 1", proposal.newObservers().size(), 1);
            List<Integer> expectedSyncReplicaBrokers = Arrays.asList(2, 3, 4);
            List<Integer> actualSyncReplicaBrokers = syncReplicas.stream().mapToInt(
                    ReplicaPlacementInfo::brokerId).boxed().collect(Collectors.toList());
            assertTrue("Sync replicas should have moved to brokers 2, 3, 4",
                    actualSyncReplicaBrokers.containsAll(expectedSyncReplicaBrokers));
            List<Integer> expectedObserverBrokers = Arrays.asList(0, 1);
            assertTrue("Observer should have moved to broker 0 or 1", proposal.newObservers().stream().allMatch(
                    placement -> expectedObserverBrokers.contains(placement.brokerId())));
        }
    }

    private List<ReplicaPlacementInfo> syncReplicas(List<ReplicaPlacementInfo> allReplicas, List<ReplicaPlacementInfo> observers) {
        List<ReplicaPlacementInfo> syncReplicas = new ArrayList<>(allReplicas);
        syncReplicas.removeAll(observers);
        return syncReplicas;
    }
}
