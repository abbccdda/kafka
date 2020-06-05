/**
 Copyright 2020 Confluent Inc.
 */

package io.confluent.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.AbstractGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Partition;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import kafka.common.TopicPlacement;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;

public class ReplicaPlacementGoal extends AbstractGoal {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaPlacementGoal.class);
    private static final String SYNC_REPLICA_NAME = "sync-replica";
    private static final String OBSERVER_NAME = "observer";
    /**
     * Assuming that the cluster already satisfies replica placement constraints, then:
     * Replica movement from broker A to broker B is accepted in two cases
     *      1. There is no TopicPlacement for the topic.
     *      2. Broker A has the same attributes as broker B. This must be the case, because the replica present on
     *      broker A must have been placed there to satisfy some rack constraint.
     *      This is because topic placement constraints have to span all replicas of a topic. If that changes, we may
     *      have to relax the actionAcceptance criteria.
     *
     * Swapping replica 1 from broker A with replica 2 from broker B is accepted when replica movement as defined above
     * in both directions is acceptable.
     */
    @Override
    public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Broker destBroker = clusterModel.broker(action.destinationBrokerId());
        Replica sourceReplica = sourceBroker.replica(action.topicPartition());
        Replica destReplica = destBroker.replica(action.destinationTopicPartition());
        switch (action.balancingAction()) {
            case LEADERSHIP_MOVEMENT:
                return destReplica.isObserver() ? REPLICA_REJECT : ACCEPT;
            case INTER_BROKER_REPLICA_MOVEMENT:
            case INTER_BROKER_REPLICA_SWAP:
                if (!isReplicaMovementValid(clusterModel, sourceBroker, destBroker, sourceReplica)) {
                    return BROKER_REJECT;
                }
                if (action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP &&
                    !isReplicaMovementValid(clusterModel, destBroker, sourceBroker, destReplica)) {
                    return REPLICA_REJECT;
                }
                return ACCEPT;
            default:
                throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
        }
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new ReplicaPlacementGoalStatsComparator();
    }

    @Override
    public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
        // We only need the latest snapshot and include all the topics.
        return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
    }

    @Override
    public String name() {
        return ReplicaPlacementGoal.class.getSimpleName();
    }

    @Override
    public void finish() {
        _finished = true;
    }

    @Override
    public boolean isHardGoal() {
        return true;
    }

    @Override
    protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
        return clusterModel.brokers();
    }

    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
        return true;
    }

    /**
     * Sanity check:
     * There must be at least enough brokers matching a particular constraint across both replicas and observers
     */
    @Override
    protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) throws OptimizationFailureException {
        SortedMap<String, List<Partition>> partitionsByTopic = clusterModel.getPartitionsByTopic();
        Set<String> excludedTopics = optimizationOptions.excludedTopics();
        partitionsByTopic.keySet().removeAll(excludedTopics);
        partitionsByTopic.keySet().removeIf(topic -> clusterModel.getTopicPlacement(topic) == null);
        for (Map.Entry<String, List<Partition>> topicEntry : partitionsByTopic.entrySet()) {
            String topic = topicEntry.getKey();
            TopicPlacement topicPlacement = clusterModel.getTopicPlacement(topic);
            // validate that there are enough brokers with the desired attributes to satisfy the placement constraints
            Map<Map<String, String>, Integer> minBrokersMatchingAttributes = numReplicasByAttribute(topicPlacement);
            for (Map.Entry<Map<String, String>, Integer> entry : minBrokersMatchingAttributes.entrySet()) {
                Map<String, String> constraintAttributes = entry.getKey();
                Integer numReplicas = entry.getValue();
                int numBrokersMatchingAttributes = clusterModel.aliveBrokersMatchingAttributes(constraintAttributes).size();
                if (numBrokersMatchingAttributes < numReplicas) {
                    throw new OptimizationFailureException(String.format("Insufficient number of brokers matching attributes" +
                            " %s to satisfy topic placement constraint. Current: %d, Required: %d", constraintAttributes,
                            numBrokersMatchingAttributes, numReplicas));
                }
            }
            //TODO: alter the number of replicas in the cluster to match the placement config
        }
    }

    /**
     * Validate that all topic placements have been satisfied.
     */
    @Override
    protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics) throws OptimizationFailureException {
        // One pass is sufficient to satisfy or alert impossibility of this goal.
        // Sanity check to confirm that the final distribution satisfies replica placement.
        Map<String, List<Partition>> topicsToBalance = clusterModel.getPartitionsByTopic();
        topicsToBalance.keySet().removeAll(excludedTopics);
        topicsToBalance.keySet().removeIf(topic -> clusterModel.getTopicPlacement(topic) == null);
        // ensure that there are no more replica movements required
        validateTopicPlacements(clusterModel, topicsToBalance);
        // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
        GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
        // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
        GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
        finish();
    }

    /**
     * Try to rebalance each broker by moving any replicas that exceed the total number of partition replicas for a rack
     * to a rack with unsatisfied constraints.
     *
     * After a broker has been rebalanced, there should not be any role-replicas on the broker that are over the
     * role-specific constraint for the replica. We satisfy this condition for the current broker by potentially
     * changing observership for any replicas that stay on the broker.
     *
     * To maintain this condition, we also have to consider role change for any replicas that are relocated to other
     * brokers.
     */
    @Override
    protected void rebalanceForBroker(Broker broker, ClusterModel clusterModel, Set<Goal> optimizedGoals, OptimizationOptions optimizationOptions) throws OptimizationFailureException {
        LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
        Set<String> excludedTopics = optimizationOptions.excludedTopics();
        boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
        SortedSet<Replica> replicas = new TreeSet<>(broker.replicas());
        for (Replica replica : replicas) {
            if (shouldExclude(replica, excludedTopics) || (onlyMoveImmigrantReplicas && !replica.isImmigrant())) {
                continue;
            }
            boolean moveOfflineReplica = !broker.isAlive() || broker.currentOfflineReplicas().contains(replica);
            if (moveOfflineReplica || shouldMoveReplica(replica, clusterModel)) {
                // replica shouldn't be on this rack, try to move it to a broker on a different rack with an unsatisfied
                // constraint
                List<Broker> eligibleBrokers = replicaMovementEligibleBrokers(replica, clusterModel);
                LOG.debug("Trying to satisfy constraint for replica {} by moving to potential brokers {}", replica, eligibleBrokers);
                if (maybeApplyBalancingAction(clusterModel, replica, eligibleBrokers, ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                        optimizedGoals, optimizationOptions) == null) {
                    throw new OptimizationFailureException(String.format("[%s] Could not move replica %s off broker %d to " +
                            "satisfy topic placement.", name(), replica, broker.id()));
                }
            }
            // adjust observership regardless of whether or not we moved the replica
            if (shouldChangeObservership(replica, clusterModel)) {
                String nextRole = replica.isObserver() ? SYNC_REPLICA_NAME : OBSERVER_NAME;
                LOG.debug("Changing observership for replica {} to {}", replica, nextRole);
                replica.setObservership(!replica.isObserver());
            }
            Partition partition = clusterModel.partition(replica.topicPartition());
            boolean tryLeadershipChange = partition.leader().isObserver();
            // leadership change can fail due to intermediate states where replicas have been demoted to observers,
            // but promotion of observers to sync-replicas hasn't happened yet. We validate that leadership has moved off
            // of observers in updateGoalState validation
            if (tryLeadershipChange && maybeApplyBalancingAction(clusterModel, partition.leader(), partition.partitionSyncBrokers(),
                    ActionType.LEADERSHIP_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
                LOG.debug("Failed to move leadership off of observer replica {} for partition {}", replica, partition);
            }
        }
    }

    /**
     * Determine whether a replica should change their observership status. At this point, the replica must be on a rack
     * to satisfy a particular rack constraint. If it is not satisfying a constraint for its current role, then it must
     * be to satisfy a constraint for another role.
     */
    private boolean shouldChangeObservership(Replica replica, ClusterModel clusterModel) {
        TopicPartition tp = replica.topicPartition();
        if (clusterModel.getTopicPlacement(tp.topic()) == null) {
            return false;
        }
        Map<String, String> attributes = replica.broker().attributes();
        int constraintCount = countForConstraint(constraintsForReplica(replica, clusterModel), attributes);
        int roleReplicasOnRack = numReplicasMatchingConstraint(attributes, tp, clusterModel, replica.isObserver());
        return roleReplicasOnRack > constraintCount;
    }

    /**
     * Validate that all topics in the cluster model with topic placement constraints have their topic placement
     * constraints satisfied.
     */
    private void validateTopicPlacements(ClusterModel clusterModel, Map<String, List<Partition>> partitionsByTopic)
            throws OptimizationFailureException {
        for (Map.Entry<String, List<Partition>> entry : partitionsByTopic.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();
            TopicPlacement topicPlacement = clusterModel.getTopicPlacement(topic);
            for (TopicPlacement.ConstraintCount constraint : topicPlacement.replicas()) {
                validateConstraint(constraint, partitions, clusterModel, false);
            }
            for (TopicPlacement.ConstraintCount constraint : topicPlacement.observers()) {
                validateConstraint(constraint, partitions, clusterModel, true);
            }
        }
    }

    /**
     * Validate that a topic placement constraint is satisfied by checking that all partitions have the specified
     * number of sync replicas or observers on brokers with matching attributes. Also validate that there are no
     * observers with leadership.
     */
    private void validateConstraint(TopicPlacement.ConstraintCount constraint, List<Partition> partitions,
                                    ClusterModel clusterModel, boolean isObserver) throws OptimizationFailureException {
        Map<String, String> attributes = constraint.constraints();
        int expectedCount = constraint.count();
        for (Partition partition : partitions) {
            TopicPartition tp = partition.topicPartition();
            int numReplicasMatchingConstraint = numReplicasMatchingConstraint(attributes, tp, clusterModel, isObserver);
            if (expectedCount != numReplicasMatchingConstraint) {
                String replicaType = isObserver ? OBSERVER_NAME : SYNC_REPLICA_NAME;
                throw new OptimizationFailureException(String.format("[%s] Violated %s topic placement requirement " +
                        "for attributes: %s with partition: %s. Required: %d replicas, Actual: %d replicas", name(),
                        replicaType, attributes, tp, expectedCount, numReplicasMatchingConstraint));
            }
            if (partition.leader().isObserver()) {
                throw new OptimizationFailureException(String.format("[%s] Violated failed goal optimization, failed " +
                        "to move leadership off of observer replica for partition %s during plan computation", name(), tp));
            }
        }
    }

    /**
     *  Replica/observer movement is valid if the destination broker has the same attributes as the source broker
     *  or if replica has no topic placement specified. This will hold when rack is the only broker attribute we
     *  allow constraints on. Once we add more broker attributes, then this validity check may be too specific, as
     *  the topic placement constraint may only consider a subset of the broker attributes.
     */
    private boolean isReplicaMovementValid(ClusterModel clusterModel, Broker sourceBroker, Broker destBroker, Replica replica) {
        List<TopicPlacement.ConstraintCount> constraints = constraintsForReplica(replica, clusterModel);
        return constraints.isEmpty() || sourceBroker.attributes().equals(destBroker.attributes());
    }

    /**
     * A broker is eligible to receive a replica if the broker does not host any replicas for the partition and the
     * broker is on a rack that can receive more replicas.
     * Visible for testing
     */
    List<Broker> replicaMovementEligibleBrokers(Replica replica, ClusterModel clusterModel) {
        List<Broker> eligibleBrokers = new ArrayList<>();
        String topic = replica.topicPartition().topic();
        Map<Map<String, String>, Integer> constraints = numReplicasByAttribute(clusterModel.getTopicPlacement(topic));
        if (constraints.isEmpty()) {
            eligibleBrokers.addAll(clusterModel.aliveBrokers());
        } else {
            for (Map.Entry<Map<String, String>, Integer> constraint : constraints.entrySet()) {
                Map<String, String> constraintAttributes = constraint.getKey();
                Integer constraintCount = constraint.getValue();
                int totalReplicasSatisfyingConstraint = totalReplicasMatchingConstraint(constraintAttributes,
                        replica.topicPartition(), clusterModel);
                // this rack has an unsatisfied constraint, we can move a replica here
                if (totalReplicasSatisfyingConstraint < constraintCount) {
                    eligibleBrokers.addAll(clusterModel.aliveBrokersMatchingAttributes(constraintAttributes));
                }
            }
        }
        Set<Broker> partitionBrokers = clusterModel.partition(replica.topicPartition()).partitionBrokers();
        eligibleBrokers.removeAll(partitionBrokers);
        // shuffle the list of brokers so that we distribute topic partition replicas evenly
        Collections.shuffle(eligibleBrokers);
        return eligibleBrokers;
    }

    /**
     * We should move a replica if it has a topic placement specified and there are more replicas on the rack than
     * specified by the constraint.
     */
    private boolean shouldMoveReplica(Replica replica, ClusterModel clusterModel) {
        String topic = replica.topicPartition().topic();
        Map<Map<String, String>, Integer> constraints = numReplicasByAttribute(clusterModel.getTopicPlacement(topic));
        if (constraints.isEmpty()) {
            return false;
        }
        Map<String, String> brokerAttributes = replica.broker().attributes();
        Integer attributeConstraintCount = constraints.getOrDefault(brokerAttributes, 0);
        return totalReplicasMatchingConstraint(brokerAttributes, replica.topicPartition(), clusterModel) > attributeConstraintCount;
    }

    /**
     * Computes the number of alive replicas on a rack. This considers both sync replicas and observer replicas.
     */
    private int totalReplicasMatchingConstraint(Map<String, String> constraint, TopicPartition topicPartition, ClusterModel clusterModel) {
        return numReplicasMatchingConstraint(constraint, topicPartition, clusterModel, true) +
                numReplicasMatchingConstraint(constraint, topicPartition, clusterModel, false);
    }

    /**
     * Compute the number of alive sync-replicas or observers that are on a rack.
     */
    private int numReplicasMatchingConstraint(Map<String, String> constraint, TopicPartition topicPartition,
                                              ClusterModel clusterModel, boolean isObserver) {
        Partition partition = clusterModel.partition(topicPartition);
        Set<Broker> partitionBrokersOnRack = isObserver ? partition.partitionObserverBrokers() : partition.partitionSyncBrokers();
        partitionBrokersOnRack.retainAll(clusterModel.aliveBrokersMatchingAttributes(constraint));
        return partitionBrokersOnRack.size();
    }

    private int countForConstraint(List<TopicPlacement.ConstraintCount> constraints, Map<String, String> attributes) {
        return constraints.stream().filter(constraint -> constraint.matches(attributes))
                .mapToInt(TopicPlacement.ConstraintCount::count).sum();
    }

    /**
     * Return the list of observer or sync-replica constraints based on the replica type
     */
    private List<TopicPlacement.ConstraintCount> constraintsForReplica(Replica replica, ClusterModel clusterModel) {
        TopicPlacement topicPlacement = clusterModel.getTopicPlacement(replica.topicPartition().topic());
        if (topicPlacement == null) {
            return Collections.emptyList();
        }
        return replica.isObserver() ? topicPlacement.observers() : topicPlacement.replicas();
    }

    /**
     * Return the total number of replicas that should be present on a rack regardless of observer/sync-replica status
     */
    private Map<Map<String, String>, Integer> numReplicasByAttribute(TopicPlacement topicPlacement) {
        if (topicPlacement == null) {
            return Collections.emptyMap();
        }
        Stream<TopicPlacement.ConstraintCount> replicaConstraints = topicPlacement.replicas().stream();
        Stream<TopicPlacement.ConstraintCount> observerConstraints = topicPlacement.observers().stream();

        return Stream.concat(replicaConstraints, observerConstraints).collect(Collectors.toMap(
                TopicPlacement.ConstraintCount::constraints, TopicPlacement.ConstraintCount::count, Integer::sum));
    }

    private static class ReplicaPlacementGoalStatsComparator implements ClusterModelStatsComparator {

        @Override
        public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
            // This goal do not care about stats. The optimization would have already failed if the goal is not met.
            return 0;
        }

        @Override
        public String explainLastComparison() {
            return null;
        }
    }
}
