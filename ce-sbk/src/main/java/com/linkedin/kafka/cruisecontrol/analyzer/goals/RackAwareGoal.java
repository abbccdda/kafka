/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.BROKER_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;


/**
 * HARD GOAL: Generate replica movement proposals to provide rack-aware replica distribution.
 */
public class RackAwareGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(RackAwareGoal.class);

  private Map<String, Integer> _replicaCountsPerRack;

  /**
   * Constructor for Rack Capacity Goal.
   */
  public RackAwareGoal() {

  }

  /**
   * Package private for unit test.
   */
  RackAwareGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
  }

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable by a goal if it satisfies
   * requirements of the goal. Requirements(hard goal): rack awareness.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#BROKER_REJECT} if the action is rejected due to violating rack awareness in the destination
   * broker after moving source replica to destination broker, {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    // If a topic has placement constraints, do not apply this goal to it
    if (clusterModel.getTopicPlacement(action.topic()) != null) {
      return ACCEPT;
    }

    switch (action.balancingAction()) {
      case LEADERSHIP_MOVEMENT:
        return ACCEPT;
      case INTER_BROKER_REPLICA_MOVEMENT:
      case INTER_BROKER_REPLICA_SWAP:
        if (isReplicaMoveViolateRackAwareness(clusterModel,
                                              c -> c.broker(action.sourceBrokerId()).replica(action.topicPartition()),
                                              c -> c.broker(action.destinationBrokerId()))) {
          return BROKER_REJECT;
        }

        if (action.balancingAction() == ActionType.INTER_BROKER_REPLICA_SWAP
            && isReplicaMoveViolateRackAwareness(clusterModel,
                                                 c -> c.broker(action.destinationBrokerId()).replica(action.destinationTopicPartition()),
                                                 c -> c.broker(action.sourceBrokerId()))) {
          return REPLICA_REJECT;
        }
        return ACCEPT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isReplicaMoveViolateRackAwareness(ClusterModel clusterModel,
                                                    Function<ClusterModel, Replica> sourceReplicaFunction,
                                                    Function<ClusterModel, Broker> destinationBrokerFunction) {
    Replica sourceReplica = sourceReplicaFunction.apply(clusterModel);
    Broker sourceBroker = sourceReplica.broker();
    Broker destinationBroker = destinationBrokerFunction.apply(clusterModel);

    // Short-circuit if the source and destination brokers are in the same rack
    if (sourceBroker.rack().id().equals(destinationBroker.rack().id())) {
      return false;
    }

    Map<String, Integer> replicaCountsPerRack = computeReplicaCounts(sourceReplica, clusterModel);

    // The move violates rack awareness if the destination rack has as many or more replicas of the partition as the source one
    return replicaCountsPerRack.get(destinationBroker.rack().id()) >= replicaCountsPerRack.get(sourceBroker.rack().id());
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new RackAwareGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    // We only need the latest snapshot and include all the topics.
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  /**
   * Get the name of this goal. Name of a goal provides an identification for the goal in human readable format.
   */
  @Override
  public String name() {
    return RackAwareGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return true;
  }

  /**
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return True if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    return true;
  }

  /**
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
   * Get brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.brokers();
  }

  /**
   * This is a hard goal; hence, the proposals are not limited to dead broker replicas in case of self-healing.
   * Sanity Check: There exists sufficient number of racks for achieving rack-awareness.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions) {
    // Initialize a map of replica counts per rack. Initializing this here allows us to check the alive racks once
    // and to re-use the map object to avoid multiple allocations
    _replicaCountsPerRack = clusterModel.aliveRackIds().stream().collect(Collectors.toMap(k -> k, k -> 0));
  }

  /**
   * Update goal state.
   * (1) Sanity check: After completion of balancing / self-healing all resources, confirm that replicas of each
   * partition reside at a separate rack and finish.
   * (2) Update the current resource that is being balanced if there are still resources to be balanced.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    // One pass is sufficient to satisfy or alert impossibility of this goal.
    // Sanity check to confirm that the final distribution is rack aware.
    ensureRackAware(clusterModel, excludedTopics);
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    GoalUtils.ensureNoOfflineReplicas(clusterModel, name());
    // Sanity check: No replica should be moved to a broker, which used to host any replica of the same partition on its broken disk.
    GoalUtils.ensureReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
  }

  @Override
  public void finish() {
    _finished = true;
  }

  /**
   * Rack-awareness violations can be resolved with replica movements.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization -- e.g. excluded topics.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    LOG.debug("balancing broker {}, optimized goals = {}", broker, optimizedGoals);
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
    // Satisfy rack awareness requirement. Note that the default replica comparator prioritizes offline replicas.
    SortedSet<Replica> replicas = new TreeSet<>(broker.replicas());
    for (Replica replica : replicas) {
      if (shouldExclude(replica, excludedTopics) ||
              (onlyMoveImmigrantReplicas && !replica.isImmigrant()) ||
              clusterModel.getTopicPlacement(replica.topicPartition().topic()) != null)   {
        continue;
      }

      Map<String, Integer> replicaCountsPerRack = computeReplicaCounts(replica, clusterModel);
      if (broker.isAlive() && !broker.currentOfflineReplicas().contains(replica) && satisfiedRackAwareness(replica, replicaCountsPerRack)) {
        continue;
      }

      // Rack awareness is violated. Move replica to a broker in another rack.
      if (maybeApplyBalancingAction(clusterModel, replica, rackAwareEligibleBrokers(replica, clusterModel, replicaCountsPerRack),
                                    ActionType.INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions) == null) {
        throw new OptimizationFailureException(
            String.format("[%s] Violated rack-awareness requirement for broker with id %d.", name(), broker.id()));
      }
    }
  }

  private void ensureRackAware(ClusterModel clusterModel, Set<String> excludedTopics) throws OptimizationFailureException {
    // Sanity check to confirm that the final distribution is rack aware.
    for (Replica leader : clusterModel.leaderReplicas()) {
      TopicPartition tp = leader.topicPartition();
      if (excludedTopics.contains(tp.topic()) || clusterModel.getTopicPlacement(tp.topic()) != null) {
        continue;
      }

      Map<String, Integer> replicaCountsPerRack = computeReplicaCounts(leader, clusterModel);

      Map.Entry<String, Integer> maxReplicaCount =
              replicaCountsPerRack.entrySet().stream().max(Comparator.comparingInt(Map.Entry::getValue)).get();
      Map.Entry<String, Integer> minReplicaCount =
              replicaCountsPerRack.entrySet().stream().min(Comparator.comparingInt(Map.Entry::getValue)).get();

      if (maxReplicaCount.getValue() - minReplicaCount.getValue() > 1) {
        throw new OptimizationFailureException("Failed to optimize goal " + name() + ": partition " + tp +
                " had " + maxReplicaCount.getValue() + " replicas on rack " + maxReplicaCount.getKey() + " but " +
                minReplicaCount.getValue() + " replicas on rack " + minReplicaCount.getKey() + " after optimization");
      }
    }
  }

  /**
   * Get a list of rack aware eligible brokers for the given replica in the given cluster. A broker is rack aware
   * eligible for a given replica if the broker resides in a rack which has more than 1 fewer replica of the partition
   * than the replica's rack does
   *
   * @param replica      Replica for which a set of rack aware eligible brokers are requested.
   * @param clusterModel The state of the cluster.
   * @return A list of rack aware eligible brokers for the given replica in the given cluster.
   */
  private SortedSet<Broker> rackAwareEligibleBrokers(Replica replica, ClusterModel clusterModel, Map<String, Integer> replicaCountsPerRack) {
    SortedSet<Broker> rackAwareEligibleBrokers =
            new TreeSet<>(Comparator.<Broker>comparingInt(b -> replicaCountsPerRack.get(b.rack().id()))
                          .thenComparingInt(Broker::id));
    String rackId = replica.broker().rack().id();
    int numReplicasInRack = replicaCountsPerRack.get(rackId);

    // Add all healthy brokers in racks that have more than 1 fewer replica of the partition
    // If the replica's broker is dead or has bad disks, instead add all healthy brokers
    // The set is sorted by replica count, so replicas on unhealthy brokers will not make rack balance worse
    // unless that is the only available option
    return replicaCountsPerRack.entrySet().stream()
            .filter(replicaCount -> numReplicasInRack - replicaCount.getValue() > 1 || replica.isCurrentOffline())
            .flatMap(replicaCount -> clusterModel.rack(replicaCount.getKey()).brokers().stream().filter(Broker::isAlive))
            .collect(Collectors.toCollection(() -> rackAwareEligibleBrokers));
  }

  /**
   * Check whether given replica satisfies rack awareness in the given cluster state. Rack awareness requires
   * replicas be placed as evenly as possible across racks, so that the difference in replica counts between any
   * two racks is no more than 1.
   *
   * @param replica              Replica to check for rack awareness
   * @param replicaCountsPerRack The number of replicas of the partition in each rack
   * @return True if rack awareness is satisfied, false otherwise
   */
  private boolean satisfiedRackAwareness(Replica replica, Map<String, Integer> replicaCountsPerRack) {
    // Rack awareness is violated if the difference in partition replica counts between the replica's rack and any
    // other rack is more than 1
    // _replicaCountsPerRack has already been populated in rebalanceForBroker()
    int replicasInRack = replicaCountsPerRack.get(replica.broker().rack().id());
    for (Map.Entry<String, Integer> replicaCountForRack : replicaCountsPerRack.entrySet()) {
      int count = replicaCountForRack.getValue();
      if (replicasInRack - count > 1) {
        return false;
      }
    }

    return true;
  }

  private Map<String, Integer> computeReplicaCounts(Replica replica, ClusterModel clusterModel) {
    TopicPartition tp = replica.topicPartition();
    _replicaCountsPerRack.replaceAll((i, v) -> 0);
    Set<Broker> brokers = clusterModel.partition(tp).partitionBrokers();

    // Add rack Id of replicas.
    for (Broker broker : brokers) {
      String rackId = broker.rack().id();
      _replicaCountsPerRack.compute(rackId, (k, v) -> v == null ? 1 : v + 1);
    }

    return _replicaCountsPerRack;
  }

  private static class RackAwareGoalStatsComparator implements ClusterModelStatsComparator {

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
