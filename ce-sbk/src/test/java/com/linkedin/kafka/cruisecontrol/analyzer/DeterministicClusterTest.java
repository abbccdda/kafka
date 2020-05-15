/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PotentialNwOutGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.RackAwareGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.common.DeterministicCluster;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.TestConstants;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.BROKEN_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.NEW_BROKERS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.NO_SOFT_GOAL_PROPOSALS;
import static com.linkedin.kafka.cruisecontrol.analyzer.OptimizationVerifier.Verification.REGRESSION;
import static org.junit.Assert.assertTrue;


/**
 * Unit test for testing with various balancing percentages and capacity thresholds using various deterministic clusters.
 */
@RunWith(Parameterized.class)
public class DeterministicClusterTest {
  private BalancingConstraint _balancingConstraint;
  private ClusterModel _cluster;
  private List<String> _goalNameByPriority;
  private List<OptimizationVerifier.Verification> _verifications;
  private Class<? extends Throwable> _expectedException;

  /**
   * Constructor for Deterministic Cluster Test.
   *
   * @param balancingConstraint Balancing constraint.
   * @param cluster             The state of the cluster.
   * @param goalNameByPriority  Name of goals by the order of execution priority.
   */
  public DeterministicClusterTest(BalancingConstraint balancingConstraint,
                                  ClusterModel cluster,
                                  List<String> goalNameByPriority,
                                  List<OptimizationVerifier.Verification> verifications,
                                  Class<? extends Throwable> expectedException) {
    _balancingConstraint = balancingConstraint;
    _cluster = cluster;
    _goalNameByPriority = goalNameByPriority;
    _verifications = verifications;
    _expectedException = expectedException;
  }

  /**
   * Populate parameters for the {@link OptimizationVerifier}. All brokers are alive.
   *
   * @return Parameters for the {@link OptimizationVerifier}.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> p = new ArrayList<>();

    List<String> goalNameByPriority = Arrays.asList(RackAwareGoal.class.getName(),
                                                    ReplicaCapacityGoal.class.getName(),
                                                    DiskCapacityGoal.class.getName(),
                                                    NetworkInboundCapacityGoal.class.getName(),
                                                    NetworkOutboundCapacityGoal.class.getName(),
                                                    CpuCapacityGoal.class.getName(),
                                                    ReplicaDistributionGoal.class.getName(),
                                                    PotentialNwOutGoal.class.getName(),
                                                    DiskUsageDistributionGoal.class.getName(),
                                                    NetworkInboundUsageDistributionGoal.class.getName(),
                                                    NetworkOutboundUsageDistributionGoal.class.getName(),
                                                    CpuUsageDistributionGoal.class.getName(),
                                                    LeaderReplicaDistributionGoal.class.getName(),
                                                    LeaderBytesInDistributionGoal.class.getName(),
                                                    TopicReplicaDistributionGoal.class.getName(),
                                                    PreferredLeaderElectionGoal.class.getName());

    List<String> hardAndResourceDistributionGoalsByPriority =
            Arrays.asList(RackAwareGoal.class.getName(),
                    ReplicaCapacityGoal.class.getName(),
                    DiskCapacityGoal.class.getName(),
                    NetworkInboundCapacityGoal.class.getName(),
                    NetworkOutboundCapacityGoal.class.getName(),
                    CpuCapacityGoal.class.getName(),
                    DiskUsageDistributionGoal.class.getName(),
                    NetworkInboundUsageDistributionGoal.class.getName(),
                    NetworkOutboundUsageDistributionGoal.class.getName(),
                    CpuUsageDistributionGoal.class.getName());

    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.MAX_REPLICAS_PER_BROKER_CONFIG, Long.toString(6L));
    BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));
    List<OptimizationVerifier.Verification> verifications = Arrays.asList(NEW_BROKERS, BROKEN_BROKERS, REGRESSION);

    // ----------##TEST: BALANCE PERCENTAGES.
    balancingConstraint.setCapacityThreshold(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    List<Double> balancePercentages = new ArrayList<>();
    balancePercentages.add(TestConstants.HIGH_BALANCE_PERCENTAGE);
    balancePercentages.add(TestConstants.MEDIUM_BALANCE_PERCENTAGE);
    balancePercentages.add(TestConstants.LOW_BALANCE_PERCENTAGE);

    // -- TEST DECK #1: SMALL CLUSTER.
    for (Double balancePercentage : balancePercentages) {
      balancingConstraint.setResourceBalancePercentage(balancePercentage);
      p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY),
                   goalNameByPriority, verifications, null));

      // With a high low utilization threshold
      BalancingConstraint lowUtilizationConstraint = new BalancingConstraint(balancingConstraint);
      lowUtilizationConstraint.setLowUtilizationThreshold(TestConstants.LOW_UTILIZATION_THRESHOLD);
      List<OptimizationVerifier.Verification> lowUtilizationVerifications = new ArrayList<>(verifications);
      lowUtilizationVerifications.add(NO_SOFT_GOAL_PROPOSALS);
      p.add(params(lowUtilizationConstraint, DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY),
                   hardAndResourceDistributionGoalsByPriority, lowUtilizationVerifications, null));
    }
    // -- TEST DECK #2: MEDIUM CLUSTER.
    for (Double balancePercentage : balancePercentages) {
      balancingConstraint.setResourceBalancePercentage(balancePercentage);
      p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY),
                   goalNameByPriority, verifications, null));
      // With a high low utilization threshold
      BalancingConstraint lowUtilizationConstraint = new BalancingConstraint(balancingConstraint);
      lowUtilizationConstraint.setLowUtilizationThreshold(TestConstants.LOW_UTILIZATION_THRESHOLD);
      List<OptimizationVerifier.Verification> lowUtilizationVerifications = new ArrayList<>(verifications);
      lowUtilizationVerifications.add(NO_SOFT_GOAL_PROPOSALS);
      p.add(params(lowUtilizationConstraint, DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY),
              hardAndResourceDistributionGoalsByPriority, lowUtilizationVerifications, null));
    }

    // ----------##TEST: CAPACITY THRESHOLD.
    balancingConstraint.setResourceBalancePercentage(TestConstants.MEDIUM_BALANCE_PERCENTAGE);
    List<Double> capacityThresholds = new ArrayList<>();
    capacityThresholds.add(TestConstants.HIGH_CAPACITY_THRESHOLD);
    capacityThresholds.add(TestConstants.MEDIUM_CAPACITY_THRESHOLD);
    capacityThresholds.add(TestConstants.LOW_CAPACITY_THRESHOLD);

    // -- TEST DECK #3: SMALL CLUSTER.
    for (Double capacityThreshold : capacityThresholds) {
      balancingConstraint.setCapacityThreshold(capacityThreshold);
      p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY),
                   goalNameByPriority, verifications, null));
    }
    // -- TEST DECK #4: MEDIUM CLUSTER.
    for (Double capacityThreshold : capacityThresholds) {
      balancingConstraint.setCapacityThreshold(capacityThreshold);
      p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY),
                   goalNameByPriority, verifications, null));
    }

    // ----------##TEST: BROKER CAPACITY.
    List<Double> brokerCapacities = new ArrayList<>();
    brokerCapacities.add(TestConstants.LARGE_BROKER_CAPACITY);
    brokerCapacities.add(TestConstants.MEDIUM_BROKER_CAPACITY);
    brokerCapacities.add(TestConstants.SMALL_BROKER_CAPACITY);

    // -- TEST DECK #5: SMALL AND MEDIUM CLUSTERS.
    for (Double capacity : brokerCapacities) {
      Map<Resource, Double> testBrokerCapacity = new HashMap<>();
      testBrokerCapacity.put(Resource.CPU, capacity);
      testBrokerCapacity.put(Resource.DISK, capacity);
      testBrokerCapacity.put(Resource.NW_IN, capacity);
      testBrokerCapacity.put(Resource.NW_OUT, capacity);

      p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.smallClusterModel(testBrokerCapacity),
                   goalNameByPriority, verifications, null));
      p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.mediumClusterModel(testBrokerCapacity),
                   goalNameByPriority, verifications, null));
    }

    BalancingConstraint lowUtilizationConstraint = new BalancingConstraint(balancingConstraint);
    lowUtilizationConstraint.setLowUtilizationThreshold(TestConstants.LOW_UTILIZATION_THRESHOLD);
    p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.singleLoadedBrokerModel(TestConstants.BROKER_CAPACITY),
            goalNameByPriority, verifications, null));

    List<String> kafkaAssignerGoals = Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getName(),
                                                    KafkaAssignerDiskUsageDistributionGoal.class.getName());
    List<OptimizationVerifier.Verification> kafkaAssignerVerifications = Arrays.asList(BROKEN_BROKERS, REGRESSION);
    // Small cluster.
    p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.smallClusterModel(TestConstants.BROKER_CAPACITY),
                 kafkaAssignerGoals, kafkaAssignerVerifications, null));
    // Medium cluster.
    p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.mediumClusterModel(TestConstants.BROKER_CAPACITY),
                 kafkaAssignerGoals, kafkaAssignerVerifications, null));
    // Rack-aware satisfiable.
    p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.rackAwareSatisfiable(),
                 kafkaAssignerGoals, kafkaAssignerVerifications, null));
    // Rack-aware unsatisfiable.
    p.add(params(new BalancingConstraint(balancingConstraint), DeterministicCluster.rackAwareUnsatisfiable(),
                 kafkaAssignerGoals, kafkaAssignerVerifications, OptimizationFailureException.class));

    return p;
  }

  private static Object[] params(BalancingConstraint balancingConstraint,
                                 ClusterModel cluster,
                                 List<String> goalNameByPriority,
                                 List<OptimizationVerifier.Verification> verifications,
                                 Class<? extends Throwable> expectedException) {
    return new Object[]{balancingConstraint, cluster, goalNameByPriority, verifications, expectedException};
  }

  @Test
  public void test() throws Exception {
    if (_expectedException == null) {
      try {
        assertTrue("Deterministic Cluster Test failed to improve the existing state.",
                _verifications.contains(NO_SOFT_GOAL_PROPOSALS) ?
                        OptimizationVerifier.executeGoalsFor(_balancingConstraint, _cluster, _goalNameByPriority,
                                Collections.emptySet(), _verifications, true, true) :
                        OptimizationVerifier.executeGoalsFor(_balancingConstraint, _cluster, _goalNameByPriority, _verifications));
      } catch (OptimizationFailureException optimizationFailureException) {
        // This exception is thrown if rebalance fails due to alive brokers having insufficient capacity.
        if (!optimizationFailureException.getMessage().contains("Insufficient healthy cluster capacity for resource")) {
          throw optimizationFailureException;
        }
      }
    } else {
      Assert.assertThrows(_expectedException,
              () -> OptimizationVerifier.executeGoalsFor(_balancingConstraint, _cluster, _goalNameByPriority, _verifications));
    }
  }
}
