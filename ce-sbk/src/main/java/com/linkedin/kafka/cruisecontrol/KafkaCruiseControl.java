/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizerResult;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.PreferredLeaderElectionGoal;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalOptions;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalPhaseBuilder;
import com.linkedin.kafka.cruisecontrol.client.BlockingSendClient;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.common.SbkAdminUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.exception.KafkaCruiseControlException;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorState;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.model.ModelUtils;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.server.BrokerShutdownManager;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.isKafkaAssignerMode;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckBrokersHavingOfflineReplicasOnBadDisks;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckNonExistingGoal;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.sanityCheckOfflineReplicaPresence;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.shouldRefreshClusterAndGeneration;
import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.ANALYZER;
import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.ANOMALY_DETECTOR;
import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.EXECUTOR;
import static com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState.SubState.MONITOR;


/**
 * The main class of Cruise Control.
 */
public class KafkaCruiseControl {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControl.class);
  private final static Integer MD_MAX_REFRESH_ATTEMPTS = 100;
  protected final KafkaCruiseControlConfig _config;
  private final LoadMonitor _loadMonitor;
  private final GoalOptimizer _goalOptimizer;
  private final PlanComputationOptions _defaultPlanComputationOptions;
  private final Long _replicationThrottle;
  private final BrokerShutdownManager _brokerShutdownManager;
  private final ExecutorService _goalOptimizerExecutor;
  private final Executor _executor;
  private final AnomalyDetector _anomalyDetector;
  private final Time _time;

  /**
   * Construct the Cruise Control
   *
   * @param config the configuration of Cruise Control.
   */
  public KafkaCruiseControl(KafkaCruiseControlConfig config, DataBalancerMetricsRegistry metricRegistry,
                            BlockingSendClient.Builder blockingSendClientBuilder) {
    _config = config;
    _time = new SystemTime();
    // initialize some of the static state of Kafka Cruise Control;
    ModelUtils.init(config);
    ModelParameters.init(config);
    SbkAdminUtils adminUtils = new SbkAdminUtils(KafkaCruiseControlUtils.createAdmin(config.originals()), config);

    // Instantiate the components.
    _loadMonitor = new LoadMonitor(config, _time, metricRegistry, KafkaMetricDef.commonMetricDef());
    _goalOptimizerExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("GoalOptimizerExecutor", true, null));
    long demotionHistoryRetentionTimeMs = config.getLong(KafkaCruiseControlConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG);
    long removalHistoryRetentionTimeMs = config.getLong(KafkaCruiseControlConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG);
    _anomalyDetector = new AnomalyDetector(config, _loadMonitor, this, _time, metricRegistry);
    _executor = new Executor(config, _time, metricRegistry, demotionHistoryRetentionTimeMs,
        removalHistoryRetentionTimeMs, _anomalyDetector);
    _goalOptimizer = new GoalOptimizer(config, _loadMonitor, _time, metricRegistry, _executor);
    _defaultPlanComputationOptions = new PlanComputationOptions(
        config.getBoolean(KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG),
        config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG),
        config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG)
    );
    _replicationThrottle = _config.getLong(KafkaCruiseControlConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    _brokerShutdownManager = new BrokerShutdownManager(adminUtils, config, blockingSendClientBuilder, _time);
  }

  /**
   * Package private for unit test.
   */
  KafkaCruiseControl(
      KafkaCruiseControlConfig config, LoadMonitor loadMonitor, GoalOptimizer goalOptimizer,
      ExecutorService goalOptimizerExecutor, Executor executor, AnomalyDetector anomalyDetector,
      BrokerShutdownManager shutdownManager, Time time) {
    this._config = config;
    this._loadMonitor = loadMonitor;
    this._goalOptimizer = goalOptimizer;
    this._goalOptimizerExecutor = goalOptimizerExecutor;
    this._executor = executor;
    this._anomalyDetector = anomalyDetector;
    this._defaultPlanComputationOptions = new PlanComputationOptions(
        config.getBoolean(KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG),
        config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG),
        config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG)
    );
    this._replicationThrottle = _config.getLong(KafkaCruiseControlConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG);
    this._brokerShutdownManager = shutdownManager;
    this._time = time;
  }

  /**
   * Start up the Cruise Control.
   */
  public void startUp() {
    LOG.info("Starting Kafka Cruise Control...");
    _executor.startUp();
    _loadMonitor.startUp();
    _anomalyDetector.startDetection();
    _goalOptimizerExecutor.submit(_goalOptimizer);
    LOG.info("Kafka Cruise Control started.");
  }

  public void shutdown() {
    Thread t = new Thread() {
      @Override
      public void run() {
        LOG.info("Shutting down Kafka Cruise Control...");
        _loadMonitor.shutdown();
        _executor.shutdown();
        _anomalyDetector.shutdown();
        _goalOptimizer.shutdown();
        LOG.info("Kafka Cruise Control shutdown completed.");
      }
    };
    t.setDaemon(true);
    t.start();
    try {
      t.join(30000);
    } catch (InterruptedException e) {
      LOG.warn("Cruise Control failed to shutdown in 30 seconds. Exit.");
    }
  }

  /**
   * Drain brokers #{@code removedBrokers} of all of their partition replicas,
   * moving them to other brokers in the cluster.
   *
   * @param removedBrokers The brokers to decommission.
   * @param goals The goal names (i.e. each matching {@link Goal#name()}) to be met when decommissioning the brokers.
   *              When empty all goals will be used.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when decomissioning brokers (if null, no throttling is applied).
   * @param uuid UUID of the execution.
   * @param opts the options to compute the plan with
   * @return The optimization result.
   *
   * @throws KafkaCruiseControlException When any exception occurred during the decommission process.
   */
  public OptimizerResult drainBrokers(Set<Integer> removedBrokers,
                                      List<String> goals,
                                      Long replicationThrottle,
                                      String uuid,
                                      PlanComputationOptions opts)
      throws KafkaCruiseControlException {
    OperationProgress operationProgress = new OperationProgress();
    sanityCheckDryRun(false);
    sanityCheckHardGoalPresence(goals, false);

    OptimizerResult result = computeDrainBrokersPlan(removedBrokers, goals, operationProgress, opts);

    executeRemoval(result.goalProposals(), removedBrokers, isKafkaAssignerMode(goals),
        replicationThrottle, uuid, null);
    return result;
  }

  /**
   * A broker removal consists of 4 steps:
   * 1. Pre-shutdown plan computation - validate that a plan can be computed successfully
   * 2. Broker shutdown - shutdown the broker to be removed and wait for it to leave the cluster
   * 3. Actual plan computation - compute the plan which we'll execute to drain the broker
   * 4. Plan execution - execute the partition reassignments to move replicas away from the broker (drain)
   *
   * Broker removal is a critical operation and overrides any ongoing reassignments
   *
   * @param broker - the ID of the broker to remove
   * @param brokerEpoch - the epoch of the broker to remove, needed for the shutdown request
   * @param progressCallback - a callback utilized for tracking the progress of the remove broker call
   * @param uuid - the unique ID of this operation
   */
  public BrokerRemovalPhaseBuilder.BrokerRemovalExecution removeBroker(int broker, Optional<Long> brokerEpoch, BrokerRemovalCallback progressCallback, String uuid) {
    OperationProgress operationProgress = new OperationProgress();
    Set<Integer> brokersToRemove = new HashSet<>();
    brokersToRemove.add(broker);

    BrokerRemovalOptions removalArgs = new BrokerRemovalOptions(brokersToRemove, brokerEpoch, progressCallback, uuid,
        _defaultPlanComputationOptions, _replicationThrottle, operationProgress);

    BrokerRemovalPhaseBuilder brokerRemovalPhaseBuilder = new BrokerRemovalPhaseBuilder();
    return brokerRemovalPhaseBuilder.composeRemoval(removalArgs, progressCallback,
        removalOpts -> { // 1. Pre-shutdown plan computation - validate that a plan can be computed successfully
          LOG.info("Reserving the Executor and aborting ongoing executions as part of broker removal operation for broker {}", broker);
          Executor.ReservationHandle handle = _executor.reserveAndAbortOngoingExecutions(Duration.ofMinutes(1));
          removalArgs.reservationHandle.set(handle);
          LOG.info("Successfully reserved the Executor");

          computeDrainBrokersPlan(removalOpts.brokersToRemove, Collections.emptyList(), removalOpts.operationProgress, removalOpts.planComputationOptions);
          LOG.info("Successfully computed the remove broker plan for broker {}", broker);
        },
        removalOptions -> { // 2. Broker shutdown - shutdown the broker to be removed and wait for it to leave the cluster
          LOG.info("Attempting to shut down broker {} as part of broker removal operation", broker);
          long start = _time.milliseconds();

          boolean wasShutdown = _brokerShutdownManager.maybeShutdownBroker(broker, brokerEpoch);

          long end = _time.milliseconds();
          if (wasShutdown) {
            LOG.info("Broker {} was shut down successfully in {} milliseconds", broker, end - start);
          } else {
            LOG.info("Broker {} was already shut down prior to broker removal - no shutdown request was sent.", broker);
          }
        },
        removalOptions -> { // 3. Actual plan computation - compute the plan which we'll execute to drain the broker
          OptimizerResult plan = computeDrainBrokersPlan(
              removalOptions.brokersToRemove, Collections.emptyList(), removalOptions.operationProgress, removalOptions.planComputationOptions
          );
          removalOptions.setProposals(plan.goalProposals());
        },
        removalOptions -> { // 4. Plan execution - execute the partition reassignments to move replicas away from the broker (drain)
          executeRemoval(removalOptions.proposals, removalOptions.brokersToRemove, false, _replicationThrottle, uuid, progressCallback);
          LOG.info("Successfully submitted the broker removal plan for broker {} (epoch {})", broker, brokerEpoch);
        }
    );
  }

  /**
   * Computes a plan to drain all of the partition replicas off of #{@code removedBrokers}
   */
  private OptimizerResult computeDrainBrokersPlan(Set<Integer> removedBrokers, List<String> overriddenGoals,
                                                  OperationProgress operationProgress, PlanComputationOptions options)
      throws KafkaCruiseControlException {
    List<Goal> goalsByPriority = goalsByPriority(overriddenGoals);

    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(), modelCompletenessRequirements(goalsByPriority),
          operationProgress);

      sanityCheckBrokersHavingOfflineReplicasOnBadDisks(overriddenGoals, clusterModel);
      removedBrokers.forEach(id -> clusterModel.setBrokerState(id, Broker.State.DEAD));
      return getProposals(clusterModel,
          goalsByPriority,
          operationProgress,
          options.toAllowCapacityEstimation(),
          null,
          options.toExcludeRecentlyDemotedBrokers(),
          options.toExcludeRecentlyRemovedBrokers(),
          false,
          Collections.emptySet());
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Fix offline replicas on cluster -- i.e. move offline replicas to alive brokers.
   *
   * @param dryRun true if no execution is required, false otherwise.
   * @param goals the goals to be met when fixing offline replicas on the given brokers. When empty all goals will be used.
   * @param requirements The cluster model completeness requirements.
   * @param operationProgress the progress to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param skipHardGoalCheck True if the provided {@code goals} do not have to contain all hard goals, false otherwise.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when fixing offline replicas (if null, no throttling is applied).
   * @param uuid UUID of the execution.
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @return the optimization result.
   *
   * @throws KafkaCruiseControlException when any exception occurred during the process of fixing offline replicas.
   */
  public OptimizerResult fixOfflineReplicas(boolean dryRun,
                                                          List<String> goals,
                                                          ModelCompletenessRequirements requirements,
                                                          OperationProgress operationProgress,
                                                          boolean allowCapacityEstimation,
                                                          Integer concurrentInterBrokerPartitionMovements,
                                                          Integer concurrentLeaderMovements,
                                                          boolean skipHardGoalCheck,
                                                          Pattern excludedTopics,
                                                          ReplicaMovementStrategy replicaMovementStrategy,
                                                          Long replicationThrottle,
                                                          String uuid,
                                                          boolean excludeRecentlyDemotedBrokers,
                                                          boolean excludeRecentlyRemovedBrokers)
      throws KafkaCruiseControlException {
    sanityCheckDryRun(dryRun);
    sanityCheckHardGoalPresence(goals, skipHardGoalCheck);
    List<Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements modelCompletenessRequirements =
        modelCompletenessRequirements(goalsByPriority).weaker(requirements);
    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(), modelCompletenessRequirements,
                                                            operationProgress);
      // Ensure that the generated cluster model contains offline replicas.
      sanityCheckOfflineReplicaPresence(clusterModel);
      OptimizerResult result = getProposals(clusterModel,
                                            goalsByPriority,
                                            operationProgress,
                                            allowCapacityEstimation,
                                            excludedTopics,
                                            excludeRecentlyDemotedBrokers,
                                            excludeRecentlyRemovedBrokers,
                                            false,
                                            Collections.emptySet());
      if (!dryRun) {
        executeProposals(result.goalProposals(),
                         Collections.emptySet(),
                         false,
                         concurrentInterBrokerPartitionMovements,
                         null,
                         concurrentLeaderMovements,
                         replicaMovementStrategy,
                         replicationThrottle,
                         uuid);
      }
      return result;
    } catch (KafkaCruiseControlException kcce) {
      throw kcce;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Check whether the given capacity estimation info indicates estimations for any broker when capacity estimation is
   * not permitted.
   *
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param capacityEstimationInfoByBrokerId Capacity estimation info by broker id for which there has been an estimation.
   */
  public static void sanityCheckCapacityEstimation(boolean allowCapacityEstimation,
                                                   Map<Integer, String> capacityEstimationInfoByBrokerId) {
    if (!(allowCapacityEstimation || capacityEstimationInfoByBrokerId.isEmpty())) {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("Allow capacity estimation or fix dependencies to capture broker capacities.%n"));
      for (Map.Entry<Integer, String> entry : capacityEstimationInfoByBrokerId.entrySet()) {
        sb.append(String.format("Broker: %d: info: %s%n", entry.getKey(), entry.getValue()));
      }
      throw new IllegalStateException(sb.toString());
    }
  }

  /**
   * Sanity check that if current request is not a dryrun, there is
   * (1) no ongoing execution in current Cruise Control deployment.
   * (2) no ongoing partition reassignment, which could be triggered by other admin tools or previous Cruise Control deployment.
   * This method helps to fail fast if a user attempts to start an execution during an ongoing admin operation.
   *
   * @param dryRun True if the request is just a dryrun, false if the intention is to start an execution.
   */
  private void sanityCheckDryRun(boolean dryRun) {
    if (dryRun) {
      return;
    }
    if (_executor.hasOngoingExecution()) {
      throw new IllegalStateException("Cannot execute new proposals while there is an ongoing execution.");
    }
    if (_executor.hasOngoingPartitionReassignments()) {
      throw new IllegalStateException("Cannot execute new proposals while there are ongoing partition reassignments.");
    }
    if (executorIsReserved()) {
      throw new IllegalStateException("Cannot execute new proposals while the Executor is reserved.");
    }
  }

  /**
   * Rebalance the cluster
   * @param goals The goal names (i.e. each matching {@link Goal#name()}) to be met during the rebalance.
   *              When empty all goals will be used.
   * @param dryRun Whether it is a dry run or not.
   * @param requirements The cluster model completeness requirements.
   * @param operationProgress The progress of the job to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentIntraBrokerPartitionMovements The maximum number of concurrent intra-broker partition movements
   *                                                (if null, use num.concurrent.intra.broker.partition.movements).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param skipHardGoalCheck True if the provided {@code goals} do not have to contain all hard goals, false otherwise.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param uuid UUID of the execution.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            during the rebalance (if null, no throttling is applied).
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @param ignoreProposalCache True to explicitly ignore the proposal cache, false otherwise.
   * @param isTriggeredByGoalViolation True if rebalance is triggered by goal violation, false otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @param isRebalanceDiskMode Whether rebalance between brokers or disks within the brokers.
   * @return The optimization result.
   * @throws KafkaCruiseControlException When the rebalance encounter errors.
   */
  public OptimizerResult rebalance(List<String> goals,
                                   boolean dryRun,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress,
                                   boolean allowCapacityEstimation,
                                   Integer concurrentInterBrokerPartitionMovements,
                                   Integer concurrentIntraBrokerPartitionMovements,
                                   Integer concurrentLeaderMovements,
                                   boolean skipHardGoalCheck,
                                   Pattern excludedTopics,
                                   ReplicaMovementStrategy replicaMovementStrategy,
                                   Long replicationThrottle,
                                   String uuid,
                                   boolean excludeRecentlyDemotedBrokers,
                                   boolean excludeRecentlyRemovedBrokers,
                                   boolean ignoreProposalCache,
                                   boolean isTriggeredByGoalViolation,
                                   Set<Integer> requestedDestinationBrokerIds,
                                   boolean isRebalanceDiskMode) throws KafkaCruiseControlException {
    sanityCheckDryRun(dryRun);
    OptimizerResult result = getProposals(goals, requirements, operationProgress,
                                          allowCapacityEstimation, skipHardGoalCheck,
                                          excludedTopics, excludeRecentlyDemotedBrokers,
                                          excludeRecentlyRemovedBrokers,
                                          ignoreProposalCache,
                                          isTriggeredByGoalViolation,
                                          requestedDestinationBrokerIds,
                                          isRebalanceDiskMode);
    if (!dryRun) {
      executeProposals(result.goalProposals(), Collections.emptySet(), isKafkaAssignerMode(goals),
                       concurrentInterBrokerPartitionMovements, concurrentIntraBrokerPartitionMovements, concurrentLeaderMovements,
                       replicaMovementStrategy, replicationThrottle, uuid);
    }
    return result;
  }


  /**
   * Add brokers.
   * @param brokerIds The broker ids.
   * @param uuid UUID of the execution.
   * @return The optimization result.
   * @throws InterruptedException if the thread was interrupted.
   * @throws KafkaCruiseControlException When any exception other than InterruptedException occurred during the broker addition.
   */
  public OptimizerResult addBrokers(Set<Integer> brokerIds,
                                    String uuid)
          throws KafkaCruiseControlException, InterruptedException {
    // Metadata is infrequently refreshed, so use its max_age configuration to set up
    // our criteria for checking when the broker is added.
    int mdWaitMs = _config.getInt(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG) / 2;
    int mdMaxWaitMs = _config.getInt(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG) * 2;

    List<String> goals = Collections.emptyList();
    List<Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements modelCompletenessRequirements =
        modelCompletenessRequirements(goalsByPriority);
    OperationProgress operationProgress = new OperationProgress();
    PlanComputationOptions planComputationOptions = _defaultPlanComputationOptions;

    try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
      KafkaCruiseControlUtils.backoff(() -> brokersAreKnown(brokerIds),
              MD_MAX_REFRESH_ATTEMPTS, mdWaitMs, mdMaxWaitMs, _time);

      ClusterModel clusterModel = _loadMonitor.clusterModel(_time.milliseconds(),
          modelCompletenessRequirements,
          operationProgress);
      sanityCheckBrokersHavingOfflineReplicasOnBadDisks(goals, clusterModel);
      brokerIds.forEach(id -> clusterModel.setBrokerState(id, Broker.State.NEW));

      OptimizerResult result = getProposals(clusterModel,
          goalsByPriority,
          operationProgress,
          planComputationOptions.toAllowCapacityEstimation(),
          null,
          true,
          planComputationOptions.toExcludeRecentlyRemovedBrokers(),
          false,
          Collections.emptySet());

      executeProposals(result.goalProposals(),
          Collections.emptySet(),
          isKafkaAssignerMode(goals),
          null,
          null,
          null,
          null,
          _replicationThrottle,
          uuid);
      return result;
    } catch (KafkaCruiseControlException kcce) {
      LOG.warn("AddBroker operation for brokers {} failed with exception ", brokerIds, kcce);
      throw kcce;
    } catch (InterruptedException ie) {
      // Nothing to do at this level, but we should make sure not to swallow it.
      throw ie;
    } catch (Exception e) {
      throw new KafkaCruiseControlException(e);
    }
  }

  /**
   * Get the optimization proposals from the current cluster. The result would be served from the cached result if
   * it is still valid.
   * @param operationProgress the job progress to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @return The optimization result.
   */
  public OptimizerResult getProposals(OperationProgress operationProgress,
                                      boolean allowCapacityEstimation)
      throws KafkaCruiseControlException {
    try {
      return _goalOptimizer.optimizations(operationProgress, allowCapacityEstimation);
    } catch (InterruptedException ie) {
      throw new KafkaCruiseControlException("Interrupted when getting the optimization proposals", ie);
    }
  }

  /**
   * Ignore the cached best proposals when:
   * 1. The caller specified goals, excluded topics, or requested to exclude brokers (e.g. recently removed brokers).
   * 2. Provided completeness requirements contain a weaker requirement than what is used by the cached proposal.
   * 3. There is an ongoing execution.
   * 4. The request is triggered by goal violation detector.
   * 5. The request involves explicitly requested destination broker Ids.
   * 6. The caller wants to rebalance across disks within the brokers.
   * 7. The caller wants to explicitly ignore the cache.
   *
   * @param goals A list of goal names (i.e. each matching {@link Goal#name()}) to optimize. When empty all goals will be used.
   * @param requirements Model completeness requirements.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param excludeBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param ignoreProposalCache True to explicitly ignore the proposal cache, false otherwise.
   * @param isTriggeredByGoalViolation True if proposals is triggered by goal violation, false otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @param isRebalanceDiskMode True to generate proposal to rebalance between disks within the brokers, false otherwise.
   * @return True to ignore proposal cache, false otherwise.
   */
  private boolean ignoreProposalCache(List<String> goals,
                                      ModelCompletenessRequirements requirements,
                                      Pattern excludedTopics,
                                      boolean excludeBrokers,
                                      boolean ignoreProposalCache,
                                      boolean isTriggeredByGoalViolation,
                                      Set<Integer> requestedDestinationBrokerIds,
                                      boolean isRebalanceDiskMode) {

    return (goals != null && !goals.isEmpty()) || excludedTopics != null || excludeBrokers ||
           hasWeakerRequirementsThan(requirements) || _executor.hasOngoingExecution() || isTriggeredByGoalViolation ||
           ignoreProposalCache || !requestedDestinationBrokerIds.isEmpty() || isRebalanceDiskMode;
  }

  /**
   * Check if the cached proposal requirement is weaker than the given requirements.
   * @param requirements Model completeness requirements.
   * @return True if it is weaker.
   */
  private boolean hasWeakerRequirementsThan(ModelCompletenessRequirements requirements) {
    ModelCompletenessRequirements requirementsForCache = _goalOptimizer.modelCompletenessRequirementsForPrecomputing();
    return requirementsForCache.minMonitoredPartitionsPercentage() > requirements.minMonitoredPartitionsPercentage()
            || requirementsForCache.minRequiredNumWindows() > requirements.minRequiredNumWindows()
            || (requirementsForCache.includeAllTopics() && !requirements.includeAllTopics());
  }

  /**
   * Optimize a cluster workload model.
   * @param goals A list of goal names (i.e. each matching {@link Goal#name()}) to optimize. When empty all goals will be used.
   * @param requirements The model completeness requirements to enforce when generating the proposals.
   * @param operationProgress The progress of the job to report.
   * @param allowCapacityEstimation Allow capacity estimation in cluster model if the requested broker capacity is unavailable.
   * @param skipHardGoalCheck True if the provided {@code goals} do not have to contain all hard goals, false otherwise.
   * @param excludedTopics Topics excluded from partition movement (if null, use topics.excluded.from.partition.movement)
   * @param excludeRecentlyDemotedBrokers Exclude recently demoted brokers from proposal generation for leadership transfer.
   * @param excludeRecentlyRemovedBrokers Exclude recently removed brokers from proposal generation for replica transfer.
   * @param ignoreProposalCache True to explicitly ignore the proposal cache, false otherwise.
   * @param isTriggeredByGoalViolation True if proposals is triggered by goal violation, false otherwise.
   * @param requestedDestinationBrokerIds Explicitly requested destination broker Ids to limit the replica movement to
   *                                      these brokers (if empty, no explicit filter is enforced -- cannot be null).
   * @param isRebalanceDiskMode True to generate proposal to rebalance between disks within the brokers, false otherwise.
   * @return The optimization result.
   * @throws KafkaCruiseControlException If anything goes wrong in optimization proposal calculation.
   */
  public OptimizerResult getProposals(List<String> goals,
                                      ModelCompletenessRequirements requirements,
                                      OperationProgress operationProgress,
                                      boolean allowCapacityEstimation,
                                      boolean skipHardGoalCheck,
                                      Pattern excludedTopics,
                                      boolean excludeRecentlyDemotedBrokers,
                                      boolean excludeRecentlyRemovedBrokers,
                                      boolean ignoreProposalCache,
                                      boolean isTriggeredByGoalViolation,
                                      Set<Integer> requestedDestinationBrokerIds,
                                      boolean isRebalanceDiskMode)
      throws KafkaCruiseControlException {
    OptimizerResult result;
    sanityCheckHardGoalPresence(goals, skipHardGoalCheck);
    List<Goal> goalsByPriority = goalsByPriority(goals);
    ModelCompletenessRequirements completenessRequirements = modelCompletenessRequirements(goalsByPriority).weaker(requirements);
    boolean excludeBrokers = excludeRecentlyDemotedBrokers || excludeRecentlyRemovedBrokers;
    if (ignoreProposalCache(goals,
                            completenessRequirements,
                            excludedTopics,
                            excludeBrokers,
                            ignoreProposalCache,
                            isTriggeredByGoalViolation,
                            requestedDestinationBrokerIds,
                            isRebalanceDiskMode)) {
      try (AutoCloseable ignored = _loadMonitor.acquireForModelGeneration(operationProgress)) {
        ClusterModel clusterModel = _loadMonitor.clusterModel(-1,
                                                              _time.milliseconds(),
                                                              completenessRequirements,
                                                              isRebalanceDiskMode,
                                                              operationProgress);
        sanityCheckBrokersHavingOfflineReplicasOnBadDisks(goals, clusterModel);
        result = getProposals(clusterModel,
                              goalsByPriority,
                              operationProgress,
                              allowCapacityEstimation,
                              excludedTopics,
                              excludeRecentlyDemotedBrokers,
                              excludeRecentlyRemovedBrokers,
                              isTriggeredByGoalViolation,
                              requestedDestinationBrokerIds);
      } catch (KafkaCruiseControlException kcce) {
        throw kcce;
      } catch (Exception e) {
        throw new KafkaCruiseControlException(e);
      }
    } else {
      result = getProposals(operationProgress, allowCapacityEstimation);
    }
    return result;
  }

  private OptimizerResult getProposals(ClusterModel clusterModel,
                                       List<Goal> goalsByPriority,
                                       OperationProgress operationProgress,
                                       boolean allowCapacityEstimation,
                                       Pattern requestedExcludedTopics,
                                       boolean excludeRecentlyDemotedBrokers,
                                       boolean excludeRecentlyRemovedBrokers,
                                       boolean isTriggeredByGoalViolation,
                                       Set<Integer> requestedDestinationBrokerIds)
      throws KafkaCruiseControlException {
    sanityCheckCapacityEstimation(allowCapacityEstimation, clusterModel.capacityEstimationInfoByBrokerId());
    if (!requestedDestinationBrokerIds.isEmpty()) {
      sanityCheckBrokerPresence(requestedDestinationBrokerIds);
    }
    synchronized (this) {
      ExecutorState executorState = _executor.state();
      Set<Integer> excludedBrokersForLeadership = excludeRecentlyDemotedBrokers ? executorState.recentlyDemotedBrokers()
                                                                                : Collections.emptySet();

      Set<Integer> excludedBrokersForReplicaMove = excludeRecentlyRemovedBrokers ? executorState.recentlyRemovedBrokers()
                                                                                 : Collections.emptySet();

      return _goalOptimizer.optimizations(clusterModel,
                                          goalsByPriority,
                                          operationProgress,
                                          requestedExcludedTopics,
                                          excludedBrokersForLeadership,
                                          excludedBrokersForReplicaMove,
                                          isTriggeredByGoalViolation,
                                          requestedDestinationBrokerIds,
                                          null,
                                          false);
    }
  }

  public KafkaCruiseControlConfig config() {
    return _config;
  }

  private static boolean hasProposalsToExecute(Collection<ExecutionProposal> proposals, String uuid) {
    if (proposals.isEmpty()) {
      LOG.info("Goals used in proposal generation for UUID {} are already satisfied.", uuid);
      return false;
    }
    return true;
  }

  /**
   * Execute the given balancing proposals for non-(demote/remove) operations.
   * @param proposals the given balancing proposals
   * @param unthrottledBrokers Brokers for which the rate of replica movements from/to will not be throttled.
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   * @param concurrentInterBrokerPartitionMovements The maximum number of concurrent inter-broker partition movements per broker
   *                                                (if null, use num.concurrent.partition.movements.per.broker).
   * @param concurrentIntraBrokerPartitionMovements The maximum number of concurrent intra-broker partition movements
   *                                                (if null, use num.concurrent.intra.broker.partition.movements).
   * @param concurrentLeaderMovements The maximum number of concurrent leader movements
   *                                  (if null, use num.concurrent.leader.movements).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks
   *                                (if null, use default.replica.movement.strategies).
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing proposals (if null, no throttling is applied).
   * @param uuid UUID of the execution.
   */
  private void executeProposals(Set<ExecutionProposal> proposals,
                                Set<Integer> unthrottledBrokers,
                                boolean isKafkaAssignerMode,
                                Integer concurrentInterBrokerPartitionMovements,
                                Integer concurrentIntraBrokerPartitionMovements,
                                Integer concurrentLeaderMovements,
                                ReplicaMovementStrategy replicaMovementStrategy,
                                Long replicationThrottle,
                                String uuid) {
    if (hasProposalsToExecute(proposals, uuid)) {
      // Set the execution mode, add execution proposals, and start execution.
      _executor.setExecutionMode(isKafkaAssignerMode);
      _executor.executeProposals(proposals, unthrottledBrokers, null, _loadMonitor,
                                 concurrentInterBrokerPartitionMovements, concurrentIntraBrokerPartitionMovements,
                                 concurrentLeaderMovements, replicaMovementStrategy, replicationThrottle,
                                 uuid);
    }
  }

  /**
   * Execute the given balancing proposals for remove operations.
   * @param proposals the given balancing proposals
   * @param removedBrokers Brokers to be removed, null if no brokers has been removed.
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing remove operations (if null, no throttling is applied).
   * @param uuid UUID of the execution.
   * @param progressCallback the broker removal callback to help track the progress of the operation
   */
  private void executeRemoval(Set<ExecutionProposal> proposals,
                              Set<Integer> removedBrokers,
                              boolean isKafkaAssignerMode,
                              Long replicationThrottle,
                              String uuid,
                              BrokerRemovalCallback progressCallback) {
    if (hasProposalsToExecute(proposals, uuid)) {
      // Set the execution mode, add execution proposals, and start execution.
      _executor.setExecutionMode(isKafkaAssignerMode);
      _executor.executeRemoveBrokerProposals(proposals, removedBrokers,
          removedBrokers, _loadMonitor, null, 0,
          null, null, replicationThrottle, uuid,
          progressCallback
      );
    } else {
      throw new IllegalStateException(
          String.format("Cannot execute a removal for brokers %s when there are no proposals to execute!", removedBrokers)
      );
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   */
  public synchronized void userTriggeredStopExecution() {
    _executor.userTriggeredStopExecution();
  }

  /**
   * Get the state with selected substates for Kafka Cruise Control.
   */
  public CruiseControlState state(OperationProgress operationProgress,
                                  Set<CruiseControlState.SubState> substates) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = null;
    // In case no substate is specified, return all substates.
    substates = !substates.isEmpty() ? substates
                                     : new HashSet<>(Arrays.asList(CruiseControlState.SubState.values()));

    if (shouldRefreshClusterAndGeneration(substates)) {
      clusterAndGeneration = _loadMonitor.refreshClusterAndGeneration();
    }

    return new CruiseControlState(substates.contains(EXECUTOR) ? _executor.state() : null,
                                  substates.contains(MONITOR) ? _loadMonitor.state(operationProgress, clusterAndGeneration) : null,
                                  substates.contains(ANALYZER) ? _goalOptimizer.state(clusterAndGeneration) : null,
                                  substates.contains(ANOMALY_DETECTOR) ? _anomalyDetector.anomalyDetectorState() : null,
                                  _config);
  }

  public ExecutorState.State executionState() {
    return _executor.state().state();
  }

  public boolean executorIsReserved() {
    return _executor.isReservedByOther();
  }

  private ModelCompletenessRequirements modelCompletenessRequirements(Collection<Goal> overrides) {
    return overrides == null || overrides.isEmpty() ?
           _goalOptimizer.defaultModelCompletenessRequirements() : MonitorUtils.combineLoadRequirementOptions(overrides);
  }

  /**
   * Check if the completeness requirements are met for the given goals.
   *
   * @param goals A list of goals to check completeness for.
   * @return True if completeness requirements are met for the given goals, false otherwise.
   */
  public boolean meetCompletenessRequirements(List<String> goals) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _loadMonitor.refreshClusterAndGeneration();
    return goalsByPriority(goals).stream().allMatch(g -> _loadMonitor.meetCompletenessRequirements(
        clusterAndGeneration, g.clusterModelCompletenessRequirements()));
  }

  /**
   * Get a goals by priority based on the goal list.
   *
   * @param goals A list of goals.
   * @return A list of goals sorted by highest to lowest priority.
   */
  private List<Goal> goalsByPriority(List<String> goals) {
    if (goals == null || goals.isEmpty()) {
      return AnalyzerUtils.getGoalsByPriority(_config);
    }
    Map<String, Goal> allGoals = AnalyzerUtils.getCaseInsensitiveGoalsByName(_config);
    sanityCheckNonExistingGoal(goals, allGoals);
    return goals.stream().map(allGoals::get).collect(Collectors.toList());
  }

  /**
   * Sanity check whether all hard goals are included in provided goal list.
   * There are two special scenarios where hard goal check is skipped.
   * <ul>
   * <li> {@code goals} is null or empty -- i.e. even if hard goals are excluded from the default goals, this check will pass</li>
   * <li> {@code goals} only has PreferredLeaderElectionGoal, denotes it is a PLE request.</li>
   * </ul>
   *
   * @param goals A list of goal names (i.e. each matching {@link Goal#name()}) to check.
   * @param skipHardGoalCheck True if hard goal checking is not needed.
   */
  public void sanityCheckHardGoalPresence(List<String> goals, boolean skipHardGoalCheck) {
    if (goals != null && !goals.isEmpty() && !skipHardGoalCheck &&
        !(goals.size() == 1 && goals.get(0).equals(PreferredLeaderElectionGoal.class.getSimpleName()))) {
      sanityCheckNonExistingGoal(goals, AnalyzerUtils.getCaseInsensitiveGoalsByName(_config));
      Set<String> hardGoals = _config.getList(KafkaCruiseControlConfig.HARD_GOALS_CONFIG).stream()
                                     .map(goalName -> goalName.substring(goalName.lastIndexOf(".") + 1)).collect(Collectors.toSet());
      if (!goals.containsAll(hardGoals)) {
        throw new IllegalArgumentException("Missing hard goals " + hardGoals + " in the provided goals: " + goals
                                           + ". Add skip_hard_goal_check=true parameter to ignore this sanity check.");
      }
    }
  }

  /**
   * Sanity check whether the provided brokers exist in cluster or not.
   * @param brokerIds A set of broker ids.
   */
  public void sanityCheckBrokerPresence(Set<Integer> brokerIds) {
    if (!brokersAreKnown(brokerIds))
      throw new IllegalArgumentException(String.format("Not all brokers in %s are known", brokerIds));
    }

  /**
   * Helper to see if all brokers in brokerIds are present in current cluster metadata.
   * Package-private for testing
   * @param brokerIds
   * @return true if all brokers in the set are present in the cluster metadata
   */
   boolean brokersAreKnown(Set<Integer> brokerIds) {
    Cluster cluster = _loadMonitor.refreshClusterAndGeneration().cluster();
    Set<Integer> invalidBrokerIds = brokerIds.stream().filter(id -> cluster.nodeById(id) == null).collect(Collectors.toSet());
    LOG.info("Search for brokers {} has invalid brokers {}", brokerIds, invalidBrokerIds);
    return invalidBrokerIds.isEmpty();
  }

  /**
   * Update the throttle used for an ongoing execution. This only changes the throttle value for the current
   * execution, it does not change the default throttle
   * @param newThrottle The new value to be used for throttling
   * @throws Exception
   */
  public void updateThrottle(long newThrottle) {
    if (!_executor.updateThrottle(newThrottle)) {
      LOG.warn("Throttle was not updated. This could be either because the set throttle is" +
          "the same as the initially configured one or because the throttle in ZooKeeper" +
          "is equal to the requested throttle");
    }
  }

  /**
   * Enable or disable self healing for the GOAL_VIOLATION anomaly type.
   *
   * @param setSelfHealingEnabled #{@code True} if self healing should be enabled for the GOAL_VIOLATION anomaly type, #{@code false} otherwise.
   * @return The old value of self healing for GOAL_VIOLATIONs.
   */
  public void setGoalViolationSelfHealing(boolean setSelfHealingEnabled) {
    if  (_anomalyDetector.setSelfHealingFor(AnomalyType.GOAL_VIOLATION, setSelfHealingEnabled) != setSelfHealingEnabled) {
      LOG.info("Goal Violation self-healing changed to {}", setSelfHealingEnabled ? "enabled" : "disabled");
    } else {
      LOG.info("Goal violation self-healing left %s (no change)", setSelfHealingEnabled ? "enabled" : "disabled");
    }
  }
}
