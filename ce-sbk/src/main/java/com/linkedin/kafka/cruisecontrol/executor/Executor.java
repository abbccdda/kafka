/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.common.SbkAdminUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.detector.AnomalyDetector;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyType;
import com.linkedin.kafka.cruisecontrol.executor.strategy.ReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import com.linkedin.kafka.cruisecontrol.brokerremoval.BrokerRemovalCallback;
import java.util.HashMap;
import java.util.Optional;
import kafka.admin.PreferredReplicaLeaderElectionCommand;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import scala.collection.JavaConverters;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.OPERATION_LOGGER;
import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils.currentUtcDate;
import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTaskTracker.ExecutionTasksSummary;
import static org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;

/**
 * Executor for Kafka GoalOptimizer.
 * <p>
 * The executor class is responsible for talking to the Kafka cluster to execute the rebalance proposals.
 *
 * The executor is thread-safe.
 */
public class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
  private static final Logger OPERATION_LOG = LoggerFactory.getLogger(OPERATION_LOGGER);
  private static final long EXECUTION_HISTORY_SCANNER_PERIOD_SECONDS = 5;
  private static final long EXECUTION_HISTORY_SCANNER_INITIAL_DELAY_SECONDS = 0;
  // The maximum time to wait for a leader movement to finish. A leader movement will be marked as failed if
  // it takes longer than this time to finish.
  private static final long LEADER_ACTION_TIMEOUT_MS = 180000L;
  private static final String ZK_EXECUTOR_METRIC_GROUP = "CruiseControlExecutor";
  private static final String ZK_EXECUTOR_METRIC_TYPE = "Executor";
  // The execution progress is controlled by the ExecutionTaskManager.
  private final ExecutionTaskManager _executionTaskManager;
  private final MetadataClient _metadataClient;
  private final long _statusCheckingIntervalMs;
  private final ExecutorService _proposalExecutor;
  private final KafkaZkClient _kafkaZkClient;
  private final ConfluentAdmin _adminClient;
  private final SbkAdminUtils adminUtils;

  // Some state for external service to query
  private final AtomicBoolean _stopRequested;
  private final Time _time;
  private volatile boolean _hasOngoingExecution;
  /*
   * A class that allows callers to reserve this Executor instance
   * A reserved Executor is eligible to execute proposals only from the thread
   * that holds the reservation.
   */
  final ExecutorReservation _reservation = new ExecutorReservation();

  private volatile ExecutorState _executorState;
  private volatile String _uuid;
  private final ExecutorNotifier _executorNotifier;

  private AtomicInteger _numExecutionStopped;
  private AtomicInteger _numExecutionStoppedByUser;
  private AtomicBoolean _executionStoppedByUser;
  private AtomicInteger _numExecutionStartedInKafkaAssignerMode;
  private AtomicInteger _numExecutionStartedInNonKafkaAssignerMode;
  private AtomicInteger _numCancelledReassignments = new AtomicInteger(0);
  private AtomicInteger _numFailedReassignmentCancellations = new AtomicInteger(0);
  private volatile boolean _isKafkaAssignerMode;

  private static final String EXECUTION_STARTED = "execution-started";
  private static final String KAFKA_ASSIGNER_MODE = "kafka_assigner";
  private static final String EXECUTION_STOPPED = "execution-stopped";

  private static final String GAUGE_EXECUTION_STOPPED = EXECUTION_STOPPED;
  private static final String GAUGE_EXECUTION_STOPPED_BY_USER = EXECUTION_STOPPED + "-by-user";
  private static final String GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-" + KAFKA_ASSIGNER_MODE;
  private static final String GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE = EXECUTION_STARTED + "-non-" + KAFKA_ASSIGNER_MODE;
  private static final String GAUGE_CANCELLED_REASSIGNMENTS = "cancelled-reassignments";
  private static final String GAUGE_FAILED_REASSIGNMENT_CANCELLATIONS = "failed-reassignment-cancellations";
  // TODO: Execution history is currently kept in memory, but ideally we should move it to a persistent store.
  private final long _demotionHistoryRetentionTimeMs;
  private final long _removalHistoryRetentionTimeMs;
  private final ConcurrentMap<Integer, Long> _latestDemoteStartTimeMsByBrokerId;
  private final ConcurrentMap<Integer, Long> _latestRemoveStartTimeMsByBrokerId;
  private final ScheduledExecutorService _executionHistoryScannerExecutor;
  private final AnomalyDetector _anomalyDetector;
  private final ReplicationThrottleHelper _throttleHelper;

  private final KafkaCruiseControlConfig _config;

  /**
   * The executor class that execute the proposals generated by optimizer.
   *
   * @param config The configurations for Cruise Control.
   */
  public Executor(KafkaCruiseControlConfig config,
                  Time time,
                  DataBalancerMetricsRegistry metricRegistry,
                  long demotionHistoryRetentionTimeMs,
                  long removalHistoryRetentionTimeMs,
                  AnomalyDetector anomalyDetector) {
    this(config, time, metricRegistry, null, demotionHistoryRetentionTimeMs, removalHistoryRetentionTimeMs,
         null, anomalyDetector);
  }

  Executor(KafkaCruiseControlConfig config,
           Time time,
           DataBalancerMetricsRegistry metricRegistry,
           MetadataClient metadataClient,
           long demotionHistoryRetentionTimeMs,
           long removalHistoryRetentionTimeMs,
           ExecutorNotifier executorNotifier,
           AnomalyDetector anomalyDetector) {
    this(config, time, metricRegistry, metadataClient, demotionHistoryRetentionTimeMs,
        removalHistoryRetentionTimeMs, executorNotifier, anomalyDetector,
         KafkaCruiseControlUtils.createAdmin(config.originals()), null);
  }

  /**
   * Package private for unit test.
   */
  Executor(KafkaCruiseControlConfig config,
           Time time,
           DataBalancerMetricsRegistry metricRegistry,
           MetadataClient metadataClient,
           long demotionHistoryRetentionTimeMs,
           long removalHistoryRetentionTimeMs,
           ExecutorNotifier executorNotifier,
           AnomalyDetector anomalyDetector,
           ConfluentAdmin adminClient,
           ReplicationThrottleHelper throttleHelper) {
      String zkUrl = config.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    _numExecutionStopped = new AtomicInteger(0);
    _numExecutionStoppedByUser = new AtomicInteger(0);
    _executionStoppedByUser = new AtomicBoolean(false);
    _numExecutionStartedInKafkaAssignerMode = new AtomicInteger(0);
    _numExecutionStartedInNonKafkaAssignerMode = new AtomicInteger(0);
    _isKafkaAssignerMode = false;
    _config = config;
    // Register gauge sensors.
    registerGaugeSensors(metricRegistry);

    _time = time;
    boolean zkSecurityEnabled = config.getBoolean(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    _kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zkUrl, ZK_EXECUTOR_METRIC_GROUP, ZK_EXECUTOR_METRIC_TYPE,
        zkSecurityEnabled);
    _adminClient = adminClient;
    adminUtils = new SbkAdminUtils(_adminClient, config);
    _executionTaskManager =
        new ExecutionTaskManager(config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG),
                                 config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG),
                                 config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG),
                                 config.getList(KafkaCruiseControlConfig.DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG),
                                 _adminClient,
                                 metricRegistry,
                                 time,
                                 config);
    _metadataClient = metadataClient != null ? metadataClient
                                             : new MetadataClient(config, -1L, time);
    _statusCheckingIntervalMs = config.getLong(KafkaCruiseControlConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    _proposalExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("ProposalExecutor", false, LOG));
    _latestDemoteStartTimeMsByBrokerId = new ConcurrentHashMap<>();
    _latestRemoveStartTimeMsByBrokerId = new ConcurrentHashMap<>();
    _executorState = ExecutorState.noTaskInProgress(recentlyDemotedBrokers(), recentlyRemovedBrokers());
    _stopRequested = new AtomicBoolean(false);
    _hasOngoingExecution = false;
    _uuid = null;
    _executorNotifier = executorNotifier != null ? executorNotifier
                                                 : config.getConfiguredInstance(KafkaCruiseControlConfig.EXECUTOR_NOTIFIER_CLASS_CONFIG,
                                                                                ExecutorNotifier.class);
    _anomalyDetector = anomalyDetector;
    _demotionHistoryRetentionTimeMs = demotionHistoryRetentionTimeMs;
    _removalHistoryRetentionTimeMs = removalHistoryRetentionTimeMs;
    _executionHistoryScannerExecutor = Executors.newSingleThreadScheduledExecutor(
        new KafkaCruiseControlThreadFactory("ExecutionHistoryScanner", true, null));
    _executionHistoryScannerExecutor.scheduleAtFixedRate(new ExecutionHistoryScanner(),
                                                         EXECUTION_HISTORY_SCANNER_INITIAL_DELAY_SECONDS,
                                                         EXECUTION_HISTORY_SCANNER_PERIOD_SECONDS,
                                                         TimeUnit.SECONDS);
    _throttleHelper = throttleHelper != null ? throttleHelper :
            new ReplicationThrottleHelper(_kafkaZkClient, _adminClient,
        config.getLong(KafkaCruiseControlConfig.DEFAULT_REPLICATION_THROTTLE_CONFIG));
  }

  @SuppressWarnings("deprecation")
  public void startUp() {
    // There is a possibility that one execution batch from a previous execution is still ongoing when the
    // Executor starts. Spin up a thread to monitor it and mark it done when it finishes
    if (!_kafkaZkClient.getPartitionReassignment().isEmpty()) {
      _hasOngoingExecution = true;
      LOG.info("Detected ongoing reassignment while starting up. Monitoring it to remove throttles once it completes.");

      Thread thread = new Thread(() -> {
        while (!_kafkaZkClient.getPartitionReassignment().isEmpty()) {
          try {
            LOG.debug("Sleeping {} ms while waiting for ongoing reassignment to complete", _statusCheckingIntervalMs);
            Thread.sleep(_statusCheckingIntervalMs);
          } catch (InterruptedException e) {
            // let it go
          }
        }

        removeThrottles();
        LOG.info("Ongoing reassignment that was detected while starting up has finished.");
        _hasOngoingExecution = false;
      });

      thread.start();
    } else {
      removeThrottles();
    }
  }

  /**
   * Attempts to acquire a reservation on the Executor, blocking other threads from executing proposals
   * and, if successfully reserved, aborts all of its ongoing executions.
   *
   * This method is blocking. After this method returns, the caller is ensured that the Executor is not executing any proposals.
   *
   * The callee is expected to check #{@link #isReservedByOther()} prior to calling this method.
   *
   * @throws IllegalStateException if the Executor is already reserved by another thread
   * @throws TimeoutException if the aborted proposal execution doesn't stop before #{@code executionAbortTimeout}
   * @return a reservation handle #{@code AutoCloseableReservationHandle} if the reservation is taken successfully
   */
  public ReservationHandle reserveAndAbortOngoingExecutions(Duration executionAbortTimeout)
      throws TimeoutException {
    ReservationHandle reservation = null;
    boolean isAbortedSuccessfully = false;
    try {
      reservation = new ReservationHandle();
      // we've got the reservation flag set -> no new executions can be scheduled at this point
      // abort any on-going executions
      abortExecution(executionAbortTimeout);
      isAbortedSuccessfully = true;
      return reservation;
    } finally {
      // give up the reservation if the abortion didn't succeed
      if (!isAbortedSuccessfully && reservation != null) {
        reservation.close();
      }
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   */
  public synchronized void userTriggeredStopExecution() {
    if (stopExecution()) {
      LOG.info("User requested to stop the ongoing proposal execution and cancel the existing reassignments.");
      _numExecutionStoppedByUser.incrementAndGet();
      _executionStoppedByUser.set(true);
    }
  }

  /**
   * Shutdown the executor.
   */
  public synchronized void shutdown() {
    LOG.info("Shutting down executor.");
    if (_hasOngoingExecution) {
      LOG.warn("Shutdown executor may take long because execution is still in progress.");
    }
    _proposalExecutor.shutdown();

    try {
      _proposalExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for anomaly detector to shutdown.");
    }
    _metadataClient.close();
    KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(_kafkaZkClient);
    KafkaCruiseControlUtils.closeAdminClientWithTimeout(_adminClient);
    _executionHistoryScannerExecutor.shutdownNow();
    LOG.info("Executor shutdown completed.");
  }

  /**
   * Whether there is an ongoing operation triggered by current Cruise Control deployment.
   *
   * @return True if there is an ongoing execution.
   */
  public boolean hasOngoingExecution() {
    return _hasOngoingExecution;
  }

  /**
   * Whether the Executor's is reserved (paused) by another thread.
   */
  public boolean isReservedByOther() {
    return _reservation.isReserved() && !_reservation.isReservedByMe();
  }

  /**
   * Update the throttle rate in ZK (for ongoing executions)
   *  and set a new throttle value, persisted in memory until the process dies
   * @param newThrottle The new throttle rate, in bytes
   */
  public boolean updateThrottle(long newThrottle) {
    if (newThrottle < 0) {
      throw new IllegalArgumentException("Cannot set a negative throttle");
    }
    int updatedBrokers;
    boolean updatedRate;
    synchronized (_throttleHelper) {
      updatedRate = _throttleHelper.setThrottleRate(newThrottle);
      updatedBrokers = _throttleHelper.updateOrRemoveThrottleRate(newThrottle);
    }

    if (updatedBrokers == 0) {
      LOG.info("No brokers had throttling rate updated");
    } else {
      LOG.info("Updated throttle rate config on {} brokers to {}", updatedBrokers, newThrottle);
    }
    return updatedBrokers != 0 && updatedRate;
  }

  /**
   * Whether there is any ongoing partition reassignment.
   * This method directly checks the existence of znode /admin/partition_reassignment.
   * Note this method returning false does not guarantee that there is no ongoing execution because when there is an ongoing
   * execution inside Cruise Control, partition reassignment task batches are writen to zookeeper periodically, there will be
   * small intervals that /admin/partition_reassignment does not exist.
   *
   * @return True if there is any ongoing partition reassignment.
   */
  public boolean hasOngoingPartitionReassignments() {
    return !partitionsBeingReassigned().isEmpty();
  }

  /**
   * Recently demoted brokers are the ones for which a demotion was started, regardless of how the process was completed.
   *
   * @return IDs of recently demoted brokers -- i.e. demoted within the last {@link #_demotionHistoryRetentionTimeMs}.
   */
  public Set<Integer> recentlyDemotedBrokers() {
    return Collections.unmodifiableSet(_latestDemoteStartTimeMsByBrokerId.keySet());
  }

  /**
   * Recently removed brokers are the ones for which a removal was started, regardless of how the process was completed.
   *
   * @return IDs of recently removed brokers -- i.e. removed within the last {@link #_removalHistoryRetentionTimeMs}.
   */
  public Set<Integer> recentlyRemovedBrokers() {
    return Collections.unmodifiableSet(_latestRemoveStartTimeMsByBrokerId.keySet());
  }

  /**
   * Check whether the executor is executing a set of proposals.
   */
  public ExecutorState state() {
    return _executorState;
  }

  /**
   * Initialize proposal execution and start execution.
   *
   * @param proposals Proposals to be executed.
   * @param unthrottledBrokers Brokers that are not throttled in terms of the number of in/out replica movements.
   * @param removedBrokers Removed brokers, null if no brokers has been removed.
   * @param loadMonitor Load monitor.
   * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
   *                                                         per broker(if null, use num.concurrent.partition.movements.per.broker).
   * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements
   *                                                         (if null, use num.concurrent.intra.broker.partition.movements).
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements
   *                                               (if null, use num.concurrent.leader.movements).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing a proposal (if null, no throttling is applied).
   * @param uuid UUID of the execution.
   */
  public synchronized void executeProposals(Collection<ExecutionProposal> proposals,
                                            Set<Integer> unthrottledBrokers,
                                            Set<Integer> removedBrokers,
                                            LoadMonitor loadMonitor,
                                            Integer requestedInterBrokerPartitionMovementConcurrency,
                                            Integer requestedIntraBrokerPartitionMovementConcurrency,
                                            Integer requestedLeadershipMovementConcurrency,
                                            ReplicaMovementStrategy replicaMovementStrategy,
                                            Long replicationThrottle,
                                            String uuid) {
    doExecuteProposals(proposals, unthrottledBrokers, loadMonitor, requestedInterBrokerPartitionMovementConcurrency,
        requestedIntraBrokerPartitionMovementConcurrency, requestedLeadershipMovementConcurrency,
        replicaMovementStrategy, uuid,
        new ProposalExecutionRunnable(loadMonitor, null, removedBrokers, replicationThrottle)
    );
  }


  /**
   * Initialize proposal execution and start execution.
   *
   * @param proposals Proposals to be executed.
   * @param unthrottledBrokers Brokers that are not throttled in terms of the number of in/out replica movements.
   * @param removedBrokers Removed brokers, null if no brokers has been removed.
   * @param loadMonitor Load monitor.
   * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
   *                                                         per broker(if null, use num.concurrent.partition.movements.per.broker).
   * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements
   *                                                         (if null, use num.concurrent.intra.broker.partition.movements).
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements
   *                                               (if null, use num.concurrent.leader.movements).
   * @param replicaMovementStrategy The strategy used to determine the execution order of generated replica movement tasks.
   * @param replicationThrottle The replication throttle (bytes/second) to apply to both leaders and followers
   *                            when executing a proposal (if null, no throttling is applied).
   * @param uuid UUID of the execution.
   * @param progressCallback nullable, the broker removal callback to help track the progress of the operation
   */
  public synchronized void executeRemoveBrokerProposals(Collection<ExecutionProposal> proposals,
                                                        Set<Integer> unthrottledBrokers,
                                                        Set<Integer> removedBrokers,
                                                        LoadMonitor loadMonitor,
                                                        Integer requestedInterBrokerPartitionMovementConcurrency,
                                                        Integer requestedIntraBrokerPartitionMovementConcurrency,
                                                        Integer requestedLeadershipMovementConcurrency,
                                                        ReplicaMovementStrategy replicaMovementStrategy,
                                                        Long replicationThrottle,
                                                        String uuid,
                                                        BrokerRemovalCallback progressCallback) {
    doExecuteProposals(proposals, unthrottledBrokers, loadMonitor, requestedInterBrokerPartitionMovementConcurrency,
        requestedIntraBrokerPartitionMovementConcurrency, requestedLeadershipMovementConcurrency,
        replicaMovementStrategy, uuid,
        new BrokerRemovalProposalExecutionRunnable(loadMonitor, null, removedBrokers, replicationThrottle, progressCallback)
    );
  }

  /**
   * see #{@link #executeProposals(Collection, Set, Set, LoadMonitor, Integer, Integer, Integer, ReplicaMovementStrategy, Long, String)} for additional information
   * @param executionRunnable the proposal execution runnable to run
   */
  private synchronized void doExecuteProposals(Collection<ExecutionProposal> proposals,
                                               Set<Integer> unthrottledBrokers,
                                               LoadMonitor loadMonitor,
                                               Integer requestedInterBrokerPartitionMovementConcurrency,
                                               Integer requestedIntraBrokerPartitionMovementConcurrency,
                                               Integer requestedLeadershipMovementConcurrency,
                                               ReplicaMovementStrategy replicaMovementStrategy,
                                               String uuid,
                                               ProposalExecutionRunnable executionRunnable) {
    if (_hasOngoingExecution) {
      throw new IllegalStateException("Cannot execute new proposals while there is an ongoing execution.");
    }
    if (isReservedByOther()) {
      throw new IllegalStateException("Cannot execute new proposals because the Executor is reserved by another thread.");
    }
    if (loadMonitor == null) {
      throw new IllegalArgumentException("Load monitor cannot be null.");
    }
    if (uuid == null) {
      throw new IllegalStateException("UUID of the execution cannot be null.");
    }

    initProposalExecution(proposals, unthrottledBrokers, requestedInterBrokerPartitionMovementConcurrency,
        requestedIntraBrokerPartitionMovementConcurrency, requestedLeadershipMovementConcurrency,
        replicaMovementStrategy, uuid);
    startExecution(executionRunnable);
  }

  // Package private for testing
  void initProposalExecution(Collection<ExecutionProposal> proposals,
                             Collection<Integer> brokersToSkipConcurrencyCheck,
                             Integer requestedInterBrokerPartitionMovementConcurrency,
                             Integer requestedIntraBrokerPartitionMovementConcurrency,
                             Integer requestedLeadershipMovementConcurrency,
                             ReplicaMovementStrategy replicaMovementStrategy,
                             String uuid) {
    _executionTaskManager.setExecutionModeForTaskTracker(_isKafkaAssignerMode);
    _executionTaskManager.addExecutionProposals(proposals, brokersToSkipConcurrencyCheck, _metadataClient.refreshMetadata().cluster(),
                                                replicaMovementStrategy);
    setRequestedInterBrokerPartitionMovementConcurrency(requestedInterBrokerPartitionMovementConcurrency);
    setRequestedIntraBrokerPartitionMovementConcurrency(requestedIntraBrokerPartitionMovementConcurrency);
    setRequestedLeadershipMovementConcurrency(requestedLeadershipMovementConcurrency);
    _uuid = uuid;
  }

  /**
   *
   * @param requestedInterBrokerPartitionMovementConcurrency The maximum number of concurrent inter-broker partition movements
   *                                                         per broker.
   */
  public void setRequestedInterBrokerPartitionMovementConcurrency(Integer requestedInterBrokerPartitionMovementConcurrency) {
    _executionTaskManager.setRequestedInterBrokerPartitionMovementConcurrency(requestedInterBrokerPartitionMovementConcurrency);
  }

  /**
   * Dynamically set the intra-broker partition movement concurrency.
   *
   * @param requestedIntraBrokerPartitionMovementConcurrency The maximum number of concurrent intra-broker partition movements.
   */
  public void setRequestedIntraBrokerPartitionMovementConcurrency(Integer requestedIntraBrokerPartitionMovementConcurrency) {
    _executionTaskManager.setRequestedIntraBrokerPartitionMovementConcurrency(requestedIntraBrokerPartitionMovementConcurrency);
  }

  /**
   * Dynamically set the leadership movement concurrency.
   *
   * @param requestedLeadershipMovementConcurrency The maximum number of concurrent leader movements.
   */
  public void setRequestedLeadershipMovementConcurrency(Integer requestedLeadershipMovementConcurrency) {
    _executionTaskManager.setRequestedLeadershipMovementConcurrency(requestedLeadershipMovementConcurrency);
  }

  /**
   * Set the execution mode of the tasks to keep track of the ongoing execution mode via sensors.
   *
   * @param isKafkaAssignerMode True if kafka assigner mode, false otherwise.
   */
  public synchronized void setExecutionMode(boolean isKafkaAssignerMode) {
    _isKafkaAssignerMode = isKafkaAssignerMode;
  }

  /**
   * Pause the load monitor and kick off the execution.
   */
  // Package private for testing
  void startExecution(ProposalExecutionRunnable runnable) {
    _executionStoppedByUser.set(false);
    sanityCheckOngoingReplicaMovement();
    _hasOngoingExecution = true;
    _anomalyDetector.maybeClearOngoingAnomalyDetectionTimeMs();
    _stopRequested.set(false);
    _executionStoppedByUser.set(false);
    if (_isKafkaAssignerMode) {
      _numExecutionStartedInKafkaAssignerMode.incrementAndGet();
    } else {
      _numExecutionStartedInNonKafkaAssignerMode.incrementAndGet();
    }
    _proposalExecutor.submit(runnable);
  }

  /**
   * Aborts the currently-running execution and awaits for it to end.
   * It is recommended that this method should be called while holding the Executor reservation,
   * to ensure no other thread triggers other executions while aborting.
   * @throws TimeoutException - if the execution was not aborted during the timeout
   */
  private void abortExecution(Duration timeout) throws TimeoutException {
    userTriggeredStopExecution();
    long startMs = _time.milliseconds();
    long timeoutMs = startMs + timeout.toMillis();

    if (hasOngoingExecution()) {
      LOG.info("Aborted executions, waiting for them to stop for {}", timeout);
    }
    while (hasOngoingExecution()) {
      if (timeoutMs <= _time.milliseconds()) {
        throw new TimeoutException(String.format("Timed out awaiting for execution to finish after %s", timeout));
      }
      _time.sleep(100);
    }
  }

  /**
   * Request the executor to stop any ongoing execution.
   *
   * package-private for testing
   *
   * @return True if the flag to stop the execution is set after the call (i.e. was not set already), false otherwise.
   */
  synchronized boolean stopExecution() {
    if (_stopRequested.compareAndSet(false, true)) {
      _numExecutionStopped.incrementAndGet();
      _executionTaskManager.setStopRequested();
      return true;
    } else {
      return false;
    }
  }

  // package-private for testing
  int numCancelledReassignments() {
    return _numCancelledReassignments.get();
  }

  /**
   * Sanity check whether there are ongoing inter-broker or intra-broker replica movements.
   */
  private void sanityCheckOngoingReplicaMovement() {
    // Note that in case there is an ongoing partition reassignment, we do not unpause metric sampling.
    if (hasOngoingPartitionReassignments()) {
      _executionTaskManager.clear();
      _uuid = null;
      throw new IllegalStateException("There are ongoing inter-broker partition movements.");
    }

    if (adminUtils.isOngoingIntraBrokerReplicaMovement(_metadataClient.cluster().nodes().stream().mapToInt(Node::id).boxed()
                                                           .collect(Collectors.toSet()))) {
      _executionTaskManager.clear();
      _uuid = null;
      throw new IllegalStateException("There are ongoing intra-broker partition movements.");
    }
  }

  /**
   * Register gauge sensors.
   */
  private void registerGaugeSensors(DataBalancerMetricsRegistry metricRegistry) {
    metricRegistry.newGauge(Executor.class, GAUGE_EXECUTION_STOPPED, this::numExecutionStopped);
    metricRegistry.newGauge(Executor.class, GAUGE_EXECUTION_STOPPED_BY_USER, this::numExecutionStoppedByUser);
    metricRegistry.newGauge(Executor.class, GAUGE_EXECUTION_STARTED_IN_KAFKA_ASSIGNER_MODE, this::numExecutionStartedInKafkaAssignerMode);
    metricRegistry.newGauge(Executor.class, GAUGE_EXECUTION_STARTED_IN_NON_KAFKA_ASSIGNER_MODE, this::numExecutionStartedInNonKafkaAssignerMode);
    metricRegistry.newGauge(Executor.class, GAUGE_CANCELLED_REASSIGNMENTS, this::numCancelledReassignments);
    metricRegistry.newGauge(Executor.class, GAUGE_FAILED_REASSIGNMENT_CANCELLATIONS, this::numFailedReassignmentCancellations);
  }

  /**
   * Remove any throttling configs that have been set. On startup, we have no state about ongoing executions, so we
   * have to try to remove throttles from every topic. When removing throttles after an execution, only check topics
   * that we know we moved.
   */
  private void removeThrottles() {
    synchronized (_throttleHelper) {
      _throttleHelper.removeAllThrottles();
    }
  }

  private void removeExpiredDemotionHistory() {
    LOG.debug("Remove expired demotion history");
    _latestDemoteStartTimeMsByBrokerId.entrySet()
        .removeIf(entry -> (entry.getValue() + _demotionHistoryRetentionTimeMs) < _time.milliseconds());
  }

  private void removeExpiredRemovalHistory() {
    LOG.debug("Remove expired broker removal history");
    _latestRemoveStartTimeMsByBrokerId.entrySet()
        .removeIf(entry -> (entry.getValue() + _removalHistoryRetentionTimeMs) < _time.milliseconds());
  }

  private int numExecutionStopped() {
    return _numExecutionStopped.get();
  }

  private int numExecutionStoppedByUser() {
    return _numExecutionStoppedByUser.get();
  }

  private int numExecutionStartedInKafkaAssignerMode() {
    return _numExecutionStartedInKafkaAssignerMode.get();
  }

  private int numExecutionStartedInNonKafkaAssignerMode() {
    return _numExecutionStartedInNonKafkaAssignerMode.get();
  }

  private int numFailedReassignmentCancellations() {
    return _numFailedReassignmentCancellations.get();
  }

  /**
   * A runnable class to remove expired execution history.
   */
  private class ExecutionHistoryScanner implements Runnable {
    @Override
    public void run() {
      try {
        removeExpiredDemotionHistory();
        removeExpiredRemovalHistory();
      } catch (Throwable t) {
        LOG.warn("Received exception when trying to expire execution history.", t);
      }
    }
  }

  /**
   * An extension of #{@link ProposalExecutionRunnable} for broker removal operations
   * that supports tracking the removal's progress.
   *
   * For more detail, see #{@link ProposalExecutionRunnable}
   */
  class BrokerRemovalProposalExecutionRunnable extends ProposalExecutionRunnable {
    private final BrokerRemovalCallback progressCallback;
    private final Collection<Integer> removedBrokers;

    /**
     * @param progressCallback nullable, the callback to help track the broker removal progress
     */
    BrokerRemovalProposalExecutionRunnable(LoadMonitor loadMonitor, Collection<Integer> demotedBrokers,
                                           Collection<Integer> removedBrokers, Long replicationThrottle,
                                           BrokerRemovalCallback progressCallback) {
      super(loadMonitor, demotedBrokers, removedBrokers, replicationThrottle);
      this.removedBrokers = removedBrokers;
      this.progressCallback = progressCallback;
    }

    public void run() {
      try {
        LOG.info("Starting execution of balancing proposals.");
        execute();
        LOG.info("Execution finished.");
      } finally {
        if (progressCallback != null) {
          if (this._executionException == null) {
            progressCallback.registerEvent(BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_SUCCESS);
            LOG.info("Successfully completed the broker removal operation for brokers {}", this.removedBrokers);
          } else {
            Exception exc;
            if (this._executionException instanceof Exception) {
              exc = (Exception) this._executionException;
            } else {
              exc = new Exception(this._executionException.getMessage());
            }
            progressCallback.registerEvent(
                BrokerRemovalCallback.BrokerRemovalEvent.PLAN_EXECUTION_FAILURE,
                exc
            );

            LOG.info("The broker removal operation for brokers {} failed due to an unexpected exception while executing the proposals.",
                this.removedBrokers, this._executionException);
          }
        }
      }
    }
  }

  /**
   * This class is thread safe.
   *
   * Note that once the thread for {@link ProposalExecutionRunnable} is submitted for running, the variable
   * _executionTaskManager can only be written within this inner class, but not from the outer Executor class.
   *
   * package-private for testing
   */
  class ProposalExecutionRunnable implements Runnable {
    private final LoadMonitor _loadMonitor;
    private ExecutorState.State _state;
    private Set<Integer> _recentlyDemotedBrokers;
    private Set<Integer> _recentlyRemovedBrokers;
    private final Long _replicationThrottle;
    private final long _executionStartMs;

    // an exception that is populated if there was an exception with the execution
    protected Throwable _executionException;

    ProposalExecutionRunnable(LoadMonitor loadMonitor,
                              Collection<Integer> demotedBrokers,
                              Collection<Integer> removedBrokers,
                              Long replicationThrottle) {
      _loadMonitor = loadMonitor;
      _state = ExecutorState.State.NO_TASK_IN_PROGRESS;
      _executionStartMs = _time.milliseconds();
      _executionException = null;

      if (_anomalyDetector == null) {
        throw new IllegalStateException("AnomalyDetector is not specified in Executor.");
      }

      if (demotedBrokers != null) {
        // Add/overwrite the latest demotion time of demoted brokers (if any).
        demotedBrokers.forEach(id -> _latestDemoteStartTimeMsByBrokerId.put(id, _time.milliseconds()));
      }
      if (removedBrokers != null) {
        // Add/overwrite the latest removal time of removed brokers (if any).
        removedBrokers.forEach(id -> _latestRemoveStartTimeMsByBrokerId.put(id, _time.milliseconds()));
      }
      _recentlyDemotedBrokers = recentlyDemotedBrokers();
      _recentlyRemovedBrokers = recentlyRemovedBrokers();
      _replicationThrottle = replicationThrottle;
    }

    public void run() {
      try {
        LOG.info("Starting execution of balancing proposals.");
        execute();
        LOG.info("Execution finished.");
      } catch (Exception e) {
        LOG.error("Exception during execution of ProposalExecutionRunnable", e);
      }
    }

    /**
     * Start the actual execution of the proposals in order: First move replicas, then transfer leadership.
     */
    protected void execute() {
      boolean isAnomaly = AnomalyType.cachedValues().stream().anyMatch(type -> _uuid.startsWith(type.toString()));

      _state = ExecutorState.State.STARTING_EXECUTION;
      _executorState = ExecutorState.executionStarted(_uuid, _recentlyDemotedBrokers, _recentlyRemovedBrokers);
      OPERATION_LOG.info("Task [{}] execution starts.", _uuid);
      try {
        // Pause the metric sampling to avoid the loss of accuracy during execution.
        while (true) {
          try {
            // Ensure that the temporary states in the load monitor are explicitly handled -- e.g. SAMPLING.
            _loadMonitor.pauseMetricSampling(String.format("Paused-By-Cruise-Control-Before-Starting-Execution (Date: %s)", currentUtcDate()));
            break;
          } catch (IllegalStateException e) {
            Thread.sleep(_statusCheckingIntervalMs);
            LOG.debug("Waiting for the load monitor to be ready to initialize the execution.", e);
          }
        }

        // 1. Inter-broker move replicas if possible.
        if (_state == ExecutorState.State.STARTING_EXECUTION) {
          _state = ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS;
          // The _executorState might be inconsistent with _state if the user checks it between the two assignments.
          _executorState = ExecutorState.operationInProgress(ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers);
          interBrokerMoveReplicas();
          updateOngoingExecutionState();
        }

        // 2. Intra-broker move replicas if possible.
        if (_state == ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          _state = ExecutorState.State.INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS;
          // The _executorState might be inconsistent with _state if the user checks it between the two assignments.
          _executorState = ExecutorState.operationInProgress(ExecutorState.State.INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers);
          intraBrokerMoveReplicas();
          updateOngoingExecutionState();
        }

        // 3. Transfer leadership if possible.
        if (_state == ExecutorState.State.INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          _state = ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS;
          // The _executorState might be inconsistent with _state if the user checks it between the two assignments.
          _executorState = ExecutorState.operationInProgress(ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS,
                                                             _executionTaskManager.getExecutionTasksSummary(
                                                                 Collections.singleton(ExecutionTask.TaskType.LEADER_ACTION)),
                                                             _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                             _executionTaskManager.leadershipMovementConcurrency(),
                                                             _uuid,
                                                             _recentlyDemotedBrokers,
                                                             _recentlyRemovedBrokers);
          moveLeaderships();
          updateOngoingExecutionState();
        }
      } catch (Throwable t) {
        LOG.error("Executor got exception during execution", t);
        _executionException = t;
      } finally {
        LOG.info("Cleaning up execution of {}", _uuid);
        // If task was triggered by an anomaly self-healing, update the task status in anomaly detector.
        if (isAnomaly) {
          _anomalyDetector.markSelfHealingFinished(_uuid);
        }
        _loadMonitor.resumeMetricSampling(String.format("Resumed-By-Cruise-Control-After-Completed-Execution (Date: %s)", currentUtcDate()));

        _throttleHelper.resetThrottleAfterExecution();

        // If execution encountered exception and isn't stopped, it's considered successful.
        boolean executionSucceeded = _executorState.state() != ExecutorState.State.STOPPING_EXECUTION && _executionException == null;
        // If we are here, either we succeeded, or we are stopped or had exception. Send notification to user.
        ExecutorNotification notification = new ExecutorNotification(_executionStartMs, _time.milliseconds(),
                                                                     _uuid, _stopRequested.get(),
                                                                     _executionStoppedByUser.get(),
                                                                     _executionException, executionSucceeded);
        _executorNotifier.sendNotification(notification);
        OPERATION_LOG.info("Task [{}] execution finishes.", _uuid);
        // Clear completed execution.
        clearCompletedExecution();
      }
    }

    private void clearCompletedExecution() {
      _executionTaskManager.clear();
      _uuid = null;
      _state = ExecutorState.State.NO_TASK_IN_PROGRESS;
      // The _executorState might be inconsistent with _state if the user checks it between the two assignments.
      _executorState = ExecutorState.noTaskInProgress(_recentlyDemotedBrokers, _recentlyRemovedBrokers);
      _hasOngoingExecution = false;
      _stopRequested.set(false);
      _executionStoppedByUser.set(false);
    }

    private void updateOngoingExecutionState() {
      if (!_stopRequested.get()) {
        switch (_state) {
          case LEADER_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(ExecutionTask.TaskType.LEADER_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers);
            break;
          case INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(ExecutorState.State.INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers);
            break;
          case INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS:
            _executorState = ExecutorState.operationInProgress(ExecutorState.State.INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                                               _executionTaskManager.getExecutionTasksSummary(
                                                                   Collections.singleton(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)),
                                                               _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                               _executionTaskManager.leadershipMovementConcurrency(),
                                                               _uuid,
                                                               _recentlyDemotedBrokers,
                                                               _recentlyRemovedBrokers);
            break;
          default:
            throw new IllegalStateException("Unexpected ongoing execution state " + _state);
        }
      } else {
        _state = ExecutorState.State.STOPPING_EXECUTION;
        _executorState = ExecutorState.operationInProgress(ExecutorState.State.STOPPING_EXECUTION,
                                                           _executionTaskManager.getExecutionTasksSummary(new HashSet<>(ExecutionTask.TaskType.cachedValues())),
                                                           _executionTaskManager.interBrokerPartitionMovementConcurrency(),
                                                           _executionTaskManager.intraBrokerPartitionMovementConcurrency(),
                                                           _executionTaskManager.leadershipMovementConcurrency(),
                                                           _uuid,
                                                           _recentlyDemotedBrokers,
                                                           _recentlyRemovedBrokers);
      }
    }

    private void interBrokerMoveReplicas() throws InterruptedException {
      synchronized (_throttleHelper) {
        _throttleHelper.setThrottleRate(_replicationThrottle);
      }
      int numTotalPartitionMovements = _executionTaskManager.numRemainingInterBrokerPartitionMovements();
      long totalDataToMoveInMB = _executionTaskManager.remainingInterBrokerDataToMoveInMB();
      LOG.info("Starting {} inter-broker partition movements.", numTotalPartitionMovements);

      int partitionsToMove = numTotalPartitionMovements;
      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || !_executionTaskManager.inExecutionTasks().isEmpty()) && !_stopRequested.get()) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getInterBrokerReplicaMovementTasks();
        LOG.info("Executor will execute {} task(s)", tasksToExecute.size());
        if (!tasksToExecute.isEmpty()) {
          synchronized (_throttleHelper) {
            _throttleHelper.setThrottles(
                tasksToExecute.stream().map(ExecutionTask::proposal).collect(Collectors.toList()), _loadMonitor);
          }
          // Execute the tasks.
          _executionTaskManager.markTasksInProgress(tasksToExecute);
          executeReplicaReassignmentTasks(tasksToExecute);
        }
        // Wait indefinitely for partition movements to finish.
        List<ExecutionTask> completedTasks = waitForExecutionTaskToFinish();
        partitionsToMove = _executionTaskManager.numRemainingInterBrokerPartitionMovements();
        int numFinishedPartitionMovements = _executionTaskManager.numFinishedInterBrokerPartitionMovements();
        long finishedDataMovementInMB = _executionTaskManager.finishedInterBrokerDataMovementInMB();
        LOG.info("{}/{} ({}%) inter-broker partition movements completed. {}/{} ({}%) MB have been moved.",
                 numFinishedPartitionMovements, numTotalPartitionMovements,
                 String.format(java.util.Locale.US, "%.2f",
                               numFinishedPartitionMovements * 100.0 / numTotalPartitionMovements),
                 finishedDataMovementInMB, totalDataToMoveInMB,
                 totalDataToMoveInMB == 0 ? 100 : String.format(java.util.Locale.US, "%.2f",
                                                  (finishedDataMovementInMB * 100.0) / totalDataToMoveInMB));
        synchronized (_throttleHelper) {
          _throttleHelper.clearThrottles(completedTasks, new ArrayList<>(_executionTaskManager.inExecutionTasks()));
        }
      }
      // After the partition movement finishes, wait for the controller to clean the reassignment zkPath. This also
      // ensures a clean stop when the execution is stopped in the middle.
      Set<ExecutionTask> inExecutionTasks = _executionTaskManager.inExecutionTasks();
      while (!inExecutionTasks.isEmpty()) {
        LOG.info("Waiting for {} tasks moving {} MB to finish.", inExecutionTasks.size(),
                 _executionTaskManager.inExecutionInterBrokerDataToMoveInMB());
        List<ExecutionTask> completedRemainingTasks = waitForExecutionTaskToFinish();
        inExecutionTasks = _executionTaskManager.inExecutionTasks();
        synchronized (_throttleHelper) {
          _throttleHelper.clearThrottles(completedRemainingTasks, new ArrayList<>(inExecutionTasks));
        }
      }
      if (_executionTaskManager.inExecutionTasks().isEmpty()) {
        LOG.info("Inter-broker partition movements finished.");
      } else if (_stopRequested.get()) {
        ExecutionTasksSummary executionTasksSummary = _executionTaskManager.getExecutionTasksSummary(Collections.emptySet());
        Map<ExecutionTask.State, Integer> partitionMovementTasksByState =
                executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION);
        LOG.info("Inter-broker partition movements stopped. For inter-broker partition movements {} tasks cancelled, {} tasks in-progress, "
                 + "{} tasks aborting, {} tasks aborted, {} tasks dead, {} tasks completed, {} remaining data to move; for intra-broker "
                 + "partition movement {} tasks cancelled; for leadership movements {} task cancelled.",
                 partitionMovementTasksByState.get(ExecutionTask.State.PENDING),
                 partitionMovementTasksByState.get(ExecutionTask.State.IN_PROGRESS),
                 partitionMovementTasksByState.get(ExecutionTask.State.ABORTING),
                 partitionMovementTasksByState.get(ExecutionTask.State.ABORTED),
                 partitionMovementTasksByState.get(ExecutionTask.State.DEAD),
                 partitionMovementTasksByState.get(ExecutionTask.State.COMPLETED),
                 executionTasksSummary.remainingInterBrokerDataToMoveInMB(),
                 executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION).get(ExecutionTask.State.PENDING),
                 executionTasksSummary.taskStat().get(ExecutionTask.TaskType.LEADER_ACTION).get(ExecutionTask.State.PENDING));
      }
    }

    private void intraBrokerMoveReplicas() {
      int numTotalPartitionMovements = _executionTaskManager.numRemainingIntraBrokerPartitionMovements();
      long totalDataToMoveInMB = _executionTaskManager.remainingIntraBrokerDataToMoveInMB();
      LOG.info("Starting {} intra-broker partition movements.", numTotalPartitionMovements);

      int partitionsToMove = numTotalPartitionMovements;
      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || !_executionTaskManager.inExecutionTasks().isEmpty()) && !_stopRequested.get()) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getIntraBrokerReplicaMovementTasks();
        LOG.info("Executor will execute {} task(s)", tasksToExecute.size());

        if (!tasksToExecute.isEmpty()) {
          // Execute the tasks.
          _executionTaskManager.markTasksInProgress(tasksToExecute);
          adminUtils.executeIntraBrokerReplicaMovements(tasksToExecute, _executionTaskManager);
        }
        // Wait indefinitely for partition movements to finish.
        waitForExecutionTaskToFinish();
        partitionsToMove = _executionTaskManager.numRemainingIntraBrokerPartitionMovements();
        int numFinishedPartitionMovements = _executionTaskManager.numFinishedIntraBrokerPartitionMovements();
        long finishedDataToMoveInMB = _executionTaskManager.finishedIntraBrokerDataToMoveInMB();
        LOG.info("{}/{} ({}%) intra-broker partition movements completed. {}/{} ({}%) MB have been moved.",
            numFinishedPartitionMovements, numTotalPartitionMovements,
            String.format(java.util.Locale.US, "%.2f",
                          numFinishedPartitionMovements * 100.0 / numTotalPartitionMovements),
            finishedDataToMoveInMB, totalDataToMoveInMB,
            totalDataToMoveInMB == 0 ? 100 : String.format(java.util.Locale.US, "%.2f",
                                                           (finishedDataToMoveInMB * 100.0) / totalDataToMoveInMB));
      }
      Set<ExecutionTask> inExecutionTasks = _executionTaskManager.inExecutionTasks();
      while (!inExecutionTasks.isEmpty()) {
        LOG.info("Waiting for {} tasks moving {} MB to finish", inExecutionTasks.size(),
                 _executionTaskManager.inExecutionIntraBrokerDataMovementInMB(), inExecutionTasks);
        waitForExecutionTaskToFinish();
        inExecutionTasks = _executionTaskManager.inExecutionTasks();
      }
      if (_executionTaskManager.inExecutionTasks().isEmpty()) {
        LOG.info("Intra-broker partition movements finished.");
      } else if (_stopRequested.get()) {
        ExecutionTasksSummary executionTasksSummary = _executionTaskManager.getExecutionTasksSummary(Collections.emptySet());
        Map<ExecutionTask.State, Integer> partitionMovementTasksByState =
                executionTasksSummary.taskStat().get(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION);
        LOG.info("Intra-broker partition movements stopped. For intra-broker partition movements {} tasks cancelled, {} tasks in-progress, "
                 + "{} tasks aborting, {} tasks aborted, {} tasks dead, {} tasks completed, {} remaining data to move; for leadership "
                 + "movements {} task cancelled.",
                 partitionMovementTasksByState.get(ExecutionTask.State.PENDING),
                 partitionMovementTasksByState.get(ExecutionTask.State.IN_PROGRESS),
                 partitionMovementTasksByState.get(ExecutionTask.State.ABORTING),
                 partitionMovementTasksByState.get(ExecutionTask.State.ABORTED),
                 partitionMovementTasksByState.get(ExecutionTask.State.DEAD),
                 partitionMovementTasksByState.get(ExecutionTask.State.COMPLETED),
                 executionTasksSummary.remainingIntraBrokerDataToMoveInMB(),
                 executionTasksSummary.taskStat().get(ExecutionTask.TaskType.LEADER_ACTION).get(ExecutionTask.State.PENDING));
      }
    }

    private void moveLeaderships() {
      int numTotalLeadershipMovements = _executionTaskManager.numRemainingLeadershipMovements();
      LOG.info("Starting {} leadership movements.", numTotalLeadershipMovements);
      int numFinishedLeadershipMovements = 0;
      while (_executionTaskManager.numRemainingLeadershipMovements() != 0 && !_stopRequested.get()) {
        updateOngoingExecutionState();
        numFinishedLeadershipMovements += moveLeadershipInBatch();
        LOG.info("{}/{} ({}%) leadership movements completed.", numFinishedLeadershipMovements,
                 numTotalLeadershipMovements, numFinishedLeadershipMovements * 100 / numTotalLeadershipMovements);
      }
      if (_executionTaskManager.inExecutionTasks().isEmpty()) {
        LOG.info("Leadership movements finished.");
      } else if (_stopRequested.get()) {
        Map<ExecutionTask.State, Integer> leadershipMovementTasksByState =
            _executionTaskManager.getExecutionTasksSummary(Collections.emptySet()).taskStat().get(ExecutionTask.TaskType.LEADER_ACTION);
        LOG.info("Leadership movements stopped. {} tasks cancelled, {} tasks in-progress, {} tasks aborting, {} tasks aborted, "
                 + "{} tasks dead, {} tasks completed.",
                 leadershipMovementTasksByState.get(ExecutionTask.State.PENDING),
                 leadershipMovementTasksByState.get(ExecutionTask.State.IN_PROGRESS),
                 leadershipMovementTasksByState.get(ExecutionTask.State.ABORTING),
                 leadershipMovementTasksByState.get(ExecutionTask.State.ABORTED),
                 leadershipMovementTasksByState.get(ExecutionTask.State.DEAD),
                 leadershipMovementTasksByState.get(ExecutionTask.State.COMPLETED));
      }
    }

    private int moveLeadershipInBatch() {
      List<ExecutionTask> leadershipMovementTasks = _executionTaskManager.getLeadershipMovementTasks();
      int numLeadershipToMove = leadershipMovementTasks.size();
      LOG.debug("Executing {} leadership movements in a batch.", numLeadershipToMove);
      // Execute the leadership movements.
      if (!leadershipMovementTasks.isEmpty() && !_stopRequested.get()) {
        // Mark leadership movements in progress.
        _executionTaskManager.markTasksInProgress(leadershipMovementTasks);

        // Run preferred leader election.
        executePreferredLeaderElection(leadershipMovementTasks);
        LOG.trace("Waiting for leadership movement batch to finish.");
        while (!_executionTaskManager.inExecutionTasks().isEmpty() && !_stopRequested.get()) {
          waitForExecutionTaskToFinish();
        }
      }
      return numLeadershipToMove;
    }

    /**
     * This method periodically check zookeeper to see if the partition reassignment has finished or not.
     */
    private List<ExecutionTask> waitForExecutionTaskToFinish() {
      List<ExecutionTask> finishedTasks = new ArrayList<>();
      do {
        // If there is no finished tasks, we need to check if anything is blocked.
        maybeReexecuteTasks();
        try {
          Thread.sleep(_statusCheckingIntervalMs);
        } catch (InterruptedException e) {
          // let it go
        }

        Cluster cluster = _metadataClient.refreshMetadata().cluster();
        Map<ExecutionTask, ReplicaLogDirInfo> logDirInfoByTask = adminUtils.getLogdirInfoForExecutionTask(
            _executionTaskManager.inExecutionTasks(Collections.singleton(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Tasks in execution: {}", _executionTaskManager.inExecutionTasks());
        }
        List<ExecutionTask> deadOrAbortingTasks = new ArrayList<>();
        List<TopicPartition> partitionReassignmentsToCancel = new ArrayList<>();
        boolean cancelRequested = _stopRequested.get();
        if (cancelRequested) {
          LOG.info("User initiated a cancellation of all ongoing tasks.");
        }
        for (ExecutionTask task : _executionTaskManager.inExecutionTasks()) {
          TopicPartition tp = task.proposal().topicPartition();
          if (cluster.partition(tp) == null) {
            // Handle topic deletion during the execution.
            LOG.debug("Task {} is marked as finished because the topic has been deleted", task);
            finishedTasks.add(task);
            _executionTaskManager.markTaskAborting(task);
            _executionTaskManager.markTaskDone(task);
          } else if (isTaskDone(cluster, logDirInfoByTask, tp, task)) {
            // Check to see if the task is done.
            finishedTasks.add(task);
            _executionTaskManager.markTaskDone(task);
          } else if (maybeMarkTaskAsDeadOrAborting(cluster, logDirInfoByTask, task, cancelRequested)) {
            // Only add the dead or aborted tasks to execute if it is not a leadership movement.
            if (task.type() != ExecutionTask.TaskType.LEADER_ACTION) {
              deadOrAbortingTasks.add(task);
            }
            if (cancelRequested && task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION) {
              // Only cancel inter broker reassignments
              partitionReassignmentsToCancel.add(tp);
            }
            // A dead or aborted task is considered as finished.
            if (task.state() == ExecutionTask.State.DEAD || task.state() == ExecutionTask.State.ABORTED) {
              finishedTasks.add(task);
            }
          }
        }
        int numReassignmentsToCancel = partitionReassignmentsToCancel.size();
        if (cancelRequested && numReassignmentsToCancel > 0) {
          LOG.info("Cancelling {} partition reassignments", numReassignmentsToCancel);
          int numCancelled = adminUtils.cancelInterBrokerReplicaMovements(partitionReassignmentsToCancel);
          int numFailedCancellations = numReassignmentsToCancel - numCancelled;

          LOG.info("Successfully cancelled {}/{} partition reassignments", numCancelled, numReassignmentsToCancel);
          _numCancelledReassignments.addAndGet(numCancelled);
          if (numFailedCancellations > 0) {
            LOG.warn("Failed to cancel {}/{} partition reassignments", numFailedCancellations, numReassignmentsToCancel);
            _numFailedReassignmentCancellations.addAndGet(numFailedCancellations);
          }
        }
        if (!deadOrAbortingTasks.isEmpty()) {
          if (!_stopRequested.get()) {
            // If there is task aborted or dead, we stop the execution.
            stopExecution();
          }
        }
        updateOngoingExecutionState();
      } while (!_executionTaskManager.inExecutionTasks().isEmpty() && finishedTasks.isEmpty());
      LOG.info("Completed tasks: {}", finishedTasks);
      return finishedTasks;
    }

    /**
     * Check if a task is done.
     */
    private boolean isTaskDone(Cluster cluster,
                               Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask,
                               TopicPartition  tp,
                               ExecutionTask task) {
      switch (task.type()) {
        case INTER_BROKER_REPLICA_ACTION:
          return isInterBrokerReplicaActionDone(cluster, tp, task);
        case INTRA_BROKER_REPLICA_ACTION:
          return isIntraBrokerReplicaActionDone(logdirInfoByTask, task);
        case LEADER_ACTION:
          return isLeadershipMovementDone(cluster, tp, task);
        default:
          return true;
      }
    }

    /**
     * For a inter-broker replica movement action, the completion depends on the task state:
     * IN_PROGRESS: when the current replica list is the same as the new replica list.
     * ABORTING: done when the current replica list is the same as the old replica list. Due to race condition,
     *           we also consider it done if the current replica list is the same as the new replica list.
     * DEAD: always considered as done because we neither move forward or rollback.
     *
     * There should be no other task state seen here.
     */
    private boolean isInterBrokerReplicaActionDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      Node[] currentOrderedReplicas = cluster.partition(tp).replicas();
      switch (task.state()) {
        case IN_PROGRESS:
          return task.proposal().isInterBrokerMovementCompleted(currentOrderedReplicas);
        case ABORTING:
          return task.proposal().isInterBrokerMovementAborted(currentOrderedReplicas);
        case DEAD:
          return true;
        default:
          throw new IllegalStateException("Should never be here. State " + task.state());
      }
    }

    /**
     * Check whether intra-broker replica movement is done by comparing replica's current logdir with the logdir proposed
     * by task's proposal.
     */
    private boolean isIntraBrokerReplicaActionDone(Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask,
                                                   ExecutionTask task) {
      if (logdirInfoByTask.containsKey(task)) {
        return logdirInfoByTask.get(task).getCurrentReplicaLogDir()
                               .equals(task.proposal().replicasToMoveBetweenDisksByBroker().get(task.brokerId()).logdir());
      }
      return false;
    }

    private boolean isInIsr(Integer leader, Cluster cluster, TopicPartition tp) {
      return Arrays.stream(cluster.partition(tp).inSyncReplicas()).anyMatch(node -> node.id() == leader);
    }

    /**
     * The completeness of leadership movement depends on the task state:
     * IN_PROGRESS: done when the leader becomes the destination.
     * ABORTING or DEAD: always considered as done the destination cannot become leader anymore.
     *
     * There should be no other task state seen here.
     */
    private boolean isLeadershipMovementDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      Node leader = cluster.leaderFor(tp);
      switch (task.state()) {
        case IN_PROGRESS:
          return (leader != null && leader.id() == task.proposal().newLeader().brokerId())
                 || leader == null
                 || !isInIsr(task.proposal().newLeader().brokerId(), cluster, tp);
        case ABORTING:
        case DEAD:
          return true;
        default:
          throw new IllegalStateException("Should never be here.");
      }
    }

    /**
     * Mark the task as aborting or dead if needed.
     *
     * Ideally, the task should be marked as:
     * 1. ABORTING: when the execution is stopped by the users.
     * 2. ABORTING: When the destination broker&disk is dead so the task cannot make progress, but the source broker&disk
     *              is still alive.
     * 3. DEAD: when any replica in the new replica list is dead. Or when a leader action times out.
     *
     * Currently KafkaController does not support updates on the partitions that is being reassigned. (KAFKA-6304)
     * Therefore once a proposals is written to ZK, we cannot revoke it. So the actual behavior we are using is to
     * set the task state to:
     * 1. IN_PROGRESS: when the execution is stopped by the users. i.e. do nothing but let the task finish normally.
     * 2. DEAD: when the destination broker&disk is dead. i.e. do not block on the execution.
     *
     * @param cluster the kafka cluster
     * @param logdirInfoByTask  disk information for ongoing intra-broker replica movement tasks
     * @param task the task to check
     * @param cancelRequested whether a cancellation of the ongoing tasks was requested
     * @return true if the task is marked as dead or aborting, false otherwise.
     */
    private boolean maybeMarkTaskAsDeadOrAborting(Cluster cluster,
                                                  Map<ExecutionTask, ReplicaLogDirInfo> logdirInfoByTask,
                                                  ExecutionTask task,
                                                  boolean cancelRequested) {
      if (cancelRequested && task.state() == ExecutionTask.State.IN_PROGRESS) {
        LOG.info("Cancelling task {}", task);
        _executionTaskManager.markTaskDead(task);
        return true;
      }
      // Only check tasks with IN_PROGRESS or ABORTING state.
      if (task.state() == ExecutionTask.State.IN_PROGRESS || task.state() == ExecutionTask.State.ABORTING) {
        switch (task.type()) {
          case LEADER_ACTION:
            if (cluster.nodeById(task.proposal().newLeader().brokerId()) == null) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("Killing execution for task {} because the target leader is down", task);
              return true;
            } else if (_time.milliseconds() > task.startTime() + LEADER_ACTION_TIMEOUT_MS) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("Failed task {} because it took longer than {} to finish.", task, LEADER_ACTION_TIMEOUT_MS);
              return true;
            }
            break;

          case INTER_BROKER_REPLICA_ACTION:
            for (ReplicaPlacementInfo broker : task.proposal().newReplicas()) {
              if (cluster.nodeById(broker.brokerId()) == null) {
                _executionTaskManager.markTaskDead(task);
                LOG.warn("Killing execution for task {} because the new replica {} is down.", task, broker);
                return true;
              }
            }
            break;

          case INTRA_BROKER_REPLICA_ACTION:
            if (!logdirInfoByTask.containsKey(task)) {
              _executionTaskManager.markTaskDead(task);
              LOG.warn("Killing execution for task {} because the destination disk is down.", task);
              return true;
            }
            break;

          default:
            throw new IllegalStateException("Unknown task type " + task.type());
        }
      }
      return false;
    }

    /**
     * Checks whether the topicPartitions of the execution tasks in the given subset is indeed a subset of the given set.
     *
     * @param set The original set.
     * @param subset The subset to validate whether it is indeed a subset of the given set.
     * @return True if the topicPartitions of the given subset constitute a subset of the given set, false otherwise.
     */
    private boolean isSubset(Set<TopicPartition> set, Collection<ExecutionTask> subset) {
      boolean isSubset = true;
      for (ExecutionTask executionTask : subset) {
        TopicPartition tp = executionTask.proposal().topicPartition();
        if (!set.contains(tp)) {
          isSubset = false;
          break;
        }
      }

      return isSubset;
    }

    /**
     * Due to the race condition between the controller and Cruise Control, some of the submitted tasks may be
     * deleted by controller without being executed. We will resubmit those tasks in that case.
     */
    private void maybeReexecuteTasks() {
      List<ExecutionTask> interBrokerReplicaActionsToReexecute =
          new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION)));
      if (!isSubset(partitionsBeingReassigned(), interBrokerReplicaActionsToReexecute)) {
        LOG.info("Reexecuting tasks {}", interBrokerReplicaActionsToReexecute);
        executeReplicaReassignmentTasks(interBrokerReplicaActionsToReexecute);
      }

      List<ExecutionTask> intraBrokerReplicaActionsToReexecute =
          new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(ExecutionTask.TaskType.INTRA_BROKER_REPLICA_ACTION)));
      adminUtils.getLogdirInfoForExecutionTask(intraBrokerReplicaActionsToReexecute).forEach((k, v) -> {
        String targetLogdir = k.proposal().replicasToMoveBetweenDisksByBroker().get(k.brokerId()).logdir();
        // If task is completed or in-progess, do not reexecute the task.
        if (targetLogdir.equals(v.getCurrentReplicaLogDir()) || targetLogdir.equals(v.getFutureReplicaLogDir())) {
          intraBrokerReplicaActionsToReexecute.remove(k);
        }
      });
      if (!intraBrokerReplicaActionsToReexecute.isEmpty()) {
        LOG.info("Reexecuting tasks {}", intraBrokerReplicaActionsToReexecute);
        adminUtils.executeIntraBrokerReplicaMovements(intraBrokerReplicaActionsToReexecute, _executionTaskManager);
      }

      // Only reexecute leader actions if there is no replica actions running.
      if (interBrokerReplicaActionsToReexecute.isEmpty() &&
          intraBrokerReplicaActionsToReexecute.isEmpty() &&
          ongoingLeaderElection().isEmpty()) {
        List<ExecutionTask> leaderActionsToReexecute = new ArrayList<>(_executionTaskManager.inExecutionTasks(Collections.singleton(ExecutionTask.TaskType.LEADER_ACTION)));
        if (!leaderActionsToReexecute.isEmpty()) {
          LOG.info("Reexecuting tasks {}", leaderActionsToReexecute);
          executePreferredLeaderElection(leaderActionsToReexecute);
        }
      }
    }
  }

  /**
   * A helper auto closeable class for acquiring the Executor's reservation.
   *
   * This reservation should be closed by the same thread that acquired it,
   * otherwise an #{@link IllegalMonitorStateException} will be thrown.
   *
   * A reservation cannot be acquired twice - any subsequent reservation
   * attempts after a successful one will result in #{@link IllegalStateException}
   */
  public class ReservationHandle implements AutoCloseable {
    /**
     * Acquires the Executor's reservation
     */
    public ReservationHandle() {
      boolean locked = _reservation.attemptReservation();
      if (!locked) {
        throw new IllegalStateException("Cannot reserve the Executor because it is already reserved by another thread!");
      }
    }

    @Override
    public void close() {
      _reservation.cancelReservation();
    }
  }

  /**
   * A wrapper over a #{@link ReentrantLock} to:
   * - use the lock as a flag
   * - understand what thread holds the lock
   * - make use of its memory-synchronization semantics (volatility) - see https://docs.oracle.com/javase/6/docs/api/java/util/concurrent/locks/Lock.html
   */
  final static class ExecutorReservation {
    private ReentrantLock _lock;

    public ExecutorReservation() {
      _lock = new ReentrantLock();
    }

    /**
     * Attempt to acquire the reservation
     *
     * @return {@code true} if the reservation was free and was acquired by the
     * current thread, or the reservation was already held by the current
     * thread; and {@code false} otherwise
     */
    boolean attemptReservation() {
      if (_lock.isHeldByCurrentThread()) {
        throw new IllegalStateException("Cannot reserve the Executor again while already holding a reservation.");
      }
      return _lock.tryLock();
    }

    void cancelReservation() {
      _lock.unlock();
    }

    /**
     * @return {@code true} if the caller is holding the reservation; {@code false} otherwise
     */
    boolean isReservedByMe() {
      return _lock.isHeldByCurrentThread();
    }

    /**
     * @return {@code true} if a reservation is held; {@code false} otherwise
     */
    boolean isReserved() {
      return _lock.isLocked();
    }
  }

  /**
   * Add a list of replica reassignment tasks to execute. Replica reassignment indicates tasks that (1) relocate a replica
   * within the cluster, (2) introduce a new replica to the cluster (3) remove an existing replica from the cluster.
   *
   * @param reassignmentTasks Replica reassignment tasks to be executed.
   */
  private void executeReplicaReassignmentTasks(List<ExecutionTask> reassignmentTasks) {
    if (reassignmentTasks != null && !reassignmentTasks.isEmpty()) {
      Set<TopicPartition> partitionsToReassign = reassignmentTasks.stream()
          .map(t -> t.proposal().topicPartition()).collect(Collectors.toSet());
      Map<TopicPartition, List<Integer>> inProgressTargetReplicaReassignment = fetchTargetReplicasBeingReassigned(
              Optional.of(partitionsToReassign));

      Map<TopicPartition, Optional<NewPartitionReassignment>> newReplicaAssignments = new HashMap<>();

      reassignmentTasks.forEach(task -> {
        TopicPartition tp = task.proposal().topicPartition();
        List<Integer> targetReplicas = replicasToWrite(task,
            Optional.ofNullable(inProgressTargetReplicaReassignment.get(tp)));

        if (!targetReplicas.isEmpty()) {
          NewPartitionReassignment reassignment = new NewPartitionReassignment(targetReplicas);
          newReplicaAssignments.put(tp, Optional.of(reassignment));
        }
      });

      if (!newReplicaAssignments.isEmpty()) {
        try {
          _adminClient.alterPartitionReassignments(newReplicaAssignments).all().get();
        } catch (Throwable t) {
          sneakyThrow(t);
        }
      }
    }
  }

  private Set<TopicPartition> partitionsBeingReassigned() {
    return fetchTargetReplicasBeingReassigned(Optional.empty()).keySet();
  }

  /**
   * Fetches the partitions being reassigned in the cluster
   *
   * @param partitionsOpt - an option of a set of partitions we want to check for reassignments.
   *                        An empty value will search for all reassigning partitions
   * @return a tuple of a map of partitions being reassigned and their target replicas and
   *                    a boolean indicating if we fell back to using the ZK API
   */
  private Map<TopicPartition, List<Integer>> fetchTargetReplicasBeingReassigned(
      Optional<Set<TopicPartition>> partitionsOpt) {

    try {
      ListPartitionReassignmentsResult listPartitionsResult;
      if (partitionsOpt.isPresent())
        listPartitionsResult = _adminClient.listPartitionReassignments(partitionsOpt.get());
      else
        listPartitionsResult = _adminClient.listPartitionReassignments();

      return listPartitionsResult.reassignments().get().entrySet().stream().collect(
        Collectors.toMap(
          entry -> entry.getKey(),
          entry -> {
            PartitionReassignment partitionReassignment = entry.getValue();
            List<Integer> targetReplicas = new ArrayList<>(partitionReassignment.replicas());
            targetReplicas.removeAll(partitionReassignment.removingReplicas());
            return targetReplicas;
          }));
    } catch (Throwable t) {
        LOG.error("Fetching reassigning replicas through the listPartitionReassignments API failed with an exception", t);
        sneakyThrow(t);
        return null;
    }
  }

  /**
   * Given an ExecutionTask, return the targetReplicas we should write to the Kafka reassignments.
   * If we should not reassign a partition as part of this task, an empty replica set will be returned
   */
  private List<Integer> replicasToWrite(ExecutionTask task,
      Optional<List<Integer>> inProgressTargetReplicasOpt) {
    TopicPartition tp = task.proposal().topicPartition();
    List<Integer> oldReplicas = task.proposal().oldReplicas().stream().map(r -> r.brokerId())
      .collect(Collectors.toList());
    List<Integer> newReplicas = task.proposal().newReplicas().stream().map(r -> r.brokerId())
      .collect(Collectors.toList());

    // If aborting an existing task, trigger a reassignment to the oldReplicas
    // If no reassignment is in progress, trigger a reassignment to newReplicas
    // else, do not trigger a reassignment
    if (inProgressTargetReplicasOpt.isPresent()) {
      List<Integer> inProgressTargetReplicas = inProgressTargetReplicasOpt.get();
      if (task.state() == ExecutionTask.State.ABORTING) {
        return oldReplicas;
      } else if (task.state() == ExecutionTask.State.DEAD
          || task.state() == ExecutionTask.State.ABORTED
          || task.state() == ExecutionTask.State.COMPLETED) {
        return Collections.emptyList();
      } else if (task.state() == ExecutionTask.State.IN_PROGRESS) {
        if (!newReplicas.equals(inProgressTargetReplicas)) {
          throw new RuntimeException("The provided new replica list " + newReplicas +
              " is different from the in progress replica list " + inProgressTargetReplicas + " for " + tp);
        }
        return Collections.emptyList();
      } else {
        throw new IllegalStateException("Should never be here, the state " + task.state());
      }
    } else {
      if (task.state() == ExecutionTask.State.ABORTED
          || task.state() == ExecutionTask.State.DEAD
          || task.state() == ExecutionTask.State.ABORTING
          || task.state() == ExecutionTask.State.COMPLETED) {
        LOG.warn("No need to abort tasks {} because the partition is not in reassignment", task);
        return Collections.emptyList();
      } else {
        // verify with current assignment
        List<Integer> currentReplicaAssignment = adminUtils.getReplicasForPartition(tp);
        if (currentReplicaAssignment.isEmpty()) {
          LOG.warn("Could not fetch the replicas for partition {}. It is possible the topic or partition doesn't exist.", tp);
          return Collections.emptyList();
        } else {
          // we are not verifying the old replicas because we may be reexecuting a task,
          // in which case the replica list could be different from the old replicas.
          return newReplicas;
        }
      }
    }
  }

  @SuppressWarnings("deprecation")
  private void executePreferredLeaderElection(List<ExecutionTask> tasks) {
    Set<TopicPartition> partitionsToExecute = tasks.stream().map(task ->
        new TopicPartition(task.proposal().topic(), task.proposal().partitionId()))
        .collect(Collectors.toSet());

    PreferredReplicaLeaderElectionCommand command = new PreferredReplicaLeaderElectionCommand(
        _kafkaZkClient, JavaConverters.asScalaSet(partitionsToExecute));
    command.moveLeaderToPreferredReplica();
  }

  @SuppressWarnings("deprecation")
  private Set<TopicPartition> ongoingLeaderElection() {
    return JavaConverters.setAsJavaSet(_kafkaZkClient.getPreferredReplicaElection());
  }

  // Simulates the behavior of the Scala compiler that throws checked exceptions as unchecked
  // This makes the migration away from the Scala code in ExecutorUtils easier, but we should
  // remove it once that's complete
  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
    throw (E) e;
  }
}
