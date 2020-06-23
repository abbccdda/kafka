/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregationResult;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleCompleteness;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.ValuesAndExtrapolations;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.async.progress.GeneratingClusterModel;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.async.progress.WaitingForClusterModel;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigResolver;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.config.TopicConfigProvider;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.SampleExtrapolation;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.BrokerStats;
import com.linkedin.kafka.cruisecontrol.servlet.response.stats.SingleBrokerStats;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.getRackHandleNull;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.getReplicaPlacementInfo;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.partitionSampleExtrapolations;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.populatePartitionLoad;
import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.setBadBrokerState;

/**
 * The LoadMonitor monitors the workload of a Kafka cluster. It periodically triggers the metric sampling and
 * maintains the collected {@link PartitionMetricSample}. It is also responsible for aggregate the metrics samples into
 * {@link AggregatedMetricValues} for the analyzer to generate the balancing proposals.
 */
public class LoadMonitor {

  // Kafka Load Monitor server log.
  private static final Logger LOG = LoggerFactory.getLogger(LoadMonitor.class);
  // Metadata TTL is set based on experience -- i.e. a short TTL with large metadata may cause excessive load on brokers.

  private final int _numPartitionMetricSampleWindows;
  private final LoadMonitorTaskRunner _loadMonitorTaskRunner;
  private final KafkaPartitionMetricSampleAggregator _partitionMetricSampleAggregator;
  private final KafkaBrokerMetricSampleAggregator _brokerMetricSampleAggregator;
  // A semaphore to help throttle the simultaneous cluster model creation
  private final Semaphore _clusterModelSemaphore;
  private final KafkaCruiseControlConfig _config;
  private final MetadataClient _metadataClient;
  private final ConfluentAdmin _adminClient;
  private final BrokerCapacityConfigResolver _brokerCapacityConfigResolver;
  private final TopicConfigProvider _topicConfigProvider;
  private final ScheduledExecutorService _loadMonitorExecutor;
  private final Timer _clusterModelCreationTimer;
  private final ThreadLocal<Boolean> _acquiredClusterModelSemaphore;
  private final ModelCompletenessRequirements _defaultModelCompletenessRequirements;
  private final int _maxVolumeThroughputMb;
  private final double _writeMultiplier;
  private final double _readMultiplier;
  private final double _networkThrottleRatio;
  private final double _diskReadRatio;

  // Sensor values
  private volatile int _numValidSnapshotWindows;
  private volatile double _monitoredPartitionsPercentage;
  private volatile int _totalMonitoredSnapshotWindows;
  private volatile int _numPartitionsWithExtrapolations;
  private volatile long _lastUpdate;

  private volatile ModelGeneration _cachedBrokerLoadGeneration;
  private volatile BrokerStats _cachedBrokerLoadStats;

  Time _time;

  /**
   * Construct a load monitor.
   *
   * @param config The load monitor configuration.
   * @param time   The time object.
   * @param metricRegistry The sensor registry for cruise control
   * @param metricDef The metric definitions.
   */
  public LoadMonitor(KafkaCruiseControlConfig config,
                     Time time,
                     DataBalancerMetricsRegistry metricRegistry,
                     MetricDef metricDef) {
    this(config,
         new MetadataClient(config, config.getLong(KafkaCruiseControlConfig.METADATA_TTL_CONFIG), time),
         KafkaCruiseControlUtils.createAdmin(config.originals()),
         time,
         metricRegistry,
         metricDef);
  }

  /**
   * Package private constructor for unit tests.
   */
  LoadMonitor(KafkaCruiseControlConfig config,
              MetadataClient metadataClient,
              ConfluentAdmin adminClient,
              Time time,
              DataBalancerMetricsRegistry metricRegistry,
              MetricDef metricDef) {
    _config = config;

    _metadataClient = metadataClient;

    _adminClient = adminClient;

    _brokerCapacityConfigResolver = config.getConfiguredInstance(KafkaCruiseControlConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                                                                 BrokerCapacityConfigResolver.class);
    _topicConfigProvider = config.getConfiguredInstance(KafkaCruiseControlConfig.TOPIC_CONFIG_PROVIDER_CLASS_CONFIG,
                                                                 TopicConfigProvider.class);
    _numPartitionMetricSampleWindows = config.getInt(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);

    _partitionMetricSampleAggregator = new KafkaPartitionMetricSampleAggregator(config, metadataClient);

    _brokerMetricSampleAggregator = new KafkaBrokerMetricSampleAggregator(config);

    _acquiredClusterModelSemaphore = ThreadLocal.withInitial(() -> false);

    // We use the number of proposal precomputing threads config to ensure there is enough concurrency if users
    // wants that.
    int numPrecomputingThread = config.getInt(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG);
    _clusterModelSemaphore = new Semaphore(Math.max(1, numPrecomputingThread), true);

    _defaultModelCompletenessRequirements =
        MonitorUtils.combineLoadRequirementOptions(AnalyzerUtils.getGoalsByPriority(config));

    _maxVolumeThroughputMb = config.getInt(KafkaCruiseControlConfig.MAX_VOLUME_THROUGHPUT_MB_CONFIG);
    _writeMultiplier = config.getDouble(KafkaCruiseControlConfig.WRITE_THROUGHPUT_MULTIPLIER_CONFIG);
    _readMultiplier = config.getDouble(KafkaCruiseControlConfig.READ_THROUGHPUT_MULTIPLIER_CONFIG);
    _networkThrottleRatio = config.getDouble(KafkaCruiseControlConfig.CALCULATED_THROTTLE_RATIO_CONFIG);
    _diskReadRatio = config.getDouble(KafkaCruiseControlConfig.DISK_READ_RATIO_CONFIG);

    _loadMonitorTaskRunner =
        new LoadMonitorTaskRunner(config, _partitionMetricSampleAggregator, _brokerMetricSampleAggregator,
                                  _metadataClient, metricDef, time, metricRegistry, _brokerCapacityConfigResolver);
    _clusterModelCreationTimer = metricRegistry.newTimer(LoadMonitor.class, "cluster-model-creation-timer");
    _loadMonitorExecutor = Executors.newScheduledThreadPool(2,
                                                            new KafkaCruiseControlThreadFactory("LoadMonitorExecutor", true, LOG));
    _loadMonitorExecutor.scheduleAtFixedRate(new SensorUpdater(), 0, SensorUpdater.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    _loadMonitorExecutor.scheduleAtFixedRate(new PartitionMetricSampleAggregatorCleaner(), 0,
                                             PartitionMetricSampleAggregatorCleaner.CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    metricRegistry.newGauge(LoadMonitor.class, "valid-windows", this::numValidSnapshotWindows);
    metricRegistry.newGauge(LoadMonitor.class, "monitored-partitions-percentage", this::monitoredPartitionsPercentage);
    metricRegistry.newGauge(LoadMonitor.class, "total-monitored-windows", this::totalMonitoredSnapshotWindows);
    metricRegistry.newGauge(LoadMonitor.class, "num-partitions-with-extrapolations", this::numPartitionsWithExtrapolations);

    _time = time;
  }


  /**
   * Start the load monitor.
   */
  public void startUp() {
    _loadMonitorTaskRunner.start();
  }

  /**
   * Shutdown the load monitor.
   */
  public void shutdown() {
    LOG.info("Shutting down load monitor.");
    try {
      _brokerCapacityConfigResolver.close();
      _topicConfigProvider.close();
      _loadMonitorExecutor.shutdown();
    } catch (Exception e) {
      LOG.warn("Received exception when closing broker capacity resolver.", e);
    }
    _loadMonitorTaskRunner.shutdown();
    _metadataClient.close();
    KafkaCruiseControlUtils.closeAdminClientWithTimeout(_adminClient);
    LOG.info("Load Monitor shutdown completed.");
  }

  /**
   * Get the state of the load monitor.
   */
  public LoadMonitorState state(OperationProgress operationProgress, MetadataClient.ClusterAndGeneration clusterAndGeneration) {
    LoadMonitorTaskRunner.LoadMonitorTaskRunnerState state = _loadMonitorTaskRunner.state();
    Cluster kafkaCluster = clusterAndGeneration.cluster();
    int totalNumPartitions = MonitorUtils.totalNumPartitions(kafkaCluster);
    double minMonitoredPartitionsPercentage = _defaultModelCompletenessRequirements.minMonitoredPartitionsPercentage();

    // Get the window to monitored partitions percentage mapping.
    SortedMap<Long, Float> validPartitionRatio =
        _partitionMetricSampleAggregator.validPartitionRatioByWindows(clusterAndGeneration);

    // Get valid snapshot window number and populate the monitored partition map.
    SortedSet<Long> validWindows = _partitionMetricSampleAggregator.validWindows(clusterAndGeneration,
                                                                                 minMonitoredPartitionsPercentage);
    int numValidSnapshotWindows = validWindows.size();

    // Get the number of valid partitions and sample extrapolations.
    int numValidPartitions = 0;
    Map<TopicPartition, List<SampleExtrapolation>> extrapolations = Collections.emptyMap();
    if (_partitionMetricSampleAggregator.numAvailableWindows() >= _numPartitionMetricSampleWindows) {
      try {
        MetricSampleAggregationResult<String, PartitionEntity> metricSampleAggregationResult =
            _partitionMetricSampleAggregator.aggregate(clusterAndGeneration, Long.MAX_VALUE, operationProgress);
        Map<PartitionEntity, ValuesAndExtrapolations> loads = metricSampleAggregationResult.valuesAndExtrapolations();
        extrapolations = partitionSampleExtrapolations(metricSampleAggregationResult.valuesAndExtrapolations());
        numValidPartitions = loads.size();
      } catch (Exception e) {
        LOG.warn("Received exception when trying to get the load monitor state", e);
      }
    }

    switch (state) {
      case NOT_STARTED:
        return LoadMonitorState.notStarted();
      case RUNNING:
        return LoadMonitorState.running(numValidSnapshotWindows,
                                        validPartitionRatio,
                                        numValidPartitions,
                                        totalNumPartitions,
                                        extrapolations,
                                        _loadMonitorTaskRunner.reasonOfLatestPauseOrResume());
      case SAMPLING:
        return LoadMonitorState.sampling(numValidSnapshotWindows,
                                         validPartitionRatio,
                                         numValidPartitions,
                                         totalNumPartitions,
                                         extrapolations);
      case PAUSED:
        return LoadMonitorState.paused(numValidSnapshotWindows,
                                       validPartitionRatio,
                                       numValidPartitions,
                                       totalNumPartitions,
                                       extrapolations,
                                       _loadMonitorTaskRunner.reasonOfLatestPauseOrResume());
      case LOADING:
        return LoadMonitorState.loading(numValidSnapshotWindows,
                                        validPartitionRatio,
                                        numValidPartitions,
                                        totalNumPartitions,
                                        _loadMonitorTaskRunner.sampleLoadingProgress());
      default:
        throw new IllegalStateException("Should never be here.");
    }
  }

  /**
   * Return the load monitor task runner state.
   */
  public LoadMonitorTaskRunner.LoadMonitorTaskRunnerState taskRunnerState() {
    return _loadMonitorTaskRunner.state();
  }

  /**
   * Get the cluster information from Kafka metadata.
   */
  public Cluster kafkaCluster() {
    return _metadataClient.cluster();
  }

  /**
   * Pause all the activities of the load monitor. The load monitor can only be paused when it is in
   * RUNNING state.
   *
   * @param reason The reason for pausing metric sampling.
   */
  public void pauseMetricSampling(String reason) {
    _loadMonitorTaskRunner.pauseSampling(reason);
  }

  /**
   * Resume the activities of the load monitor.
   *
   * @param reason The reason for resuming metric sampling.
   */
  public void resumeMetricSampling(String reason) {
    _loadMonitorTaskRunner.resumeSampling(reason);
  }

  /**
   * Acquire the semaphore for the cluster model generation.
   * @param operationProgress the progress for the job.
   * @throws InterruptedException
   */
  public AutoCloseableSemaphore acquireForModelGeneration(OperationProgress operationProgress)
      throws InterruptedException {
    if (_acquiredClusterModelSemaphore.get()) {
      throw new IllegalStateException("The thread has already acquired the semaphore for cluster model generation.");
    }
    WaitingForClusterModel step = new WaitingForClusterModel();
    operationProgress.addStep(step);
    _clusterModelSemaphore.acquire();
    _acquiredClusterModelSemaphore.set(true);
    step.done();
    return new AutoCloseableSemaphore();
  }

  /**
   * Get the latest metric values of the brokers. The metric values are from the current active metric window.
   *
   * @return the latest metric values of brokers.
   */
  public Map<BrokerEntity, ValuesAndExtrapolations> currentBrokerMetricValues() {
    return _brokerMetricSampleAggregator.peekCurrentWindow();
  }

  /**
   * Get the latest metric values of the partitions. The metric values are from the current active metric window.
   *
   * @return the latest metric values of partitions.
   */
  public Map<PartitionEntity, ValuesAndExtrapolations> currentPartitionMetricValues() {
    return _partitionMetricSampleAggregator.peekCurrentWindow();
  }

  /**
   * Get the most recent cluster load model before the given timestamp.
   *
   * @param now The current time in millisecond.
   * @param requirements the load requirements for getting the cluster model.
   * @param operationProgress the progress to report.
   * @return A cluster model with the configured number of windows whose timestamp is before given timestamp.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   */
  public ClusterModel clusterModel(long now,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    ClusterModel clusterModel = clusterModel(-1L, now, requirements, operationProgress);
    // Micro optimization: put the broker stats construction out of the lock.
    BrokerStats brokerStats = clusterModel.brokerStats(_config);
    // update the cached brokerLoadStats
    synchronized (this) {
      _cachedBrokerLoadStats = brokerStats;
      _cachedBrokerLoadGeneration = clusterModel.generation();
    }
    return clusterModel;
  }

  /**
   * Get the cluster load model for a time range.
   *
   * @param from start of the time window
   * @param to end of the time window
   * @param requirements the load completeness requirements.
   * @param operationProgress the progress of the job to report.
   * @return A cluster model with the available snapshots whose timestamp is in the given window.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   ModelCompletenessRequirements requirements,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    return clusterModel(from, to, requirements, false, operationProgress);
  }

  /**
   * Get the cluster load model for a time range.
   *
   * @param from start of the time window
   * @param to end of the time window
   * @param requirements the load completeness requirements.
   * @param populateReplicaPlacementInfo whether populate replica placement information.
   * @param operationProgress the progress of the job to report.
   * @return A cluster model with the available snapshots whose timestamp is in the given window.
   * @throws NotEnoughValidWindowsException If there is not enough sample to generate cluster model.
   */
  public ClusterModel clusterModel(long from,
                                   long to,
                                   ModelCompletenessRequirements requirements,
                                   boolean populateReplicaPlacementInfo,
                                   OperationProgress operationProgress)
      throws NotEnoughValidWindowsException {
    long start = System.currentTimeMillis();

    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    Cluster cluster = clusterAndGeneration.cluster();

    // Get the metric aggregation result.
    MetricSampleAggregationResult<String, PartitionEntity> partitionMetricSampleAggregationResult =
        _partitionMetricSampleAggregator.aggregate(clusterAndGeneration, from, to, requirements, operationProgress);
    Map<PartitionEntity, ValuesAndExtrapolations> partitionValuesAndExtrapolations = partitionMetricSampleAggregationResult.valuesAndExtrapolations();
    GeneratingClusterModel step = new GeneratingClusterModel(partitionValuesAndExtrapolations.size());
    operationProgress.addStep(step);

    // Create an empty cluster model first.
    long currentLoadGeneration = partitionMetricSampleAggregationResult.generation();
    ModelGeneration modelGeneration = new ModelGeneration(clusterAndGeneration.generation(), currentLoadGeneration);
    MetricSampleCompleteness<String, PartitionEntity> completeness = partitionMetricSampleAggregationResult.completeness();
    ClusterModel clusterModel = new ClusterModel(modelGeneration, completeness.validEntityRatio());

    final TimerContext ctx = _clusterModelCreationTimer.time();
    try {
      // Create the racks and brokers.
      // Shuffle nodes before getting their capacity from the capacity resolver.
      // This enables a capacity resolver to estimate the capacity of the nodes, for which the capacity retrieval has
      // failed.
      // The use case for this estimation is that if the capacity of one of the nodes is not available (e.g. due to some
      // 3rd party service issue), the capacity resolver may want to use the capacity of a peer node as the capacity for
      // that node.
      // To this end, Cruise Control handles the case that the first node is problematic so the capacity resolver does
      // not have the chance to get the capacity for the other nodes.
      // Shuffling the node order helps, as the problematic node is unlikely to always be the first node in the list.
      List<Node> shuffledNodes = new ArrayList<>(cluster.nodes());
      Collections.shuffle(shuffledNodes);
      for (Node node : shuffledNodes) {
        // If the rack is not specified, we use the host info as rack info.
        String rack = getRackHandleNull(node);
        clusterModel.createRack(rack);
        BrokerCapacityInfo brokerCapacity = _brokerCapacityConfigResolver.capacityForBroker(rack, node.host(), node.id());
        LOG.debug("Get capacity info for broker {}: total capacity {}, capacity by logdir {}.",
                  node.id(), brokerCapacity.capacity().get(Resource.DISK), brokerCapacity.diskCapacityByLogDir());
        clusterModel.createBroker(rack, node.host(), node.id(), brokerCapacity, populateReplicaPlacementInfo);
      }

      // Populate replica placement information for the cluster model if requested.
      Map<TopicPartition, Map<Integer, String>> replicaPlacementInfo = null;
      if (populateReplicaPlacementInfo) {
        replicaPlacementInfo = getReplicaPlacementInfo(clusterModel, cluster, _adminClient, _config);
      }

      // Populate snapshots for the cluster model.
      for (Map.Entry<PartitionEntity, ValuesAndExtrapolations> entry : partitionValuesAndExtrapolations.entrySet()) {
        TopicPartition tp = entry.getKey().tp();
        ValuesAndExtrapolations leaderLoad = entry.getValue();
        populatePartitionLoad(cluster, clusterModel, tp, leaderLoad, replicaPlacementInfo, _brokerCapacityConfigResolver);
        step.incrementPopulatedNumPartitions();
      }
      // Set the state of bad brokers in clusterModel based on the Kafka cluster state.
      setBadBrokerState(clusterModel, cluster);

      clusterModel.setTopicPlacements(clusterAndGeneration.topicPlacements());

      if (LOG.isDebugEnabled()) {
        LOG.debug("Generated cluster model in {} ms", System.currentTimeMillis() - start);
      }
    } finally {
      ctx.stop();
    }
    return clusterModel;
  }

  /**
   * Get the current cluster model generation. This is useful to avoid unnecessary cluster model creation which is
   * expensive.
   */
  public ModelGeneration clusterModelGeneration() {
    int clusterGeneration = _metadataClient.refreshMetadata().generation();
    return new ModelGeneration(clusterGeneration, _partitionMetricSampleAggregator.generation());
  }

  /**
   * Get the cached load.
   * @return The cached load, or null if (1) load or metadata is stale or (2) cached load violates capacity requirements.
   */
  public synchronized BrokerStats cachedBrokerLoadStats(boolean allowCapacityEstimation) {
    if (_cachedBrokerLoadGeneration != null
        && (allowCapacityEstimation || !_cachedBrokerLoadStats.isBrokerStatsEstimated())
        && _partitionMetricSampleAggregator.generation() == _cachedBrokerLoadGeneration.loadGeneration()
        && _metadataClient.refreshMetadata().generation() == _cachedBrokerLoadGeneration.clusterGeneration()) {
      return _cachedBrokerLoadStats;
    }
    return null;
  }

  /**
   * Get all the active brokers in the cluster based on the replica assignment. If a metadata refresh failed due to
   * timeout, the current metadata information will be used. This is to handle the case that all the brokers are down.
   * @param timeout the timeout in milliseconds.
   * @return All the brokers in the cluster that has at least one replica assigned.
   */
  public Set<Integer> brokersWithReplicas(int timeout) {
    Cluster kafkaCluster = _metadataClient.refreshMetadata(timeout).cluster();
    return MonitorUtils.brokersWithReplicas(kafkaCluster);
  }

  public MetadataClient.ClusterAndGeneration refreshClusterAndGeneration() {
    return _metadataClient.refreshMetadata();
  }

  /**
   * Check whether the monitored load meets the load requirements.
   */
  public boolean meetCompletenessRequirements(MetadataClient.ClusterAndGeneration clusterAndGeneration,
                                              ModelCompletenessRequirements requirements) {
    int numValidWindows =
        _partitionMetricSampleAggregator.validWindows(clusterAndGeneration,
                                                      requirements.minMonitoredPartitionsPercentage()).size();
    int requiredNumValidWindows = requirements.minRequiredNumWindows();
    return numValidWindows >= requiredNumValidWindows;
  }

  /**
   * Check whether the monitored load meets the load requirements.
   */
  public boolean meetCompletenessRequirements(ModelCompletenessRequirements requirements) {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();
    return meetCompletenessRequirements(clusterAndGeneration, requirements);
  }

  /**
   * @return all the available broker level metrics. Null is returned if nothing is available.
   */
  public MetricSampleAggregationResult<String, BrokerEntity> brokerMetrics() {
    List<Node> nodes = _metadataClient.cluster().nodes();
    Set<BrokerEntity> brokerEntities = new HashSet<>(nodes.size());
    for (Node node : nodes) {
      brokerEntities.add(new BrokerEntity(node.host(), node.id()));
    }
    return _brokerMetricSampleAggregator.aggregate(brokerEntities);
  }

  /**
   * Computes a replication throttle based on the network capacity and the current network usage
   *
   * @return The computed throttle rate in bytes per second
   */
  public long computeThrottle() {
    int networkCapacityMb = kafkaCluster().nodes().stream().map(node ->
            _brokerCapacityConfigResolver.capacityForBroker(node.rack(), node.host(), node.id()).capacity().get(Resource.NW_IN).intValue()
    ).min(Comparator.naturalOrder()).get() / 1024;

    // Use the cached broker stats; if there are none, generate the cluster model first
    BrokerStats brokerStats = cachedBrokerLoadStats(true);
    if (brokerStats == null) {
      try {
        clusterModel(_time.milliseconds(), _defaultModelCompletenessRequirements, new OperationProgress());
        brokerStats = cachedBrokerLoadStats(true);
        if (brokerStats == null) {
          throw new IllegalStateException("Cannot compute throttle because broker load stats are unavailable");
        }
      } catch (NotEnoughValidWindowsException e) {
        throw new IllegalStateException("Cannot compute throttle because there are not enough valid metrics windows");
      }
    }

    double maxBrokerIngressMb = brokerStats.stats().stream()
            .map(SingleBrokerStats::bytesIn).max(Comparator.naturalOrder()).get() / 1024;
    double maxBrokerEgressMb = brokerStats.stats().stream()
            .map(SingleBrokerStats::bytesOut).max(Comparator.naturalOrder()).get() / 1024;
    double instanceLimitMb = networkCapacityMb - (_writeMultiplier * maxBrokerIngressMb + _readMultiplier * maxBrokerEgressMb);
    double volumeLimitMb = _maxVolumeThroughputMb - (maxBrokerIngressMb + _diskReadRatio * maxBrokerEgressMb);
    long throttle = (long) (_networkThrottleRatio * 1024 * 1024 * Math.min(instanceLimitMb, volumeLimitMb));
    String parametersStr = String.format(
        "networkCapacityMb: %s, maxBrokerIngressMb: %s, maxBrokerEgressMb: %s, instanceLimitMb: %s, volumeLimitMb: %s",
        networkCapacityMb, maxBrokerIngressMb, maxBrokerEgressMb, instanceLimitMb, volumeLimitMb);

    if (throttle < 0) {
      LOG.error("Failed to compute a valid throttle - {} ({})",
          throttle, parametersStr);

      throw new IllegalStateException("Could not compute a positive throttle value");
    }
    LOG.debug(parametersStr);
    return throttle;
  }

  /**
   * Package private for unit test.
   */
  KafkaPartitionMetricSampleAggregator partitionSampleAggregator() {
    return _partitionMetricSampleAggregator;
  }

  /**
   * Package private for unit test.
   */
  KafkaBrokerMetricSampleAggregator brokerSampleAggregator() {
    return _brokerMetricSampleAggregator;
  }

  /**
   * Get all the dead brokers in the cluster based on the replica assignment. If a metadata refresh failed due to
   * timeout, the current metadata information will be used. This is to handle the case that all the brokers are down.
   * @param timeout the timeout in milliseconds.
   * @return All the dead brokers which host some replicas in the cluster.
   */
  public Set<Integer> deadBrokersWithReplicas(int timeout) {
    Cluster kafkaCluster = _metadataClient.refreshMetadata(timeout).cluster();
    return MonitorUtils.deadBrokersWithReplicas(kafkaCluster);
  }

  /**
   * Get all the brokers having offline replca in the cluster based on the partition assignment. If a metadata refresh failed
   * due to timeout, the current metadata information will be used. This is to handle the case that all the brokers are down.
   * @param timeout the timeout in milliseconds.
   * @return All the brokers in the cluster that has at least one offline replica.
   */
  public Set<Integer> brokersWithOfflineReplicas(int timeout) {
    Cluster kafkaCluster = _metadataClient.refreshMetadata(timeout).cluster();
    return MonitorUtils.brokersWithOfflineReplicas(kafkaCluster);
  }

  private int numValidSnapshotWindows() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _numValidSnapshotWindows : -1;
  }

  private int totalMonitoredSnapshotWindows() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _totalMonitoredSnapshotWindows : -1;
  }

  private double monitoredPartitionsPercentage() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _monitoredPartitionsPercentage : 0.0;
  }

  private int numPartitionsWithExtrapolations() {
    return _lastUpdate + SensorUpdater.UPDATE_TIMEOUT_MS > System.currentTimeMillis() ? _numPartitionsWithExtrapolations : -1;
  }

  private double getMonitoredPartitionsPercentage() {
    MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.refreshMetadata();

    Cluster kafkaCluster = clusterAndGeneration.cluster();
    MetricSampleAggregationResult<String, PartitionEntity> metricSampleAggregationResult;
    try {
      metricSampleAggregationResult = _partitionMetricSampleAggregator.aggregate(clusterAndGeneration,
                                                                                 System.currentTimeMillis(),
                                                                                 new OperationProgress());
    } catch (NotEnoughValidWindowsException e) {
      return 0.0;
    }
    Map<PartitionEntity, ValuesAndExtrapolations> partitionLoads = metricSampleAggregationResult.valuesAndExtrapolations();
    AtomicInteger numPartitionsWithExtrapolations = new AtomicInteger(0);
    partitionLoads.values().forEach(valuesAndExtrapolations -> {
      if (!valuesAndExtrapolations.extrapolations().isEmpty()) {
        numPartitionsWithExtrapolations.incrementAndGet();
      }
    });
    _numPartitionsWithExtrapolations = numPartitionsWithExtrapolations.get();
    int totalNumPartitions = MonitorUtils.totalNumPartitions(kafkaCluster);
    return totalNumPartitions > 0 ? metricSampleAggregationResult.completeness().validEntityRatio() : 0.0;
  }

  /**
   * We have a separate class to update values for sensors.
   */
  private class SensorUpdater implements Runnable {
    // The interval for sensor value update.
    static final long UPDATE_INTERVAL_MS = 30000;
    // The maximum time allowed to make an update. If the sensor value cannot be updated in time, the sensor value
    // will be invalidated.
    static final long UPDATE_TIMEOUT_MS = 10 * UPDATE_INTERVAL_MS;

    @Override
    public void run() {
      try {
        MetadataClient.ClusterAndGeneration clusterAndGeneration = _metadataClient.clusterAndGeneration();
        double minMonitoredPartitionsPercentage = _defaultModelCompletenessRequirements.minMonitoredPartitionsPercentage();
        _numValidSnapshotWindows = _partitionMetricSampleAggregator.validWindows(clusterAndGeneration,
                                                                                 minMonitoredPartitionsPercentage)
                                                                   .size();
        _monitoredPartitionsPercentage = getMonitoredPartitionsPercentage();
        _totalMonitoredSnapshotWindows = _partitionMetricSampleAggregator.allWindows().size();
        _lastUpdate = System.currentTimeMillis();
      } catch (Throwable t) {
        // We catch all the throwables because we don't want the sensor updater to die
        LOG.warn("Load monitor sensor updater received exception ", t);
      }
    }
  }

  /**
   * Background task to clean up the partition metric samples in case of topic deletion.
   *
   * Due to Kafka bugs, the returned metadata may not contain all the topics during broker bounce.
   * To handle that, we refresh metadata a few times and take a union of all the topics seen as the existing topics
   * -- in intervals of ({@link #CHECK_INTERVAL_MS} * {@link #REFRESH_LIMIT}).
   */
  private class PartitionMetricSampleAggregatorCleaner implements Runnable {
    static final long CHECK_INTERVAL_MS = 37500;
    static final short REFRESH_LIMIT = 8;
    // A set remember all the topics seen from last metadata refresh.
    private final Set<String> _allTopics = new HashSet<>();
    // The metadata refresh count.
    private int _refreshCount = 0;
    @Override
    public void run() {
      _allTopics.addAll(_metadataClient.refreshMetadata().cluster().topics());
      _refreshCount++;
      if (_refreshCount % REFRESH_LIMIT == 0) {
        _partitionMetricSampleAggregator.retainEntityGroup(_allTopics);
        _allTopics.clear();
      }
    }
  }

  public class AutoCloseableSemaphore implements AutoCloseable {
    private AtomicBoolean _closed = new AtomicBoolean(false);
    @Override
    public void close() {
      if (_closed.compareAndSet(false, true)) {
        _clusterModelSemaphore.release();
        _acquiredClusterModelSemaphore.set(false);
      }
    }
  }
}
