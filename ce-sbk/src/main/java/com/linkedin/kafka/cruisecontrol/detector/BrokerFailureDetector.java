/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.detector.Anomaly;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import kafka.utils.ShutdownableThread;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import static com.linkedin.kafka.cruisecontrol.detector.AnomalyDetectorUtils.MAX_METADATA_WAIT_MS;
import static java.util.stream.Collectors.toSet;


/**
 * This class detects broker failures.
 */
public class BrokerFailureDetector extends ShutdownableThread {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerFailureDetector.class);
  private static final String ZK_BROKER_FAILURE_METRIC_GROUP = "CruiseControlAnomaly";
  private static final String ZK_BROKER_FAILURE_METRIC_TYPE = "BrokerFailure";
  private static final String THREAD_NAME = "SBK_BrokerFailureDetector";
  private final KafkaCruiseControl _kafkaCruiseControl;
  private final KafkaZkClient kafkaZkClient;
  private final Map<Integer, Long> _failedBrokers;
  private final LoadMonitor _loadMonitor;
  private final Queue<Anomaly> _anomalies;
  private final Time _time;
  private final boolean _allowCapacityEstimation;
  private final boolean _excludeRecentlyDemotedBrokers;
  private final boolean _excludeRecentlyRemovedBrokers;
  private final List<String> _selfHealingGoals;
  private final ArrayBlockingQueue<BrokerChangedEvent> eventQueue;
  private boolean initialized;

  public BrokerFailureDetector(KafkaCruiseControlConfig config,
      LoadMonitor loadMonitor,
      Queue<Anomaly> anomalies,
      Time time,
      KafkaCruiseControl kafkaCruiseControl,
      List<String> selfHealingGoals) {
    super(THREAD_NAME, true);
    String zkUrl = config.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    boolean zkSecurityEnabled = config.getBoolean(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    // Do not support secure ZK at this point.
    kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zkUrl, ZK_BROKER_FAILURE_METRIC_GROUP, ZK_BROKER_FAILURE_METRIC_TYPE,
        zkSecurityEnabled);
    _failedBrokers = new HashMap<>();
    _loadMonitor = loadMonitor;
    _anomalies = anomalies;
    _time = time;
    _kafkaCruiseControl = kafkaCruiseControl;
    _allowCapacityEstimation = config.getBoolean(KafkaCruiseControlConfig.ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG);
    _excludeRecentlyDemotedBrokers = config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG);
    _excludeRecentlyRemovedBrokers = config.getBoolean(KafkaCruiseControlConfig.BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG);
    _selfHealingGoals = selfHealingGoals;

    // Currently, the only event that we expect is the broker failure event. On
    // a normal day, when we have a few Broker failures, we should be able to
    // process the failure immediately. In the exceptional case when the queue is
    // full, we just drop the change events and log an error.
    // Dropping events if the queue is full, is fine and does not affect
    // detection. We fetch the current brokers from ZK after receiving a change
    // events. Any additional events that arrive after we fetched brokers,
    // will result in a new call to fetch brokers from ZK.
    eventQueue = new ArrayBlockingQueue<BrokerChangedEvent>(10);
  }

  private class BrokerChangedEvent {
  }

  void startDetection() {
    start();
  }

  private void detectBrokerFailures(Set<Integer> aliveBrokers) {
    // update the failed broker information based on the current state.
    boolean updated = updateFailedBrokers(aliveBrokers);
    if (updated) {
      // persist the updated failed broker list.
      persistFailedBrokerList();
    }
    // Report the failures to anomaly detector to handle.
    reportBrokerFailures();
  }

  private void detectBrokerFailures() {
    detectBrokerFailures(aliveBrokers());
  }

  void scheduleDetection() {
    if (!eventQueue.offer(new BrokerChangedEvent())) {
      // Dropping events if the queue is full, is fine and does not affect
      // detection. We fetch the current brokers from ZK after receiving a change
      // event. Any additional events that arrive after we fetched brokers,
      // will result in a new call to fetch brokers from ZK.
      LOG.warn("Broker Failed Event Queue is full.");
    }
  }

  /**
   * "// Package-private for testing"
   * This function is not thread-safe and should not be used otherwise.
   */
  Map<Integer, Long> failedBrokers() {
    return new HashMap<>(_failedBrokers);
  }

  void shutdownNow() {
    this.shutdown();
    KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
  }

  private void persistFailedBrokerList() {
    kafkaZkClient.setOrCreateFailedBrokerList(failedBrokerString());
  }

  private void loadPersistedFailedBrokerList() {
    String failedBrokerListString = kafkaZkClient.getFailedBrokerList();
    parsePersistedFailedBrokers(failedBrokerListString);
  }

  /**
   * If {@link #_failedBrokers} has changed, update it.
   *
   * @param aliveBrokers Alive brokers in the cluster.
   * @return true if {@link #_failedBrokers} has been updated, false otherwise.
   */
  private boolean updateFailedBrokers(Set<Integer> aliveBrokers) {
    // We get the complete broker list from metadata. i.e. any broker that still has a partition assigned to it is
    // included in the broker list. If we cannot update metadata in 60 seconds, skip
    Set<Integer> currentFailedBrokers = _loadMonitor.brokersWithReplicas(MAX_METADATA_WAIT_MS);
    currentFailedBrokers.removeAll(aliveBrokers);
    LOG.debug("Alive brokers: {}, failed brokers: {}", aliveBrokers, currentFailedBrokers);
    // Remove broker that is no longer failed.
    boolean updated = _failedBrokers.entrySet().removeIf(entry -> !currentFailedBrokers.contains(entry.getKey()));
    // Add broker that has just failed.
    for (Integer brokerId : currentFailedBrokers) {
      if (_failedBrokers.putIfAbsent(brokerId, _time.milliseconds()) == null) {
        updated = true;
      }
    }
    return updated;
  }

  private Set<Integer> aliveBrokers() {
    // We get the alive brokers from ZK directly.
    return JavaConverters.asJavaCollection(kafkaZkClient.getSortedBrokerList())
      .stream().map(n -> (Integer) n).collect(toSet());
  }

  private String failedBrokerString() {
    StringBuilder sb = new StringBuilder();
    for (Iterator<Map.Entry<Integer, Long>> iter = _failedBrokers.entrySet().iterator(); iter.hasNext(); ) {
      Map.Entry<Integer, Long> entry = iter.next();
      sb.append(entry.getKey()).append("=").append(entry.getValue());
      if (iter.hasNext()) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  private void parsePersistedFailedBrokers(String failedBrokerListString) {
    if (failedBrokerListString != null && !failedBrokerListString.isEmpty()) {
      String[] entries = failedBrokerListString.split(",");
      for (String entry : entries) {
        String[] idAndTimestamp = entry.split("=");
        if (idAndTimestamp.length != 2) {
          throw new IllegalStateException(
              "The persisted failed broker string cannot be parsed. The string is " + failedBrokerListString);
        }
        _failedBrokers.putIfAbsent(Integer.parseInt(idAndTimestamp[0]), Long.parseLong(idAndTimestamp[1]));
      }
    }
  }

  private void reportBrokerFailures() {
    if (!_failedBrokers.isEmpty()) {
      _anomalies.add(new BrokerFailures(_kafkaCruiseControl,
            failedBrokers(),
            _allowCapacityEstimation,
            _excludeRecentlyDemotedBrokers,
            _excludeRecentlyRemovedBrokers,
            _selfHealingGoals));
    }
  }

  private class BrokerFailureListener implements KafkaZkClient.BrokerChangeListener {
    @Override
    public void handleChildChange() {
      scheduleDetection();
    }
  }

  private void initialize() {
    kafkaZkClient.registerBrokerChangeHandler(new BrokerFailureListener());
    // Load the failed broker information from zookeeper.
    loadPersistedFailedBrokerList();
    // Detect broker failures.
    detectBrokerFailures();
    initialized = true;
  }

  /**
   * This function is called continuously in a loop, till the failure detector
   * thread is shutdown.
   */
  @Override
  public void doWork() {
    if (!initialized) {
      initialize();
      LOG.debug("Broker Failure Detector initialized.");
    }

    try {
      eventQueue.take();
      LOG.debug("Broker Failure detected.");
      //Drain all the events and fetch the brokers from zookeeper once. Since,
      //the queue only contains BrokerFailedEvents, we only process the changes
      //once.
      eventQueue.clear();
      detectBrokerFailures();
    } catch (InterruptedException e) {
      LOG.info("Broker failure detector interrupted. Exiting the doWork loop.");
    }
  }
}
