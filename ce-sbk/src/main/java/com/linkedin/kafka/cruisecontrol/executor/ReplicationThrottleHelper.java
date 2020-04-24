/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.cruisecontrol.common.utils.Utils;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import kafka.admin.AdminOperationException;
import kafka.log.LogConfig;
import kafka.server.ConfigType;
import kafka.server.KafkaConfig;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig.AUTO_THROTTLE;
import static java.lang.String.valueOf;
import static scala.collection.JavaConverters.asJavaCollection;

/**
 * CONFLUENT: We use a single instance of this helper throughout the
 * code in order to allow our proprietary update_throttle endpoint to
 * modify the throttle for ongoing rebalances
 * See https://kafka.apache.org/documentation/#rep-throttle
 */
class ReplicationThrottleHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationThrottleHelper.class);

  static final String LEADER_THROTTLED_RATE = KafkaConfig.LeaderReplicationThrottledRateProp();
  static final String FOLLOWER_THROTTLED_RATE = KafkaConfig.FollowerReplicationThrottledRateProp();
  static final String LEADER_THROTTLED_REPLICAS = LogConfig.LeaderReplicationThrottledReplicasProp();
  static final String FOLLOWER_THROTTLED_REPLICAS = LogConfig.FollowerReplicationThrottledReplicasProp();

  private final KafkaZkClient _kafkaZkClient;
  private final AdminZkClient _adminZkClient;
  private Admin _adminClient;
  private Long _throttleRate;
  private final Long _originalThrottleRate;

  ReplicationThrottleHelper(KafkaZkClient kafkaZkClient, Admin adminClient, Long throttleRate) {
    this._kafkaZkClient = kafkaZkClient;
    this._adminZkClient = new AdminZkClient(kafkaZkClient);
    this._adminClient = adminClient;
    this._throttleRate = throttleRate;
    this._originalThrottleRate = throttleRate;
    LOG.info("Set throttle rate {}", this._throttleRate);
  }

  Long getThrottleRate() {
    return this._throttleRate;
  }

  boolean setThrottleRate(Long throttleRate) {
    if ((_originalThrottleRate == null && throttleRate == null) ||
        (_originalThrottleRate != null && throttleRate != null) && _originalThrottleRate.equals(throttleRate)) {
      LOG.warn("Ignored setting requested throttle rate {} " +
          "because it is the same as the originally configured rate", throttleRate);
      return false;
    }
    this._throttleRate = throttleRate;
    return true;
  }

  void setThrottles(List<ExecutionProposal> replicaMovementProposals, LoadMonitor loadMonitor)
      throws InterruptedException {
    if (throttlingEnabled()) {

      // Compute the throttle from the current load. Because this method is only called during proposal execution,
      // the load monitor will have enough metrics for this computation
      if (this._throttleRate == AUTO_THROTTLE) {
        this._throttleRate = loadMonitor.computeThrottle();
      }
      Set<Integer> participatingBrokers = getParticipatingBrokers(replicaMovementProposals);
      Set<Integer> brokersWithStaticThrottles = getBrokersWithStaticThrottles(participatingBrokers);
      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(replicaMovementProposals,
          brokersWithStaticThrottles);
      LOG.info("Setting a rebalance throttle of {} bytes/sec to {} brokers and {} topics. " +
              "Brokers {} already have static throttles set",
          _throttleRate,
          Utils.join(participatingBrokers, ", "),
          Utils.join(throttledReplicas.keySet(), ", "),
          Utils.join(brokersWithStaticThrottles, ", "));

      participatingBrokers.forEach(this::setLeaderThrottledRateIfUnset);
      participatingBrokers.forEach(this::setFollowerThrottledRateIfUnset);
      throttledReplicas.forEach(this::setLeaderThrottledReplicas);
      throttledReplicas.forEach(this::setFollowerThrottledReplicas);
    } else {
      LOG.info("Skipped setting rebalance throttle because it is not enabled");
    }
  }

  /**
   * @param participatingBrokers - the brokers to check for static throttles
   * @return - a set of broker IDs whose static replication throttle for all replicas is enabled
   */
  private Set<Integer> getBrokersWithStaticThrottles(Set<Integer> participatingBrokers) throws InterruptedException {
    // filter only the brokers who have both leader and follower throttling enabled
    Predicate<Map.Entry<ConfigResource, Config>> staticThrottleFilter =
        configResourceConfigEntry -> {
      ConfigEntry leaderConfig =
          configResourceConfigEntry.getValue().get(
              LogConfig.LeaderReplicationThrottledReplicasProp());
      ConfigEntry followerConfig =
          configResourceConfigEntry.getValue().get(
              LogConfig.FollowerReplicationThrottledReplicasProp());
      return (leaderConfig != null && leaderConfig.value() != null &&
              leaderConfig.value().equals("*"))
          && (followerConfig != null && followerConfig.value() != null &&
              followerConfig.value().equals("*"));
    };

    List<ConfigResource> configResources = participatingBrokers.stream()
        .map(brokerId -> new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString()))
        .collect(Collectors.toList());

    try {
      Map<ConfigResource, Config> configs = _adminClient.describeConfigs(configResources).all().get();

      return configs.entrySet().stream().filter(staticThrottleFilter)
          .map(entry -> Integer.parseInt(entry.getKey().name()))
          .collect(Collectors.toSet());
    } catch (InterruptedException ie) {
      throw ie;
    } catch (Exception e) {
      LOG.error("Caught Exception while describing static throttle configs", e);
      return Collections.emptySet();
    }
  }

  // Reset back to AUTO_THROTTLE after an execution completes. This allows us to compute a new throttle once at the
  // beginning of each execution
  void resetThrottleAfterExecution() {
    if (this._originalThrottleRate != null && this._originalThrottleRate == AUTO_THROTTLE) {
      this._throttleRate = AUTO_THROTTLE;
    }
  }

  // Determines if a candidate task is ready to have its throttles removed.
  boolean shouldRemoveThrottleForTask(ExecutionTask task) {
    return
      // the task should not be in progress
      task.state() != ExecutionTask.State.IN_PROGRESS &&
        // the task should not be pending
        task.state() != ExecutionTask.State.PENDING &&
        // replica throttles only apply to inter-broker replica movement
        task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // determines if a candidate task is in progress and related to inter-broker
  // replica movement.
  boolean taskIsInProgress(ExecutionTask task) {
    return
      task.state() == ExecutionTask.State.IN_PROGRESS &&
        task.type() == ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION;
  }

  // clear throttles for a specific list of execution tasks
  void clearThrottles(List<ExecutionTask> completedTasks, List<ExecutionTask> inProgressTasks) {
    if (throttlingEnabled()) {
      List<ExecutionProposal> completedProposals =
        completedTasks
          .stream()
          // Filter for completed tasks related to inter-broker replica movement
          .filter(this::shouldRemoveThrottleForTask)
          .map(ExecutionTask::proposal)
          .collect(Collectors.toList());

      // These are the brokers which have completed a task with
      // inter-broker replica movement
      Set<Integer> participatingBrokers = getParticipatingBrokers(completedProposals);

      List<ExecutionProposal> inProgressProposals =
        inProgressTasks
          .stream()
          .filter(this::taskIsInProgress)
          .map(ExecutionTask::proposal)
          .collect(Collectors.toList());

      // These are the brokers which currently have in-progress
      // inter-broker replica movement
      Set<Integer> brokersWithInProgressTasks = getParticipatingBrokers(inProgressProposals);

      // Remove the brokers with in-progress replica moves from the brokers that have
      // completed inter-broker replica moves
      Set<Integer> brokersToRemoveThrottlesFrom = new TreeSet<>(participatingBrokers);
      brokersToRemoveThrottlesFrom.removeAll(brokersWithInProgressTasks);

      LOG.info("Removing replica movement throttles from brokers in the cluster: {}", brokersToRemoveThrottlesFrom);
      brokersToRemoveThrottlesFrom.forEach(this::removeThrottledRateFromBroker);

      Map<String, Set<String>> throttledReplicas = getThrottledReplicasByTopic(completedProposals, Collections.emptySet());
      throttledReplicas.forEach(this::removeThrottledReplicasFromTopic);
    }
  }

  private boolean throttlingEnabled() {
    return _throttleRate != null;
  }

  private Set<Integer> getParticipatingBrokers(List<ExecutionProposal> replicaMovementProposals) {
    Set<Integer> participatingBrokers = new TreeSet<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      participatingBrokers.addAll(proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
      participatingBrokers.addAll(proposal.newReplicas().stream().map(ReplicaPlacementInfo::brokerId).collect(Collectors.toSet()));
    }
    return participatingBrokers;
  }

  /**
   * Builds up the throttled.replicas config per topic.
   * It skips partitions if all replicas have static replication throttling enabled
   *
   * @param brokerWithStaticThrottles - the broker IDs which have replication throttling
   *                                  enabled for all replicas
   * @return A map of topics to sets of replica pairs
   */
  private Map<String, Set<String>> getThrottledReplicasByTopic(List<ExecutionProposal> replicaMovementProposals,
                                                               Set<Integer> brokerWithStaticThrottles) {
    Map<String, Set<String>> throttledReplicasByTopic = new HashMap<>();
    for (ExecutionProposal proposal : replicaMovementProposals) {
      String topic = proposal.topic();
      int partitionId = proposal.partitionId();
      List<Integer> brokers = Stream.concat(
        proposal.oldReplicas().stream().map(ReplicaPlacementInfo::brokerId),
        proposal.replicasToAdd().stream().map(ReplicaPlacementInfo::brokerId)
      ).collect(Collectors.toList());

      if (brokerWithStaticThrottles.containsAll(brokers)) {
        // skip setting a throttle as all brokers have it enabled anyway
        continue;
      }
      // if all brokers don't have static throttles, we want
      // to set all brokers' throttle, otherwise it would override the static setting
      // and unthrottle the ones we haven't set it for
      Set<String> throttledReplicas = throttledReplicasByTopic
        .computeIfAbsent(topic, x -> new TreeSet<>());
      brokers.forEach(brokerId -> throttledReplicas.add(partitionId + ":" + brokerId));
    }

    return throttledReplicasByTopic;
  }

  private void setLeaderThrottledRateIfUnset(int brokerId) {
    setThrottledRateIfUnset(brokerId, LEADER_THROTTLED_RATE);
  }

  private void setFollowerThrottledRateIfUnset(int brokerId) {
    setThrottledRateIfUnset(brokerId, FOLLOWER_THROTTLED_RATE);
  }

  private void setThrottledRateIfUnset(int brokerId, String configKey) {
    assert _throttleRate != null;
    assert configKey.equals(LEADER_THROTTLED_RATE) || configKey.equals(FOLLOWER_THROTTLED_RATE);
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Broker(), String.valueOf(brokerId));
    Object oldThrottleRate = config.setProperty(configKey, String.valueOf(_throttleRate));
    if (oldThrottleRate == null) {
      LOG.debug("Setting {} to {} bytes/second for broker {}", configKey, _throttleRate, brokerId);
      ExecutorUtils.changeBrokerConfig(_adminZkClient, brokerId, config);
    } else {
      LOG.debug("Not setting {} for broker {} because pre-existing throttle of {} was already set",
        configKey, brokerId, oldThrottleRate);
    }
  }

  private void setLeaderThrottledReplicas(String topic, Set<String> replicas) {
    setThrottledReplicas(topic, replicas, LEADER_THROTTLED_REPLICAS);
  }

  private void setFollowerThrottledReplicas(String topic, Set<String> replicas) {
    setThrottledReplicas(topic, replicas, FOLLOWER_THROTTLED_REPLICAS);
  }

  private void setThrottledReplicas(String topic, Set<String> replicas, String configKey) {
    assert configKey.equals(LEADER_THROTTLED_REPLICAS) || configKey.equals(FOLLOWER_THROTTLED_REPLICAS);
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    // Merge new throttled replicas with existing configuration values.
    Set<String> newThrottledReplicas = new TreeSet<>(replicas);
    String oldThrottledReplicas = config.getProperty(configKey);
    if (oldThrottledReplicas != null) {
      newThrottledReplicas.addAll(Arrays.asList(oldThrottledReplicas.split(",")));
    }
    String throttledReplicasStr = String.join(",", newThrottledReplicas);
    config.setProperty(configKey, throttledReplicasStr);
    try {
      ExecutorUtils.changeTopicConfig(_adminZkClient, topic, config);
    } catch (AdminOperationException e) {
      LOG.info("Not setting throttled replicas {} for topic {} because it does not exist",
          throttledReplicasStr, topic);
    }
  }

  static String removeReplicasFromConfig(String throttleConfig, Set<String> replicas) {
    ArrayList<String> throttles = new ArrayList<>(Arrays.asList(throttleConfig.split(",")));
    throttles.removeIf(replicas::contains);
    return String.join(",", throttles);
  }

  private void removeLeaderThrottledReplicasFromTopic(Properties config, String topic, Set<String> replicas) {
    String oldLeaderThrottledReplicas = config.getProperty(LEADER_THROTTLED_REPLICAS);
    if (oldLeaderThrottledReplicas != null) {
      replicas.forEach(r -> LOG.debug("Removing leader throttles for topic {} on replica {}", topic, r));
      String newLeaderThrottledReplicas = removeReplicasFromConfig(oldLeaderThrottledReplicas, replicas);
      if (newLeaderThrottledReplicas.isEmpty()) {
        config.remove(LEADER_THROTTLED_REPLICAS);
      } else {
        config.setProperty(LEADER_THROTTLED_REPLICAS, newLeaderThrottledReplicas);
      }
    }
  }

  private void removeFollowerThrottledReplicasFromTopic(Properties config, String topic, Set<String> replicas) {
    String oldLeaderThrottledReplicas = config.getProperty(FOLLOWER_THROTTLED_REPLICAS);
    if (oldLeaderThrottledReplicas != null) {
      replicas.forEach(r -> LOG.debug("Removing follower throttles for topic {} and replica {}", topic, r));
      String newLeaderThrottledReplicas = removeReplicasFromConfig(oldLeaderThrottledReplicas, replicas);
      if (newLeaderThrottledReplicas.isEmpty()) {
        config.remove(FOLLOWER_THROTTLED_REPLICAS);
      } else {
        config.setProperty(FOLLOWER_THROTTLED_REPLICAS, newLeaderThrottledReplicas);
      }
    }
  }

  private void removeThrottledReplicasFromTopic(String topic, Set<String> replicas) {
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    removeLeaderThrottledReplicasFromTopic(config, topic, replicas);
    removeFollowerThrottledReplicasFromTopic(config, topic, replicas);
    try {
      ExecutorUtils.changeTopicConfig(_adminZkClient, topic, config);
    } catch (AdminOperationException e) {
      LOG.warn("Skip removing throttled replicas {} for topic {} due to error {}",
              replicas, topic, e);
    }
  }

  private void removeAllThrottledReplicasFromTopic(String topic) {
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic);
    Object oldLeaderThrottle = config.remove(LEADER_THROTTLED_REPLICAS);
    Object oldFollowerThrottle = config.remove(FOLLOWER_THROTTLED_REPLICAS);
    if (oldLeaderThrottle != null) {
      LOG.debug("Removing leader throttled replicas for topic {}", topic);
    }
    if (oldFollowerThrottle != null) {
      LOG.debug("Removing follower throttled replicas for topic {}", topic);
    }
    if (oldLeaderThrottle != null || oldFollowerThrottle != null) {
      ExecutorUtils.changeTopicConfig(_adminZkClient, topic, config);
    }
  }

  private void removeThrottledRateFromBroker(Integer brokerId) {
    Properties config = _kafkaZkClient.getEntityConfigs(ConfigType.Broker(), String.valueOf(brokerId));
    Object oldLeaderThrottle = config.remove(LEADER_THROTTLED_RATE);
    Object oldFollowerThrottle = config.remove(FOLLOWER_THROTTLED_RATE);
    if (oldLeaderThrottle != null) {
      LOG.debug("Removing leader throttle on broker {}", brokerId);
    }
    if (oldFollowerThrottle != null) {
      LOG.debug("Removing follower throttle on broker {}", brokerId);
    }
    if (oldLeaderThrottle != null || oldFollowerThrottle != null) {
      ExecutorUtils.changeBrokerConfig(_adminZkClient, brokerId, config);
    }
  }

  /**
   * Updates or sets the given throttle rate on the brokers
   * @param newThrottle - the new throttle, in bytes per second. If null, we remove throttles
   */
  public int updateOrRemoveThrottleRate(Long newThrottle) {
    int updatedBrokers = 0;
    for (kafka.cluster.Broker broker : asJavaCollection(_kafkaZkClient.getAllBrokersInCluster())) {
      Properties configs = new Properties();
      configs.putAll(_kafkaZkClient.getEntityConfigs(ConfigType.Broker(), valueOf(broker.id())));

      boolean updatedLeaderRate = false;
      boolean updatedFollowerRate = false;

      if (newThrottle == null) {
        updatedLeaderRate = configs.remove(KafkaConfig.LeaderReplicationThrottledRateProp()) != null;
        updatedFollowerRate = configs.remove(KafkaConfig.FollowerReplicationThrottledRateProp()) != null;
      } else {
        if (!configs.getProperty(KafkaConfig.LeaderReplicationThrottledRateProp(), "").equals(Long.toString(newThrottle))) {
          configs.setProperty(KafkaConfig.LeaderReplicationThrottledRateProp(), Long.toString(newThrottle));
          updatedLeaderRate = true;
        }

        if (!configs.getProperty(KafkaConfig.FollowerReplicationThrottledRateProp(), "").equals(Long.toString(newThrottle))) {
          configs.setProperty(KafkaConfig.FollowerReplicationThrottledRateProp(), Long.toString(newThrottle));
          updatedFollowerRate = true;
        }
      }

      if (updatedLeaderRate || updatedFollowerRate) {
        _kafkaZkClient.setOrCreateEntityConfigs(ConfigType.Broker(), valueOf(broker.id()), configs);
        ++updatedBrokers;
      }
    }

    return updatedBrokers;
  }

  /**
   * Removes all throttles from all the topics and brokers in the cluster
   */
  void removeAllThrottles() {
    int throttledTopics = 0;
    for (String topic : asJavaCollection(_kafkaZkClient.getAllTopicsInCluster(false))) {
      Properties configs = new Properties();
      configs.putAll(_kafkaZkClient.getEntityConfigs(ConfigType.Topic(), topic));
      boolean removedLeaderReplicas = configs.remove(LogConfig.LeaderReplicationThrottledReplicasProp()) != null;
      boolean removedFollowerReplicas = configs.remove(LogConfig.FollowerReplicationThrottledReplicasProp()) != null;
      if (removedLeaderReplicas || removedFollowerReplicas) {
        _kafkaZkClient.setOrCreateEntityConfigs(ConfigType.Topic(), topic, configs);
        ++throttledTopics;
      }
    }
    LOG.info("Removed throttled replicas config from {} topics", throttledTopics);

    int throttledBrokers = updateOrRemoveThrottleRate(null);

    LOG.info("Removed throttle rate config from {} brokers", throttledBrokers);
  }
}
