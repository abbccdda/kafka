/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.kafkaassigner.KafkaAssignerEvenRackAwareGoal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.servlet.response.CruiseControlState;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Util class for convenience.
 */
public class KafkaCruiseControlUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlUtils.class);
  public static final double MAX_BALANCEDNESS_SCORE = 100.0;
  public static final int ZK_SESSION_TIMEOUT = 30000;
  public static final int ZK_CONNECTION_TIMEOUT = 30000;
  public static final long KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS = 10000;
  public static final long ADMIN_CLIENT_CLOSE_TIMEOUT_MS = 10000;
  public static final String DATE_FORMAT = "YYYY-MM-dd_HH:mm:ss z";
  public static final String DATE_FORMAT2 = "dd/MM/yyyy HH:mm:ss";
  public static final String TIME_ZONE = "UTC";
  public static final int SEC_TO_MS = 1000;
  public static final int BYTES_IN_MB = 1024 * 1024;
  private static final int MIN_TO_MS = SEC_TO_MS * 60;
  private static final int HOUR_TO_MS = MIN_TO_MS * 60;
  private static final int DAY_TO_MS = HOUR_TO_MS * 24;
  private static final Set<String> KAFKA_ASSIGNER_GOALS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(KafkaAssignerEvenRackAwareGoal.class.getSimpleName(),
                                                              KafkaAssignerDiskUsageDistributionGoal.class.getSimpleName())));
  public static final String OPERATION_LOGGER = "operationLogger";
  public static final String ZK_CLIENT_NAME = "Cruise Control ZK Client";

  private KafkaCruiseControlUtils() {

  }

  public static String currentUtcDate() {
    return utcDateFor(System.currentTimeMillis());
  }

  public static String utcDateFor(long timeMs) {
    Date date = new Date(timeMs);
    DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
    formatter.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
    return formatter.format(date);
  }

  /**
   * Format the timestamp from long to a human readable string.
   */
  public static String toDateString(long time) {
    return toDateString(time, DATE_FORMAT2, "");
  }

  /**
   * Format the timestamp from long to human readable string. Allow customization of date format and time zone.
   * @param time time in milliseconds
   * @param dateFormat see formats above
   * @param timeZone will use default if timeZone is set to empty string
   * @return string representation of date
   */
  public static String toDateString(long time, String dateFormat, String timeZone) {
    if (time < 0) {
      throw new IllegalArgumentException(String.format("Attempt to convert negative time %d to date.", time));
    }
    DateFormat formatter = new SimpleDateFormat(dateFormat);
    if (!timeZone.isEmpty()) {
      formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
    }
    return formatter.format(new Date(time));
  }

  /**
   * Format the duration from double to human readable string.
   * @param durationMs Duration in milliseconds
   * @return String representation of duration
   */
  public static String toPrettyDuration(double durationMs) {
    if (durationMs < 0) {
      throw new IllegalArgumentException(String.format("Duration cannot be negative value, get %f", durationMs));
    }

    // If the duration is less than one second, represent in milliseconds.
    if (durationMs < SEC_TO_MS) {
      return String.format("%.2f milliseconds", durationMs);
    }

    // If the duration is less than one minute, represent in seconds.
    if (durationMs < MIN_TO_MS) {
      return String.format("%.2f seconds", durationMs / SEC_TO_MS);
    }

    // If the duration is less than one hour, represent in minutes.
    if (durationMs < HOUR_TO_MS) {
      return String.format("%.2f minutes", durationMs / MIN_TO_MS);
    }

    // If the duration is less than one day, represent in hours.
    if (durationMs < DAY_TO_MS) {
      return String.format("%.2f hours", durationMs / HOUR_TO_MS);
    }

    // Represent in days.
    return String.format("%.2f days", durationMs / DAY_TO_MS);
  }

  /**
   * Get a configuration and throw exception if the configuration was not provided.
   * @param configs the config map.
   * @param configName the config to get.
   * @return the configuration string.
   */
  public static String getRequiredConfig(Map<String, ?> configs, String configName) {
    String value = (String) configs.get(configName);
    if (value == null || value.isEmpty()) {
      throw new ConfigException(String.format("Configuration %s must be provided.", configName));
    }
    return value;
  }

  private static void closeClientWithTimeout(Runnable clientCloseTask, long timeoutMs) {
    Thread t = new Thread(clientCloseTask);
    t.setDaemon(true);
    t.start();
    try {
      t.join(timeoutMs);
    } catch (InterruptedException e) {
      // let it go
    }
    if (t.isAlive()) {
      t.interrupt();
    }
  }

  /**
   * Close the given KafkaZkClient with the default timeout of {@link #KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS}.
   *
   * @param kafkaZkClient KafkaZkClient to be closed
   */
  public static void closeKafkaZkClientWithTimeout(KafkaZkClient kafkaZkClient) {
    closeKafkaZkClientWithTimeout(kafkaZkClient, KAFKA_ZK_CLIENT_CLOSE_TIMEOUT_MS);
  }

  public static void closeKafkaZkClientWithTimeout(KafkaZkClient kafkaZkClient, long timeoutMs) {
    closeClientWithTimeout(kafkaZkClient::close, timeoutMs);
  }

  /**
   * Check if set a contains any element in set b.
   * @param a the first set.
   * @param b the second set.
   * @return true if a contains at least one of the element in b. false otherwise;
   */
  public static boolean containsAny(Set<Integer> a, Set<Integer> b) {
    return b.stream().mapToInt(i -> i).anyMatch(a::contains);
  }

  /**
   * Check if the ClusterAndGeneration needs to be refreshed to retrieve the requested substates.
   *
   * @param substates Substates for which the need for refreshing the ClusterAndGeneration will be evaluated.
   * @return True if substates contain {@link CruiseControlState.SubState#ANALYZER} or
   * {@link CruiseControlState.SubState#MONITOR}, false otherwise.
   */
  public static boolean shouldRefreshClusterAndGeneration(Set<CruiseControlState.SubState> substates) {
    return substates.stream()
        .anyMatch(substate -> substate == CruiseControlState.SubState.ANALYZER
            || substate == CruiseControlState.SubState.MONITOR);
  }

  /**
   * Create an instance of KafkaZkClient with security disabled.
   *
   * @param connectString Comma separated host:port pairs, each corresponding to a zk server
   * @param metricGroup Metric group
   * @param metricType Metric type
   * @param zkSecurityEnabled True if zkSecurityEnabled, false otherwise.
   * @return A new instance of KafkaZkClient
   */
  public static KafkaZkClient createKafkaZkClient(String connectString,
                                                  String metricGroup,
                                                  String metricType,
                                                  boolean zkSecurityEnabled) {
    return createKafkaZkClient(connectString, metricGroup, metricType, zkSecurityEnabled, Option.empty());
  }

  public static KafkaZkClient createKafkaZkClient(KafkaCruiseControlConfig config,
                                                  String metricGroup,
                                                  String metricType,
                                                  Option<ZKClientConfig> zkClientConfig) {
    String zkUrl = config.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    boolean zkSecurityEnabled = config.getBoolean(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    return createKafkaZkClient(zkUrl, metricGroup, metricType, zkSecurityEnabled, zkClientConfig);
  }

  public static KafkaZkClient createKafkaZkClient(String connectString,
                                                  String metricGroup,
                                                  String metricType,
                                                  boolean zkSecurityEnabled,
                                                  Option<ZKClientConfig> zkClientConfig) {
    return KafkaZkClient.apply(connectString, zkSecurityEnabled, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT, Integer.MAX_VALUE,
        new SystemTime(), metricGroup, metricType, Option.apply(ZK_CLIENT_NAME), zkClientConfig);
  }

  /**
   * Create an instance of ConfluentAdmin using the given configurations.
   *
   * @param adminClientConfigs Configurations used for the AdminClient.
   * @return A new instance of AdminClient.
   */
  public static ConfluentAdmin createAdmin(Map<String, ?> adminClientConfigs) {
    return ConfluentAdmin.create(filterAdminClientConfigs(adminClientConfigs));
  }

  /**
   * Close the given AdminClient with the default timeout of {@link #ADMIN_CLIENT_CLOSE_TIMEOUT_MS}.
   *
   * @param adminClient AdminClient to be closed
   */
  public static void closeAdminClientWithTimeout(Admin adminClient) {
    closeAdminClientWithTimeout(adminClient, ADMIN_CLIENT_CLOSE_TIMEOUT_MS);
  }

  public static void closeAdminClientWithTimeout(Admin adminClient, long timeoutMs) {
    closeClientWithTimeout(adminClient::close, timeoutMs);
  }

  public static Map<String, Object> filterAdminClientConfigs(Map<String, ?> configs) {
    Map<String, Object> adminClientConfig = new HashMap<>(configs);
    Set<String> validAdminClientProps = AdminClientConfig.configNames();
    adminClientConfig.keySet().retainAll(validAdminClientProps);
    // Internal clients do not need the metric reporter
    adminClientConfig.remove(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG);
    return adminClientConfig;
  }

  public static Map<String, Object> filterConsumerConfigs(Map<String, ?> configs) {
    Map<String, Object> consumerConfig = new HashMap<>(configs);
    Set<String> validConsumerProps = ConsumerConfig.configNames();
    consumerConfig.keySet().retainAll(validConsumerProps);
    consumerConfig.remove(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG);
    return consumerConfig;
  }

  public static Map<String, Object> filterProducerConfigs(Map<String, ?> configs) {
    Map<String, Object> producerConfig = new HashMap<>(configs);
    Set<String> validProducerProps = ProducerConfig.configNames();
    producerConfig.keySet().retainAll(validProducerProps);
    producerConfig.remove(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG);
    return producerConfig;
  }

  /**
   * Check if the partition is currently under replicated.
   * @param cluster The current cluster state.
   * @param tp The topic partition to check.
   * @return True if the partition is currently under replicated.
   */
  public static boolean isPartitionUnderReplicated(Cluster cluster, TopicPartition tp) {
    PartitionInfo partitionInfo = cluster.partition(tp);
    return partitionInfo.inSyncReplicas().length != partitionInfo.replicas().length;
  }

  /**
   * Sanity check whether the given goals exist in the given supported goals.
   * @param goals A list of goals.
   * @param supportedGoals Supported goals.
   */
  public static void sanityCheckNonExistingGoal(List<String> goals, Map<String, Goal> supportedGoals) {
    Set<String> nonExistingGoals = new HashSet<>();
    goals.stream().filter(goalName -> supportedGoals.get(goalName) == null).forEach(nonExistingGoals::add);

    if (!nonExistingGoals.isEmpty()) {
      throw new IllegalArgumentException("Goals " + nonExistingGoals + " are not supported. Supported: " + supportedGoals.keySet());
    }
  }

  /**
   * Sanity check to ensure that the given cluster model contains brokers with offline replicas.
   * @param clusterModel Cluster model for which the existence of an offline replica will be verified.
   */
  public static void sanityCheckOfflineReplicaPresence(ClusterModel clusterModel) {
    if (clusterModel.brokersHavingOfflineReplicasOnBadDisks().isEmpty()) {
      for (Broker deadBroker : clusterModel.deadBrokers()) {
        if (!deadBroker.replicas().isEmpty()) {
          // Has offline replica(s) on a dead broker.
          return;
        }
      }
      throw new IllegalStateException("Cluster has no offline replica on brokers " + clusterModel.brokers() + " to fix.");
    }
    // Has offline replica(s) on a broken disk.
  }

  /**
   * Sanity check to ensure that the given cluster model has no offline replicas on bad disks in Kafka Assigner mode.
   * @param goals Goals to check whether it is Kafka Assigner mode or not.
   * @param clusterModel Cluster model for which the existence of an offline replicas on bad disks will be verified.
   */
  public static void sanityCheckBrokersHavingOfflineReplicasOnBadDisks(List<String> goals, ClusterModel clusterModel) {
    if (isKafkaAssignerMode(goals) && !clusterModel.brokersHavingOfflineReplicasOnBadDisks().isEmpty()) {
      throw new IllegalStateException("Kafka Assigner mode is not supported when there are offline replicas on bad disks."
                                      + " Please run fix_offline_replicas before using Kafka Assigner mode.");
    }
  }

  /**
   * Check whether any of the given goals contain a Kafka Assigner goal.
   *
   * @param goals The goals to check
   * @return True if the given goals contain a Kafka Assigner goal, false otherwise.
   */
  public static boolean isKafkaAssignerMode(Collection<String> goals) {
    return goals.stream().anyMatch(KAFKA_ASSIGNER_GOALS::contains);
  }

  /**
   * Get the balancedness cost of violating goals by their name, where the sum of costs is {@link #MAX_BALANCEDNESS_SCORE}.
   *
   * @param goals The goals to be used for balancing (sorted by priority).
   * @param priorityWeight The impact of having one level higher goal priority on the relative balancedness score.
   * @param strictnessWeight The impact of strictness on the relative balancedness score.
   * @return the balancedness cost of violating goals by their name.
   */
  public static Map<String, Double> balancednessCostByGoal(List<Goal> goals, double priorityWeight, double strictnessWeight) {
    if (goals.isEmpty()) {
      throw new IllegalArgumentException("At least one goal must be provided to get the balancedness cost.");
    } else if (priorityWeight <= 0 || strictnessWeight <= 0) {
      throw new IllegalArgumentException(String.format("Balancedness weights must be positive (priority:%f, strictness:%f).",
                                                       priorityWeight, strictnessWeight));
    }
    Map<String, Double> balancednessCostByGoal = new HashMap<>(goals.size());
    // Step-1: Get weights.
    double weightSum = 0.0;
    double previousGoalPriorityWeight = 1 / priorityWeight;
    for (int i = goals.size() - 1; i >= 0; i--) {
      Goal goal = goals.get(i);
      double currentGoalPriorityWeight = priorityWeight * previousGoalPriorityWeight;
      double cost = currentGoalPriorityWeight * (goal.isHardGoal() ? strictnessWeight : 1);
      weightSum += cost;
      balancednessCostByGoal.put(goal.name(), cost);
      previousGoalPriorityWeight = currentGoalPriorityWeight;
    }

    // Step-2: Set costs.
    for (Map.Entry<String, Double> entry : balancednessCostByGoal.entrySet()) {
      entry.setValue(MAX_BALANCEDNESS_SCORE * entry.getValue() / weightSum);
    }

    return balancednessCostByGoal;
  }
  
  /**
   * Run some boolean operation with exponential backoff until it succeeds or maxTimeout is hit.
   * Success is indicated by a non-exceptional return.
   * @param f Boolean-returning function which is executed until it succeeds or #{@code maxRetries} is hit
   * @param maxRetries Maxmimum number of times this should be executed.
   * @param initialWaitMs How long to sleep at first.
   * @param maxWaitMs Maximum length of a single wait in ms. If 0, no upper bound.
   * @param time time source (useful for test mocks)
   *
   * @throws InterruptedException
   * @throws TimeoutException if the function never succeeded after #{@code maxRetries}
   */
  public static void backoff(Supplier<Boolean> f, int maxRetries, int initialWaitMs, int maxWaitMs, Time time) throws InterruptedException, TimeoutException {
    int waitMsBound = maxWaitMs > 0 ? maxWaitMs : Integer.MAX_VALUE;
    int currentWaitMs = initialWaitMs;
    if (f.get()) {
      return;
    }
    for (int currentRetry = 1; currentRetry < maxRetries; currentRetry++) {
      time.sleep(currentWaitMs);
      currentWaitMs = Math.min(2 * currentWaitMs, waitMsBound);
      if (f.get()) {
        return;
      }
    }
    // Made it here, no luck.
    throw new TimeoutException(String.format("Exceeded max retry count (%d)", maxRetries));
  }
}
