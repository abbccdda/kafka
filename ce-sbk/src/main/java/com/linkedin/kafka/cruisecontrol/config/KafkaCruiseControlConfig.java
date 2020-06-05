/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.config;

import com.linkedin.cruisecontrol.common.CruiseControlConfigurable;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.IntraBrokerDiskUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderBytesInDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.LeaderReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkInboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.NetworkOutboundUsageDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.TopicReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import com.linkedin.kafka.cruisecontrol.executor.ExecutorNoopNotifier;
import com.linkedin.kafka.cruisecontrol.executor.strategy.BaseReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PostponeUrpReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeLargeReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.executor.strategy.PrioritizeSmallReplicaMovementStrategy;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.DefaultMetricSamplerPartitionAssignor;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import io.confluent.cruisecontrol.analyzer.goals.ReplicaPlacementGoal;
import io.confluent.cruisecontrol.analyzer.goals.SequentialReplicaMovementGoal;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.ConfluentConfigs;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;

/**
 * The configuration class of Kafka Cruise Control.
 */
public class KafkaCruiseControlConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  // Monitor configs
  /**
   * <code>bootstrap.servers</code>
   */
  public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

  /**
   * <code>metadata.max.age.ms</code>
   */
  public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
  private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;
  public static final Integer DEFAULT_METADATA_MAX_AGE_MS = 180 * 1000;

  /**
   * <code>client.id</code>
   */
  public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

  /**
   * <code>send.buffer.bytes</code>
   */
  public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

  /**
   * <code>receive.buffer.bytes</code>
   */
  public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

  /**
   * <code>connections.max.idle.ms</code>
   */
  public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

  /**
   * <code>reconnect.backoff.ms</code>
   */
  public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

  /**
   * <code>request.timeout.ms</code>
   */
  public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
  private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;


  /**
   * <code>default.api.timeout.ms</code>
   */
  public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG;
  private static final String DEFAULT_API_TIMEOUT_MS_DOC = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC;
  public static final int DEFAULT_API_TIMEOUT_MS_DEFAULT = 60_000; // 1 minute

  /**
   * <code>partition.metrics.windows.ms</code>
   */
  public static final String PARTITION_METRICS_WINDOW_MS_CONFIG = "partition.metrics.window.ms";
  private static final String PARTITION_METRICS_WINDOW_MS_DOC = "The size of the window in milliseconds to aggregate "
      + "the Kafka partition metrics.";
  public static final Long DEFAULT_PARTITION_METRICS_MS = 300L * 1000; // 5 min

  /**
   * <code>num.partition.metrics.windows</code>
   */
  public static final String NUM_PARTITION_METRICS_WINDOWS_CONFIG = "num.partition.metrics.windows";
  private static final String NUM_PARTITION_METRICS_WINDOWS_DOC = "The total number of windows to keep for partition "
      + "metric samples";
  public static final Integer DEFAULT_NUM_PARTITION_METRICS_WINDOWS = 6;

  /**
   * <code>min.samples.per.partition.metrics.window</code>
   */
  public static final String MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG = "min.samples.per.partition.metrics.window";
  private static final String MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_DOC = "The minimum number of "
      + "PartitionMetricSamples needed to make a partition metrics window valid without extrapolation.";
  public static final Integer DEFAULT_MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW = 1;

  /**
   * <code>max.allowed.extrapolations.per.partition</code>
   */
  public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_CONFIG = "max.allowed.extrapolations.per.partition";
  private static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_DOC = "The maximum allowed number of extrapolations "
      + "for each partition. A partition will be considered as invalid if the total number extrapolations in all the "
      + "windows goes above this number.";

  /**
   * <code>partition.metric.sample.aggregator.completeness.cache.size</code>
   */
  public static final String PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG =
      "partition.metric.sample.aggregator.completeness.cache.size";
  private static final String PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC = "The metric sample "
      + "aggregator caches the completeness metadata for fast query. The completeness describes the confidence "
      + "level of the data in the metric sample aggregator. It is primarily measured by the validity of the metrics"
      + "samples in different windows. This configuration configures The number of completeness cache slots to "
      + "maintain.";

  /**
   * <code>broker.metrics.window.ms</code>
   */
  public static final String BROKER_METRICS_WINDOW_MS_CONFIG = "broker.metrics.window.ms";
  private static final String BROKER_METRICS_WINDOW_MS_DOC = "The size of the window in milliseconds to aggregate the"
      + " Kafka broker metrics.";
  public static final Long DEFAULT_BROKER_METRICS_WINDOW_MS = 300L * 1000;

  /**
   * <code>num.broker.metrics.windows</code>
   */
  public static final String NUM_BROKER_METRICS_WINDOWS_CONFIG = "num.broker.metrics.windows";
  private static final String NUM_BROKER_METRICS_WINDOWS_DOC = "The total number of windows to keep for broker metric"
      + " samples";
  public static final Integer DEFAULT_NUM_BROKER_METRICS_WINDOWS = 20;

  /**
   * <code>min.samples.per.broker.metrics.window</code>
   */
  public static final String MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG = "min.samples.per.broker.metrics.window";
  private static final String MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_DOC = "The minimum number of BrokerMetricSamples "
      + "needed to make a broker metrics window valid without extrapolation.";
  public static final Integer DEFAULT_MIN_SAMPLES_PER_BROKER_METRICS_WINDOW = 1;

  /**
   * <code>max.allowed.extrapolations.per.broker</code>
   */
  public static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_CONFIG = "max.allowed.extrapolations.per.broker";
  private static final String MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_DOC = "The maximum allowed number of extrapolations "
      + "for each broker. A broker will be considered as invalid if the total number extrapolations in all the windows"
      + " goes above this number.";

  /**
   * <code>broker.metric.sample.aggregator.completeness.cache.size</code>
   */
  public static final String BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG =
      "broker.metric.sample.aggregator.completeness.cache.size";
  private static final String BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC = "The metric sample "
      + "aggregator caches the completeness metadata for fast query. The completeness describes the confidence "
      + "level of the data in the metric sample aggregator. It is primarily measured by the validity of the metrics"
      + "samples in different windows. This configuration configures The number of completeness cache slots to "
      + "maintain.";

  /**
   * @deprecated (i.e. cannot be configured to a value other than 1).
   * <code>num.metric.fetchers</code>
   */
  @Deprecated
  public static final String NUM_METRIC_FETCHERS_CONFIG = "num.metric.fetchers";
  private static final String NUM_METRIC_FETCHERS_DOC = "The number of metric fetchers to fetch from the Kafka cluster.";

  /**
   * <code>num.cached.recent.anomaly.states</code>
   */
  public static final String NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG = "num.cached.recent.anomaly.states";
  public static final String NUM_CACHED_RECENT_ANOMALY_STATES_DOC = "The number of recent anomaly states cached for "
      + "different anomaly types presented via the anomaly substate response of the state endpoint.";

  /**
   * <code>metric.sampler.class</code>
   */
  public static final String METRIC_SAMPLER_CLASS_CONFIG = "metric.sampler.class";
  private static final String METRIC_SAMPLER_CLASS_DOC = "The class name of the metric sampler";
  private static final String METRIC_SAMPLER_CLASS_DEFAULT = "io.confluent.cruisecontrol.metricsreporter.ConfluentMetricsReporterSampler";

  /**
   * <code>metric.sampler.partition.assignor.class</code>
   */
  public static final String METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_CONFIG = "metric.sampler.partition.assignor.class";
  private static final String METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_DOC = "The class used to assign the partitions to " +
      "the metric samplers.";

  /**
   * <code>metric.sampling.interval.ms</code>
   */
  public static final String METRIC_SAMPLING_INTERVAL_MS_CONFIG = "metric.sampling.interval.ms";
  private static final String METRIC_SAMPLING_INTERVAL_MS_DOC = "The interval of metric sampling.";
  public static final Long DEFAULT_METRIC_SAMPLING_INTERVAL_MS = 180L * 1000;

  /**
   * <code>broker.capacity.config.resolver.class</code>
   */
  public static final String BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG = "broker.capacity.config.resolver.class";
  private static final String BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_DOC = "The broker capacity configuration resolver "
      + "class name. The broker capacity configuration resolver is responsible for getting the broker capacity. The "
      + "default implementation is a file based solution.";

  /**
   * <code>min.valid.partition.ratio</code>
   */
  public static final String MIN_VALID_PARTITION_RATIO_CONFIG = "min.valid.partition.ratio";
  private static final String MIN_VALID_PARTITION_RATIO_DOC = "The minimum percentage of the total partitions " +
      "required to be monitored in order to generate a valid load model. Because the topic and partitions in a " +
      "Kafka cluster are dynamically changing. The load monitor will exclude some of the topics that does not have " +
      "sufficient metric samples. This configuration defines the minimum required percentage of the partitions that " +
      "must be included in the load model.";
  public static final Double DEFAULT_MIN_VALID_PARTITION_RATIO = 0.95;

  /**
   * <code>leader.network.inbound.weight.for.cpu.util</code>
   */
  public static final String LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "leader.network.inbound.weight.for.cpu.util";
  private static final String LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the " +
      "following model to derive replica level CPU utilization: " +
      "REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + c * FOLLOWER_BYTES_IN_RATE." +
      "This configuration will be used as the weight for LEADER_BYTES_IN_RATE.";

  /**
   * <code>leader.network.outbound.weight.for.cpu.util</code>
   */
  public static final String LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "leader.network.outbound.weight.for.cpu.util";
  private static final String LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the " +
      "following model to derive replica level CPU utilization: " +
      "REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + c * FOLLOWER_BYTES_IN_RATE." +
      "This configuration will be used as the weight for LEADER_BYTES_OUT_RATE.";

  /**
   * <code>follower.network.inbound.weight.for.cpu.util</code>
   */
  public static final String FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG = "follower.network.inbound.weight.for.cpu.util";
  private static final String FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC = "Kafka Cruise Control uses the " +
      "following model to derive replica level CPU utilization: " +
      "REPLICA_CPU_UTIL = a * LEADER_BYTES_IN_RATE + b * LEADER_BYTES_OUT_RATE + c * FOLLOWER_BYTES_IN_RATE." +
      "This configuration will be used as the weight for FOLLOWER_BYTES_IN_RATE.";

  /**
   * <code>linear.regression.model.cpu.util.bucket.size</code>
   */
  public static final String LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_CONFIG = "linear.regression.model.cpu.util.bucket.size";
  private static final String LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_DOC = "The CPU utilization bucket size for linear"
      + " regression model training data. The unit is percents.";

  /**
   * <code>linear.regression.model.required.samples.per.bucket</code>
   */
  public static final String LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_CONFIG =
      "linear.regression.model.required.samples.per.bucket";
  private static final String LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_DOC = "The number of training samples"
      + " required in each CPU utilization bucket specified by linear.regression.model.cpu.util.bucket";

  /**
   * <code>linear.regression.model.min.num.cpu.util.buckets</code>
   */
  public static final String LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_CONFIG =
      "linear.regression.model.min.num.cpu.util.buckets";
  private static final String LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_DOC = "The minimum number of full CPU"
      + " utilization buckets required to generate a linear regression model.";

  // Analyzer configs
  /**
   * <code>cpu.balance.threshold</code>
   */
  public static final String CPU_BALANCE_THRESHOLD_CONFIG = "cpu.balance.threshold";
  private static final String CPU_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for CPU utilization. " +
      "For example, 1.10 means the highest CPU usage of a broker should not be above 1.10x of average " +
      "CPU utilization of all the brokers.";

  /**
   * <code>disk.balance.threshold</code>
   */
  public static final String DISK_BALANCE_THRESHOLD_CONFIG = "disk.balance.threshold";
  private static final String DISK_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for disk utilization. " +
      "For example, 1.10 means the highest disk usage of a broker should not be above 1.10x of average " +
      "disk utilization of all the brokers.";

  /**
   * <code>network.inbound.balance.threshold</code>
   */
  public static final String NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG = "network.inbound.balance.threshold";
  private static final String NETWORK_INBOUND_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for " +
      "network inbound usage. For example, 1.10 means the highest network inbound usage of a broker should not " +
      "be above 1.10x of average network inbound usage of all the brokers.";

  /**
   * <code>network.outbound.balance.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG = "network.outbound.balance.threshold";
  private static final String NETWORK_OUTBOUND_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for " +
      "network outbound usage. For example, 1.10 means the highest network outbound usage of a broker should not " +
      "be above 1.10x of average network outbound usage of all the brokers.";

  /**
   * <code>replica.count.balance.threshold</code>
   */
  public static final String REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "replica.count.balance.threshold";
  private static final String REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for replica "
      + "distribution. For example, 1.10 means the highest replica count of a broker should not be above 1.10x of "
      + "average replica count of all brokers.";

  /**
   * <code>leader.replica.count.balance.threshold</code>
   */
  public static final String LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "leader.replica.count.balance.threshold";
  private static final String LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for "
      + "leader replica distribution. For example, 1.10 means the highest leader replica count of a broker should not be "
      + "above 1.10x of average leader replica count of all alive brokers.";

  /**
   * <code>topic.replica.count.balance.threshold</code>
   */
  public static final String TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG = "topic.replica.count.balance.threshold";
  private static final String TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_DOC = "The maximum allowed extent of unbalance for "
      + "replica distribution from each topic. For example, 1.80 means the highest topic replica count of a broker "
      + "should not be above 1.80x of average replica count of all brokers for the same topic.";

  /**
   * <code>goal.violation.distribution.threshold.multiplier</code>
   */
  public static final String GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG = "goal.violation.distribution.threshold.multiplier";
  private static final String GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_DOC = "The multiplier applied to the threshold"
      + " of distribution goals used for detecting and fixing anomalies. For example, 2.50 means the threshold for each "
      + "distribution goal (i.e. Replica Distribution, Leader Replica Distribution, Resource Distribution, and Topic Replica "
      + "Distribution Goals) will be 2.50x of the value used in manual goal optimization requests (e.g. rebalance).";
  public static final Double DEFAULT_GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER = 1.1;

  /**
   * <code>cpu.capacity.threshold</code>
   */
  public static final String CPU_CAPACITY_THRESHOLD_CONFIG = "cpu.capacity.threshold";
  private static final String CPU_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total broker.cpu.capacity " +
      "that is allowed to be used on a broker. The analyzer will enforce a hard goal that the cpu utilization " +
      "of a broker cannot be higher than (broker.cpu.capacity * cpu.capacity.threshold).";
  public static final Double DEFAULT_CPU_CAPACITY_THRESHOLD = 1.0;

  /**
   * Disk capacity: <code>disk.max.load</code>
   */
  public static final String DISK_CAPACITY_THRESHOLD_CONFIG = ConfluentConfigs.BALANCER_DISK_CAPACITY_THRESHOLD_BASE_CONFIG;
  private static final String DISK_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total broker.disk.capacity " +
      "that is allowed to be used on a broker. The analyzer will enforce a hard goal that the disk usage " +
      "of a broker cannot be higher than (broker.disk.capacity * disk.capacity.threshold).";
  public static final Double DEFAULT_DISK_CAPACITY_THRESHOLD = ConfluentConfigs.BALANCER_DISK_CAPACITY_THRESHOLD_DEFAULT;

  /**
   * <code>network.inbound.capacity.threshold</code>
   */
  public static final String NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG = "network.inbound.capacity.threshold";
  private static final String NETWORK_INBOUND_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total " +
      "broker.network.inbound.capacity that is allowed to be used on a broker. The analyzer will enforce a hard goal " +
      "that the disk usage of a broker cannot be higher than " +
      "(broker.network.inbound.capacity * network.inbound.capacity.threshold).";
  public static final Double DEFAULT_NETWORK_INBOUND_CAPACITY_THRESHOLD = 0.8;

  /**
   * <code>network.outbound.capacity.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG = "network.outbound.capacity.threshold";
  private static final String NETWORK_OUTBOUND_CAPACITY_THRESHOLD_DOC = "The maximum percentage of the total " +
      "broker.network.outbound.capacity that is allowed to be used on a broker. The analyzer will enforce a hard goal " +
      "that the disk usage of a broker cannot be higher than " +
      "(broker.network.outbound.capacity * network.outbound.capacity.threshold).";
  public static final Double DEFAULT_NETWORK_OUTBOUND_CAPACITY_THRESHOLD = 0.8;

  /**
   * <code>cpu.low.utilization.threshold</code>
   */
  public static final String CPU_LOW_UTILIZATION_THRESHOLD_CONFIG = "cpu.low.utilization.threshold";
  private static final String CPU_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define " +
      "the utilization of CPU is low enough that rebalance is not worthwhile. The cluster will only be in a low " +
      "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";
  public static final Double DEFAULT_CPU_LOW_UTILIZATION_THRESHOLD = 0.2;

  /**
   * <code>disk.low.utilization.threshold</code>
   */
  public static final String DISK_LOW_UTILIZATION_THRESHOLD_CONFIG = "disk.low.utilization.threshold";
  private static final String DISK_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define " +
      "the utilization of DISK is low enough that rebalance is not worthwhile. The cluster will only be in a low " +
      "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";
  public static final Double DEFAULT_DISK_LOW_UTILIZATION_THRESHOLD = 0.2;

  /**
   * <code>network.inbound.low.utilization.threshold</code>
   */
  public static final String NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG = "network.inbound.low.utilization.threshold";
  private static final String NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define " +
      "the utilization of network inbound rate is low enough that rebalance is not worthwhile. The cluster will only be in a low " +
      "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";
  public static final Double DEFAULT_NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD = 0.2;

  /**
   * <code>network.outbound.low.utilization.threshold</code>
   */
  public static final String NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG = "network.outbound.low.utilization.threshold";
  private static final String NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_DOC = "The threshold for Kafka Cruise Control to define " +
      "the utilization of network outbound rate is low enough that rebalance is not worthwhile. The cluster will only be in a low " +
      "utilization state when all the brokers are below the low utilization threshold. The threshold is in percentage.";
  public static final Double DEFAULT_NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD = 0.2;

  /**
   * <code>metric.anomaly.finder.class</code>
   */
  public static final String METRIC_ANOMALY_FINDER_CLASSES_CONFIG = "metric.anomaly.finder.class";
  private static final String METRIC_ANOMALY_FINDER_CLASSES_DOC = "A list of metric anomaly finder classes to find "
                                                                    + "the current state to identify metric anomalies.";
  private static final String DEFAULT_METRIC_ANOMALY_FINDER_CLASS = KafkaMetricAnomalyFinder.class.getName();

  /**
   * <code>proposal.expiration.ms</code>
   */
  public static final String PROPOSAL_EXPIRATION_MS_CONFIG = "proposal.expiration.ms";
  private static final String PROPOSAL_EXPIRATION_MS_DOC = "Kafka Cruise Control will cache one of the best proposal "
      + "among all the optimization proposal candidates it recently computed. This configuration defines when will the"
      + "cached proposal be invalidated and needs a recomputation. If proposal.expiration.ms is set to 0, Cruise Control"
      + "will continuously compute the proposal candidates.";
  public static final Integer DEFAULT_PROPOSAL_EXPIRATION_MS = 60 * 1000;

  /**
   * Broker replica capacity: <code>max.replicas</code>
   */
  public static final String MAX_REPLICAS_PER_BROKER_CONFIG = ConfluentConfigs.BALANCER_REPLICA_CAPACITY_BASE_CONFIG;
  private static final String MAX_REPLICAS_PER_BROKER_DOC = "The maximum number of replicas allowed to reside on a "
      + "broker. The analyzer will enforce a hard goal that the number of replica on a broker cannot be higher than "
      + "this config.";

  /**
   * <code>num.proposal.precompute.threads</code>
   */
  public static final String NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG = "num.proposal.precompute.threads";
  private static final String NUM_PROPOSAL_PRECOMPUTE_THREADS_DOC = "The number of thread used to precompute the "
      + "optimization proposal candidates. The more threads are used, the more memory and CPU resource will be used.";

  // Executor configs
  /**
   * <code>zookeeper.connect</code>
   */
  public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
  private static final String ZOOKEEPER_CONNECT_DOC = "The zookeeper path used by the Kafka cluster.";

  /**
   * <code>zookeeper.security.enabled</code>
   */
  public static final String ZOOKEEPER_SECURITY_ENABLED_CONFIG = "zookeeper.security.enabled";
  private static final String ZOOKEEPER_SECURITY_ENABLED_DOC = "Specify if zookeeper is secured, true or false";

  /**
   * <code>num.concurrent.partition.movements.per.broker</code>
   */
  public static final String NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG = "num.concurrent.partition.movements.per.broker";
  private static final String NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_DOC = "The maximum number of partitions " +
      "the executor will move to or out of a broker at the same time. e.g. setting the value to 10 means that the " +
      "executor will at most allow 10 partitions move out of a broker and 10 partitions move into a broker at any " +
      "given point. This is to avoid overwhelming the cluster by inter-broker partition movements.";

  /**
   * <code>num.concurrent.intra.broker.partition.movements</code>
   */
  public static final String NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG = "num.concurrent.intra.broker.partition.movements";
  private static final String NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_DOC = "The maximum number of partitions " +
      "the executor will move across disks within a broker at the same time. e.g. setting the value to 10 means that the " +
      "executor will at most allow 10 partitions to move across disks within a broker at any given point. This is to avoid " +
      "overwhelming the cluster by intra-broker partition movements.";

  /**
   * <code>num.concurrent.leader.movements</code>
   */
  public static final String NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG =
      "num.concurrent.leader.movements";
  private static final String NUM_CONCURRENT_LEADER_MOVEMENTS_DOC = "The maximum number of leader " +
      "movements the executor will take as one batch. This is mainly because the ZNode has a 1 MB size upper limit. And it " +
      "will also reduce the controller burden.";

  /**
   * Replication throttle: <code>throttle.bytes.per.second</code>
   */
  public static final String DEFAULT_REPLICATION_THROTTLE_CONFIG = ConfluentConfigs.BALANCER_THROTTLE_BASE_CONFIG;
  public static final Long NO_THROTTLE = null;
  public static final long AUTO_THROTTLE = ConfluentConfigs.BALANCER_THROTTLE_AUTO_THROTTLE;
  private static final String DEFAULT_REPLICATION_THROTTLE_DOC = "The replication throttle applied to " +
          "replicas being moved, in bytes per second. A value of null means no throttle is" +
          "applied. A value of -2 means the throttle is automatically calculated based on the cluster metrics.";


  /**
   * <code>replica.movement.strategies</code>
   */
  public static final String REPLICA_MOVEMENT_STRATEGIES_CONFIG = "replica.movement.strategies";
  private static final String REPLICA_MOVEMENT_STRATEGIES_DOC = "A list of supported strategies used to determine execution order "
      + "for generated partition movement tasks.";

  /**
   * <code>default.replica.movement.strategies</code>
   */
  public static final String DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG = "default.replica.movement.strategies";
  private static final String DEFAULT_REPLICA_MOVEMENT_STRATEGIES_DOC = "The list of replica movement strategies that will be used "
      + "by default if no replica movement strategy list is provided.";

  /**
   * <code>execution.progress.check.interval.ms</code>
   */
  public static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG = "execution.progress.check.interval.ms";
  private static final String EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC = "The interval in milliseconds that the " +
      "executor will check on the execution progress.";
  private static final long DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS = 7000L;

  /**
   * <code>goals</code>
   */
  public static final String GOALS_CONFIG = "goals";
  private static final String GOALS_DOC = "A list of case insensitive goals in the order of priority. The high "
      + "priority goals will be executed first.";
  public static final List<String> DEFAULT_GOALS_LIST = Arrays.asList(
          ReplicaPlacementGoal.class.getName(),
          SequentialReplicaMovementGoal.class.getName(),
          ReplicaCapacityGoal.class.getName(),
          DiskCapacityGoal.class.getName(),
          NetworkInboundCapacityGoal.class.getName(),
          NetworkOutboundCapacityGoal.class.getName(),
          ReplicaDistributionGoal.class.getName(),
          DiskUsageDistributionGoal.class.getName(),
          NetworkInboundUsageDistributionGoal.class.getName(),
          NetworkOutboundUsageDistributionGoal.class.getName(),
          CpuUsageDistributionGoal.class.getName(),
          TopicReplicaDistributionGoal.class.getName(),
          LeaderReplicaDistributionGoal.class.getName(),
          LeaderBytesInDistributionGoal.class.getName());

  /**
   * <code>intra.broker.goals</code>
   */
  public static final String INTRA_BROKER_GOALS_CONFIG = "intra.broker.goals";
  private static final String INTRA_BROKER_GOALS_DOC = "A list of case insensitive intra-broker goals in the order of priority. "
      + "The high priority goals will be executed first. The intra-broker goals are only relevant if intra-broker operation is "
      + "supported(i.e. in  Cruise Control versions above 2.*), otherwise this list should be empty.";

  /**
   * <code>hard.goals</code>
   */
  public static final String HARD_GOALS_CONFIG = "hard.goals";
  private static final String HARD_GOALS_DOC = "A list of case insensitive hard goals. Hard goals will be enforced to execute "
      + "if Cruise Control runs in non-kafka-assigner mode and skip_hard_goal_check parameter is not set in request.";
  public static final List<String> DEFAULT_HARD_GOALS_LIST = Arrays.asList(
          ReplicaPlacementGoal.class.getName(),
          SequentialReplicaMovementGoal.class.getName(),
          ReplicaCapacityGoal.class.getName(),
          DiskCapacityGoal.class.getName(),
          NetworkInboundCapacityGoal.class.getName(),
          NetworkOutboundCapacityGoal.class.getName());


  /**
   * <code>default.goals</code>
   */
  public static final String DEFAULT_GOALS_CONFIG = "default.goals";
  private static final String DEFAULT_GOALS_DOC = "The list of goals that will be used by default if no goal list "
      + "is provided. This list of goal will also be used for proposal pre-computation. If default.goals is not "
      + "specified, it will be default to goals config.";

  /**
   * <code>self.healing.goals</code>
   */
  public static final String SELF_HEALING_GOALS_CONFIG = "self.healing.goals";
  private static final String SELF_HEALING_GOALS_DOC = "The list of goals to be used for self-healing relevant anomalies."
      + " If empty, uses the default.goals for self healing.";

  /**
   * <code>anomaly.notifier.class</code>
   */
  public static final String ANOMALY_NOTIFIER_CLASS_CONFIG = "anomaly.notifier.class";
  private static final String ANOMALY_NOTIFIER_CLASS_DOC = "The notifier class to trigger an alert when an "
      + "anomaly is violated. The anomaly could be either a goal violation, broker failure, or metric anomaly.";
  // We have to define this so we don't need to move every package to scala src folder.
  private static final String DEFAULT_ANOMALY_NOTIFIER_CLASS = SelfHealingNotifier.class.getName();

  /*
   * <code>executor.notifier.class</code>
   */
  public static final String EXECUTOR_NOTIFIER_CLASS_CONFIG = "executor.notifier.class";
  private static final String EXECUTOR_NOTIFIER_CLASS_DOC = "The executor notifier class to trigger an alert when an "
                                                            + "execution finishes or is stopped (by a user or "
                                                            + "by Cruise Control).";
  // We have to define this to support the use of network clients with different Kafka client versions.
  private static final String DEFAULT_EXECUTOR_NOTIFIER_CLASS = ExecutorNoopNotifier.class.getName();

  /**
   * <code>anomaly.detection.interval.ms</code>
   */
  public static final String ANOMALY_DETECTION_INTERVAL_MS_CONFIG = "anomaly.detection.interval.ms";
  private static final String ANOMALY_DETECTION_INTERVAL_MS_DOC = "The interval in millisecond that the detectors will "
      + "run to detect the anomalies.";
  public static final Integer DEFAULT_ANOMALY_DETECTION_INTERVAL_MS = 10 * 1000;

  /**
   * <code>anomaly.detection.allow.capacity.estimation</code>
   */
  public static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG = "anomaly.detection.allow.capacity.estimation";
  private static final String ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_DOC = "The flag to indicate whether anomaly "
      + "detection threads allow capacity estimation in the generated cluster model they use.";

  /**
   * <code>sampling.allow.cpu.capacity.estimation</code>
   */
  public static final String SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG = "sampling.allow.cpu.capacity.estimation";
  private static final String SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_DOC = "The flag to indicate whether sampling "
      + "process allows CPU capacity estimation of brokers used for CPU utilization estimation.";

  /**
   * <code>anomaly.detection.goals</code>
   */
  public static final String ANOMALY_DETECTION_GOALS_CONFIG = "anomaly.detection.goals";
  private static final String ANOMALY_DETECTION_GOALS_DOC = "The goals that anomaly detector should detect if they are"
      + "violated.";
  public static final List<String> DEFAULT_ANOMALY_DETECTION_GOALS_LIST = Arrays.asList(
          ReplicaPlacementGoal.class.getName(),
          SequentialReplicaMovementGoal.class.getName(),
          ReplicaCapacityGoal.class.getName(),
          DiskCapacityGoal.class.getName(),
          NetworkInboundCapacityGoal.class.getName(),
          NetworkOutboundCapacityGoal.class.getName(),
          ReplicaDistributionGoal.class.getName(),
          DiskUsageDistributionGoal.class.getName());


  /**
   * <code>broker.failure.exclude.recently.demoted.brokers</code>
   */
  public static final String BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG = "broker.failure.exclude.recently.demoted.brokers";
  private static final String BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC = "True if recently demoted brokers "
      + "are excluded from optimizations during broker failure self healing, false otherwise.";

  /**
   * <code>broker.failure.exclude.recently.removed.brokers</code>
   */
  public static final String BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG = "broker.failure.exclude.recently.removed.brokers";
  private static final String BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC = "True if recently removed brokers "
      + "are excluded from optimizations during broker failure self healing, false otherwise.";

  /**
   * <code>goal.violation.exclude.recently.demoted.brokers</code>
   */
  public static final String GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG = "goal.violation.exclude.recently.demoted.brokers";
  private static final String GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC = "True if recently demoted brokers "
      + "are excluded from optimizations during goal violation self healing, false otherwise.";

  /**
   * <code>goal.violation.exclude.recently.removed.brokers</code>
   */
  public static final String GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG = "goal.violation.exclude.recently.removed.brokers";
  private static final String GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC = "True if recently removed brokers "
      + "are excluded from optimizations during goal violation self healing, false otherwise.";

  /**
   * <code>failed.brokers.zk.path</code>
   */
  public static final String FAILED_BROKERS_ZK_PATH_CONFIG = "failed.brokers.zk.path";
  private static final String FAILED_BROKERS_ZK_PATH_DOC = "The zk path to store the failed broker list. This is to "
      + "persist the broker failure time in case Cruise Control failed and restarted when some brokers are down.";
  public static final String DEFAULT_FAILED_BROKERS_ZK_PATH = "/DataBalancerBrokerList";

  /**
   * <code>use.linear.regression.model</code>
   */
  public static final String USE_LINEAR_REGRESSION_MODEL_CONFIG = "use.linear.regression.model";
  private static final String USE_LINEAR_REGRESSION_MODEL_DOC = "Use the linear regression model to estimate the "
      + "cpu utilization.";

  /**
   * <code>topics.excluded.from.partition.movement</code>
   */
  public static final String TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG = "topics.excluded.from.partition.movement";
  private static final String TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_DOC = "The topics that should be excluded from the "
      + "partition movement. It is a regex. Notice that this regex will be ignored when decommission a broker is invoked.";

  /**
   * <code>sample.store.class</code>
   */
  public static final String SAMPLE_STORE_CLASS_CONFIG = "sample.store.class";
  private static final String SAMPLE_STORE_CLASS_DOC = "The sample store class name. User may configure a sample store "
      + "that persist the metric samples that have already been aggregated into Kafka Cruise Control. Later on the "
      + "persisted samples can be reloaded from the sample store to Kafka Cruise Control.";

  /**
   * <code>topic.config.provider.class</code>
   */
  public static final String TOPIC_CONFIG_PROVIDER_CLASS_CONFIG = "topic.config.provider.class";
  private static final String TOPIC_CONFIG_PROVIDER_CLASS_DOC = "The provider class that reports the active configuration of topics.";

  /**
   * <code>demotion.history.retention.time.ms</code>
   */
  public static final String DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG = "demotion.history.retention.time.ms";
  private static final String DEMOTION_HISTORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to retain the"
      + " demotion history of brokers.";

  /**
   * <code>removal.history.retention.time.ms</code>
   */
  public static final String REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG = "removal.history.retention.time.ms";
  private static final String REMOVAL_HISTORY_RETENTION_TIME_MS_DOC = "The maximum time in milliseconds to retain the"
      + " removal history of brokers.";

  /**
   * <code>goal.balancedness.priority.weight</code>
   */
  public static final String GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG = "goal.balancedness.priority.weight";
  private static final String GOAL_BALANCEDNESS_PRIORITY_WEIGHT_DOC = "The impact of having one level higher goal priority"
       + " on the relative balancedness score. For example, 1.1 means that a goal with higher priority will have the 1.1x"
       + " balancedness weight of the lower priority goal (assuming the same goal.balancedness.strictness.weight values for"
       + " both goals).";

  /**
   * <code>goal.balancedness.strictness.weight</code>
   */
  public static final String GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG = "goal.balancedness.strictness.weight";
  private static final String GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_DOC = "The impact of strictness (i.e. hard or soft goal)"
       + " on the relative balancedness score. For example, 1.5 means that a hard goal will have the 1.5x balancedness "
       + "weight of a soft goal (assuming goal.balancedness.priority.weight is 1).";

  // Self Healing Notifier configurations
  /**
   * <code>self.healing.broker.failure.enabled</code>
   */
  public static final String SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG = "self.healing.broker.failure.enabled";
  public static final String SELF_HEALING_BROKER_FAILURE_ENABLED_DOC = "Enable self healing for broker failure detector.";

  /**
   * <code>self.healing.goal.violation.enabled</code>
   */
  public static final String SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG = "self.healing.goal.violation.enabled";
  public static final String SELF_HEALING_GOAL_VIOLATION_ENABLED_DOC = "Enable self healing for goal violation detector.";

  /**
   * <code>self.healing.metric.anomaly.enabled</code>
   */
  public static final String SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG = "self.healing.metric.anomaly.enabled";
  public static final String SELF_HEALING_METRIC_ANOMALY_ENABLED_DOC = "Enable self healing for metric anomaly detector.";

  /**
   * <code>self.healing.disk.failure.enabled</code>
   */
  public static final String SELF_HEALING_DISK_FAILURE_ENABLED_CONFIG = "self.healing.disk.failure.enabled";
  public static final String SELF_HEALING_DISK_FAILURE_ENABLED_DOC = "Enable self healing for disk failure detector.";

  /**
   * <code>broker.failure.alert.threshold.ms</code>
   */
  public static final String BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG = "broker.failure.alert.threshold.ms";
  public static final String BROKER_FAILURE_ALERT_THRESHOLD_MS_DOC = "Grace period in milliseconds for a failed broker to "
          + "rejoin the cluster before firing an alert";
  public static final Long DEFAULT_BROKER_FAILURE_ALERT_THRESHOLD_MS = 0L;

  /**
   * <code>broker.failure.self.healing.threshold.ms</code>
   */
  public static final String BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG = "broker.failure.self.healing.threshold.ms";
  public static final String BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_DOC = "Grace period in milliseconds for a failed broker to "
          + "rejoin the cluster before triggering a self-healing action. Must not be less than 'broker.failure.alert.threshold.ms'";
  public static final Long DEFAULT_BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS = 1800000L;

  /**
   * <code>logdir.response.timeout.ms</code>
   */
  public static final String LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG = "logdir.response.timeout.ms";
  private static final String LOGDIR_RESPONSE_TIMEOUT_MS_DOC = "Timeout in ms for broker logdir to respond";

  /**
   * <code>describe.topics.response.timeout.ms</code>
   */
  public static final String DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS_CONFIG = "describe.topics.response.timeout.ms";
  private static final String DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS_DOC = "Timeout in ms for the broker to respond to a describe topics request";
  public static final Long DEFAULT_DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS = 10000L;

  /**
   * <code>describe.cluster.response.timeout.ms</code>
   */
  public static final String DESCRIBE_CLUSTER_RESPONSE_TIMEOUT_MS_CONFIG = "describe.cluster.response.timeout.ms";
  private static final String DESCRIBE_CLUSTER_RESPONSE_TIMEOUT_MS_DOC = "Timeout in ms for the broker to respond to a describe cluster request";
  public static final Long DEFAULT_DESCRIBE_CLUSTER_RESPONSE_TIMEOUT_MS = 10_000L;

  /**
   * The amount of time to block for when waiting for the broker to be shut down.
   * <code>broker.removal.shutdown.timeout.ms</code>
   */
  public static final String BROKER_REMOVAL_SHUTDOWN_MS_CONFIG = "broker.removal.shutdown.timeout.ms";
  private static final String BROKER_REMOVAL_SHUTDOWN_MS_DOC = "Timeout in ms for the broker to respond to a describe cluster request";
  public static final Long DEFAULT_BROKER_REMOVAL_SHUTDOWN_MS = 600000L; // 10 minutes

  /**
   * <code>max.volume.throughput</code>
   */
  public static final String MAX_VOLUME_THROUGHPUT_MB_CONFIG = "max.volume.throughput.mb";
  private static final String MAX_VOLUME_THROUGHPUT_MB_DOC = "The maximum throughput of the storage volume. This should" +
          " be the minimum of the maximum disk throughput on the volume and the maximum network throughput to the volume";

  /**
   * <code>write.throughput.multiplier</code>
   */
  public static final String WRITE_THROUGHPUT_MULTIPLIER_CONFIG = "write.throughput.multiplier";
  private static final String WRITE_THROUGHPUT_MULTIPLIER_DOC = "The factor that write throughput is multiplied by when computing" +
          " network usage. GCP counts communication with the storage volumes and replication of the written data against" +
          " the overall network usage";
  public static final Double DEFAULT_WRITE_THROUGHPUT_MULTIPLIER = 1.0;

  /**
   * <code>read.throughput.multiplier</code>
   */
  public static final String READ_THROUGHPUT_MULTIPLIER_CONFIG = "read.throughput.multiplier";
  private static final String READ_THROUGHPUT_MULTIPLIER_DOC = "The factor that read throughput is multiplied by when computing" +
          " network usage. GCP counts communication with the storage volumes against the overall network usage";
  public static final Double DEFAULT_READ_THROUGHPUT_MULTIPLIER = 1.0;

  /**
   * <code>calculated.throttle.ratio</code>
   */
  public static final String CALCULATED_THROTTLE_RATIO_CONFIG = "calculated.throttle.ratio";
  private static final String CALCULATED_THROTTLE_RATIO_DOC = "When the throttling rate is calculated, this is the amount" +
          " of available network throughput (max throughput - currently used throughput) that is given to reassignment traffic";

  /**
   * <code>disk.read.ratio</code>
   */
  public static final String DISK_READ_RATIO_CONFIG = "disk.read.ratio";
  private static final String DISK_READ_RATIO_DOC = "When calculating a replication throttle, this is the ratio of reads" +
          " that are assumed to read from the actual disk, rather than the page cache";

  static {
    CONFIG = new ConfigDef()
        .define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
        .define(CLIENT_ID_CONFIG, ConfigDef.Type.STRING, "kafka-cruise-control", ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.CLIENT_ID_DOC)
        .define(SEND_BUFFER_CONFIG, ConfigDef.Type.INT, 128 * 1024, atLeast(0), ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.SEND_BUFFER_DOC)
        .define(RECEIVE_BUFFER_CONFIG, ConfigDef.Type.INT, 32 * 1024, atLeast(0), ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.RECEIVE_BUFFER_DOC)
        .define(RECONNECT_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, 50L, atLeast(0L), ConfigDef.Importance.LOW,
                CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
        .define(METADATA_MAX_AGE_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_METADATA_MAX_AGE_MS,
                atLeast(0),
                ConfigDef.Importance.LOW,
                METADATA_MAX_AGE_DOC)
        .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                ConfigDef.Type.LONG,
                9 * 60 * 1000,
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
        .define(REQUEST_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                30 * 1000, // 30 seconds
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                REQUEST_TIMEOUT_MS_DOC)
        .define(DEFAULT_API_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_API_TIMEOUT_MS_DEFAULT,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                DEFAULT_API_TIMEOUT_MS_DOC)
        .define(PARTITION_METRICS_WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_PARTITION_METRICS_MS,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                PARTITION_METRICS_WINDOW_MS_DOC)
        .define(NUM_PARTITION_METRICS_WINDOWS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_NUM_PARTITION_METRICS_WINDOWS,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NUM_PARTITION_METRICS_WINDOWS_DOC)
        .define(MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_DOC)
        .define(MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_ALLOWED_EXTRAPOLATIONS_PER_PARTITION_DOC)
        .define(PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.LOW,
                PARTITION_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC)
        .define(BROKER_METRICS_WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_BROKER_METRICS_WINDOW_MS,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                BROKER_METRICS_WINDOW_MS_DOC)
        .define(DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG,
                ConfigDef.Type.LONG,
                TimeUnit.HOURS.toMillis(336),
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                DEMOTION_HISTORY_RETENTION_TIME_MS_DOC)
        .define(REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG,
                ConfigDef.Type.LONG,
                TimeUnit.HOURS.toMillis(336),
                atLeast(0),
                ConfigDef.Importance.MEDIUM, REMOVAL_HISTORY_RETENTION_TIME_MS_DOC)
        .define(NUM_BROKER_METRICS_WINDOWS_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_NUM_BROKER_METRICS_WINDOWS,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NUM_BROKER_METRICS_WINDOWS_DOC)
        .define(MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG,
                ConfigDef.Type.INT,
                DEFAULT_MIN_SAMPLES_PER_BROKER_METRICS_WINDOW,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_DOC)
        .define(MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_ALLOWED_EXTRAPOLATIONS_PER_BROKER_DOC)
        .define(BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(0),
                ConfigDef.Importance.LOW,
                BROKER_METRIC_SAMPLE_AGGREGATOR_COMPLETENESS_CACHE_SIZE_DOC)
        .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                ConfigDef.Type.STRING,
                CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.SECURITY_PROTOCOL_DOC)
        .define(NUM_METRIC_FETCHERS_CONFIG,
                ConfigDef.Type.INT,
                1,
                ConfigDef.Importance.HIGH,
                NUM_METRIC_FETCHERS_DOC)
        .define(NUM_CACHED_RECENT_ANOMALY_STATES_CONFIG,
            ConfigDef.Type.INT,
            10,
            between(1, 100),
            ConfigDef.Importance.LOW,
            NUM_CACHED_RECENT_ANOMALY_STATES_DOC)
        .define(METRIC_SAMPLER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                METRIC_SAMPLER_CLASS_DEFAULT,
                ConfigDef.Importance.HIGH,
                METRIC_SAMPLER_CLASS_DOC)
        .define(METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                DefaultMetricSamplerPartitionAssignor.class.getName(),
                ConfigDef.Importance.LOW,
                METRIC_SAMPLER_PARTITION_ASSIGNOR_CLASS_DOC)
        .define(METRIC_SAMPLING_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_METRIC_SAMPLING_INTERVAL_MS,
                atLeast(0),
                ConfigDef.Importance.HIGH,
                METRIC_SAMPLING_INTERVAL_MS_DOC)
        .define(BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                BrokerCapacityResolver.class.getName(),
                ConfigDef.Importance.MEDIUM,
                BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_DOC)
        .define(GOAL_BALANCEDNESS_PRIORITY_WEIGHT_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.1,
                between(1, 2),
                ConfigDef.Importance.LOW,
                GOAL_BALANCEDNESS_PRIORITY_WEIGHT_DOC)
        .define(GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.5,
                between(1, 2),
                ConfigDef.Importance.LOW,
                GOAL_BALANCEDNESS_STRICTNESS_WEIGHT_DOC)
        .define(MIN_VALID_PARTITION_RATIO_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_MIN_VALID_PARTITION_RATIO,
                between(0, 1),
                ConfigDef.Importance.HIGH, MIN_VALID_PARTITION_RATIO_DOC)
        .define(LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.7,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
        .define(LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.15,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
        .define(FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.15,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_DOC)
        .define(LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_CONFIG,
                ConfigDef.Type.INT,
                5,
                between(0, 100),
                ConfigDef.Importance.MEDIUM,
                LINEAR_REGRESSION_MODEL_CPU_UTIL_BUCKET_SIZE_DOC)
        .define(LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_CONFIG,
                ConfigDef.Type.INT,
                5,
                ConfigDef.Importance.MEDIUM,
                LINEAR_REGRESSION_MODEL_MIN_NUM_CPU_UTIL_BUCKETS_DOC)
        .define(LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_CONFIG,
                ConfigDef.Type.INT,
                100,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                LINEAR_REGRESSION_MODEL_REQUIRED_SAMPLES_PER_CPU_UTIL_BUCKET_DOC)
        .define(CPU_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                CPU_BALANCE_THRESHOLD_DOC)
        .define(DISK_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                DISK_BALANCE_THRESHOLD_DOC)
        .define(NETWORK_INBOUND_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NETWORK_INBOUND_BALANCE_THRESHOLD_DOC)
        .define(NETWORK_OUTBOUND_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                NETWORK_OUTBOUND_BALANCE_THRESHOLD_DOC)
        .define(REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
        .define(LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                1.10,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
        .define(TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                3.00,
                atLeast(1),
                ConfigDef.Importance.HIGH,
                TOPIC_REPLICA_COUNT_BALANCE_THRESHOLD_DOC)
        .define(GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER,
                atLeast(1),
                ConfigDef.Importance.MEDIUM, GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_DOC)
        .define(CPU_CAPACITY_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_CPU_CAPACITY_THRESHOLD,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                CPU_CAPACITY_THRESHOLD_DOC)
        .define(DISK_CAPACITY_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_DISK_CAPACITY_THRESHOLD,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                DISK_CAPACITY_THRESHOLD_DOC)
        .define(NETWORK_INBOUND_CAPACITY_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_NETWORK_INBOUND_CAPACITY_THRESHOLD,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                NETWORK_INBOUND_CAPACITY_THRESHOLD_DOC)
        .define(NETWORK_OUTBOUND_CAPACITY_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_NETWORK_OUTBOUND_CAPACITY_THRESHOLD,
                between(0, 1),
                ConfigDef.Importance.HIGH,
                NETWORK_OUTBOUND_CAPACITY_THRESHOLD_DOC)
        .define(CPU_LOW_UTILIZATION_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_CPU_LOW_UTILIZATION_THRESHOLD,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                CPU_LOW_UTILIZATION_THRESHOLD_DOC)
        .define(DISK_LOW_UTILIZATION_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_DISK_LOW_UTILIZATION_THRESHOLD,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                DISK_LOW_UTILIZATION_THRESHOLD_DOC)
        .define(NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                NETWORK_INBOUND_LOW_UTILIZATION_THRESHOLD_DOC)
        .define(NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD,
                between(0, 1),
                ConfigDef.Importance.MEDIUM,
                NETWORK_OUTBOUND_LOW_UTILIZATION_THRESHOLD_DOC)
        .define(METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
                ConfigDef.Type.LIST,
                DEFAULT_METRIC_ANOMALY_FINDER_CLASS,
                ConfigDef.Importance.MEDIUM, METRIC_ANOMALY_FINDER_CLASSES_DOC)
        .define(PROPOSAL_EXPIRATION_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_PROPOSAL_EXPIRATION_MS,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                PROPOSAL_EXPIRATION_MS_DOC)
        .define(MAX_REPLICAS_PER_BROKER_CONFIG,
                ConfigDef.Type.LONG,
                10000,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_REPLICAS_PER_BROKER_DOC)
        .define(NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG,
                ConfigDef.Type.INT,
                1,
                between(0, 1),
                ConfigDef.Importance.LOW,
                NUM_PROPOSAL_PRECOMPUTE_THREADS_DOC)
        .define(ZOOKEEPER_CONNECT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ZOOKEEPER_CONNECT_DOC)
        .define(ZOOKEEPER_SECURITY_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.HIGH,
                ZOOKEEPER_SECURITY_ENABLED_DOC)
        .define(NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG,
                ConfigDef.Type.INT,
                5,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_DOC)
        .define(NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_CONFIG,
                ConfigDef.Type.INT,
                2,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                NUM_CONCURRENT_INTRA_BROKER_PARTITION_MOVEMENTS_DOC)
        .define(NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG,
                ConfigDef.Type.INT,
                1000,
                atLeast(1),
                ConfigDef.Importance.MEDIUM,
                NUM_CONCURRENT_LEADER_MOVEMENTS_DOC)
        .define(DEFAULT_REPLICATION_THROTTLE_CONFIG,
                ConfigDef.Type.LONG,
                null,
                ConfigDef.Importance.MEDIUM,
                DEFAULT_REPLICATION_THROTTLE_DOC)
        .define(REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                ConfigDef.Type.LIST,
                String.join(",",
                        PostponeUrpReplicaMovementStrategy.class.getName(),
                        PrioritizeLargeReplicaMovementStrategy.class.getName(),
                        PrioritizeSmallReplicaMovementStrategy.class.getName(),
                        BaseReplicaMovementStrategy.class.getName()),
                ConfigDef.Importance.MEDIUM,
                REPLICA_MOVEMENT_STRATEGIES_DOC)
        .define(DEFAULT_REPLICA_MOVEMENT_STRATEGIES_CONFIG,
                ConfigDef.Type.LIST,
                BaseReplicaMovementStrategy.class.getName(),
                ConfigDef.Importance.MEDIUM,
                DEFAULT_REPLICA_MOVEMENT_STRATEGIES_DOC)
        .define(EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_EXECUTION_PROGRESS_CHECK_INTERVAL_MS,
                atLeast(0),
                ConfigDef.Importance.LOW,
                EXECUTION_PROGRESS_CHECK_INTERVAL_MS_DOC)
        .define(GOALS_CONFIG,
                ConfigDef.Type.LIST,
                String.join(",", DEFAULT_GOALS_LIST),
                ConfigDef.Importance.HIGH,
                GOALS_DOC)
        .define(INTRA_BROKER_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                String.join(",",
                    IntraBrokerDiskCapacityGoal.class.getName(),
                    IntraBrokerDiskUsageDistributionGoal.class.getName()),
                ConfigDef.Importance.HIGH,
                INTRA_BROKER_GOALS_DOC)
        .define(HARD_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                String.join(",", DEFAULT_HARD_GOALS_LIST),
                ConfigDef.Importance.HIGH,
                HARD_GOALS_DOC)
        .define(DEFAULT_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                ConfigDef.Importance.HIGH,
                DEFAULT_GOALS_DOC)
        .define(SELF_HEALING_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                ConfigDef.Importance.HIGH,
                SELF_HEALING_GOALS_DOC)
        .define(ANOMALY_NOTIFIER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                DEFAULT_ANOMALY_NOTIFIER_CLASS,
                ConfigDef.Importance.LOW, ANOMALY_NOTIFIER_CLASS_DOC)
        .define(EXECUTOR_NOTIFIER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                DEFAULT_EXECUTOR_NOTIFIER_CLASS,
                ConfigDef.Importance.LOW,
                EXECUTOR_NOTIFIER_CLASS_DOC)
        .define(ANOMALY_DETECTION_INTERVAL_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_ANOMALY_DETECTION_INTERVAL_MS,
                ConfigDef.Importance.LOW,
                ANOMALY_DETECTION_INTERVAL_MS_DOC)
        .define(ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                ANOMALY_DETECTION_ALLOW_CAPACITY_ESTIMATION_DOC)
        .define(SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                SAMPLING_ALLOW_CPU_CAPACITY_ESTIMATION_DOC)
        .define(ANOMALY_DETECTION_GOALS_CONFIG,
                ConfigDef.Type.LIST,
                String.join(",", DEFAULT_ANOMALY_DETECTION_GOALS_LIST),
                ConfigDef.Importance.MEDIUM,
                ANOMALY_DETECTION_GOALS_DOC)
        .define(BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                BROKER_FAILURE_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC)
        .define(BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                BROKER_FAILURE_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC)
        .define(GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                GOAL_VIOLATION_EXCLUDE_RECENTLY_DEMOTED_BROKERS_DOC)
        .define(GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                GOAL_VIOLATION_EXCLUDE_RECENTLY_REMOVED_BROKERS_DOC)
        .define(FAILED_BROKERS_ZK_PATH_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_FAILED_BROKERS_ZK_PATH,
                ConfigDef.Importance.LOW, FAILED_BROKERS_ZK_PATH_DOC)
        .define(USE_LINEAR_REGRESSION_MODEL_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.MEDIUM,
                USE_LINEAR_REGRESSION_MODEL_DOC)
        .define(TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_DOC)
        .define(SAMPLE_STORE_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                KafkaSampleStore.class.getName(),
                ConfigDef.Importance.LOW,
                SAMPLE_STORE_CLASS_DOC)
        .define(TOPIC_CONFIG_PROVIDER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                KafkaTopicConfigProvider.class.getName(),
                ConfigDef.Importance.LOW,
                TOPIC_CONFIG_PROVIDER_CLASS_DOC)
        .define(LOGDIR_RESPONSE_TIMEOUT_MS_CONFIG,
            ConfigDef.Type.LONG,
            10000L,
            ConfigDef.Importance.LOW,
            LOGDIR_RESPONSE_TIMEOUT_MS_DOC)
        .define(DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS,
                ConfigDef.Importance.LOW,
                DESCRIBE_TOPICS_RESPONSE_TIMEOUT_MS_DOC)
        .define(DESCRIBE_CLUSTER_RESPONSE_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_DESCRIBE_CLUSTER_RESPONSE_TIMEOUT_MS,
                ConfigDef.Importance.LOW,
                DESCRIBE_CLUSTER_RESPONSE_TIMEOUT_MS_DOC)
        .define(BROKER_REMOVAL_SHUTDOWN_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_BROKER_REMOVAL_SHUTDOWN_MS,
                ConfigDef.Importance.LOW,
                BROKER_REMOVAL_SHUTDOWN_MS_DOC)
        .define(MAX_VOLUME_THROUGHPUT_MB_CONFIG,
                ConfigDef.Type.INT,
                0,
                ConfigDef.Importance.MEDIUM,
                MAX_VOLUME_THROUGHPUT_MB_DOC)
        .define(WRITE_THROUGHPUT_MULTIPLIER_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_WRITE_THROUGHPUT_MULTIPLIER,
                ConfigDef.Importance.MEDIUM,
                WRITE_THROUGHPUT_MULTIPLIER_DOC)
        .define(READ_THROUGHPUT_MULTIPLIER_CONFIG,
                ConfigDef.Type.DOUBLE,
                DEFAULT_READ_THROUGHPUT_MULTIPLIER,
                ConfigDef.Importance.MEDIUM,
                READ_THROUGHPUT_MULTIPLIER_DOC)
        .define(CALCULATED_THROTTLE_RATIO_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.8,
                ConfigDef.Importance.MEDIUM,
                CALCULATED_THROTTLE_RATIO_DOC)
        .define(DISK_READ_RATIO_CONFIG,
                ConfigDef.Type.DOUBLE,
                0.2,
                ConfigDef.Importance.MEDIUM,
                DISK_READ_RATIO_DOC)
        .define(SELF_HEALING_BROKER_FAILURE_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.HIGH,
                SELF_HEALING_BROKER_FAILURE_ENABLED_DOC)
        .define(SELF_HEALING_GOAL_VIOLATION_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.HIGH,
                SELF_HEALING_GOAL_VIOLATION_ENABLED_DOC)
        .define(SELF_HEALING_METRIC_ANOMALY_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.MEDIUM,
                SELF_HEALING_METRIC_ANOMALY_ENABLED_DOC)
        .define(SELF_HEALING_DISK_FAILURE_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.MEDIUM,
                SELF_HEALING_DISK_FAILURE_ENABLED_DOC)
        .define(BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_BROKER_FAILURE_ALERT_THRESHOLD_MS,
                ConfigDef.Importance.LOW,
                BROKER_FAILURE_ALERT_THRESHOLD_MS_DOC)
        .define(BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS,
                ConfigDef.Importance.HIGH,
                BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_DOC)
        .withClientSslSupport()
        .withClientSaslSupport();
  }

  public Map<String, Object> mergedConfigValues() {
    Map<String, Object> conf = originals();

    // Use parsed non-null value to overwrite originals.
    // This will keep default values and also keep values that are not defined under ConfigDef.
    values().forEach((k, v) -> {
      if (v != null) {
        conf.put(k, v);
      }
    });
    return conf;
  }

  @Override
  public <T> T getConfiguredInstance(String key, Class<T> t) {
    T o = super.getConfiguredInstance(key, t);
    if (o instanceof CruiseControlConfigurable) {
      ((CruiseControlConfigurable) o).configure(mergedConfigValues());
    }
    return o;
  }

  @Override
  public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
    List<T> objects = super.getConfiguredInstances(key, t);
    for (T o : objects) {
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(mergedConfigValues());
      }
    }
    return objects;
  }

  @Override
  public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides) {
    List<T> objects = super.getConfiguredInstances(key, t, configOverrides);
    Map<String, Object> configPairs = mergedConfigValues();
    configPairs.putAll(configOverrides);
    for (T o : objects) {
      if (o instanceof CruiseControlConfigurable) {
        ((CruiseControlConfigurable) o).configure(configPairs);
      }
    }
    return objects;
  }

  public <T> T getConfiguredInstance(String key, Class<T> t, Map<String, Object> configOverrides) {
    Class<?> c = getClass(key);
    Object instance;
    try {
      instance = c.newInstance();
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Could not instantiate class " + c.getName(), e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Could not instantiate class " + c.getName() + " Does it have a public no-argument constructor?", e);
    } catch (NullPointerException e) {
      throw new IllegalArgumentException("Attempt to get configured instance of null configuration " + t.getName(), e);
    }

    if (!t.isInstance(instance)) {
      throw new IllegalArgumentException(c.getName() + " is not an instance of " + t.getName());
    }
    T o = t.cast(instance);
    if (o instanceof CruiseControlConfigurable) {
      Map<String, Object> configPairs = mergedConfigValues();
      configPairs.putAll(configOverrides);
      ((CruiseControlConfigurable) o).configure(configPairs);
    }
    return o;
  }

  /**
   * Sanity check for
   * <ul>
   * <li>{@link KafkaCruiseControlConfig#GOALS_CONFIG} and
   * {@link KafkaCruiseControlConfig#INTRA_BROKER_GOALS_CONFIG} are non-empty.</li>
   * <li>Case insensitive goal names.</li>
   * <li>{@link KafkaCruiseControlConfig#DEFAULT_GOALS_CONFIG} is non-empty.</li>
   * <li>{@link KafkaCruiseControlConfig#SELF_HEALING_GOALS_CONFIG} is a sublist of
   * {@link KafkaCruiseControlConfig#GOALS_CONFIG}.</li>
   * <li>{@link KafkaCruiseControlConfig#ANOMALY_DETECTION_GOALS_CONFIG} is a sublist of
   * (1) {@link KafkaCruiseControlConfig#SELF_HEALING_GOALS_CONFIG} if it is not empty,
   * (2) {@link KafkaCruiseControlConfig#DEFAULT_GOALS_CONFIG} otherwise.</li>
   * </ul>
   */
  private void sanityCheckGoalNames() {
    List<String> goalNames = getList(KafkaCruiseControlConfig.GOALS_CONFIG);
    // Ensure that goals are non-empty.
    if (goalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure goals configuration with an empty list of goals.");
    }

    List<String> intraBrokerGoalNames = getList(KafkaCruiseControlConfig.INTRA_BROKER_GOALS_CONFIG);
    // Ensure that intra-broker goals are non-empty.
    if (intraBrokerGoalNames.isEmpty()) {
      throw new ConfigException("Attempt to configure intra-broker goals configuration with an empty list of goals.");
    }

    Set<String> caseInsensitiveGoalNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (String goalName: goalNames) {
      if (!caseInsensitiveGoalNames.add(goalName.replaceAll(".*\\.", ""))) {
        throw new ConfigException("Attempt to configure goals with case sensitive names.");
      }
    }
    for (String goalName: intraBrokerGoalNames) {
      if (!caseInsensitiveGoalNames.add(goalName.replaceAll(".*\\.", ""))) {
        throw new ConfigException("Attempt to configure intra-broker goals with case sensitive names.");
      }
    }
    // If default goals is empty, just use the regular goals list.
    List<String> defaultGoals = getList(KafkaCruiseControlConfig.DEFAULT_GOALS_CONFIG);
    List<String> defaultGoalNames = !defaultGoals.isEmpty() ? defaultGoals : goalNames;

    // Ensure that goals used for self-healing are supported goals.
    List<String> selfHealingGoalNames = getList(KafkaCruiseControlConfig.SELF_HEALING_GOALS_CONFIG);
    if (selfHealingGoalNames.stream().anyMatch(g -> !defaultGoalNames.contains(g))) {
      throw new ConfigException("Attempt to configure self healing goals with unsupported goals.");
    }

    // Ensure that goals used for anomaly detection are a subset of goals used for fixing the anomaly.
    List<String> anomalyDetectionGoalNames = getList(KafkaCruiseControlConfig.ANOMALY_DETECTION_GOALS_CONFIG);
    if (anomalyDetectionGoalNames.stream().anyMatch(g -> selfHealingGoalNames.isEmpty() ? !defaultGoalNames.contains(g)
                                                                                        : !selfHealingGoalNames.contains(g))) {
      throw new ConfigException("Attempt to configure anomaly detection goals as a superset of self healing goals {}.", anomalyDetectionGoalNames.toString());
    }
  }

  /**
   * Sanity check to ensure that
   * <ul>
   *   <li>{@link KafkaCruiseControlConfig#METADATA_MAX_AGE_CONFIG} is not longer than
   *   {@link KafkaCruiseControlConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG},</li>
   *   <li>sampling frequency per partition window is within the limits -- i.e.
   *   ({@link KafkaCruiseControlConfig#PARTITION_METRICS_WINDOW_MS_CONFIG} /
   *   {@link KafkaCruiseControlConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}) <= {@link Byte#MAX_VALUE}, and</li>
   *   <li>sampling frequency per broker window is within the limits -- i.e.
   *   ({@link KafkaCruiseControlConfig#BROKER_METRICS_WINDOW_MS_CONFIG} /
   *   {@link KafkaCruiseControlConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}) <= {@link Byte#MAX_VALUE}, and</li>
   *   <li>{@link CruiseControlMetricsReporterConfig#CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG} is not longer than
   *   {@link KafkaCruiseControlConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}</li>
   * </ul>
   *
   * Sampling process involves a potential metadata update if the current metadata is stale. The configuration
   * {@link KafkaCruiseControlConfig#METADATA_MAX_AGE_CONFIG} indicates the timeout of such a metadata update. Hence,
   * this subprocess of the sampling process cannot be set with a timeout larger than the total sampling timeout of
   * {@link KafkaCruiseControlConfig#METRIC_SAMPLING_INTERVAL_MS_CONFIG}.
   *
   * The number of samples at a given window cannot exceed a predefined maximum limit.
   *
   * Metrics reporting frequency should be larger than metric sampling frequency to ensure there is always data to be collected.
   */
  private void sanityCheckSamplingPeriod(Map<?, ?> originals) {
    long samplingIntervalMs = getLong(KafkaCruiseControlConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG);
    int metadataTimeoutMs = getInt(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG);
    if (metadataTimeoutMs >  samplingIntervalMs) {
      throw new ConfigException("Attempt to set metadata refresh timeout [" + metadataTimeoutMs +
                                "] to be longer than sampling period [" + samplingIntervalMs + "].");
    }

    // Ensure that the sampling frequency per partition window is within the limits.
    long partitionSampleWindowMs = getLong(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);
    short partitionSamplingFrequency = (short) (partitionSampleWindowMs / samplingIntervalMs);
    if (partitionSamplingFrequency > Byte.MAX_VALUE) {
      throw new ConfigException(String.format("Configured sampling frequency (%d) exceeds the maximum allowed value (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that their"
                                              + " ratio is under this limit.", partitionSamplingFrequency, Byte.MAX_VALUE,
                                              KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG,
                                              KafkaCruiseControlConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG));
    }

    // Ensure that the sampling frequency per broker window is within the limits.
    long brokerSampleWindowMs = getLong(KafkaCruiseControlConfig.BROKER_METRICS_WINDOW_MS_CONFIG);
    short brokerSamplingFrequency = (short) (brokerSampleWindowMs / samplingIntervalMs);
    if (brokerSamplingFrequency > Byte.MAX_VALUE) {
      throw new ConfigException(String.format("Configured sampling frequency (%d) exceeds the maximum allowed value (%d). "
                                              + "Decrease the value of %s or increase the value of %s to ensure that their"
                                              + " ratio is under this limit.", brokerSamplingFrequency, Byte.MAX_VALUE,
                                              KafkaCruiseControlConfig.BROKER_METRICS_WINDOW_MS_CONFIG,
                                              KafkaCruiseControlConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG));
    }
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals) {
    super(CONFIG, originals);
    sanityCheckGoalNames();
    sanityCheckSamplingPeriod(originals);
  }

  public KafkaCruiseControlConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
    sanityCheckGoalNames();
    sanityCheckSamplingPeriod(originals);
  }
}
