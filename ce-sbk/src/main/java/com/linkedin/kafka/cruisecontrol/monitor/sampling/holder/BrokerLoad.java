/*
 * Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling.holder;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.CruiseControlMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.PartitionMetric;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.TopicMetric;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.BROKER;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.PARTITION;
import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.MetricScope.TOPIC;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.METRIC_TYPES_TO_SUM;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.MISSING_BROKER_METRIC_VALUE;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.allowMissingBrokerMetric;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.convertUnit;
import static com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.HolderUtils.sanityCheckMetricScope;


/**
 * A helper class to hold broker load.
 */
public class BrokerLoad {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerLoad.class);
  private static final double MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT = 0.05;
  private static final double MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT = 0.05;
  private final RawMetricsHolder _brokerMetrics;
  private final Map<String, RawMetricsHolder> _topicMetrics;
  private final Map<TopicPartition, RawMetricsHolder> _partitionMetrics;

  public BrokerLoad() {
    _brokerMetrics = new RawMetricsHolder();
    _topicMetrics = new HashMap<>();
    _partitionMetrics = new HashMap<>();
  }

  // Remember which topic has partition size reported. Because the topic level IO metrics are only created when
  // there is IO, the topic level IO metrics may be missing if there was no traffic to the topic on the broker.
  // However, because the partition size will always be reported, when we see partition size was reported for
  // a topic but the topic level IO metrics are not reported, we assume there was no traffic to the topic.
  private final Set<String> _topicsWithPartitionSizeReported = new HashSet<>();
  private final Set<RawMetricType> _missingBrokerMetricsInMinSupportedVersion = new HashSet<>();
  private boolean _minRequiredBrokerMetricsAvailable = false;
  // Set to the latest possible deserialization version based on the sampled data.
  private byte _brokerSampleDeserializationVersion = -1;

  public void recordMetric(CruiseControlMetric ccm) {
    RawMetricType rawMetricType = ccm.rawMetricType();
    switch (rawMetricType.metricScope()) {
      case BROKER:
        _brokerMetrics.recordCruiseControlMetric(ccm);
        break;
      case TOPIC:
        TopicMetric tm = (TopicMetric) ccm;
        _topicMetrics.computeIfAbsent(tm.topic(), t -> new RawMetricsHolder())
                               .recordCruiseControlMetric(ccm);
        break;
      case PARTITION:
        PartitionMetric pm = (PartitionMetric) ccm;
        _partitionMetrics.computeIfAbsent(new TopicPartition(pm.topic(), pm.partition()), tp -> new RawMetricsHolder())
                                   .recordCruiseControlMetric(ccm);
        _topicsWithPartitionSizeReported.add(pm.topic());
        break;
      default:
        throw new IllegalStateException(String.format("Should never be here. Unrecognized metric scope %s",
                                                      rawMetricType.metricScope()));
    }
  }

  public boolean topicMetricsAvailable(String topic) {
    // We rely on the partition size metric to determine whether a topic metric is available or not.
    return _topicsWithPartitionSizeReported.contains(topic);
  }

  public boolean minRequiredBrokerMetricsAvailable() {
    return _minRequiredBrokerMetricsAvailable;
  }

  public boolean brokerMetricAvailable(RawMetricType rawMetricType) {
    return _brokerMetrics.metricValue(rawMetricType) != null;
  }

  public boolean partitionMetricAvailable(TopicPartition topic, RawMetricType rawMetricType) {
    RawMetricsHolder rawMetricsHolder = _partitionMetrics.get(topic);
    return rawMetricsHolder != null && rawMetricsHolder.metricValue(rawMetricType) != null;
  }

  public Set<RawMetricType> missingBrokerMetricsInMinSupportedVersion() {
    return _missingBrokerMetricsInMinSupportedVersion;
  }

  public double brokerMetric(RawMetricType rawMetricType) {
    sanityCheckMetricScope(rawMetricType, BROKER);
    ValueHolder valueHolder = _brokerMetrics.metricValue(rawMetricType);
    if (valueHolder == null) {
      throw new IllegalArgumentException(String.format("Broker metric %s does not exist.", rawMetricType));
    } else {
      return convertUnit(valueHolder.value(), rawMetricType);
    }
  }

  public double topicMetrics(String topic, RawMetricType rawMetricType) {
    return topicMetrics(topic, rawMetricType, true);
  }

  private double topicMetrics(String topic, RawMetricType rawMetricType, boolean convertUnit) {
    sanityCheckMetricScope(rawMetricType, TOPIC);
    if (!topicMetricsAvailable(topic)) {
      throw new IllegalArgumentException(String.format("Topic metric %s does not exist for topic name %s.",
                                                       rawMetricType, topic));
    }
    RawMetricsHolder rawMetricsHolder = _topicMetrics.get(topic);
    if (rawMetricsHolder == null || rawMetricsHolder.metricValue(rawMetricType) == null) {
      return 0.0;
    }
    double rawMetricValue = rawMetricsHolder.metricValue(rawMetricType).value();
    return convertUnit ? convertUnit(rawMetricValue, rawMetricType) : rawMetricValue;
  }

  public double partitionMetric(String topic, int partition, RawMetricType rawMetricType) {
    sanityCheckMetricScope(rawMetricType, PARTITION);
    RawMetricsHolder metricsHolder = _partitionMetrics.get(new TopicPartition(topic, partition));
    if (metricsHolder == null || metricsHolder.metricValue(rawMetricType) == null) {
      throw new IllegalArgumentException(String.format("Partition metric %s does not exist for topic %s"
                                                       + " and partition %d.", rawMetricType, topic, partition));
    } else {
      return convertUnit(metricsHolder.metricValue(rawMetricType).value(), rawMetricType);
    }
  }

  /**
   * Due to the yammer metric exponential decaying mechanism, the broker metric and the sum of the partition metrics
   * on the same broker may differ by a lot. Our experience shows that in that case, the sum of the topic/partition
   * level metrics are more accurate. So we will just replace the following metrics with the sum of topic/partition
   * level metrics:
   * <ul>
   *   <li>BrokerProduceRate</li>
   *   <li>BrokerFetchRate</li>
   *   <li>BrokerLeaderBytesInRate</li>
   *   <li>BrokerLeaderBytesOutRate</li>
   *   <li>BrokerReplicationBytesInRate</li>
   *   <li>BrokerReplicationBytesOutRate</li>
   *   <li>BrokerMessagesInRate</li>
   * </ul>
   *
   * We use the cluster metadata to check if the reported topic level metrics are complete. If the reported topic
   * level metrics are not complete, we ignore the broker metric sample by setting the _minRequiredBrokerMetricsAvailable
   * flag to false.
   *
   * @param cluster The Kafka cluster.
   * @param brokerId The broker id to prepare metrics for.
   * @param time The last sample time.
   */
  public void prepareBrokerMetrics(Cluster cluster, int brokerId, long time) {
    boolean enoughTopicPartitionMetrics = enoughTopicPartitionMetrics(cluster, brokerId);
    // Ensure there are enough topic level metrics.
    if (enoughTopicPartitionMetrics) {
      Map<RawMetricType, Double> sumOfTopicMetrics = new HashMap<>();
      for (String topic : _topicsWithPartitionSizeReported) {
        METRIC_TYPES_TO_SUM.keySet().forEach(type -> {
          double value = topicMetrics(topic, type, false);
          sumOfTopicMetrics.compute(type, (t, v) -> (v == null ? 0 : v) + value);
        });
      }
      for (Map.Entry<RawMetricType, Double> entry : sumOfTopicMetrics.entrySet()) {
        RawMetricType rawTopicMetricType = entry.getKey();
        double value = entry.getValue();
        _brokerMetrics.setRawMetricValue(METRIC_TYPES_TO_SUM.get(rawTopicMetricType), value, time);
      }
    }
    // Check if all the broker raw metrics are available.
    maybeSetBrokerRawMetrics(cluster, brokerId, time);

    // A broker metric is only available if it has enough valid topic metrics and it has reported
    // replication bytes in/out metrics.
    _minRequiredBrokerMetricsAvailable = enoughTopicPartitionMetrics && _missingBrokerMetricsInMinSupportedVersion.isEmpty();
  }

  /**
   * Use the latest supported version for which the raw broker metrics are available.
   *
   * 1) If broker metrics are incomplete for the {@link BrokerMetricSample#MIN_SUPPORTED_VERSION}, then broker metrics
   * are insufficient to create a broker sample.
   * 2) If broker metrics are complete in a version-X but not in (version-X + 1), then use version-X.
   *
   * {@link #_missingBrokerMetricsInMinSupportedVersion} is relevant only for {@link BrokerMetricSample#MIN_SUPPORTED_VERSION}.
   *
   * @param cluster The Kafka cluster.
   * @param brokerId The broker id to prepare metrics for.
   * @param time The last sample time.
   */
  private void maybeSetBrokerRawMetrics(Cluster cluster, int brokerId, long time) {
    for (byte v = BrokerMetricSample.MIN_SUPPORTED_VERSION; v <= BrokerMetricSample.LATEST_SUPPORTED_VERSION; v++) {
      Set<RawMetricType> missingBrokerMetrics = new HashSet<>();
      for (RawMetricType rawBrokerMetricType : RawMetricType.brokerMetricTypesDiffForVersion(v)) {
        if (_brokerMetrics.metricValue(rawBrokerMetricType) == null) {
          if (allowMissingBrokerMetric(cluster, brokerId, rawBrokerMetricType)) {
            // If the metric is allowed to be missing, we simply use MISSING_BROKER_METRIC_VALUE as the value.
            _brokerMetrics.setRawMetricValue(rawBrokerMetricType, MISSING_BROKER_METRIC_VALUE, time);
          } else {
            missingBrokerMetrics.add(rawBrokerMetricType);
          }
        }
      }

      if (!missingBrokerMetrics.isEmpty()) {
        if (_brokerSampleDeserializationVersion == -1) {
          _missingBrokerMetricsInMinSupportedVersion.addAll(missingBrokerMetrics);
        }
        break;
      } else {
        // Set supported deserialization version so far.
        _brokerSampleDeserializationVersion = v;
      }
    }
    // Set nullable broker metrics for missing metrics if a valid deserialization exists with an old version.
    setNullableBrokerMetrics();
  }

  public byte brokerSampleDeserializationVersion() {
    return _brokerSampleDeserializationVersion;
  }

  private void setNullableBrokerMetrics() {
    if (_brokerSampleDeserializationVersion != -1) {
      Set<RawMetricType> nullableBrokerMetrics = new HashSet<>();
      for (byte v = (byte) (_brokerSampleDeserializationVersion + 1); v <= BrokerMetricSample.LATEST_SUPPORTED_VERSION; v++) {
        Set<RawMetricType> nullableMetrics = new HashSet<>(RawMetricType.brokerMetricTypesDiffForVersion(v));
        nullableBrokerMetrics.addAll(nullableMetrics);
      }
      nullableBrokerMetrics.forEach(nullableMetric -> _brokerMetrics.setRawMetricValue(nullableMetric, 0.0, 0L));
    }
  }

  /**
   * Verify whether we have collected enough metrics to generate the broker metric samples. The broker must have
   * missed less than {@link #MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT} of the topic level
   * and {@link #MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT} partition level metrics in the
   * broker to generate broker level metrics.
   *
   * @param cluster The Kafka cluster.
   * @param brokerId The broker id to check.
   * @return True if there are enough topic level metrics, false otherwise.
   */
  private boolean enoughTopicPartitionMetrics(Cluster cluster, int brokerId) {
    Set<String> missingTopics = new HashSet<>();
    Set<String> topicsInBroker = new HashSet<>();
    AtomicInteger missingPartitions = new AtomicInteger(0);
    List<PartitionInfo> leaderPartitionsInNode = cluster.partitionsForNode(brokerId);
    if (leaderPartitionsInNode.isEmpty()) {
      // If the broker does not have any leader partition, return true immediately.
      return true;
    }
    leaderPartitionsInNode.forEach(info -> {
      String topic = info.topic();
      topicsInBroker.add(topic);
      if (!_topicsWithPartitionSizeReported.contains(topic)) {
        missingPartitions.incrementAndGet();
        missingTopics.add(topic);
      }
    });
    boolean result = ((double) missingTopics.size() / topicsInBroker.size()) <= MAX_ALLOWED_MISSING_TOPIC_METRIC_PERCENT
                     && ((double) missingPartitions.get() / cluster.partitionsForNode(brokerId).size() <= MAX_ALLOWED_MISSING_PARTITION_METRIC_PERCENT);
    if (!result) {
      LOG.warn("Broker {} is missing {}/{} topics metrics and {}/{} leader partition metrics. Missing leader topics: {}.", brokerId,
               missingTopics.size(), topicsInBroker.size(), missingPartitions.get(), cluster.partitionsForNode(brokerId).size(), missingTopics);
    }
    return result;
  }

  public double diskUsage() {
    double result = 0.0;
    for (RawMetricsHolder rawMetricsHolder : _partitionMetrics.values()) {
      result += rawMetricsHolder.metricValue(RawMetricType.PARTITION_SIZE).value();
    }
    return convertUnit(result, RawMetricType.PARTITION_SIZE);
  }
}
