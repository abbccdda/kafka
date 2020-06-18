/*
 Copyright 2020 Confluent Inc.
 */
package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.kafka.cruisecontrol.exception.MetricSamplingException;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;


/**
 * A mock metric sampler that provides fake samples of metric readings for partitions and brokers.
 * This is particularly useful for integration tests where we see issues with metrics being collected,
 * perhaps related to the fact that all brokers run in the same JVM and use the same JMX registry
 */
public class MockSampler implements MetricSampler {
  private int _exceptionsLeft;
  private Time time;

  // empty constructor is needed for initialization via config
  public MockSampler() {
    this(0, Time.SYSTEM);
  }

  public MockSampler(int numExceptions, Time time) {
    _exceptionsLeft = numExceptions;
    this.time = time;
  }

  /**
   * Record metric samples for all present partitions, for the COMMON metrics defined in
   * #{@link KafkaMetricDef#commonMetricDef()}
   */
  @Override
  public Samples getSamples(Cluster cluster,
                            Set<TopicPartition> assignedPartitions,
                            long startTime,
                            long endTime,
                            SamplingMode mode,
                            MetricDef metricDef,
                            long timeout) throws MetricSamplingException {

    if (_exceptionsLeft > 0) {
      _exceptionsLeft--;
      throw new MetricSamplingException("Error");
    }
    Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>(assignedPartitions.size());
    for (TopicPartition tp : assignedPartitions) {
      Node leader = cluster.partition(tp).leader();
      if (leader == null) {
        continue;
      }
      PartitionMetricSample sample = new PartitionMetricSample(leader.id(), tp);
      long now = time.milliseconds();

      for (MetricInfo metricInfo : KafkaMetricDef.commonMetricDef().all()) {
        sample.record(metricInfo, 1);
      }

      sample.close(now);
      partitionMetricSamples.add(sample);
    }

    return new Samples(partitionMetricSamples, Collections.emptySet());
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public void close() {

  }
}