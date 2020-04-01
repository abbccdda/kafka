/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.easymock.EasyMock;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * A test util class.
 */
public class KafkaCruiseControlUnitTestUtils {

  private KafkaCruiseControlUnitTestUtils() {

  }

  public static void mockDescribeTopics(AdminClient mockAdminClient, Collection<String> expectedTopicsToDescribe,
                                 Map<String, TopicDescription> topicDescriptions, long expectedTimeoutMs)
      throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
    DescribeTopicsResult mockDescribeTopicsResult = EasyMock.mock(DescribeTopicsResult.class);
    KafkaFuture<Map<String, TopicDescription>> mockKafkaFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockKafkaFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(topicDescriptions).times(1, 2);
    EasyMock.expect(mockDescribeTopicsResult.all()).andReturn(mockKafkaFuture).times(1, 2);

    EasyMock.expect(mockAdminClient.describeTopics(expectedTopicsToDescribe)).andReturn(mockDescribeTopicsResult);
    // cannot mock properly due to 1. EasyMock requiring both matchers to be concise at once
    // and 2. DescribeTopicsOptions being compared by reference, resulting in an invalid expectation
    EasyMock.expect(mockAdminClient
        .describeTopics(EasyMock.<Collection<String>>anyObject(),
            EasyMock.anyObject(DescribeTopicsOptions.class)))
        .andReturn(mockDescribeTopicsResult);
    EasyMock.expect(mockAdminClient
        .describeTopics(EasyMock.<Collection<String>>anyObject()))
        .andReturn(mockDescribeTopicsResult);
    EasyMock.replay(mockDescribeTopicsResult, mockKafkaFuture);
  }

  public static Properties getKafkaCruiseControlProperties() {
    Properties props = new Properties();
    String capacityConfigFile =
        KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource("DefaultCapacityConfig.json").getFile();
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2121");
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "aaa");
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, NoopSampler.class.getName());
    props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFile);
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_BROKER_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(KafkaCruiseControlConfig.COMPLETED_USER_TASK_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(6)));
    props.setProperty(KafkaCruiseControlConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(24)));
    props.setProperty(KafkaCruiseControlConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(12)));
    props.setProperty(KafkaCruiseControlConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG, "2.0");
    return props;
  }

  /**
   * Get the aggregated metric values with the given resource usage.
   */
  public static AggregatedMetricValues getAggregatedMetricValues(double cpuUsage,
                                                                 double networkInBoundUsage,
                                                                 double networkOutBoundUsage,
                                                                 double diskUsage) {
    AggregatedMetricValues aggregateMetricValues = new AggregatedMetricValues();
    setValueForResource(aggregateMetricValues, Resource.CPU, cpuUsage);
    setValueForResource(aggregateMetricValues, Resource.NW_IN, networkInBoundUsage);
    setValueForResource(aggregateMetricValues, Resource.NW_OUT, networkOutBoundUsage);
    setValueForResource(aggregateMetricValues, Resource.DISK, diskUsage);
    return aggregateMetricValues;
  }

  /**
   * Set the utilization values of all metrics for a resource in the given AggregatedMetricValues.
   * The first metric has the full resource utilization value, all the rest of the metrics has 0.
   */
  public static void setValueForResource(AggregatedMetricValues aggregatedMetricValues,
                                         Resource resource,
                                         double value) {
    boolean set = false;
    for (short id : KafkaMetricDef.resourceToMetricIds(resource)) {
      MetricValues metricValues = new MetricValues(1);
      if (!set) {
        metricValues.set(0, value);
        set = true;
      }
      aggregatedMetricValues.add(id, metricValues);
    }
  }
}
