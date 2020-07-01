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
import com.yammer.metrics.core.MetricsRegistry;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.easymock.EasyMock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * A test util class.
 */
public class KafkaCruiseControlUnitTestUtils {

  private KafkaCruiseControlUnitTestUtils() {

  }

  public static void mockDescribeCluster(Admin mockAdminClient, String clusterId,
                                         Node controller, Collection<Node> nodes,
                                         long expectedTimeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
    DescribeClusterResult mockDescribeClusterResult = EasyMock.mock(DescribeClusterResult.class);

    KafkaFuture<String> mockClusterIdFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockClusterIdFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(clusterId);
    EasyMock.expect(mockDescribeClusterResult.clusterId()).andReturn(mockClusterIdFuture);

    KafkaFuture<Node> mockControllerFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockControllerFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(controller);
    EasyMock.expect(mockDescribeClusterResult.controller()).andReturn(mockControllerFuture);

    KafkaFuture<Collection<Node>> mockNodesFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockNodesFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(nodes);
    EasyMock.expect(mockDescribeClusterResult.nodes()).andReturn(mockNodesFuture);

    KafkaFuture<Set<AclOperation>> mockAuthorizedOperations = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockAuthorizedOperations.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andReturn(Collections.emptySet());
    EasyMock.expect(mockDescribeClusterResult.authorizedOperations()).andReturn(mockAuthorizedOperations);

    EasyMock.expect(mockAdminClient.describeCluster()).andReturn(mockDescribeClusterResult);
    EasyMock.expect(mockAdminClient.describeCluster(EasyMock.anyObject())).andReturn(mockDescribeClusterResult);

    EasyMock.replay(mockDescribeClusterResult, mockClusterIdFuture,
        mockControllerFuture, mockNodesFuture);
  }

  public static void mockDescribeClusterThrows(Admin mockAdminClient, ExecutionException exception,
                                               long expectedTimeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
    DescribeClusterResult mockDescribeClusterResult = EasyMock.mock(DescribeClusterResult.class);

    KafkaFuture<String> mockClusterIdFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockClusterIdFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andThrow(exception);
    EasyMock.expect(mockDescribeClusterResult.clusterId()).andReturn(mockClusterIdFuture);

    KafkaFuture<Node> mockControllerFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockControllerFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andThrow(exception);
    EasyMock.expect(mockDescribeClusterResult.controller()).andReturn(mockControllerFuture);

    KafkaFuture<Collection<Node>> mockNodesFuture = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockNodesFuture.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andThrow(exception);
    EasyMock.expect(mockDescribeClusterResult.nodes()).andReturn(mockNodesFuture);

    KafkaFuture<Set<AclOperation>> mockAuthorizedOperations = EasyMock.mock(KafkaFuture.class);
    EasyMock.expect(mockAuthorizedOperations.get(expectedTimeoutMs, TimeUnit.MILLISECONDS))
        .andThrow(exception);
    EasyMock.expect(mockDescribeClusterResult.authorizedOperations()).andReturn(mockAuthorizedOperations);

    EasyMock.expect(mockAdminClient.describeCluster()).andReturn(mockDescribeClusterResult);
    EasyMock.expect(mockAdminClient.describeCluster(EasyMock.anyObject())).andReturn(mockDescribeClusterResult);

    EasyMock.replay(mockDescribeClusterResult, mockClusterIdFuture,
        mockControllerFuture, mockNodesFuture);
  }

  public static void mockDescribeTopics(ConfluentAdmin mockAdminClient, Collection<String> expectedTopicsToDescribe,
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

  /**
   * Mock describing broker configs
   */
  public static void mockDescribeConfigs(ConfluentAdmin mockAdminClient, Collection<ConfigResource> expectedResourcesToDescribe,
                                         Map<String, List<ConfigEntry>> entries)
      throws InterruptedException, ExecutionException {
    DescribeConfigsResult mockDescribeConfigsResult = EasyMock.mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> mockKafkaFuture = EasyMock.mock(KafkaFuture.class);

    Map<ConfigResource, Config> returnConfig = new HashMap<>();
    Map<ConfigResource, KafkaFuture<Config>> futureMap = new HashMap<>();
    for (Map.Entry<String, List<ConfigEntry>> entry : entries.entrySet()) {
      KafkaFuture<Config> mockFuture = EasyMock.mock(KafkaFuture.class);
      ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, entry.getKey());
      Config config = new Config(entry.getValue());
      returnConfig.put(resource, config);
      futureMap.put(resource, mockFuture);
      EasyMock.expect(mockFuture.get()).andReturn(config).anyTimes();
      EasyMock.replay(mockFuture);
    }
    EasyMock.expect(mockKafkaFuture.get())
        .andReturn(returnConfig).times(0, 3);
    EasyMock.expect(mockDescribeConfigsResult.all()).andReturn(mockKafkaFuture).anyTimes();
    EasyMock.expect(mockDescribeConfigsResult.values()).andReturn(futureMap).anyTimes();

    EasyMock.expect(mockAdminClient.describeConfigs(expectedResourcesToDescribe)).andReturn(mockDescribeConfigsResult).times(1, 3);
    EasyMock.replay(mockDescribeConfigsResult, mockKafkaFuture);
  }

  public static Collection<ConfigResource> configResourcesForBrokers(List<Integer> brokers) {
    return brokers.stream()
        .map(brokerId -> new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString()))
        .collect(Collectors.toList());
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
    props.setProperty(KafkaCruiseControlConfig.DEMOTION_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(24)));
    props.setProperty(KafkaCruiseControlConfig.REMOVAL_HISTORY_RETENTION_TIME_MS_CONFIG, Long.toString(TimeUnit.HOURS.toMillis(12)));
    props.setProperty(KafkaCruiseControlConfig.GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG, "2.0");
    props.setProperty(KafkaCruiseControlConfig.REPLICATION_THROTTLE_CONFIG, KafkaCruiseControlConfig.DISABLED_THROTTLE.toString());

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

  public static DataBalancerMetricsRegistry getMetricsRegistry(MetricsRegistry metricsRegistry) {
    // CruiseControl shouldn't be making any long-lived metrics
    return new DataBalancerMetricsRegistry(metricsRegistry, Collections.emptySet());
  }
}
