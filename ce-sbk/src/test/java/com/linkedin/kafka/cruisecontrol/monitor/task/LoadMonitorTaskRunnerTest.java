/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.task;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaIntegrationTestHarness;
import com.linkedin.kafka.cruisecontrol.monitor.MockSampler;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricFetcherManager;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.MetricSampler;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.yammer.metrics.core.MetricsRegistry;
import io.confluent.databalancer.metrics.DataBalancerMetricsRegistry;
import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import scala.Option$;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The unit test for metric fetcher manager.
 */
public class LoadMonitorTaskRunnerTest extends CCKafkaIntegrationTestHarness {
  private static final long WINDOW_MS = 10000L;
  private static final int NUM_WINDOWS = 5;
  private static final int NUM_TOPICS = 100;
  private static final int NUM_PARTITIONS = 4;
  private static final String TEST_TOPIC_NAME = "LoadMonitorTaskRunnerTest";
  private static final long SAMPLING_INTERVAL = KafkaCruiseControlConfig.DEFAULT_METRIC_SAMPLING_INTERVAL_MS;
  private static final MetricDef METRIC_DEF = KafkaMetricDef.commonMetricDef();
  // Using autoTick = 1
  private static final Time TIME = new MockTime(1L);

  @Rule
  final public Timeout globalTimeout = Timeout.millis(120000);

  private MetricsRegistry metricsRegistry;
  @Before
  public void setUp() {
    super.setUp();
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(zookeeper().connectionString(),
                                                                              "LoadMonitorTaskRunnerGroup",
                                                                              "LoadMonitorTaskRunnerSetup",
                                                                              false);
    AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
    for (int i = 0; i < NUM_TOPICS; i++) {
      adminZkClient.createTopic(TEST_TOPIC_NAME + "-" + i, NUM_PARTITIONS, 1, new Properties(), RackAwareMode.Safe$.MODULE$, false, Option$.MODULE$.empty());
    }
    KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
    metricsRegistry = new MetricsRegistry();
  }

  @After
  public void tearDown() {
    super.tearDown();
    metricsRegistry.shutdown();
  }

  @Override
  public int clusterSize() {
    return 1;
  }

  @Test
  public void testSimpleFetch() throws InterruptedException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    MetadataClient metadataClient = new MetadataClient(config, -1L, TIME);
    MockPartitionMetricSampleAggregator mockPartitionMetricSampleAggregator =
        new MockPartitionMetricSampleAggregator(config, metadataClient);
    KafkaBrokerMetricSampleAggregator mockBrokerMetricSampleAggregator =
        EasyMock.mock(KafkaBrokerMetricSampleAggregator.class);
    DataBalancerMetricsRegistry metricRegistry = KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry);
    MetricSampler sampler = new MockSampler(0, TIME);
    MetricFetcherManager fetcherManager =
        new MetricFetcherManager(config, mockPartitionMetricSampleAggregator, mockBrokerMetricSampleAggregator,
                                 metadataClient, METRIC_DEF, TIME, metricRegistry, null, sampler);
    LoadMonitorTaskRunner loadMonitorTaskRunner =
        new LoadMonitorTaskRunner(config, fetcherManager, mockPartitionMetricSampleAggregator,
                                  mockBrokerMetricSampleAggregator, metadataClient, TIME);

    awaitTopicCreation(metadataClient);

    loadMonitorTaskRunner.start(true);

    Set<TopicPartition> partitionsToSample = new HashSet<>(NUM_TOPICS * NUM_PARTITIONS);
    for (int i = 0; i < NUM_TOPICS; i++) {
      for (int j = 0; j < NUM_PARTITIONS; j++) {
        partitionsToSample.add(new TopicPartition(TEST_TOPIC_NAME + "-" + i, j));
      }
    }

    long startMs = System.currentTimeMillis();
    BlockingQueue<PartitionMetricSample> sampleQueue = mockPartitionMetricSampleAggregator.metricSampleQueue();
    while (!partitionsToSample.isEmpty() && System.currentTimeMillis() < startMs + 10000) {
      PartitionMetricSample sample = sampleQueue.poll();
      if (sample != null) {
        TopicPartition tp = sample.entity().tp();
        if (tp.topic().contains(TEST_TOPIC_NAME)) {
          assertTrue(String.format("The topic partition %s should have been sampled and sampled only once.", tp),
              partitionsToSample.contains(tp));
          partitionsToSample.remove(tp);
        }
      }
    }
    assertTrue("Did not see sample for partitions " + Arrays.toString(partitionsToSample.toArray()),
        partitionsToSample.isEmpty());
    fetcherManager.shutdown();
    assertTrue(sampleQueue.isEmpty());
    loadMonitorTaskRunner.shutdown();
    metadataClient.close();
  }

  @Test
  public void testSamplingError() throws InterruptedException {
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(getLoadMonitorProperties());
    MetadataClient metadataClient = new MetadataClient(config, -1L, TIME);
    MockPartitionMetricSampleAggregator mockMetricSampleAggregator =
        new MockPartitionMetricSampleAggregator(config, metadataClient);
    KafkaBrokerMetricSampleAggregator mockBrokerMetricSampleAggregator =
        EasyMock.mock(KafkaBrokerMetricSampleAggregator.class);
    DataBalancerMetricsRegistry metricRegistry = KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry);
    MetricSampler sampler = new MockSampler(0, TIME);
    MetricFetcherManager fetcherManager =
        new MetricFetcherManager(config, mockMetricSampleAggregator, mockBrokerMetricSampleAggregator, metadataClient,
                                 METRIC_DEF, TIME, metricRegistry, null, sampler);
    LoadMonitorTaskRunner loadMonitorTaskRunner =
        new LoadMonitorTaskRunner(config, fetcherManager, mockMetricSampleAggregator, mockBrokerMetricSampleAggregator,
                                  metadataClient, TIME);

    awaitTopicCreation(metadataClient);

    loadMonitorTaskRunner.start(true);

    int numSamples = 0;
    long startMs = System.currentTimeMillis();
    BlockingQueue<PartitionMetricSample> sampleQueue = mockMetricSampleAggregator.metricSampleQueue();
    while (numSamples < (NUM_PARTITIONS * NUM_TOPICS) * 10 && System.currentTimeMillis() < startMs + 10000) {
      PartitionMetricSample sample = sampleQueue.poll();
      if (sample != null && sample.entity().tp().topic().contains(TEST_TOPIC_NAME)) {
        numSamples++;
      }
    }
    int expectedNumSamples = NUM_TOPICS * NUM_PARTITIONS;
    assertEquals("Only see " + numSamples + " samples. Expecting " + expectedNumSamples + " samples",
        expectedNumSamples, numSamples);
    fetcherManager.shutdown();
    loadMonitorTaskRunner.shutdown();
    metadataClient.close();
  }

  private void awaitTopicCreation(MetadataClient metadataClient) throws InterruptedException {
    while (NUM_TOPICS != metadataClient.cluster().topics().stream()
        .filter(tpName -> tpName.contains(TEST_TOPIC_NAME)).count()) {
      Thread.sleep(10);
      metadataClient.refreshMetadata();
    }
  }


  private Properties getLoadMonitorProperties() {
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    props.setProperty(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, Long.toString(WINDOW_MS));
    props.setProperty(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, Integer.toString(NUM_WINDOWS));
    // The configuration does not matter here, we pass in the fetcher explicitly.
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLER_CLASS_CONFIG, MockSampler.class.getName());
    props.setProperty(KafkaCruiseControlConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG, "2");
    props.setProperty(KafkaCruiseControlConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG, Long.toString(SAMPLING_INTERVAL));
    props.setProperty(KafkaCruiseControlConfig.SAMPLE_STORE_CLASS_CONFIG, NoopSampleStore.class.getName());
    return props;
  }

  private static class MockPartitionMetricSampleAggregator extends KafkaPartitionMetricSampleAggregator {
    private final BlockingQueue<PartitionMetricSample> _partitionMetricSamples;
    /**
     * Construct the metric sample aggregator.
     * @param config   The load monitor configurations.
     * @param metadataClient The client to fetch metadata of the cluster.
     */
    MockPartitionMetricSampleAggregator(KafkaCruiseControlConfig config, MetadataClient metadataClient) {
      super(config, metadataClient);
      _partitionMetricSamples = new ArrayBlockingQueue<>(10000);
    }

    @Override
    public synchronized boolean addSample(PartitionMetricSample sample) {
      _partitionMetricSamples.add(sample);
      return true;
    }

    @Override
    public synchronized boolean addSample(PartitionMetricSample sample, boolean skipLeaderCheck) {
      _partitionMetricSamples.add(sample);
      return true;
    }

    public BlockingQueue<PartitionMetricSample> metricSampleQueue() {
      return _partitionMetricSamples;
    }


  }
}
