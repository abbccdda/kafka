/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor;

import com.linkedin.cruisecontrol.CruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.cruisecontrol.exception.NotEnoughValidWindowsException;
import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.kafka.cruisecontrol.async.progress.OperationProgress;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityConfigFileResolver;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ModelParameters;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.NoopSampleStore;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaBrokerMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.aggregator.KafkaPartitionMetricSampleAggregator;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.task.LoadMonitorTaskRunner;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.yammer.metrics.core.MetricsRegistry;
import kafka.log.LogConfig;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static kafka.log.LogConfig.CleanupPolicyProp;


/**
 * Unit test for LoadMonitor
 */
public class LoadMonitorTest {
  private static final int P0 = 0;
  private static final int P1 = 1;
  private static final TopicPartition T0P0 = new TopicPartition(TOPIC0, P0);
  private static final TopicPartition T0P1 = new TopicPartition(TOPIC0, P1);
  private static final TopicPartition T1P0 = new TopicPartition(TOPIC1, P0);
  private static final TopicPartition T1P1 = new TopicPartition(TOPIC1, P1);
  private static final PartitionEntity PE_T0P0 = new PartitionEntity(T0P0);
  private static final PartitionEntity PE_T0P1 = new PartitionEntity(T0P1);
  private static final PartitionEntity PE_T1P0 = new PartitionEntity(T1P0);
  private static final PartitionEntity PE_T1P1 = new PartitionEntity(T1P1);
  private static final BrokerEntity NODE_0 = new BrokerEntity("localhost", 0);
  private static final BrokerEntity NODE_1 = new BrokerEntity("localhost", 1);
  private static final MetricDef COMMON_METRIC_DEF = KafkaMetricDef.commonMetricDef();
  private static final MetricDef BROKER_METRIC_DEF = KafkaMetricDef.brokerMetricDef();

  private static final int NUM_WINDOWS = 2;
  private static final int MIN_SAMPLES_PER_WINDOW = 4;
  private static final long WINDOW_MS = 1000;
  private static final String DEFAULT_CLEANUP_POLICY = LogConfig.Delete();

  private final Time _time = new MockTime(0);

  private MetricsRegistry metricsRegistry;

  @Before
  public void setUp() {
    metricsRegistry = new MetricsRegistry();
  }

  @After
  public void tearDown() {
    metricsRegistry.shutdown();
  }

  @Test
  public void testStateWithOnlyActiveSnapshotWindow() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // four samples for each partition
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P1, 0, WINDOW_MS, COMMON_METRIC_DEF);

    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    LoadMonitorState state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // The load monitor only has an active window. There is no stable window.
    assertEquals(0, state.numValidPartitions());
    assertEquals(0, state.numValidWindows());
    assertTrue(state.monitoredWindows().isEmpty());
  }

  @Test
  public void testStateWithoutEnoughSnapshotWindows() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // four samples for each partition except T1P1. T1P1 has no sample in the first window and one in the second window.
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);

    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    LoadMonitorState state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // The load monitor has 1 stable window with 0.5 of valid partitions ratio.
    assertEquals(0, state.numValidPartitions());
    assertEquals(0, state.numValidWindows());
    assertEquals(1, state.monitoredWindows().size());
    assertEquals(0.5, state.monitoredWindows().get(WINDOW_MS), 0.0);

    // Back fill for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, PE_T1P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // The load monitor now has one stable window with 1.0 of valid partitions ratio.
    assertEquals(0, state.numValidPartitions());
    assertEquals(1, state.numValidWindows());
    assertEquals(1, state.monitoredWindows().size());
    assertEquals(1.0, state.monitoredWindows().get(WINDOW_MS), 0.0);
  }

  @Test
  public void testStateWithInvalidSnapshotWindows() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    // populate the metrics aggregator.
    // four samples for each partition except T1P1. T1P1 has 2 samples in the first window, and 2 samples in the
    // active window.
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 2, aggregator, PE_T1P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 2, aggregator, PE_T1P1, 2, WINDOW_MS, COMMON_METRIC_DEF);

    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    LoadMonitorState state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // Both partitions for topic 0 should be valid.
    assertEquals(2, state.numValidPartitions());
    // Both topic should be valid in the first window.
    assertEquals(1, state.numValidWindows());
    // There should be 2 monitored windows.
    assertEquals(2, state.monitoredWindows().size());
    // Both topic should be valid in the first window.
    assertEquals(1.0, state.monitoredWindows().get(WINDOW_MS), 0.0);
    // Only topic 2 is valid in the second window.
    assertEquals(0.5, state.monitoredWindows().get(WINDOW_MS * 2), 0.0);

    // Back fill 3 samples for T1P1 in the second window.
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 3, aggregator, PE_T1P1, 1, WINDOW_MS, COMMON_METRIC_DEF);
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    state = loadMonitor.state(new OperationProgress(), clusterAndGeneration);
    // All the partitions should be valid now.
    assertEquals(4, state.numValidPartitions());
    // All the windows should be valid now.
    assertEquals(2, state.numValidWindows());
    // There should be two monitored windows.
    assertEquals(2, state.monitoredWindows().size());
    // Both monitored windows should have 100% completeness.
    assertEquals(1.0, state.monitoredWindows().get(WINDOW_MS), 0.0);
    assertEquals(1.0, state.monitoredWindows().get(WINDOW_MS * 2), 0.0);
  }

  @Test
  public void testMeetCompletenessRequirements() {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    // Require at least 1 valid window with 1.0 of valid partitions ratio.
    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    // Require at least 1 valid window with 0.5 of valid partitions ratio.
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    // Require at least 2 valid windows with 1.0 of valid partitions ratio.
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    // Require at least 2 valid windows with 0.5 of valid partitions ratio.
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // One stable window + one active window, enough samples for each partition except T1P1.
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    // The load monitor has one window with 0.5 valid partitions ratio.
    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements3));
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements4));

    // Add more samples, two stable windows + one active window. enough samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 2, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 2, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 2, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, PE_T1P1, 2, WINDOW_MS, COMMON_METRIC_DEF);
    // The load monitor has two windows, both with 0.5 valid partitions ratio
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements4));

    // Back fill the first stable window for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, PE_T1P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    // The load monitor has two windows with 1.0 and 0.5 of completeness respectively.
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements2));
    assertFalse(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements4));

    // Back fill all stable windows for T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 3, aggregator, PE_T1P1, 1, WINDOW_MS, COMMON_METRIC_DEF);
    // The load monitor has two windows both with 1.0 of completeness.
    clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements1));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements2));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements3));
    assertTrue(loadMonitor.meetCompletenessRequirements(clusterAndGeneration, requirements4));
  }

  // Test the case with enough snapshot windows and valid partitions.
  @Test
  public void testBasicClusterModel() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P1, 0, WINDOW_MS, COMMON_METRIC_DEF);

    ClusterModel clusterModel = loadMonitor.clusterModel(-1, Long.MAX_VALUE,
                                                         new ModelCompletenessRequirements(2, 1.0, false),
                                                         new OperationProgress());
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
  }

  // Test build cluster model for JBOD broker.
  @Test
  public void testJBODClusterModel() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext(NUM_WINDOWS, true);
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P1, 0, WINDOW_MS, COMMON_METRIC_DEF);

    ClusterModel clusterModel = loadMonitor.clusterModel(-1, Long.MAX_VALUE,
                                                         new ModelCompletenessRequirements(2, 1.0, false),
                                                         true,
                                                         new OperationProgress());

    assertEquals(4, clusterModel.broker(0).disk("/tmp/kafka-logs").replicas().size());
    assertEquals(3, clusterModel.broker(1).disk("/tmp/kafka-logs-1").replicas().size());
    assertEquals(1, clusterModel.broker(1).disk("/tmp/kafka-logs-2").replicas().size());
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
  }

  // Not enough snapshot windows and some partitions are missing from all snapshot windows.
  @Test
  public void testClusterModelWithInvalidPartitionAndInsufficientSnapshotWindows()
      throws NotEnoughValidWindowsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(2, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);

    try {
      loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    ClusterModel clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(1, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(3, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(1.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(3.0, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(3.0, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }
  }

  // Enough snapshot windows, some partitions are invalid in all snapshot windows.
  @Test
  public void testClusterWithInvalidPartitions() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // two samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);

    try {
      loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    ClusterModel clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
  }

  // Enough snapshot windows, some partitions are not available in some snapshot windows.
  @Test
  public void testClusterModelWithPartlyInvalidPartitions() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext();
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    ModelCompletenessRequirements requirements1 = new ModelCompletenessRequirements(1, 1.0, false);
    ModelCompletenessRequirements requirements2 = new ModelCompletenessRequirements(1, 0.5, false);
    ModelCompletenessRequirements requirements3 = new ModelCompletenessRequirements(2, 1.0, false);
    ModelCompletenessRequirements requirements4 = new ModelCompletenessRequirements(2, 0.5, false);

    // populate the metrics aggregator.
    // four samples for each partition except T1P1
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 1, aggregator, PE_T1P1, 1, WINDOW_MS, COMMON_METRIC_DEF);

    ClusterModel clusterModel =  loadMonitor.clusterModel(-1, Long.MAX_VALUE, requirements1, new OperationProgress());
    for (TopicPartition tp : Arrays.asList(T0P0, T0P1, T1P0, T1P1)) {
      assertNotNull(clusterModel.partition(tp));
    }
    assertEquals(1, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(11.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(23, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(23, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    assertEquals(10, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(10, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(20, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(20, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements2, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13.0, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13.0, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);

    try {
      loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements3, new OperationProgress());
      fail("Should have thrown NotEnoughValidWindowsException.");
    } catch (NotEnoughValidWindowsException nevwe) {
      // let it go
    }

    clusterModel = loadMonitor.clusterModel(-1L, Long.MAX_VALUE, requirements4, new OperationProgress());
    assertNull(clusterModel.partition(T1P0));
    assertNull(clusterModel.partition(T1P1));
    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(6.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(13, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
  }

  @Test
  public void testClusterModelWithInvalidSnapshotWindows() throws NotEnoughValidWindowsException {
    TestContext context = prepareContext(4, false);
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator aggregator = context.aggregator();

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P1, 0, WINDOW_MS, COMMON_METRIC_DEF);

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 3, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 3, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 3, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P1, 3, WINDOW_MS, COMMON_METRIC_DEF);

    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P0, 4, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T0P1, 4, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P0, 4, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(1, 4, aggregator, PE_T1P1, 4, WINDOW_MS, COMMON_METRIC_DEF);

    ClusterModel clusterModel = loadMonitor.clusterModel(-1, Long.MAX_VALUE,
        new ModelCompletenessRequirements(2, 0, false),
        new OperationProgress());

    assertEquals(2, clusterModel.partition(T0P0).leader().load().numWindows());
    assertEquals(16.5, clusterModel.partition(T0P0).leader().load().expectedUtilizationFor(Resource.CPU), 0.0);
    assertEquals(2, clusterModel.partition(T0P1).leader().load().numWindows());
    assertEquals(33, clusterModel.partition(T0P1).leader().load().expectedUtilizationFor(Resource.DISK), 0.0);
    assertEquals(2, clusterModel.partition(T1P0).leader().load().numWindows());
    assertEquals(33, clusterModel.partition(T1P0).leader().load().expectedUtilizationFor(Resource.NW_IN), 0.0);
    assertEquals(2, clusterModel.partition(T1P1).leader().load().numWindows());
    assertEquals(33, clusterModel.partition(T1P1).leader().load().expectedUtilizationFor(Resource.NW_OUT), 0.0);
  }

  @Test
  public void testComputeThrottle() throws Exception {
    TestContext context = prepareContext(4, false);
    LoadMonitor loadMonitor = context.loadmonitor();
    KafkaPartitionMetricSampleAggregator partitionSampleAggregator = context.aggregator();
    KafkaBrokerMetricSampleAggregator brokerSampleAggregator = loadMonitor.brokerSampleAggregator();

    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, partitionSampleAggregator, PE_T0P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, partitionSampleAggregator, PE_T0P1, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, partitionSampleAggregator, PE_T1P0, 0, WINDOW_MS, COMMON_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, partitionSampleAggregator, PE_T1P1, 0, WINDOW_MS, COMMON_METRIC_DEF);

    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, brokerSampleAggregator, NODE_0, 0, WINDOW_MS, BROKER_METRIC_DEF);
    CruiseControlUnitTestUtils.populateSampleAggregator(3, 4, brokerSampleAggregator, NODE_1, 0, WINDOW_MS, BROKER_METRIC_DEF);

    loadMonitor.clusterModel(Long.MAX_VALUE,
            new ModelCompletenessRequirements(2, 1.0, false),
            new OperationProgress());

    long throttle = loadMonitor.computeThrottle();
    // Ingress and egress are each 52 KB/s (from CruiseControlUnitTestutils.populateSampleAggregator)
    // Network in and out capacity are 156 MB/s (from prepareContext)
    // Based on these numbers, the calculated throttle is 65388544
    Assert.assertEquals(65388544, throttle);
  }

  private TestContext prepareContext() {
    return prepareContext(NUM_WINDOWS, false);
  }

  private TestContext prepareContext(int numWindowToPreserve, boolean isClusterJBOD) {
    // Create mock metadata client.
    Cluster cluster = getCluster(Arrays.asList(T0P0, T0P1, T1P0, T1P1));
    MetadataClient.ClusterAndPlacements clusterAndPlacements = new MetadataClient.ClusterAndPlacements(cluster, Collections.emptyMap());
    MetadataClient mockMetadataClient = EasyMock.mock(MetadataClient.class);
    EasyMock.expect(mockMetadataClient.cluster())
        .andReturn(getCluster(Collections.emptyList()))
        .once(); // mock stale metadata
    EasyMock.expect(mockMetadataClient.cluster())
            .andReturn(cluster)
            .anyTimes();
    EasyMock.expect(mockMetadataClient.clusterAndGeneration())
            .andReturn(new MetadataClient.ClusterAndGeneration(clusterAndPlacements, 0))
            .anyTimes();
    EasyMock.expect(mockMetadataClient.refreshMetadata())
            .andReturn(new MetadataClient.ClusterAndGeneration(clusterAndPlacements, 0))
            .anyTimes();
    EasyMock.expect(mockMetadataClient.refreshMetadata(EasyMock.anyInt()))
            .andReturn(new MetadataClient.ClusterAndGeneration(clusterAndPlacements, 0))
            .anyTimes();
    EasyMock.replay(mockMetadataClient);

    // Create mock admin client.
    ConfluentAdmin mockAdminClient = EasyMock.mock(ConfluentAdmin.class);
    EasyMock.expect(mockAdminClient.describeLogDirs(Arrays.asList(0, 1)))
            .andReturn(getDescribeLogDirsResult())
            .anyTimes();
    EasyMock.expect(mockAdminClient.describeLogDirs(Arrays.asList(1, 0)))
            .andReturn(getDescribeLogDirsResult())
            .anyTimes();
    EasyMock.replay(mockAdminClient);


    // Create load monitor.
    Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
    props.put(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, Integer.toString(numWindowToPreserve));
    props.put(KafkaCruiseControlConfig.MIN_SAMPLES_PER_PARTITION_METRICS_WINDOW_CONFIG,
              Integer.toString(MIN_SAMPLES_PER_WINDOW));
    props.put(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, Long.toString(WINDOW_MS));
    props.put(CleanupPolicyProp(), DEFAULT_CLEANUP_POLICY);
    props.put(KafkaCruiseControlConfig.SAMPLE_STORE_CLASS_CONFIG, NoopSampleStore.class.getName());
    props.put(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG, "false");
    props.put(KafkaCruiseControlConfig.MAX_VOLUME_THROUGHPUT_MB_CONFIG, "156");
    props.put(KafkaCruiseControlConfig.WRITE_THROUGHPUT_MULTIPLIER_CONFIG, "1.0");
    props.put(KafkaCruiseControlConfig.READ_THROUGHPUT_MULTIPLIER_CONFIG, "1.0");
    props.put(KafkaCruiseControlConfig.CALCULATED_THROTTLE_RATIO_CONFIG, "0.4");
    props.put("test.disk.capacity", Double.toString(1024 * 1024 * 4.0)); // 4 TB disk
    props.put("test.cpu.capacity", "100.0");
    props.put("test.nwin.capacity", Double.toString(1024 * 156.0)); // 156 MB/s network
    props.put("test.nwout.capacity", Double.toString(1024 * 156.0));
    props.put("estimation.info", "testInfo");
    if (isClusterJBOD) {
      // JBOD monitoring requires the ConfigFileResolver. Keep this for now.
      props.put(KafkaCruiseControlConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
              BrokerCapacityConfigFileResolver.class);
      String capacityConfigFileJBOD =
          KafkaCruiseControlUnitTestUtils.class.getClassLoader().getResource("testCapacityConfigJBOD.json").getFile();
      props.setProperty(BrokerCapacityConfigFileResolver.CAPACITY_CONFIG_FILE, capacityConfigFileJBOD);
    } else {
      props.put(KafkaCruiseControlConfig.BROKER_CAPACITY_CONFIG_RESOLVER_CLASS_CONFIG,
                "com.linkedin.kafka.cruisecontrol.common.TestBrokerCapacityConfigResolver");
    }
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);
    LoadMonitor loadMonitor = new LoadMonitor(config, mockMetadataClient, mockAdminClient, _time,
            KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry), COMMON_METRIC_DEF);

    KafkaPartitionMetricSampleAggregator aggregator = loadMonitor.partitionSampleAggregator();
    assertFalse(aggregator.isValidLeader(new PartitionMetricSample(0, T0P0))); // stale metadata
    assertTrue(aggregator.isValidLeader(new PartitionMetricSample(0, T0P0)));

    ModelParameters.init(config);
    loadMonitor.startUp();
    MetadataClient.ClusterAndGeneration clusterAndGeneration = loadMonitor.refreshClusterAndGeneration();
    while (loadMonitor.state(new OperationProgress(), clusterAndGeneration).state() != LoadMonitorTaskRunner.LoadMonitorTaskRunnerState.RUNNING) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        // let it go.
      }
    }

    return new TestContext(loadMonitor, aggregator, config, cluster);
  }

  private Cluster getCluster(Collection<TopicPartition> partitions) {
    Node node0 = new Node(0, "localhost", 100, "rack0");
    Node node1 = new Node(1, "localhost", 100, "rack1");
    Node[] nodes = {node0, node1};
    Set<Node> allNodes = new HashSet<>(2);
    allNodes.add(node0);
    allNodes.add(node1);
    Set<PartitionInfo> parts = new HashSet<>(partitions.size());
    for (TopicPartition tp : partitions) {
      parts.add(new PartitionInfo(tp.topic(), tp.partition(), node0, nodes, nodes));
    }
    return new Cluster("cluster-id", allNodes, parts, Collections.emptySet(), Collections.emptySet());
  }

  private DescribeLogDirsResult getDescribeLogDirsResult() {
    try {
      // Reflectively set DescribeLogDirsResult's constructor from package private to public.
      Constructor<DescribeLogDirsResult> constructor = DescribeLogDirsResult.class.getDeclaredConstructor(Map.class);
      constructor.setAccessible(true);

      Map<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> futureByBroker = new HashMap<>();
      Map<String, DescribeLogDirsResponse.LogDirInfo> logdirInfoBylogdir =  new HashMap<>();
      Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfoByPartition = new HashMap<>();
      replicaInfoByPartition.put(T0P0, new DescribeLogDirsResponse.ReplicaInfo(0, 0, false));
      replicaInfoByPartition.put(T0P1, new DescribeLogDirsResponse.ReplicaInfo(0, 0, false));
      replicaInfoByPartition.put(T1P0, new DescribeLogDirsResponse.ReplicaInfo(0, 0, false));
      replicaInfoByPartition.put(T1P1, new DescribeLogDirsResponse.ReplicaInfo(0, 0, false));
      logdirInfoBylogdir.put("/tmp/kafka-logs", new DescribeLogDirsResponse.LogDirInfo(Errors.NONE, replicaInfoByPartition));
      futureByBroker.put(0, completedFuture(logdirInfoBylogdir));

      logdirInfoBylogdir =  new HashMap<>();
      replicaInfoByPartition = new HashMap<>();
      replicaInfoByPartition.put(T0P0, new DescribeLogDirsResponse.ReplicaInfo(0, 0, false));
      replicaInfoByPartition.put(T0P1, new DescribeLogDirsResponse.ReplicaInfo(0, 0, false));
      replicaInfoByPartition.put(T1P0, new DescribeLogDirsResponse.ReplicaInfo(0, 0, false));
      logdirInfoBylogdir.put("/tmp/kafka-logs-1", new DescribeLogDirsResponse.LogDirInfo(Errors.NONE, replicaInfoByPartition));
      logdirInfoBylogdir.put("/tmp/kafka-logs-2",
                             new DescribeLogDirsResponse.LogDirInfo(Errors.NONE,
                                                                    Collections.singletonMap(T1P1, new DescribeLogDirsResponse.ReplicaInfo(0, 0, false))));
      futureByBroker.put(1, completedFuture(logdirInfoBylogdir));
      return constructor.newInstance(futureByBroker);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      // Let it go.
    }
    return null;
  }

  private static class TestContext {
    private final LoadMonitor _loadMonitor;
    private final KafkaPartitionMetricSampleAggregator _aggregator;
    private final KafkaCruiseControlConfig _config;
    private final Cluster _cluster;

    private TestContext(LoadMonitor loadMonitor,
                        KafkaPartitionMetricSampleAggregator aggregator,
                        KafkaCruiseControlConfig config,
                        Cluster cluster) {
      _loadMonitor = loadMonitor;
      _aggregator = aggregator;
      _config = config;
      _cluster = cluster;
    }

    private LoadMonitor loadmonitor() {
      return _loadMonitor;
    }

    private KafkaPartitionMetricSampleAggregator aggregator() {
      return _aggregator;
    }

    private KafkaCruiseControlConfig config() {
      return _config;
    }

    private Cluster cluster() {
      return _cluster;
    }
  }
}
