/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.executor.Executor;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.Properties;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.utils.SystemTime;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class GoalOptimizerTest {
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
  public void testNoPreComputingThread() {
    Properties props = new Properties();
    props.setProperty(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap.servers");
    props.setProperty(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG, "connect:1234");
    props.setProperty(KafkaCruiseControlConfig.NUM_PROPOSAL_PRECOMPUTE_THREADS_CONFIG, "0");
    KafkaCruiseControlConfig config = new KafkaCruiseControlConfig(props);

    GoalOptimizer goalOptimizer = new GoalOptimizer(config, EasyMock.mock(LoadMonitor.class), new SystemTime(),
            KafkaCruiseControlUnitTestUtils.getMetricsRegistry(metricsRegistry), EasyMock.mock(Executor.class));
    // Should exit immediately.
    goalOptimizer.run();
  }
}
