// Copyright 2019 Confluent Inc.
package io.confluent.metrics.reporter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class ConfluentMetricsReporterTest {

  private final AtomicInteger runnableInstanceCount = new AtomicInteger();
  private final AtomicInteger runnableInvocationCount = new AtomicInteger();
  private ConfluentMetricsReporter metricsReporter;

  @Before
  public void setUp() {
    metricsReporter = new ConfluentMetricsReporter() {
      @Override
      protected Runnable metricReportRunnable() {
        runnableInstanceCount.incrementAndGet();
        return runnableInvocationCount::incrementAndGet;
      }
    };

    Map<String, Object> props = new HashMap<>();
    props.put(ConfluentMetricsReporterConfig.METRICS_REPORTER_PREFIX
        + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    props.put(ConfluentMetricsReporterConfig.PUBLISH_PERIOD_CONFIG, "1");
    metricsReporter.configure(props);
  }

  @After
  public void tearDown() {
    if (metricsReporter != null)
      metricsReporter.close();
  }

  @Test
  public void testClusterIdUpdate() throws Exception {
    assertEquals(0, runnableInstanceCount.get());
    assertEquals(0, runnableInvocationCount.get());
    assertNull(metricsReporter.clusterId());

    for (int i = 0; i < 2; i++) {
      verifyClusterIdUpdate("clusterA", "clusterA");
    }
    verifyClusterIdUpdate("clusterB", "clusterB");
  }

  @Test
  public void testNullClusterIdUpdate() throws Exception {
    for (int i = 0; i < 2; i++) {
      verifyClusterIdUpdate(null, null);
    }
    verifyClusterIdUpdate("clusterA", "clusterA");
    verifyClusterIdUpdate(null, "clusterA");
    verifyClusterIdUpdate("clusterB", "clusterB");
  }

  private void verifyClusterIdUpdate(String newClusterId, String expectedClusterId) throws Exception {
    metricsReporter.onUpdate(new ClusterResource(newClusterId));
    assertEquals(expectedClusterId, metricsReporter.clusterId());
    assertEquals(1, runnableInstanceCount.get());
    waitForNextInvocation();
  }

  private void waitForNextInvocation() throws Exception {
    int count = runnableInvocationCount.get();
    TestUtils.waitForCondition(() -> runnableInvocationCount.get() > count, "Runnable not invoked");
  }
}
