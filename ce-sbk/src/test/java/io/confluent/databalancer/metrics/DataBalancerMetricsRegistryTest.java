/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer.metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DataBalancerMetricsRegistryTest {
    private DataBalancerMetricsRegistry dataBalancerMetricsRegistry;
    private MetricsRegistry metricsRegistry;
    private String whitelistMetricName = "whitelistGauge";

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistry();
        DataBalancerMetricsRegistry.MetricsWhitelistBuilder builder = new DataBalancerMetricsRegistry.MetricsWhitelistBuilder();
        builder.addMetric(this.getClass(), whitelistMetricName);
        dataBalancerMetricsRegistry = new DataBalancerMetricsRegistry(metricsRegistry, builder.buildWhitelist());
    }

    @Test
    public void testAddDuplicateMetricNameThrowsException() {
        Metric testGauge = dataBalancerMetricsRegistry.newGauge(this.getClass(), "test", () -> true);
        assertThrows("Expected adding duplicate metric name to throw exception", IllegalStateException.class,
                () -> dataBalancerMetricsRegistry.newGauge(this.getClass(), "test", () -> true));
        assertThrows("Expected adding duplicate metric name to throw exception", IllegalStateException.class,
                () -> dataBalancerMetricsRegistry.newMeter(this.getClass(), "test", "", TimeUnit.SECONDS));
        assertThrows("Expected adding duplicate metric name to throw exception", IllegalStateException.class,
                () -> dataBalancerMetricsRegistry.newTimer(this.getClass(), "test"));

        Map<MetricName, Metric> addedMetrics = metricsRegistry.allMetrics();
        assertEquals("Expected only one metric to be added to registry", 1, addedMetrics.size());
        assertTrue("Expected gauge to be present in registry", addedMetrics.containsValue(testGauge));
    }

    @Test
    public void testCruiseControlMetricsCleared() {
        Set<Metric> expectedMetrics = new HashSet<>();
        Metric notCCGauge = dataBalancerMetricsRegistry.newGauge(this.getClass(), whitelistMetricName, () -> true, false);

        expectedMetrics.add(dataBalancerMetricsRegistry.newGauge(this.getClass(), "ccGauge", () -> true));
        expectedMetrics.add(notCCGauge);
        expectedMetrics.add(dataBalancerMetricsRegistry.newMeter(this.getClass(), "testMeter", "", TimeUnit.SECONDS));
        expectedMetrics.add(dataBalancerMetricsRegistry.newTimer(this.getClass(), "testTimer"));

        Map<MetricName, Metric> registeredMetrics = metricsRegistry.allMetrics();
        assertEquals("Expected all metrics to be added to registry", expectedMetrics.size(), registeredMetrics.size());
        assertTrue("Expected all metrics to be present in registry", registeredMetrics.values().containsAll(expectedMetrics));

        dataBalancerMetricsRegistry.clearShortLivedMetrics();
        expectedMetrics = new HashSet<>();
        expectedMetrics.add(notCCGauge);
        registeredMetrics = metricsRegistry.allMetrics();

        assertEquals("Expected only one metric in registry after clearing", expectedMetrics.size(), registeredMetrics.size());
        assertTrue("Expected non-transient metric to be present after clearing", registeredMetrics.values().containsAll(expectedMetrics));
        assertThrows("Expected adding duplicate metric name to throw exception", IllegalStateException.class,
                () -> dataBalancerMetricsRegistry.newGauge(this.getClass(), whitelistMetricName, () -> true, false));

        expectedMetrics.add(dataBalancerMetricsRegistry.newGauge(this.getClass(), "ccGauge", () -> true));
        expectedMetrics.add(dataBalancerMetricsRegistry.newMeter(this.getClass(), "testMeter", "", TimeUnit.SECONDS));
        expectedMetrics.add(dataBalancerMetricsRegistry.newTimer(this.getClass(), "testTimer"));
        registeredMetrics = metricsRegistry.allMetrics();

        assertEquals("Expected all metrics to be added to registry", expectedMetrics.size(), registeredMetrics.size());
        assertTrue("Expected all metrics to be present in registry", registeredMetrics.values().containsAll(expectedMetrics));
    }

    @Test
    public void testDataBalancerMetricsWhitelist() {
        assertThrows("Expected adding metric with non-whitelisted name to throw exception", IllegalStateException.class,
                () -> dataBalancerMetricsRegistry.newGauge(this.getClass(), "wrongName", () -> true, false));
        assertThrows("Expected adding metric with non-whitelisted class to throw exception", IllegalStateException.class,
                () -> dataBalancerMetricsRegistry.newGauge(DataBalancerMetricsRegistry.class, whitelistMetricName, () -> true, false));
        Metric gauge = dataBalancerMetricsRegistry.newGauge(this.getClass(), whitelistMetricName, () -> true, false);
        Map<MetricName, Metric> registeredMetrics = metricsRegistry.allMetrics();
        assertEquals("Expected only one metric to be present in registry", 1, registeredMetrics.size());
        assertTrue("Expected whitelisted metric to be added to registry", registeredMetrics.values().contains(gauge));
    }
}
