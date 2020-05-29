/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer.metrics;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.confluent.databalancer.metrics.DataBalancerMetricsRegistry.metricName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DataBalancerMetricsRegistryTest {
    private DataBalancerMetricsRegistry dataBalancerMetricsRegistry;
    private MetricsRegistry metricsRegistry;
    private String whitelistMetricName = "whitelistGauge";
    private String whitelistMetricName2 = "secondWhiteListGauge";

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistry();
        DataBalancerMetricsRegistry.MetricsWhitelistBuilder builder = new DataBalancerMetricsRegistry.MetricsWhitelistBuilder();
        builder.addMetric(this.getClass(), whitelistMetricName);
        builder.addMetric(this.getClass(), whitelistMetricName2);
        dataBalancerMetricsRegistry = new DataBalancerMetricsRegistry(metricsRegistry, builder.buildWhitelist());
    }

    boolean isShortLivedMetricRegistered(DataBalancerMetricsRegistry dmr, String metricName) {
        MetricName mName = metricName(DataBalancerMetricsRegistry.GROUP, this.getClass().getSimpleName(), metricName);
        return dmr.shortLivedMetrics.contains(mName);
    }

    boolean isLongLivedMetricRegistered(DataBalancerMetricsRegistry dmr, String metricName) {
        MetricName mName = metricName(DataBalancerMetricsRegistry.GROUP, this.getClass().getSimpleName(), metricName);
        return dmr.longLivedMetrics.contains(mName);
    }

    boolean isMetricRegistered(DataBalancerMetricsRegistry dmr, String metricName) {
        return isLongLivedMetricRegistered(dmr, metricName) ||
                isShortLivedMetricRegistered(dmr, metricName);
    }

    @Test
    public void testAddDuplicateMetricName() {
        // Duplicate metric names are expected to be ignored, but divergent types still fail.
        // Initial gauge
        final String testGaugeName = "test";
        Metric testGauge = dataBalancerMetricsRegistry.newGauge(this.getClass(), testGaugeName, () -> true);
        // Add a duplicate gauge
        dataBalancerMetricsRegistry.newGauge(this.getClass(), testGaugeName, () -> true);

        Map<MetricName, Metric> addedMetrics = metricsRegistry.allMetrics();
        assertEquals("Expected only one metric to be added to registry", 1, addedMetrics.size());
        assertTrue("expected gauge to be registered", isMetricRegistered(dataBalancerMetricsRegistry, testGaugeName));
        assertTrue("expected short-lived gauge to be registered", isShortLivedMetricRegistered(dataBalancerMetricsRegistry, testGaugeName));
        assertFalse("expected long-lived gauge to not be registered", isLongLivedMetricRegistered(dataBalancerMetricsRegistry, testGaugeName));

        assertTrue("Expected gauge to be present in registry", addedMetrics.containsValue(testGauge));
    }

    @Test
    public void testAddDuplicateShortAndLongLivedMetrics() {
        // Test adding gauges with the same name but different lifespans; this should be an Exceptional error
        // and should not affect the metrics registry
        final String shortLivedGaugeName = whitelistMetricName;
        Metric testShortLivedGauge = dataBalancerMetricsRegistry.newGauge(this.getClass(), shortLivedGaugeName, () -> true, true);
        // Add a long-lived duplicate
        assertThrows("Expected a long-lived metric with same name as short-lived to throw", IllegalStateException.class,
                () -> dataBalancerMetricsRegistry.newGauge(this.getClass(), shortLivedGaugeName, () -> true, false));

        Map<MetricName, Metric> addedMetrics = metricsRegistry.allMetrics();
        assertEquals("Expected only one metric to be in registry", 1, addedMetrics.size());
        assertTrue("expected gauge to be registered", isMetricRegistered(dataBalancerMetricsRegistry, shortLivedGaugeName));
        assertTrue("expected short-lived gauge to be registered", isShortLivedMetricRegistered(dataBalancerMetricsRegistry, shortLivedGaugeName));
        assertFalse("expected long-lived gauge to not be registered", isLongLivedMetricRegistered(dataBalancerMetricsRegistry, shortLivedGaugeName));
        assertTrue("Expected gauge to be present in registry", addedMetrics.containsValue(testShortLivedGauge));

        // Add a different gauge
        final String longLivedGaugeName = whitelistMetricName2;
        Metric testLongLivedGauge = dataBalancerMetricsRegistry.newGauge(this.getClass(), longLivedGaugeName, () -> true, false);
        // Add a long-lived duplicate
        assertThrows("Expected a short-lived metric with same name as long-lived to throw", IllegalStateException.class,
                () -> dataBalancerMetricsRegistry.newGauge(this.getClass(), longLivedGaugeName, () -> true, true));

        addedMetrics = metricsRegistry.allMetrics();
        assertEquals("Expected only two metrics in registry", 2, addedMetrics.size());
        assertTrue("expected gauge to be registered", isMetricRegistered(dataBalancerMetricsRegistry, longLivedGaugeName));
        assertFalse("expected short-lived gauge to not be registered", isShortLivedMetricRegistered(dataBalancerMetricsRegistry, longLivedGaugeName));
        assertTrue("expected long-lived gauge to be registered", isLongLivedMetricRegistered(dataBalancerMetricsRegistry, longLivedGaugeName));

        assertTrue("Expected gauge to be present in registry", addedMetrics.containsValue(testLongLivedGauge));
    }

    @Test
    public void testAddGaugeWithTags() {
        Map<String, String> tags = new HashMap<>();
        tags.put("a", "b");
        tags.put("b", "2");
        tags.put("c", "3");
        String expectedTags = "a=b,b=2,c=3";
        String name = "test";
        String expectedFullName = "kafka.databalancer:type=DataBalancerMetricsRegistryTest,name=" + name + "," + expectedTags;

        Metric testGauge = dataBalancerMetricsRegistry.newGauge(this.getClass(), name, () -> true, true, tags);

        Map<MetricName, Metric> addedMetrics = metricsRegistry.allMetrics();
        assertEquals("Expected only one metric to be added to registry", 1, addedMetrics.size());
        assertTrue("Expected gauge to be present in registry", addedMetrics.containsValue(testGauge));
        MetricName metricName = addedMetrics.keySet().iterator().next();
        assertEquals(expectedFullName, metricName.toString());
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
        // Add a metric again. This should be silently swallowed but not bump the metric count.
        dataBalancerMetricsRegistry.newGauge(this.getClass(), whitelistMetricName, () -> true, false);

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
