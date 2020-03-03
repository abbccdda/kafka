// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class IntGaugeSuiteTest {
    private static final Logger log = LoggerFactory.getLogger(IntCounterSuiteTest.class);

    private static IntGaugeSuite<String> createIntGaugeMap() {
        MetricConfig config = new MetricConfig();
        Metrics metrics = new Metrics(config);
        IntGaugeSuite<String> suite = new IntGaugeSuite<>(log,
            "mySuite",
            metrics,
            name -> new MetricName(name, "group", "myMetric", Collections.emptyMap()));
        return suite;
    }

    @Test
    public void testCreateAndClose() {
        IntGaugeSuite<String> suite = createIntGaugeMap();
        suite.close();
        suite.close();
        suite.metrics().close();
    }

    @Test
    public void testCreateMetrics() {
        IntGaugeSuite<String> suite = createIntGaugeMap();
        suite.update("foo", 10);
        Map<String, Integer> values = suite.values();
        assertEquals(Integer.valueOf(10), values.get("foo"));
        assertEquals(Integer.valueOf(10), metricValue(suite.metrics(), "foo"));
        assertEquals(1, values.size());
        suite.update("foo", 5);
        suite.update("bar", 6);
        suite.update("baz", 7);
        suite.update("quux", 8);
        values = suite.values();
        assertEquals(Integer.valueOf(5), values.get("foo"));
        assertEquals(Integer.valueOf(6), values.get("bar"));
        assertEquals(Integer.valueOf(7), values.get("baz"));
        assertEquals(Integer.valueOf(8), values.get("quux"));
        assertEquals(Integer.valueOf(5), metricValue(suite.metrics(), "foo"));
        assertEquals(Integer.valueOf(6), metricValue(suite.metrics(), "bar"));
        assertEquals(Integer.valueOf(7), metricValue(suite.metrics(), "baz"));
        assertEquals(Integer.valueOf(8), metricValue(suite.metrics(), "quux"));
        assertEquals(4, values.size());
        suite.close();
        suite.metrics().close();
    }

    @Test
    public void testCreateAndRemoveMetrics() {
        IntGaugeSuite<String> suite = createIntGaugeMap();
        suite.update("foo", 5);
        suite.update("bar", 6);
        suite.remove("foo");
        suite.remove("bar");
        suite.update("baz", 7);
        suite.update("quux", 8);
        Map<String, Integer> values = suite.values();
        assertFalse(values.containsKey("foo"));
        assertFalse(values.containsKey("bar"));
        assertEquals(Integer.valueOf(7), values.get("baz"));
        assertEquals(Integer.valueOf(8), values.get("quux"));
        assertNull(metricValue(suite.metrics(), "foo"));
        assertNull(metricValue(suite.metrics(), "bar"));
        assertEquals(Integer.valueOf(7), metricValue(suite.metrics(), "baz"));
        assertEquals(Integer.valueOf(8), metricValue(suite.metrics(), "quux"));
        assertEquals(2, values.size());
        suite.close();
        suite.metrics().close();
    }

    private Integer metricValue(Metrics metrics, String name) {
        MetricName metricName = metrics.metricName(name, "group", "", Collections.emptyMap());
        KafkaMetric metric = metrics.metric(metricName);
        if (metric != null) {
            return (Integer) metric.metricValue();
        } else {
            return null;
        }
    }
}
