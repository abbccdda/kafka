/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer.metrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Sanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * DataBalancerMetricsRegistry tracks and manages the object lifetime of various DataBalancer short-lived metrics,
 * i.e.: metrics that should only be alive as long as some other object is active.
 *
 * DataBalancerMetricsRegistry can also be used to format the mBean for metrics created through its interface.
 * Long-lived metrics that do not want DataBalancerMetricsRegistry to manage their lifetime may use this class for that
 * purpose, but we require any long-lived metrics to be whitelisted before being added to the registry to be pro-active
 * about potential metrics memory leaks
 */
public class DataBalancerMetricsRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(DataBalancerMetricsRegistry.class);
    // Package-private for test access
    static final String GROUP = "kafka.databalancer";

    // whitelist because registry does not manage lifetime of data balancer metrics
    private final Set<MetricName> longLivedMetricsWhitelist;
    private final MetricsRegistry metricsRegistry;
    // Package-private for testing
    final Set<MetricName> longLivedMetrics;
    final Set<MetricName> shortLivedMetrics;

    public DataBalancerMetricsRegistry(MetricsRegistry metricsRegistry, Set<MetricName> longLivedMetricsWhitelist) {
        this.metricsRegistry = metricsRegistry;
        this.longLivedMetricsWhitelist = Collections.unmodifiableSet(longLivedMetricsWhitelist);
        this.shortLivedMetrics = new HashSet<>();
        this.longLivedMetrics = new HashSet<>();
    }

    public <T> Gauge<T> newGauge(Class<?> klass, String name, Supplier<T> valueSupplier) {
        return newGauge(klass, name, valueSupplier, true);
    }

    public <T> Gauge<T> newGauge(Class<?> klass, String name, Supplier<T> valueSupplier, Map<String, String> tags) {
        return newGauge(klass, name, valueSupplier, true, tags);
    }

    public <T> Gauge<T> newGauge(Class<?> klass, String name, Supplier<T> valueSupplier, boolean isShortLivedMetric) {
        return newGauge(klass, name, valueSupplier, isShortLivedMetric, Collections.emptyMap());
    }

    public <T> Gauge<T> newGauge(Class<?> klass, String name, Supplier<T> valueSupplier, boolean isShortLivedMetric, Map<String, String> tags) {
        MetricName metricName = metricName(GROUP, klass.getSimpleName(), name, tags);
        registerMetric(metricName, isShortLivedMetric);
        return metricsRegistry.newGauge(metricName, new Gauge<T>() {
            @Override
            public T value() {
                return valueSupplier.get();
            }
        });
    }

    public Timer newTimer(Class<?> klass, String name) {
        MetricName metricName = metricName(GROUP, klass.getSimpleName(), name);
        registerMetric(metricName, true);
        return metricsRegistry.newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    }

    public Meter newMeter(Class<?> klass, String name, String eventType, TimeUnit timeUnit) {
        MetricName metricName = metricName(GROUP, klass.getSimpleName(), name);
        registerMetric(metricName, true);
        return metricsRegistry.newMeter(metricName, eventType, timeUnit);
    }

    public void clearShortLivedMetrics() {
        LOG.debug("Attempting to clear metrics registry of metrics: {}. Will not clear metrics: {}",
                shortLivedMetrics, longLivedMetrics);
        shortLivedMetrics.forEach(metricsRegistry::removeMetric);
        shortLivedMetrics.clear();
    }

    private void registerMetric(MetricName metricName, boolean isShortLivedMetric) {
        Set<MetricName> addedMetrics = isShortLivedMetric ? shortLivedMetrics : longLivedMetrics;
        if (addedMetrics.contains(metricName)) {
            // For integration tests, duplicate metrics arise. Don't throw, just ignore.
            LOG.warn("Adding metric {} a second time is a no-op, ignoring", metricName);
            return;
        }
        boolean allowMetric = isShortLivedMetric || longLivedMetricsWhitelist.contains(metricName);
        if (!allowMetric) {
            throw new IllegalStateException("Attempt to add non-whitelisted databalancer metric to DataBalancerMetricsRegistry");
        }
        Set<MetricName> addedElsewhereMetrics = isShortLivedMetric ? longLivedMetrics : shortLivedMetrics;
        if (addedElsewhereMetrics.contains(metricName)) {
            // Adding a metric with the same name but different lifespan is a bad thing
            throw new IllegalStateException(String.format("Attempt to add databalancer metric metric %s with different lifespan than current",
                    metricName));
        }

        if (metricsRegistry.allMetrics().containsKey(metricName)) {
            // For integration tests, duplicate metrics arise. Don't throw exceptions, just ignore the duplicate.
            LOG.warn("Adding metric {} a second time is a no-op, ignoring", metricName);
            return;
        }
        addedMetrics.add(metricName);
    }

    // Package-private for testing
    static MetricName metricName(String group, String type, String name) {
        return metricName(group, type, name, Collections.emptyMap());
    }

    private static MetricName metricName(String group, String type, String name, Map<String, String> tags) {
        String mbeanName = String.format("%s:type=%s,name=%s", group, type, name);
        String tagsName = tagsToMBeanName(tags);
        if (!tagsName.isEmpty()) {
            mbeanName += "," + tagsName;
        }

        return new MetricName(group, type, name, null, mbeanName);
    }

    private static String tagsToMBeanName(Map<String, String> tags) {
        List<String> tagsList = tags.entrySet().stream()
            .filter(x -> x.getValue() != null && !x.getValue().equals(""))
            .map(kv -> String.format("%s=%s", kv.getKey(), Sanitizer.jmxSanitize(kv.getValue())))
            .collect(Collectors.toList());
        return String.join(",", tagsList);
    }

    public static class MetricsWhitelistBuilder {
        private Set<MetricName> whitelistedMetrics;

        public MetricsWhitelistBuilder() {
            this.whitelistedMetrics = new HashSet<>();
        }

        public void addMetric(Class<?> klass, String name) {
            whitelistedMetrics.add(metricName(GROUP, klass.getSimpleName(), name));
        }

        public Set<MetricName> buildWhitelist() {
            return Collections.unmodifiableSet(whitelistedMetrics);
        }
    }
}
