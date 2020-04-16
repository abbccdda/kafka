/**
 * Copyright (C) 2020 Confluent Inc.
 */

package io.confluent.databalancer.metrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
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
    private static final String GROUP = "kafka.databalancer";

    // whitelist because registry does not manage lifetime of data balancer metrics
    private final Set<MetricName> longLivedMetricsWhitelist;
    private final MetricsRegistry metricsRegistry;
    private final Set<MetricName> longLivedMetrics;
    private final Set<MetricName> shortLivedMetrics;

    public DataBalancerMetricsRegistry(MetricsRegistry metricsRegistry, Set<MetricName> longLivedMetricsWhitelist) {
        this.metricsRegistry = metricsRegistry;
        this.longLivedMetricsWhitelist = Collections.unmodifiableSet(longLivedMetricsWhitelist);
        this.shortLivedMetrics = new HashSet<>();
        this.longLivedMetrics = new HashSet<>();
    }

    public <T> Gauge<T> newGauge(Class<?> klass, String name, Supplier<T> valueSupplier) {
        return newGauge(klass, name, valueSupplier, true);
    }

    public <T> Gauge<T> newGauge(Class<?> klass, String name, Supplier<T> valueSupplier, boolean isShortLivedMetric) {
        MetricName metricName = metricName(GROUP, klass.getSimpleName(), name);
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
            throw new IllegalStateException("Attempt to add already-registered metric to DataBalancerMetricsRegistry");
        }
        boolean allowMetric = isShortLivedMetric || longLivedMetricsWhitelist.contains(metricName);
        if (!allowMetric) {
            throw new IllegalStateException("Attempt to add non-whitelisted databalancer metric to DataBalancerMetricsRegistry");
        }
        if (metricsRegistry.allMetrics().containsKey(metricName)) {
            throw new IllegalStateException("Attempt to add already-registered metric to metrics registry");
        }
        addedMetrics.add(metricName);
    }

    private static MetricName metricName(String group, String type, String name) {
        String mbeanName = String.format("%s:type=%s,name=%s", group, type, name);
        return new MetricName(group, type, name, null, mbeanName);
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
