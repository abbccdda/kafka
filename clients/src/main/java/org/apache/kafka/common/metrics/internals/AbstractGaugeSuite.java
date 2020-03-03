/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Provides the base functionalities to implement a suite of metrics.
 */
public abstract class AbstractGaugeSuite<K> implements AutoCloseable {
    /**
     * The log4j logger.
     */
    protected final Logger log;

    /**
     * The name of this suite.
     */
    protected final String suiteName;

    /**
     * The metrics object to use.
     */
    protected final Metrics metrics;

    /**
     * A user-supplied callback which translates keys into unique metric names.
     */
    protected final Function<K, MetricName> metricNameCalculator;

    /**
     * A map from keys to gauges. Protected by the object monitor.
     */
    protected final Map<K, StoredIntGauge> gauges;

    /**
     * A lockless list of pending metrics additions and removals.
     */
    protected final ConcurrentLinkedDeque<PendingMetricsChange> pending;

    /**
     * A lock which serializes modifications to metrics. This lock is not
     * required to create a new pending operation.
     */
    protected final Lock modifyMetricsLock;

    /**
     * True if this suite is closed. Protected by the object monitor.
     */
    protected boolean closed;

    /**
     * A pending metrics addition or removal.
     */
    protected static class PendingMetricsChange {
        /**
         * The name of the metric to add or remove.
         */
        private final MetricName metricName;

        /**
         * In an addition, this field is the MetricValueProvider to add.
         * In a removal, this field is null.
         */
        private final MetricValueProvider<?> provider;

        PendingMetricsChange(MetricName metricName, MetricValueProvider<?> provider) {
            this.metricName = metricName;
            this.provider = provider;
        }
    }

    /**
     * The gauge object which we register with the metrics system.
     */
    protected static class StoredIntGauge implements Gauge<Integer> {
        private final MetricName metricName;
        private int value;

        StoredIntGauge(MetricName metricName, int value) {
            this.metricName = metricName;
            this.value = value;
        }

        StoredIntGauge(MetricName metricName) {
            this.metricName = metricName;
            this.value = 1;
        }

        /**
         * This callback is invoked when the metrics system retrieves the value of this gauge.
         */
        @Override
        public synchronized Integer value(MetricConfig config, long now) {
            return value;
        }

        synchronized int update(int value) {
            return this.value = value;
        }

        synchronized int increment() {
            return ++value;
        }

        synchronized int decrement() {
            return --value;
        }

        synchronized int value() {
            return value;
        }

        MetricName metricName() {
            return metricName;
        }
    }

    public AbstractGaugeSuite(Logger log,
        String suiteName,
        Metrics metrics,
        Function<K, MetricName> metricNameCalculator) {
        this.log = log;
        this.suiteName = suiteName;
        this.metrics = metrics;
        this.metricNameCalculator = metricNameCalculator;
        this.gauges = new HashMap<>(1);
        this.pending = new ConcurrentLinkedDeque<>();
        this.modifyMetricsLock = new ReentrantLock();
        this.closed = false;
    }

    /**
     * Perform pending metrics additions or removals.
     * It is important to perform them in order.  For example, we don't want to try
     * to remove a metric that we haven't finished adding yet.
     */
    protected void performPendingMetricsOperations() {
        modifyMetricsLock.lock();
        try {
            log.trace("{}: entering performPendingMetricsOperations", suiteName);
            for (PendingMetricsChange change = pending.pollLast();
                change != null;
                change = pending.pollLast()) {
                if (change.provider == null) {
                    if (log.isTraceEnabled()) {
                        log.trace("{}: removing metric {}", suiteName, change.metricName);
                    }
                    metrics.removeMetric(change.metricName);
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("{}: adding metric {}", suiteName, change.metricName);
                    }
                    metrics.addMetric(change.metricName, change.provider);
                }
            }
            log.trace("{}: leaving performPendingMetricsOperations", suiteName);
        } finally {
            modifyMetricsLock.unlock();
        }
    }

    /**
     * Adds a new gauge with the given value and adds a pending metric change to
     * register the mbean.
     */
    protected void addGauge(K key, int value) {
        MetricName metricNameToAdd = metricNameCalculator.apply(key);
        StoredIntGauge gauge = new StoredIntGauge(metricNameToAdd, value);
        gauges.put(key, gauge);
        pending.push(new PendingMetricsChange(metricNameToAdd, gauge));
        log.trace("{}: Adding a new metric {}.", suiteName, key.toString());
    }

    /**
     * Removes the gauge and adds a pending metric change to remove the mbean.
     */
    protected void removeGauge(K key) {
        MetricName metricNameToRemove = gauges.get(key).metricName();
        gauges.remove(key);
        pending.push(new PendingMetricsChange(metricNameToRemove, null));
        log.trace("{}: Removing the metric {}, which has a value of 0.",
            suiteName, key.toString());
    }

    @Override
    public synchronized void close() {
        if (closed) {
            log.trace("{}: gauge suite is already closed.", suiteName);
            return;
        }
        closed = true;
        int prevSize = 0;
        for (Iterator<StoredIntGauge> iter = gauges.values().iterator(); iter.hasNext(); ) {
            pending.push(new PendingMetricsChange(iter.next().metricName, null));
            prevSize++;
            iter.remove();
        }
        performPendingMetricsOperations();
        log.trace("{}: closed {} metric(s).", suiteName, prevSize);
    }

    // Visible for testing only.
    Metrics metrics() {
        return metrics;
    }

    /**
     * Return a map from keys to current reference counts.
     * Visible for testing only.
     */
    synchronized Map<K, Integer> values() {
        HashMap<K, Integer> values = new HashMap<>();
        for (Map.Entry<K, StoredIntGauge> entry : gauges.entrySet()) {
            values.put(entry.getKey(), entry.getValue().value());
        }
        return values;
    }
}
