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

import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Manages a suite of integer Counters. The number of counters in the suite is limited by
 * {@link IntCounterSuite#maxEntries}. When the suite is full, registering a new counter will
 * try to evict one counter whose value is zero or the registration will fail.
 */
public final class IntCounterSuite<K> extends AbstractGaugeSuite<K> {
    /**
     * The maximum number of counters that we will ever create at once.
     */
    private final int maxEntries;

    /**
     * The keys of counters that can be removed, since their value is zero.
     * Protected by the object monitor.
     */
    private final Set<K> removable;

    public IntCounterSuite(Logger log,
            String suiteName,
            Metrics metrics,
            Function<K, MetricName> metricNameCalculator,
            int maxEntries) {
        super(log, suiteName, metrics, metricNameCalculator);
        this.removable = new HashSet<>();
        this.maxEntries = maxEntries;
        log.trace("{}: created new counter suite with maxEntries = {}.",
            suiteName, maxEntries);
    }

    public void increment(K key) {
        synchronized (this) {
            if (closed) {
                log.warn("{}: Attempted to increment {}, but the GaugeSuite was closed.",
                    suiteName, key.toString());
                return;
            }
            StoredIntGauge gauge = gauges.get(key);
            if (gauge != null) {
                // Fast path: increment the existing counter.
                if (gauge.increment() > 0) {
                    removable.remove(key);
                }
                return;
            }
            if (gauges.size() == maxEntries) {
                if (removable.isEmpty()) {
                    log.debug("{}: Attempted to increment {}, but there are already {} entries.",
                        suiteName, key.toString(), maxEntries);
                    return;
                }
                Iterator<K> iter = removable.iterator();
                K keyToRemove = iter.next();
                iter.remove();
                removeGauge(keyToRemove);
            }
            addGauge(key, 1);
        }
        // Drop the object monitor and perform any pending metrics additions or removals.
        performPendingMetricsOperations();
    }

    public synchronized void decrement(K key) {
        if (closed) {
            log.warn("{}: Attempted to decrement {}, but the gauge suite was closed.",
                suiteName, key.toString());
            return;
        }
        StoredIntGauge gauge = gauges.get(key);
        if (gauge == null) {
            log.debug("{}: Attempted to decrement {}, but no such metric was registered.",
                suiteName, key.toString());
        } else {
            int cur = gauge.decrement();
            log.trace("{}: Removed a reference to {}. {} reference(s) remaining.",
                suiteName, key.toString(), cur);
            if (cur <= 0) {
                removable.add(key);
            }
        }
    }

    /**
     * Get the maximum number of metrics this suite can create.
     */
    public int maxEntries() {
        return maxEntries;
    }
}
