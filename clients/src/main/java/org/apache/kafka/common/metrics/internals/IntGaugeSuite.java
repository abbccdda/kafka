// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;

import java.util.function.Function;

/**
 * Manages a collection of integer Gauges. The number of gauges in the suite is unlimited
 * and gauges are removed when requested.
 */
public final class IntGaugeSuite<K> extends AbstractGaugeSuite<K> {

    public IntGaugeSuite(Logger log,
            String suiteName,
            Metrics metrics,
            Function<K, MetricName> metricNameCalculator) {
        super(log, suiteName, metrics, metricNameCalculator);
        log.trace("{}: created new gauge suite.", suiteName);
    }

    public void update(K key, int value) {
        synchronized (this) {
            if (closed) {
                log.warn("{}: Attempted to update {}, but the gauge map was closed.",
                    suiteName, key.toString());
                return;
            }
            StoredIntGauge gauge = gauges.get(key);
            if (gauge != null) {
                gauge.update(value);
            } else {
                addGauge(key, value);
            }
        }
        // Drop the object monitor and perform any pending metrics additions or removals.
        performPendingMetricsOperations();
    }

    public synchronized void remove(K key) {
        if (closed) {
            log.warn("{}: Attempted to remove {}, but the gauge map was closed.",
                suiteName, key.toString());
            return;
        }
        StoredIntGauge gauge = gauges.get(key);
        if (gauge == null) {
            log.debug("{}: Attempted to remove {}, but no such metric was registered.",
                suiteName, key.toString());
        } else {
            log.trace("{}: Marked {} for removal.", suiteName, key.toString());
            gauge.update(0);
            removeGauge(key);
        }
    }
}
