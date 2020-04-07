package io.confluent.telemetry.collector;

import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;

import java.util.function.Predicate;

/**
 * If an Exporter implements this interface, then we will also capture the metrics from this
 * MetricsCollector and send them along.
 */
public interface MetricsCollectorProvider {

    MetricsCollector collector(Predicate<MetricKey> metricsWhitelistFilter, Context context, String domain);
}
