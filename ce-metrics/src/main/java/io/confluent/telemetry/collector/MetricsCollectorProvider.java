package io.confluent.telemetry.collector;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;

/**
 * If an Exporter implements this interface, then we will also capture the metrics from this
 * MetricsCollector and send them along.
 */
public interface MetricsCollectorProvider {

    MetricsCollector collector(ConfluentTelemetryConfig config, Context context, String domain);
}
