package io.confluent.telemetry.collector;


import io.opencensus.proto.metrics.v1.Metric;
import java.util.Collection;

// A collector is responsible for scraping a source of metrics and converting them to the canonical format
// For eg: we will have collectors for system metrics, kafka metrics, yammer metrics, opencensus metric ....
public interface MetricsCollector {
    String ORIGINAL = "original";
    String LIBRARY = "library";
    String NO_LIBRARY = "none";

    Collection<Metric> collect();
}
