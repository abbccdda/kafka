package io.confluent.telemetry.collector;


import io.opencensus.proto.metrics.v1.Metric;
import java.util.Collection;

// A collector is responsible for scraping a source of metrics and converting them to the canonical format
// For eg: we will have collectors for system metrics, kafka metrics, yammer metrics, opencensus metric ....
public interface MetricsCollector {

    /**
     * Label for the collector that collected the metrics
     */
    String LABEL_COLLECTOR = "collector";

    /**
     * Label for the metric instrumentation library
     */
    String LABEL_LIBRARY = "library";

    /**
     * "none" value for {@link #LABEL_LIBRARY}
     */
    String LIBRARY_NONE = "none";

    /**
     * Label for the original (untranslated) metric name
     */
    String LABEL_ORIGINAL = "metric_name_original";

    Collection<Metric> collect();
}
