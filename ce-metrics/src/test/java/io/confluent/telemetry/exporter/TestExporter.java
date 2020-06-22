package io.confluent.telemetry.exporter;

import java.util.ArrayList;
import java.util.List;

import io.confluent.telemetry.MetricKey;
import io.opencensus.proto.metrics.v1.Metric;

public class TestExporter extends AbstractExporter {

    private final List<Metric> metrics = new ArrayList<>();

    public void reset() {
        metrics.clear();
    }
    public List<Metric> emittedMetrics() {
        return metrics;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void doEmit(MetricKey metricKey, Metric metric) {
        metrics.add(metric);
    }
}
