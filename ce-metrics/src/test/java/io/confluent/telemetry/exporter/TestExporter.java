package io.confluent.telemetry.exporter;

import java.util.ArrayList;
import java.util.List;

import io.opencensus.proto.metrics.v1.Metric;

public class TestExporter implements Exporter {

    private final List<Metric> metrics = new ArrayList<>();

    public void reset() {
        metrics.clear();
    }
    public List<Metric> emittedMetrics() {
        return metrics;
    }

    @Override
    public void emit(Metric metric) {
        metrics.add(metric);
    }

    @Override
    public void close() throws Exception {

    }
}
