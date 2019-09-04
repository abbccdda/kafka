package io.confluent.telemetry.exporter;

import io.opencensus.proto.metrics.v1.Metric;

import java.util.Collection;

// A client is responsible for sending metrics correctly to backend
public interface Exporter extends AutoCloseable {

    /*
     * Export the metrics to the destination. This method takes care
     * of batching, serialization and retries.
     */
    void export(Collection<Metric> metrics) throws Exception;
}


