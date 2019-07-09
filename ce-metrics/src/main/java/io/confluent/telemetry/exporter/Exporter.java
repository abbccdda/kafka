package io.confluent.telemetry.exporter;

import io.opencensus.proto.metrics.v1.Metric;

import java.util.Collection;

// A client is responsible for sending metrics correctly to backend
public interface Exporter extends AutoCloseable {

    /*
     * Write the metrics to the backend service. This method takes care
     * of batching, serialization and retries.
     */
    void write(Collection<Metric> metrics) throws RuntimeException;
}


