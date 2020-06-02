package io.confluent.telemetry.exporter;

import io.opencensus.proto.metrics.v1.Metric;

// A client is responsible for sending metrics correctly to backend
public interface Exporter extends AutoCloseable {

  /*
   * Export the metric to the destination. This method takes care
   * of batching, serialization and retries.
   */
  void emit(Metric metric);
}
