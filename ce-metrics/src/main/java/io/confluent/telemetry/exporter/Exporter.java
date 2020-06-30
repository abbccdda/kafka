package io.confluent.telemetry.exporter;

import io.confluent.telemetry.MetricKey;
import io.opencensus.proto.metrics.v1.Metric;

import java.util.function.Predicate;

// A client is responsible for sending metrics correctly to backend
public interface Exporter extends AutoCloseable {

  /**
   * Reconfigure the metrics predicate.
   * @param metricsPredicate metrics predicate to switch to
   */
  void reconfigurePredicate(Predicate<MetricKey> metricsPredicate);

  /*
   * Export the metric to the destination. This method takes care
   * of batching, serialization and retries.
   */
  void doEmit(MetricKey metricKey, Metric metric);

  boolean emit(MetricKey metricKey, Metric metric);
}
