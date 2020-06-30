package io.confluent.telemetry.exporter;

import io.confluent.telemetry.MetricKey;
import io.opencensus.proto.metrics.v1.Metric;

import java.util.function.Predicate;

public abstract class AbstractExporter implements Exporter {

  private volatile Predicate<MetricKey> metricsPredicate = metricKey -> true;

  /**
   * Reconfigure the metrics predicate.
   * @param metricsPredicate predicate to switch to
   */
  public void reconfigurePredicate(Predicate<MetricKey> metricsPredicate) {
    this.metricsPredicate = metricsPredicate;
  }

  /*
   * Export the metric to the destination. This method takes care
   * of batching, serialization and retries.
   */
  public abstract void doEmit(MetricKey metricKey, Metric metric);

  public final boolean emit(MetricKey metricKey, Metric metric) {
    if (metricsPredicate.test(metricKey)) {
      doEmit(metricKey, metric);
      return true;
    }
    return false;
  }
}
