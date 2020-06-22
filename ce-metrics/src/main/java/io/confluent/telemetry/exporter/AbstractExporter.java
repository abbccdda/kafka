package io.confluent.telemetry.exporter;

import io.confluent.telemetry.MetricKey;
import io.opencensus.proto.metrics.v1.Metric;

import java.util.function.Predicate;

public abstract class AbstractExporter implements Exporter {

  private volatile Predicate<MetricKey> whitelistPredicate = metricKey -> true;

  /**
   * Reconfigure the metrics whitelist predicate.
   * @param whitelistPredicate whitelist predicate to switch to
   */
  public void reconfigureWhitelist(Predicate<MetricKey> whitelistPredicate) {
    this.whitelistPredicate = whitelistPredicate;
  }

  /*
   * Export the metric to the destination. This method takes care
   * of batching, serialization and retries.
   */
  public abstract void doEmit(MetricKey metricKey, Metric metric);

  public final boolean emit(MetricKey metricKey, Metric metric) {
    if (whitelistPredicate.test(metricKey)) {
      doEmit(metricKey, metric);
      return true;
    }
    return false;
  }
}
