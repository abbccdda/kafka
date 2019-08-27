package io.confluent.telemetry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;

/**
 * Value object that contains the name and labels for a Metric.
 *
 * <p>
 * Rather than a Metrics Protobuf, we use objects of this class for filtering Metrics and as entries
 * in the LastValueTracker's underlying Map. We do this because 1) we want to avoid the CPU overhead
 * by filtering metrics before we materialize them 2) the OpenCensus Metric format is not ideal
 * for writing a predicate, since you have to stitch back together the metric labelKeys and
 * labelValues (and this latter one lives inside of an array) and 3) the metric name + labels
 * uniquely identify a metric.
 * </p>
 */
public class MetricKey {

  private final String name;
  private final Map<String, String> labels;

  /**
   * Create a MetricsDetails
   * @param name metric name. This should be the _converted_ name of the metric (the final name
   *   under which this metric is emitted).
   * @param labels mapping of tag keys to values.
   */
  public MetricKey(String name, Map<String, String> labels) {
    this.name = Objects.requireNonNull(name, "name");
    this.labels = ImmutableMap.copyOf(labels);
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricKey that = (MetricKey) o;
    return name.equals(that.name) &&
        labels.equals(that.labels);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, labels);
  }

  @Override
  public String toString() {
    return "MetricKey{" +
        "name='" + name + '\'' +
        ", labels=" + labels +
        '}';
  }
}
