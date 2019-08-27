package io.confluent.telemetry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.Metric.Builder;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.TimeSeries;
import io.opencensus.proto.resource.v1.Resource;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A wrapper on the protobuf {@link Metric.Builder} that provides:
 * <ul>
 *   <li>A simpler interface for applying labels as a flat set of key-value pairs</li>
 *   <li>Convenience methods for building a <code>TimeSeries</code> containing a single <code>Point</code></li>
 *   <li>Enforcement of Confluent conventions</li>
 * </ul>
 *
 * <p>
 * When using the raw OpenCensus <code>Metric.Builder</code>, labels keys must be applied to the
 * <code>MetricDescriptor</code> and label values must be applied separately on each individual
 * <code>Timeseries</code>.
 *
 * <p>
 * When using this facade, labels can be set directly as key-value pairs. Upon {@link #build()}
 * the label keys are applied to the <code>MetricDescriptor</code> and the label values are
 * applied to each <code>TimeSeries</code>.
 *
 * TODO Push this into the <code>telemetry-api</code> module once PR is merged
 */
public class MetricBuilderFacade {

  private final Builder metricBuilder;
  private final Map<String, String> labels;

  public MetricBuilderFacade() {
    this(Metric.newBuilder());
  }

  private MetricBuilderFacade(Builder metricBuilder) {
    this.metricBuilder = metricBuilder;
    this.labels = new HashMap<>();
  }

  @VisibleForTesting
  Map<String, String> getLabels() {
    return Collections.unmodifiableMap(labels);
  }

  public MetricBuilderFacade withResource(Resource resource) {
    ResourceBuilderFacade.validate(resource);
    metricBuilder.setResource(resource);
    return this;
  }

  public MetricBuilderFacade withLabel(String key, String value) {
    this.labels.put(key, value);
    return this;
  }

  public MetricBuilderFacade withLabels(Map<String, String> labels) {
    this.labels.putAll(labels);
    return this;
  }

  public MetricBuilderFacade withName(String name) {
    metricBuilder.getMetricDescriptorBuilder().setName(name);
    return this;
  }

  public MetricBuilderFacade withType(Type type) {
    metricBuilder.getMetricDescriptorBuilder().setType(type);
    return this;
  }

  /**
   * Add a {@link TimeSeries} containing a single {@link Point}.
   */
  public MetricBuilderFacade addSinglePointTimeseries(Point point) {
    return addSinglePointTimeseries(point, null);
  }

  /**
   * Add a {@link TimeSeries} containing a single {@link Point}
   * and the given <code>startTimestamp</code>.
   */
  public MetricBuilderFacade addSinglePointTimeseries(
      Point point,
      Timestamp startTimestamp
  ) {
    TimeSeries.Builder timeseries = metricBuilder.addTimeseriesBuilder()
        .addPoints(point);

    if (startTimestamp != null) {
      timeseries.setStartTimestamp(startTimestamp);
    }

    return this;
  }

  /**
   * Build the {@link Metric}.
   *
   * This will apply the label keys to the <code>MetricDescriptor</code> and the label values
   * to each <code>TimeSeries</code>.
   */
  public Metric build() {
    validate();

    for (Entry<String, String> label : labels.entrySet()) {
      metricBuilder.getMetricDescriptorBuilder().addLabelKeysBuilder().setKey(label.getKey());
      for (TimeSeries.Builder timeseries : metricBuilder.getTimeseriesBuilderList()) {
        timeseries.addLabelValuesBuilder().setValue(label.getValue());
      }
    }
    return metricBuilder.build();
  }

  /**
   * Validate the Metric is properly constructed per Confluent standard conventions.
   */
  private void validate() {
    Preconditions.checkState(metricBuilder.hasResource(), "Metric Resource must be set");
  }

  /**
   * Perform a deep clone of this builder.
   *
   * <p>
   * This can be used to create a "prototype" builder with a default set of labels which can
   * be cloned into builders for individual metrics.
   * See <code>MetricBuilderFacadeTest</code> for an example of this pattern.
   *
   * @see Builder#clone()
   */
  @Override
  public MetricBuilderFacade clone() {
    return new MetricBuilderFacade(metricBuilder.clone()).withLabels(labels);
  }

}
