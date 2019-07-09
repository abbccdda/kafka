package io.confluent.telemetry.collector;

import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.LabelValue;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.TimeSeries;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MetricsTestUtils {

  /**
   * Convert descriptor + timeseries to a Map<String, String> of labels.
   */
  public static Map<String, String> toMap(MetricDescriptor descriptor, TimeSeries timeSeries) {
    List<LabelKey> keys = descriptor.getLabelKeysList();
    List<LabelValue> values = timeSeries.getLabelValuesList();

    return IntStream.range(0, keys.size())
        .boxed()
        .collect(Collectors.toMap(i -> keys.get(i).getKey(), i -> values.get(i).getValue()));
  }
}
