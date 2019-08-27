package io.confluent.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.LabelValue;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.resource.v1.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class MetricBuilderFacadeTest {

  private final Resource resource = new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
        .withVersion("mockVersion")
        .withId("mockId")
        .build();

  @Test
  public void testBuild() {
    Timestamp startTimestamp = Timestamp.newBuilder().setSeconds(1L).build();

    Point point1 = Point.newBuilder()
        .setInt64Value(100L)
        .setTimestamp(Timestamp.newBuilder().setSeconds(100L).build())
        .build();
    Point point2 = Point.newBuilder()
        .setInt64Value(99L)
        .setTimestamp(Timestamp.newBuilder().setSeconds(200L).build())
        .build();

    Metric metric = new MetricBuilderFacade()
        .withResource(resource)
        .withName("metricName")
        .withType(Type.GAUGE_INT64)
        .withLabel("label1", "value1")
        .withLabels(Collections.singletonMap("label2", "value2"))
        .addSinglePointTimeseries(point1)
        .addSinglePointTimeseries(point2, startTimestamp)
        .build();

    assertThat(metric.getResource()).isEqualTo(resource);
    assertThat("metricName").isEqualTo(metric.getMetricDescriptor().getName());
    assertThat(Type.GAUGE_INT64).isEqualTo(metric.getMetricDescriptor().getType());

    assertThat(metric.getMetricDescriptor().getLabelKeysList())
        .containsOnly(
            LabelKey.newBuilder().setKey("label1").build(),
            LabelKey.newBuilder().setKey("label2").build()
        );

    List<String> labelKeys = metric.getMetricDescriptor().getLabelKeysList()
        .stream()
        .map(LabelKey::getKey)
        .collect(Collectors.toList());

    assertThat(metric.getTimeseriesCount()).isEqualTo(2);

    assertThat(metric.getTimeseries(0).getPointsList())
        .containsExactly(point1);

    assertThat(metric.getTimeseries(1).getPointsList())
        .containsExactly(point2);

    assertThat(metric.getTimeseries(1).getStartTimestamp())
        .isEqualTo(startTimestamp);

    Map<String, String> expectedLabels = ImmutableMap.of(
        "label1", "value1",
        "label2", "value2"
    );

    assertThat(metric.getTimeseriesList()).allSatisfy(timeSeries -> {
      List<String> labelValues = timeSeries.getLabelValuesList()
          .stream()
          .map(LabelValue::getValue)
          .collect(Collectors.toList());
      assertThat(labelValues).isEqualTo(
          labelKeys
              .stream()
              .map(expectedLabels::get)
              .collect(Collectors.toList())
      );
    });
  }

  @Test
  public void testClone() {
    MetricBuilderFacade prototype = new MetricBuilderFacade()
        .withLabel("commonLabel", "value")
        .withResource(resource);

    Metric metric1 = prototype.clone()
        .withLabel("label1", "value1")
        .withName("metric1")
        .build();

    assertThat(metric1.getResource()).isEqualTo(resource);
    assertThat(metric1.getMetricDescriptor().getName()).isEqualTo("metric1");
    assertThat(metric1.getMetricDescriptor().getLabelKeysList()).containsOnly(
        LabelKey.newBuilder().setKey("commonLabel").build(),
        LabelKey.newBuilder().setKey("label1").build()
    );

    Metric metric2 = prototype.clone()
        .withLabel("label2", "value2")
        .withName("metric2")
        .build();

    assertThat(metric2.getResource()).isEqualTo(resource);
    assertThat(metric2.getMetricDescriptor().getName()).isEqualTo("metric2");
    assertThat(metric2.getMetricDescriptor().getLabelKeysList()).containsOnly(
        LabelKey.newBuilder().setKey("commonLabel").build(),
        LabelKey.newBuilder().setKey("label2").build()
    );
  }

  @Test
  public void testMissingResource() {
    assertThatThrownBy(new MetricBuilderFacade()::build)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Metric Resource must be set");
  }

}