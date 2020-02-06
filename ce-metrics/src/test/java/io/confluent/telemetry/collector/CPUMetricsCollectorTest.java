package io.confluent.telemetry.collector;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import io.confluent.observability.telemetry.ResourceBuilderFacade;
import io.confluent.observability.telemetry.TelemetryResourceType;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.exporter.TestExporter;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.Collections;
import org.junit.Test;

public class CPUMetricsCollectorTest {

  private final TestExporter exporter = new TestExporter();

  private final Context context = new Context(
      new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
          .withVersion("mockVersion")
          .withId("mockId")
          .build()
  );

  @Test
  public void collect() {
    CPUMetricsCollector metrics = CPUMetricsCollector.newBuilder().setDomain("test").setContext(context).build();

    metrics.collect(exporter);
    Metric metric = Iterables.getOnlyElement(exporter.emittedMetrics());
    assertEquals("Resource should match", context.getResource(), metric.getResource());
  }

  @Test
  public void collectFilteredOut() {
    CPUMetricsCollector metrics = CPUMetricsCollector.newBuilder()
        .setMetricWhitelistFilter(key -> !key.getName().contains("cpu_usage"))
        .setDomain("empty")
        .setContext(context)
        .build();
    metrics.collect(exporter);
    assertEquals(Collections.emptyList(), exporter.emittedMetrics());
  }
}
