package io.confluent.telemetry.collector;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import io.confluent.observability.telemetry.ResourceBuilderFacade;
import io.confluent.observability.telemetry.TelemetryResourceType;
import io.confluent.telemetry.Context;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.Collections;
import org.junit.Test;

public class CPUMetricsCollectorTest {

  private final Context context = new Context(
      new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
          .withVersion("mockVersion")
          .withId("mockId")
          .build()
  );

  @Test
  public void collect() {
    CPUMetricsCollector metrics = CPUMetricsCollector.newBuilder().setDomain("test").setContext(context).build();

    Metric metric = Iterables.getOnlyElement(metrics.collect());
    assertEquals("Resource should match", context.getResource(), metric.getResource());
  }

  @Test
  public void collectFilteredOut() {
    CPUMetricsCollector metrics = CPUMetricsCollector.newBuilder()
        .setMetricFilter(key -> !key.getName().contains("cpu_usage"))
        .setDomain("empty")
        .setContext(context)
        .build();
    assertEquals(Collections.emptyList(), metrics.collect());
  }
}
