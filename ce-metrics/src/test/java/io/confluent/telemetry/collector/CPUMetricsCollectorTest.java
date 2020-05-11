package io.confluent.telemetry.collector;

import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.exporter.TestExporter;
import io.confluent.telemetry.reporter.KafkaServerMetricsReporter;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.Collections;

import org.junit.Test;

public class CPUMetricsCollectorTest {

  private final TestExporter exporter = new TestExporter();

  private final Context context = new Context(
      new ResourceBuilderFacade(KafkaServerMetricsReporter.RESOURCE_TYPE_KAFKA)
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

  @Test
  public void collectFilteredOutDynamicWhitelist() {
    CPUMetricsCollector metrics = CPUMetricsCollector.newBuilder()
        .setMetricWhitelistFilter(key -> true)
        .setDomain("empty")
        .setContext(context)
        .build();

    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSize(1);

    exporter.reset();
    metrics.reconfigureWhitelist(key -> !key.getName().contains("cpu_usage"));
    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSize(0);

    exporter.reset();
    metrics.reconfigureWhitelist(key -> true);
    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSize(1);
  }
}
