package io.confluent.telemetry.collector;

import static io.confluent.telemetry.collector.MetricsTestUtils.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.TelemetryResourceType;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.exporter.TestExporter;
import io.opencensus.proto.metrics.v1.Metric;
import org.junit.Test;

public class VolumeMetricsCollectorTest {

  private final TestExporter exporter = new TestExporter();
  private final Context context = new Context(
      new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
          .withVersion("mockVersion")
          .withId("mockId")
          .build()
  );

  @Test
  public void collectFilterTotalBytes() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test")
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .setMetricWhitelistFilter(key -> !key.getName().contains("disk_total_bytes"))
        .build();

    metrics.collect(exporter);
    Metric metric = Iterables.getOnlyElement(exporter.emittedMetrics());

    assertEquals(context.getResource(), metric.getResource());
    assertEquals("test/volume/disk_usable_bytes", metric.getMetricDescriptor().getName());
    assertTrue(toMap(metric.getMetricDescriptor(), metric.getTimeseries(0)).containsKey("volume"));
    assertNotNull(metric.getTimeseries(0).getStartTimestamp());
  }

  @Test
  public void collectFilterUsableBytes() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test")
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .setMetricWhitelistFilter(key -> !key.getName().contains("disk_usable_bytes"))
        .build();

    metrics.collect(exporter);
    Metric metric = Iterables.getOnlyElement(exporter.emittedMetrics());

    assertEquals(context.getResource(), metric.getResource());
    assertEquals("test/volume/disk_total_bytes", metric.getMetricDescriptor().getName());
    assertTrue(toMap(metric.getMetricDescriptor(), metric.getTimeseries(0)).containsKey("volume"));
    assertNotNull(metric.getTimeseries(0).getStartTimestamp());
  }

  @Test
  public void collectCachedLabels() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test")
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .setMetricWhitelistFilter(key -> !key.getName().contains("disk_usable_bytes"))
        .build();

    // collect twice so that we have a cached set of labels.
    metrics.collect(exporter);
    exporter.reset();
    metrics.collect(exporter);
    Metric metric = Iterables.getOnlyElement(exporter.emittedMetrics());

    assertEquals(context.getResource(), metric.getResource());
    assertEquals("test/volume/disk_total_bytes", metric.getMetricDescriptor().getName());
    assertTrue(toMap(metric.getMetricDescriptor(), metric.getTimeseries(0)).containsKey("volume"));
  }

}
