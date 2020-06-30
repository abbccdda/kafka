package io.confluent.telemetry.collector;

import static io.confluent.telemetry.collector.MetricsTestUtils.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.exporter.TestExporter;
import io.opencensus.proto.metrics.v1.Metric;
import org.junit.Test;

public class VolumeMetricsCollectorTest {

  private final TestExporter exporter = new TestExporter();
  private final Context context = new Context(
      new ResourceBuilderFacade("kafka")
          .withVersion("mockVersion")
          .withId("mockId")
          .build(),
          "test"
  );

  @Test
  public void collectFilterTotalBytes() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .build();

    exporter.reconfigurePredicate(key -> !key.getName().contains("disk_total_bytes"));
    metrics.collect(exporter);
    Metric metric = Iterables.getOnlyElement(exporter.emittedMetrics());

    assertEquals(context.getResource(), metric.getResource());
    assertThat(metric.getMetricDescriptor().getName()).startsWith(JvmMetricsCollector.SYSTEM_DOMAIN);
    assertEquals("io.confluent.system/volume/disk_usable_bytes", metric.getMetricDescriptor().getName());
    assertTrue(toMap(metric.getMetricDescriptor(), metric.getTimeseries(0)).containsKey("volume"));
    assertNotNull(metric.getTimeseries(0).getStartTimestamp());
  }

  @Test
  public void collectFilterUsableBytes() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .build();

    exporter.reconfigurePredicate(key -> !key.getName().contains("disk_usable_bytes"));
    metrics.collect(exporter);
    Metric metric = Iterables.getOnlyElement(exporter.emittedMetrics());

    assertEquals(context.getResource(), metric.getResource());
    assertEquals("io.confluent.system/volume/disk_total_bytes", metric.getMetricDescriptor().getName());
    assertTrue(toMap(metric.getMetricDescriptor(), metric.getTimeseries(0)).containsKey("volume"));
    assertNotNull(metric.getTimeseries(0).getStartTimestamp());
  }

  @Test
  public void collectCachedLabels() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .build();

    // collect twice so that we have a cached set of labels.
    exporter.reconfigurePredicate(key -> !key.getName().contains("disk_usable_bytes"));
    metrics.collect(exporter);
    exporter.reset();
    metrics.collect(exporter);
    Metric metric = Iterables.getOnlyElement(exporter.emittedMetrics());

    assertEquals(context.getResource(), metric.getResource());
    assertEquals("io.confluent.system/volume/disk_total_bytes", metric.getMetricDescriptor().getName());
    assertTrue(toMap(metric.getMetricDescriptor(), metric.getTimeseries(0)).containsKey("volume"));
  }

  @Test
  public void collectFilterDynamicConfig() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .build();

    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSize(2); // disk_total_bytes, disk_usable_bytes

    exporter.reset();
    exporter.reconfigurePredicate(key -> key.getName().endsWith("/disk_total_bytes"));
    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSize(1); // disk_total_bytes

    exporter.reset();
    exporter.reconfigurePredicate(key -> true);
    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSize(2); // disk_total_bytes, disk_usable_bytes
  }
}
