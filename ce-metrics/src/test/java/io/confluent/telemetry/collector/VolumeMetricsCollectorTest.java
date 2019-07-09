package io.confluent.telemetry.collector;

import static io.confluent.telemetry.collector.MetricsTestUtils.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.telemetry.Context;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.List;
import org.junit.Test;

public class VolumeMetricsCollectorTest {

  Context context = new Context(ImmutableMap.of("test", "value"));

  @Test
  public void collectFilterTotalBytes() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test")
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .setMetricFilter(key -> !key.getName().contains("disk_total_bytes"))
        .build();

    List<Metric> out = metrics.collect();

    assertEquals(1, out.size());
    assertEquals("test/volume/disk_usable_bytes", out.get(0).getMetricDescriptor().getName());
    assertEquals("value", toMap(out.get(0).getMetricDescriptor(), out.get(0).getTimeseries(0)).get("test"));
    assertNotNull(out.get(0).getTimeseries(0).getStartTimestamp());
  }

  @Test
  public void collectFilterUsableBytes() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test")
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .setMetricFilter(key -> !key.getName().contains("disk_usable_bytes"))
        .build();

    List<Metric> out = metrics.collect();

    assertEquals(1, out.size());
    assertEquals("test/volume/disk_total_bytes", out.get(0).getMetricDescriptor().getName());
    assertEquals("value", toMap(out.get(0).getMetricDescriptor(), out.get(0).getTimeseries(0)).get("test"));
    assertNotNull(out.get(0).getTimeseries(0).getStartTimestamp());
  }

  @Test
  public void collectCachedLabels() {
    VolumeMetricsCollector metrics = VolumeMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test")
        .setUpdatePeriodMs(100L)
        .setLogDirs(new String[] {System.getProperties().get("java.io.tmpdir").toString()})
        .setMetricFilter(key -> !key.getName().contains("disk_usable_bytes"))
        .build();

    // collect twice so that we have a cached set of labels.
    metrics.collect();
    List<Metric> out = metrics.collect();

    assertEquals(1, out.size());
    assertEquals("test/volume/disk_total_bytes", out.get(0).getMetricDescriptor().getName());
    assertEquals("value", toMap(out.get(0).getMetricDescriptor(), out.get(0).getTimeseries(0)).get("test"));
  }

}