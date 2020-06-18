package io.confluent.telemetry.collector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.exporter.TestExporter;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Test;

public class JvmMetricsCollectorTest {

  private final TestExporter exporter = new TestExporter();

  private final Context context = new Context(
      new ResourceBuilderFacade("kafka")
          .withVersion("mockVersion")
          .withId("mockId")
          .build(),
      "test"
  );

  @Test
  public void collect() {
    JvmMetricsCollector metrics =
        JvmMetricsCollector
          .newBuilder()
          .setMetricWhitelistFilter(key -> key.getName().contains("cpu_usage"))
          .setContext(context)
          .build();

    metrics.collect(exporter);
    Metric metric = Iterables.getOnlyElement(exporter.emittedMetrics());
    assertEquals("Resource should match", context.getResource(), metric.getResource());
  }
  @Test
  public void collectAll() {
    List<String> metricNames = ImmutableList.of(
      "jvm/os/process_cpu_time",
      "cpu/cpu_usage",
      "jvm/os/system_cpu_load",
      "jvm/os/system_load_average",
      "jvm/os/free_physical_memory_size",
      "jvm/os/total_physical_memory_size",
      // Unix specific metrics
      "jvm/os/max_file_descriptor_count",
      "jvm/os/open_file_descriptor_count",
      // Memory specific metrics
      "jvm/mem/heap/committed",
      "jvm/mem/heap/used_memory",
      "jvm/mem/nonheap/committed",
      "jvm/mem/nonheap/used");

    AtomicInteger count = new AtomicInteger(metricNames.size());

    JvmMetricsCollector metrics =
      JvmMetricsCollector
        .newBuilder()
        .setMetricWhitelistFilter(key -> metricNames.stream().anyMatch(s -> {
            if (key.getName().contains(s)) {
              count.decrementAndGet();
              return true;
            }
            return false;
        }))
        .setContext(context)
        .build();


    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSize(metricNames.size() - count.get());
  }

  @Test
  public void checkLabels() {
    List<String> osLabels = ImmutableList.of("os.name", "os.version", "os.arch", "os.processors");
    JvmMetricsCollector metrics =
      JvmMetricsCollector
        .newBuilder()
        .setMetricWhitelistFilter(key -> true)
        .setContext(context)
        .build();

    metrics.collect(exporter);
    for (Metric emittedMetric : exporter.emittedMetrics()) {
      assertTrue(emittedMetric
        .getMetricDescriptor()
        .getLabelKeysList()
        .stream()
        .anyMatch(labelKey -> osLabels.contains(labelKey.getKey())));
    }
  }

  @Test
  public void collectFilteredOut() {
    JvmMetricsCollector metrics = JvmMetricsCollector.newBuilder()
        .setMetricWhitelistFilter(key -> !key.getName().contains("cpu_usage"))
        .setContext(context)
        .build();
    metrics.collect(exporter);
    List<Metric> filtered = exporter
      .emittedMetrics()
      .stream()
      .filter(metric -> metric.getMetricDescriptor().getName().contains("cpu_usage"))
      .collect(Collectors.toList());
    assertThat(exporter.emittedMetrics()).hasSizeGreaterThan(0);
  }

  @Test
  public void collectFilteredOutDynamicWhitelist() {
    JvmMetricsCollector metrics = JvmMetricsCollector.newBuilder()
        .setMetricWhitelistFilter(key -> true)
        .setContext(context)
        .build();

    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSizeGreaterThan(0);

    exporter.reset();
    metrics.reconfigureWhitelist(key -> key.getName().contains("cpu_usage"));
    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSize(1);

    exporter.reset();
    metrics.reconfigureWhitelist(key -> true);
    metrics.collect(exporter);
    assertThat(exporter.emittedMetrics()).hasSizeGreaterThan(0);
  }
}
