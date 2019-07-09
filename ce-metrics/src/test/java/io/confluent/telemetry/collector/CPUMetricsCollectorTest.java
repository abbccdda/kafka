package io.confluent.telemetry.collector;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.confluent.telemetry.Context;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class CPUMetricsCollectorTest {

  Context context = new Context(ImmutableMap.of("test", "value"));
  @Test
  public void collect() {
    CPUMetricsCollector metrics = CPUMetricsCollector.newBuilder().setDomain("test").setContext(context).build();

    List<Metric> cpuMetrics = metrics.collect();
    assertEquals(1, cpuMetrics.size());
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