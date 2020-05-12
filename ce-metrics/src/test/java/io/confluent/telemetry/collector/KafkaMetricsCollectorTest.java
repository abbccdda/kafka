package io.confluent.telemetry.collector;


import static io.confluent.telemetry.collector.MetricsTestUtils.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.exporter.TestExporter;
import io.confluent.telemetry.reporter.KafkaServerMetricsReporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class KafkaMetricsCollectorTest {

  private final TestExporter exporter = new TestExporter();

  private Map<String, String> tags;
  private Map<String, String> labels;
  private Metrics metrics;
  private MetricName metricName;
  private KafkaMetricsCollector.StateLedger ledger;


  private final Context context = new Context(
      new ResourceBuilderFacade(KafkaServerMetricsReporter.RESOURCE_TYPE_KAFKA)
          .withVersion("mockVersion")
          .withId("mockId")
          .build()
  );

  @Before
  public void setUp() {
    metrics = new Metrics();
    tags = new HashMap<>();
    tags.put("tag", "value");

    metricName = metrics.metricName("name1", "group1", tags);

    labels = new HashMap<>();
    labels.putAll(tags);
  }

  @Test
  public void testMeasurableCounter() {
    Sensor sensor = metrics.sensor("test");
    sensor.add(metricName, new WindowedCount());

    sensor.record();
    sensor.record();

    ledger = new KafkaMetricsCollector.StateLedger();
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .build();


    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();

    assertEquals("Should get exactly 3 Kafka measurables since Metrics always includes a count measurable", 3, result.size());

    Metric counter = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1")).findFirst().get();
    Metric delta = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1/delta")).findFirst().get();

    assertEquals("Types should match", Type.CUMULATIVE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Types should match", Type.CUMULATIVE_DOUBLE, delta.getMetricDescriptor().getType());
    assertEquals("Labels should match", labels, toMap(counter.getMetricDescriptor(), counter.getTimeseries(0)));
    assertEquals("Labels should match", labels, toMap(delta.getMetricDescriptor(), delta.getTimeseries(0)));
    assertEquals("Value should match", 2.0, counter.getTimeseries(0).getPoints(0).getDoubleValue(), 0.0);
    assertEquals("Value should match", 2.0, delta.getTimeseries(0).getPoints(0).getDoubleValue(), 0.0);
  }

  @Test
  public void testMeasurableTotal() {
    Sensor sensor = metrics.sensor("test");
    sensor.add(metricName, new CumulativeSum());

    sensor.record(10L);
    sensor.record(5L);

    ledger = new KafkaMetricsCollector.StateLedger();
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .build();

    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();

    assertEquals("Should get exactly 3 Kafka measurables since Metrics always includes a count measurable", 3, result.size());

    Metric counter = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1")).findFirst().get();
    Metric delta = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1/delta")).findFirst().get();

    assertEquals("Resource should match", context.getResource(), counter.getResource());
    assertEquals("Resource should match", context.getResource(), delta.getResource());
    assertEquals("Types should match", Type.CUMULATIVE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Types should match", Type.CUMULATIVE_DOUBLE, delta.getMetricDescriptor().getType());
    assertEquals("Labels should match", labels, toMap(counter.getMetricDescriptor(), counter.getTimeseries(0)));
    assertEquals("Labels should match", labels, toMap(delta.getMetricDescriptor(), delta.getTimeseries(0)));
    assertEquals("Value should match", 15.0, counter.getTimeseries(0).getPoints(0).getDoubleValue(), 0.0);
    assertEquals("Value should match", 15.0, delta.getTimeseries(0).getPoints(0).getDoubleValue(), 0.0);
  }

  @Test
  public void testMeasurableGauge() {
    metrics.addMetric(metricName, new Measurable() {
      public double measure(MetricConfig config, long now) {
        return 100.0;
      }
    });
    ledger = new KafkaMetricsCollector.StateLedger();
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .build();

    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();

    assertEquals("Should get exactly 2 Kafka measurables since Metrics always includes a count measurable", 2, result.size());

    Metric counter = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1")).findFirst().get();

    assertEquals("Resource should match", context.getResource(), counter.getResource());
    assertEquals("Types should match", Type.GAUGE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Labels should match", labels, toMap(counter.getMetricDescriptor(), counter.getTimeseries(0)));
    assertEquals("Value should match", 100L, counter.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-9);
  }

  @Test
  public void testNonMeasurable() {
    metrics.addMetric(metrics.metricName("float", "group1", tags), (Gauge<Float>) (config, now) -> 99f);
    metrics.addMetric(metrics.metricName("double", "group1", tags), (Gauge<Double>) (config, now) -> 99d);
    metrics.addMetric(metrics.metricName("int", "group1", tags), (Gauge<Integer>) (config, now) -> 100);
    metrics.addMetric(metrics.metricName("long", "group1", tags), (Gauge<Long>) (config, now) -> 100L);

    ledger = new KafkaMetricsCollector.StateLedger();
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .build();

    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();

    assertEquals("Should get exactly 5 Kafka measurables since Metrics always includes a count measurable", 5, result.size());


    result.stream().filter(metric -> metric.getMetricDescriptor().getName().matches("test-domain/group1/(float|double)")).forEach(
        doubleGauge -> {
          assertEquals("Resource should match", context.getResource(), doubleGauge.getResource());
          assertEquals("Labels should match", labels,
                       toMap(doubleGauge.getMetricDescriptor(), doubleGauge.getTimeseries(0)));
          assertEquals("Types should match", Type.GAUGE_DOUBLE, doubleGauge.getMetricDescriptor().getType());
          assertEquals("Value should match", 99d, doubleGauge.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-9);
        });

    result.stream().filter(metric -> metric.getMetricDescriptor().getName().matches("test-domain/group1/(int|long)")).forEach(
        intGauge -> {
          assertEquals("Resource should match", context.getResource(), intGauge.getResource());
          assertEquals("Labels should match", labels,
                       toMap(intGauge.getMetricDescriptor(), intGauge.getTimeseries(0)));
          assertEquals("Types should match", Type.GAUGE_INT64, intGauge.getMetricDescriptor().getType());
          assertEquals("Value should match", 100, intGauge.getTimeseries(0).getPoints(0).getInt64Value());
        });
  }

  @Test
  public void metricRemoval() {
    LastValueTracker<Double> lastValueTracker = Mockito.spy(new LastValueTracker<>());

    ledger = new KafkaMetricsCollector.StateLedger(lastValueTracker, Clock.systemUTC());
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .build();

    Map<String, String> tags = new HashMap<>();
    tags.put("tag", "value");

    MetricName metricName = metrics.metricName("name1", "group1", tags);
    metrics.addMetric(metricName, new Measurable() {
      public double measure(MetricConfig config, long now) {
        return 100.0;
      }
    });

    collector.collect(exporter);
    assertEquals(2, exporter.emittedMetrics().size());

    metrics.removeMetric(metricName);
    // verify that remove was called on the lastValueTracker
    Mockito.verify(lastValueTracker).remove(ledger.toKey(metricName));

    // verify that the metric was removed and that all that remains is the global count of metrics.
    exporter.reset();
    collector.collect(exporter);
    List<Metric> collected = exporter.emittedMetrics();
    assertEquals(1, collected.size()); // metric for count of metrics.
    assertEquals("test-domain/count/count", collected.get(0).getMetricDescriptor().getName());
  }

  @Test
  public void initialDeltaDouble() {
    Clock clock = mock(Clock.class);
    Instant reference = Instant.ofEpochMilli(1000L);
    when(clock.instant()).thenReturn(reference);

    LastValueTracker<Double> lastValueTracker = Mockito.spy(new LastValueTracker<>());

    ledger = new KafkaMetricsCollector.StateLedger(lastValueTracker, clock);
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .setClock(clock)
        .build();

    Map<String, String> tags = new HashMap<>();
    tags.put("tag", "value");

    MetricName metricName = metrics.metricName("name1", "group1", tags);
    Sensor sensor = metrics.sensor("test");
    sensor.add(metricName, new WindowedCount());

    sensor.record();
    sensor.record();
    when(clock.instant()).thenReturn(reference.plusSeconds(60));

    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();

    assertEquals(3, result.size()); // kafka_metrics_count and two metrics -> total, total/delta

    Metric deltaMetric = result.stream().filter(metric -> metric.getMetricDescriptor().getName().endsWith("/delta")).findFirst().get();

    assertEquals(2d, deltaMetric.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-6);
    assertEquals(61L, deltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(1L, deltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());
  }

  @Test
  public void secondDeltaDouble() {
    Clock clock = mock(Clock.class);
    Instant reference = Instant.ofEpochMilli(1000L);
    when(clock.instant()).thenReturn(reference);

    ledger = new KafkaMetricsCollector.StateLedger(new LastValueTracker<>(), clock);
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .setClock(clock)
        .build();


    Map<String, String> tags = new HashMap<>();
    tags.put("tag", "value");

    MetricName metricName = metrics.metricName("name1", "group1", tags);
    Sensor sensor = metrics.sensor("test");
    sensor.add(metricName, new CumulativeSum());

    sensor.record();
    sensor.record();
    when(clock.instant()).thenReturn(reference.plusSeconds(60));

    collector.collect(exporter);

    // Update it again by 5 and advance time by another 60 seconds.
    sensor.record();
    sensor.record();
    sensor.record();
    sensor.record();
    sensor.record();
    when(clock.instant()).thenReturn(reference.plusSeconds(120));

    exporter.reset();
    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();

    assertEquals(3, result.size()); // kafka_metrics_count and two metrics -> total, total/delta


    Metric deltaMetric = result.stream().filter(metric -> metric.getMetricDescriptor().getName().endsWith("/delta")).findFirst().get();

    assertEquals(5d, deltaMetric.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-6);
    assertEquals(121L, deltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(61L, deltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());
  }

  @Test
  public void testCollectFilter() {
    metrics.addMetric(metricName, new Measurable() {
      public double measure(MetricConfig config, long now) {
        return 100.0;
      }
    });
    ledger = new KafkaMetricsCollector.StateLedger();
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .setMetricWhitelistFilter(metric -> !metric.getName().endsWith("/count"))
        .build();

    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();

    assertEquals("Should get exactly 1 Kafka measurables because we exclued the count measurable", 1, result.size());

    Metric counter = result.get(0);

    assertEquals("Resource should match", context.getResource(), counter.getResource());
    assertEquals("Types should match", Type.GAUGE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Labels should match", labels, toMap(counter.getMetricDescriptor(), counter.getTimeseries(0)));
    assertEquals("Value should match", 100L, counter.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-9);
  }

  @Test
  public void testCollectFilterDynamicWhitelist() {
    metrics.addMetric(metricName, new Measurable() {
      public double measure(MetricConfig config, long now) {
        return 100.0;
      }
    });
    metrics.addMetric(metrics.metricName("name2", "group2", tags), new Measurable() {
      public double measure(MetricConfig config, long now) {
        return 100.0;
      }
    });
    ledger = new KafkaMetricsCollector.StateLedger();
    metrics.addReporter(ledger);
    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .setMetricWhitelistFilter(key -> true)
        .build();

    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();
    assertThat(result).hasSize(3);  // name1, name2, count

    exporter.reset();
    collector.reconfigureWhitelist(key -> key.getName().endsWith("/count"));
    collector.collect(exporter);
    result = exporter.emittedMetrics();
    assertThat(result).hasSize(1);  // count

    exporter.reset();
    collector.reconfigureWhitelist(key -> key.getName().contains("name"));
    collector.collect(exporter);
    result = exporter.emittedMetrics();
    assertThat(result).hasSize(2);  // name1, name2

    exporter.reset();
    collector.reconfigureWhitelist(key -> true);
    collector.collect(exporter);
    result = exporter.emittedMetrics();
    assertThat(result).hasSize(3);  // name1, name2, count
  }

  @Test
  public void testCollectFilterWithDerivedMetrics() {
    MetricName name1 = metrics.metricName("nonMeasurable", "group1", tags);
    MetricName name2 = metrics.metricName("windowed", "group1", tags);
    MetricName name3 = metrics.metricName("cumulative", "group1", tags);

    metrics.addMetric(name1, (Gauge<Double>) (config, now) -> 99d);

    Sensor sensor = metrics.sensor("test");
    sensor.add(name2, new WindowedCount());
    sensor.add(name3, new CumulativeSum());

    ledger = new KafkaMetricsCollector.StateLedger();
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .build();


    collector.collect(exporter);
    List<Metric> result = exporter.emittedMetrics();

    // no-filter shall result in all 6 data metrics.
    assertThat(result).hasSize(6);

    exporter.reset();
    collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .setMetricWhitelistFilter(metric -> !metric.getName().endsWith("/count"))
        .build();
    collector.collect(exporter);
    result = exporter.emittedMetrics();

    // Drop metrics for Count type (Measurable metric but other that Windowed or Cumulative).
    assertThat(result).hasSize(5);

    exporter.reset();
    collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .setMetricWhitelistFilter(metric -> !metric.getName().endsWith("/non_measurable"))
        .build();
    collector.collect(exporter);
    result = exporter.emittedMetrics();

    // Drop non-measurable metric.
    assertThat(result).hasSize(5);

    exporter.reset();
    collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .setMetricWhitelistFilter(metric -> !metric.getName().endsWith("/delta"))
        .build();
    collector.collect(exporter);
    result = exporter.emittedMetrics();

    // Drop all delta derived metrics.
    assertThat(result).hasSize(4);
  }
}
