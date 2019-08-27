package io.confluent.telemetry.collector;


import static io.confluent.telemetry.collector.MetricsTestUtils.toMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.telemetry.Context;
import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.TelemetryResourceType;
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

  private Map<String, String> tags;
  private Map<String, String> labels;
  private Metrics metrics;
  private MetricName metricName;
  private KafkaMetricsCollector.StateLedger ledger;

  private final Context context = new Context(
      new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
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


    List<Metric> result = collector.collect();

    assertEquals("Should get exactly 3 Kafka measurables since Metrics always includes a count measurable", 3, result.size());

    Metric counter = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1")).findFirst().get();
    Metric delta = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1/delta")).findFirst().get();

    assertEquals("Types should match", Type.CUMULATIVE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Types should match", Type.GAUGE_DOUBLE, delta.getMetricDescriptor().getType());
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

    List<Metric> result = collector.collect();

    assertEquals("Should get exactly 3 Kafka measurables since Metrics always includes a count measurable", 3, result.size());

    Metric counter = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1")).findFirst().get();
    Metric delta = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1/delta")).findFirst().get();

    assertEquals("Resource should match", context.getResource(), counter.getResource());
    assertEquals("Resource should match", context.getResource(), delta.getResource());
    assertEquals("Types should match", Type.CUMULATIVE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Types should match", Type.GAUGE_DOUBLE, delta.getMetricDescriptor().getType());
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

    List<Metric> result = collector.collect();

    assertEquals("Should get exactly 2 Kafka measurables since Metrics always includes a count measurable", 2, result.size());

    Metric counter = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1")).findFirst().get();

    assertEquals("Resource should match", context.getResource(), counter.getResource());
    assertEquals("Types should match", Type.GAUGE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Labels should match", labels, toMap(counter.getMetricDescriptor(), counter.getTimeseries(0)));
    assertEquals("Value should match", 100L, counter.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-9);
  }

  @Test
  public void testNonMeasurable() {
    metrics.addMetric(metricName, new Gauge<Double>() {

      @Override
      public Double value(MetricConfig config, long now) {
        return 99d;
      }
    });
    ledger = new KafkaMetricsCollector.StateLedger();
    metrics.addReporter(ledger);

    KafkaMetricsCollector collector = KafkaMetricsCollector.newBuilder()
        .setContext(context)
        .setDomain("test-domain")
        .setLedger(ledger)
        .build();

    List<Metric> result = collector.collect();

    assertEquals("Should get exactly 2 Kafka measurables since Metrics always includes a count measurable", 2, result.size());

    Metric counter = result.stream().filter(metric -> metric.getMetricDescriptor().getName().equals("test-domain/group1/name1")).findFirst().get();

    assertEquals("Resource should match", context.getResource(), counter.getResource());
    assertEquals("Types should match", Type.GAUGE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Labels should match", labels, toMap(counter.getMetricDescriptor(), counter.getTimeseries(0)));
    assertEquals("Value should match", 99d, counter.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-9);
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

    assertEquals(2, collector.collect().size());

    metrics.removeMetric(metricName);
    // verify that remove was called on the lastValueTracker
    Mockito.verify(lastValueTracker).remove(ledger.toKey(metricName));

    // verify that the metric was removed and that all that remains is the global count of metrics.
    List<Metric> collected = collector.collect();
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

    List<Metric> collected = collector.collect();

    assertEquals(3, collected.size()); // kafka_metrics_count and two metrics -> total, total/delta

    Metric deltaMetric = collected.stream().filter(metric -> metric.getMetricDescriptor().getName().endsWith("/delta")).findFirst().get();

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

    collector.collect();

    // Update it again by 5 and advance time by another 60 seconds.
    sensor.record();
    sensor.record();
    sensor.record();
    sensor.record();
    sensor.record();
    when(clock.instant()).thenReturn(reference.plusSeconds(120));

    List<Metric> collected = collector.collect();

    assertEquals(3, collected.size()); // kafka_metrics_count and two metrics -> total, total/delta


    Metric deltaMetric = collected.stream().filter(metric -> metric.getMetricDescriptor().getName().endsWith("/delta")).findFirst().get();

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
        .setMetricFilter(metric -> !metric.getName().endsWith("/count"))
        .build();

    List<Metric> result = collector.collect();

    assertEquals("Should get exactly 1 Kafka measurables because we exclued the count measurable", 1, result.size());

    Metric counter = result.get(0);

    assertEquals("Resource should match", context.getResource(), counter.getResource());
    assertEquals("Types should match", Type.GAUGE_DOUBLE, counter.getMetricDescriptor().getType());
    assertEquals("Labels should match", labels, toMap(counter.getMetricDescriptor(), counter.getTimeseries(0)));
    assertEquals("Value should match", 100L, counter.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-9);
  }
}