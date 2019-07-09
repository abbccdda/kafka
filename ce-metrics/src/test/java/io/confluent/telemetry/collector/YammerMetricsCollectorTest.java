package io.confluent.telemetry.collector;


import static io.confluent.telemetry.collector.MetricsTestUtils.toMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int64Value;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.SummaryValue;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class YammerMetricsCollectorTest {

  private YammerMetricsCollector.Builder collectorBuilder;
  private MetricsRegistry metricsRegistry;
  private MetricName metricName;
  private Map<String, String> labels;

  @Before
  public void setUp() {
    metricsRegistry = new MetricsRegistry();
    collectorBuilder = YammerMetricsCollector.newBuilder()
        .setContext(new Context(ImmutableMap.<String, String>builder().build()))
        .setDomain("test-domain")
        .setMetricsRegistry(metricsRegistry);
    metricName = new MetricName("group1", "type1", "name1", "scope1");
    labels = new HashMap<>();

  }

  @Test
  public void simpleGauge() {
    metricsRegistry.newGauge(metricName,
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            return 100;
          }
        });

    YammerMetricsCollector collector = collectorBuilder.build();
    List<Metric> result = collector.collect();

    assertEquals("Should get exactly 1 metric", 1, result.size());

    // Name, Type, value, labels
    Metric metric = result.get(0);

    assertEquals("Name should match", "test-domain/type1/name1", metric.getMetricDescriptor().getName());
    assertEquals("Type should match", Type.GAUGE_INT64, metric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(metric.getMetricDescriptor(), metric.getTimeseries(0)));
    assertEquals("values should match", 100L, metric.getTimeseries(0).getPoints(0).getInt64Value());
  }

  @Test
  public void simpleMeter() {
    Meter meter = metricsRegistry.newMeter(metricName, "meterType", TimeUnit.SECONDS);
    meter.mark(100L);

    YammerMetricsCollector collector = collectorBuilder.build();
    List<Metric> result = collector.collect();


    assertEquals("Should get exactly 2 metrics", 2, result.size());


    // meter (counter) metric
    Metric meterMetric = result.stream()
        .filter(m -> m.getMetricDescriptor().getName().equals("test-domain/type1/name1/total"))
        .findFirst().get();

    assertEquals("Type should match", Type.CUMULATIVE_INT64, meterMetric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(meterMetric.getMetricDescriptor(), meterMetric.getTimeseries(0)));
    assertEquals("values should match", 100L, meterMetric.getTimeseries(0).getPoints(0).getInt64Value());


    // getAndSet metric.
    Metric deltaMetric = result.stream().filter(m -> m.getMetricDescriptor().getName().contains("/delta")).findFirst().get();

    assertEquals("Name should match", "test-domain/type1/name1/total/delta", deltaMetric.getMetricDescriptor().getName());
    assertEquals("Type should match", Type.GAUGE_INT64, deltaMetric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(deltaMetric.getMetricDescriptor(), deltaMetric.getTimeseries(0)));
    assertEquals("values should match", 100L, deltaMetric.getTimeseries(0).getPoints(0).getInt64Value());


    // mark and check getAndSet again.
    meter.mark(150);
    meter.mark(175);

    result = collector.collect();
    deltaMetric = result.stream().filter(m -> m.getMetricDescriptor().getName().contains("/delta")).findFirst().get();
    assertEquals("InstantAndValue should match", 325L, deltaMetric.getTimeseries(0).getPoints(0).getInt64Value());
  }

  @Test
  public void simpleHistogram() {
    Histogram histogram = metricsRegistry.newHistogram(metricName, false);
    histogram.update(15L);
    histogram.update(95L);

    YammerMetricsCollector collector = collectorBuilder.build();
    List<Metric> result = collector.collect();

    // we get a /time/delta and a /total/delta in addition to the main metric.

    assertEquals("Should get exactly 3 metrics", 3, result.size());

    Metric summaryMetric = result.stream()
        .filter(m -> m.getMetricDescriptor().getName().equals("test-domain/type1/name1"))
        .findFirst().get();

    SummaryValue expectedSummary = SummaryValue.newBuilder()
        .setCount(Int64Value.newBuilder().setValue(2).build())
        .setSnapshot(
            SummaryValue.Snapshot.newBuilder()
              .setSum(DoubleValue.newBuilder().setValue(110d).build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(50.0)
                    .setValue(55d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(75.0)
                    .setValue(95d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(95.0)
                    .setValue(95d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(98.0)
                    .setValue(95d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(99.0)
                    .setValue(95d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(99.9)
                    .setValue(95d)
                    .build())
                .build()
        )
        .build();

    assertEquals("Type should match", Type.SUMMARY, summaryMetric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(summaryMetric.getMetricDescriptor(), summaryMetric.getTimeseries(0)));
    assertEquals("summaries should match", expectedSummary, summaryMetric.getTimeseries(0).getPoints(0).getSummaryValue());

    // time getAndSet
    Metric deltaMetric = result.stream().filter(m -> m.getMetricDescriptor().getName().contains("/time/delta")).findFirst().get();

    assertEquals("Name should match", "test-domain/type1/name1/time/delta", deltaMetric.getMetricDescriptor().getName());
    assertEquals("Type should match", Type.GAUGE_DOUBLE, deltaMetric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(deltaMetric.getMetricDescriptor(), deltaMetric.getTimeseries(0)));
    assertEquals("values should match", 110d, deltaMetric.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-9);

    // total getAndSet
    deltaMetric = result.stream().filter(m -> m.getMetricDescriptor().getName().contains("/total/delta")).findFirst().get();

    assertEquals("Name should match", "test-domain/type1/name1/total/delta", deltaMetric.getMetricDescriptor().getName());
    assertEquals("Type should match", Type.GAUGE_INT64, deltaMetric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(deltaMetric.getMetricDescriptor(), deltaMetric.getTimeseries(0)));
    assertEquals("values should match", 2L, deltaMetric.getTimeseries(0).getPoints(0).getInt64Value());
  }

  @Test
  public void simpleTimer() {
    Timer timer = metricsRegistry.newTimer(metricName, TimeUnit.SECONDS, TimeUnit.SECONDS);
    timer.update(15L, TimeUnit.SECONDS);
    timer.update(95L, TimeUnit.SECONDS);

    YammerMetricsCollector collector = collectorBuilder.build();
    List<Metric> result = collector.collect();

    // we get a /time/delta and a /total/delta in addition to the main metric.

    assertEquals("Should get exactly 3 metrics", 3, result.size());

    Metric summaryMetric = result.stream()
        .filter(m -> m.getMetricDescriptor().getName().equals("test-domain/type1/name1"))
        .findFirst().get();

    SummaryValue expectedSummary = SummaryValue.newBuilder()
        .setCount(Int64Value.newBuilder().setValue(2).build())
        .setSnapshot(
            SummaryValue.Snapshot.newBuilder()
                .setSum(DoubleValue.newBuilder().setValue(110d).build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(50.0)
                    .setValue(55.0d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(75.0)
                    .setValue(95d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(95.0)
                    .setValue(95d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(98.0)
                    .setValue(95d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(99.0)
                    .setValue(95d)
                    .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                    .setPercentile(99.9)
                    .setValue(95d)
                    .build())
                .build()
        )
        .build();

    assertEquals("Type should match", Type.SUMMARY, summaryMetric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(summaryMetric.getMetricDescriptor(), summaryMetric.getTimeseries(0)));
    assertEquals("summaries should match", expectedSummary, summaryMetric.getTimeseries(0).getPoints(0).getSummaryValue());

    // time getAndSet
    Metric deltaMetric = result.stream().filter(m -> m.getMetricDescriptor().getName().contains("/time/delta")).findFirst().get();

    assertEquals("Name should match", "test-domain/type1/name1/time/delta", deltaMetric.getMetricDescriptor().getName());
    assertEquals("Type should match", Type.GAUGE_DOUBLE, deltaMetric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(deltaMetric.getMetricDescriptor(), deltaMetric.getTimeseries(0)));
    assertEquals("values should match", 110d, deltaMetric.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-9);

    // total getAndSet
    deltaMetric = result.stream().filter(m -> m.getMetricDescriptor().getName().contains("/total/delta")).findFirst().get();

    assertEquals("Name should match", "test-domain/type1/name1/total/delta", deltaMetric.getMetricDescriptor().getName());
    assertEquals("Type should match", Type.GAUGE_INT64, deltaMetric.getMetricDescriptor().getType());
    assertEquals("labels should match", labels, toMap(deltaMetric.getMetricDescriptor(), deltaMetric.getTimeseries(0)));
    assertEquals("values should match", 2L, deltaMetric.getTimeseries(0).getPoints(0).getInt64Value());
  }

  @Test
  public void metricRemoval() {
    LastValueTracker<Long> lastValueTracker = Mockito.spy(new LastValueTracker<>());

    YammerMetricsCollector collector = collectorBuilder
        .setLongDeltas(lastValueTracker)
        .build();
    MetricName name = new MetricName("group", "gauge", "test", "scope");

    metricsRegistry.newGauge(name, new Gauge<Long>() {

      @Override
      public Long value() {
        return 1L;
      }
    });

    assertEquals(1, collector.collect().size());

    metricsRegistry.removeMetric(name);

    // verify that remove was called on the lastValueTracker
    Mockito.verify(lastValueTracker).remove(collector.toMetricKey(name));

    // verify that the metric was removed and that all that remains is the global count of metrics.
    List<Metric> collected = collector.collect();
    assertEquals(Collections.emptyList(), collected);
  }

  @Test
  public void initialDeltaLong() {
    Clock clock = mock(Clock.class);
    Instant reference = Instant.ofEpochMilli(1000L);
    when(clock.instant()).thenReturn(reference);
    YammerMetricsCollector collector = collectorBuilder
        .setClock(clock)
        .build();

    MetricName name = new MetricName("group", "counter", "test", "scope");

    Counter counter = metricsRegistry.newCounter(name);
    counter.inc(32L);

    when(clock.instant()).thenReturn(reference.plusSeconds(60));
    List<Metric> metrics = collector.collect();
    assertEquals(2, metrics.size());
    Metric deltaMetric = metrics.stream().filter(metric -> metric.getMetricDescriptor().getName().endsWith("/delta")).findFirst().get();

    assertEquals(32L, deltaMetric.getTimeseries(0).getPoints(0).getInt64Value());
    assertEquals(61L, deltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(1L, deltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());
  }

  @Test
  public void secondDeltaLong() {
    Clock clock = mock(Clock.class);
    Instant reference = Instant.ofEpochMilli(1000L);
    when(clock.instant()).thenReturn(reference);
    YammerMetricsCollector collector = collectorBuilder
        .setClock(clock)
        .build();

    MetricName name = new MetricName("group", "counter", "test", "scope");

    Counter counter = metricsRegistry.newCounter(name);

    // increment by 32 and advance time by 60 seconds. Do the initial collection
    counter.inc(32L);
    when(clock.instant()).thenReturn(reference.plusSeconds(60));
    collector.collect();

    // Increment it again by 5 and advance time by another 60 seconds.
    counter.inc(5);
    when(clock.instant()).thenReturn(reference.plusSeconds(120));

    List<Metric> metrics = collector.collect();
    assertEquals(2, metrics.size());
    Metric deltaMetric = metrics.stream()
        .filter(metric -> metric.getMetricDescriptor().getName().endsWith("/delta")).findFirst()
        .get();

    assertEquals(5L, deltaMetric.getTimeseries(0).getPoints(0).getInt64Value());
    assertEquals(121L, deltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(61L, deltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());
  }


  @Test
  public void initialDeltaDouble() {
    Clock clock = mock(Clock.class);
    Instant reference = Instant.ofEpochMilli(1000L);
    when(clock.instant()).thenReturn(reference);
    YammerMetricsCollector collector = collectorBuilder
        .setClock(clock)
        .build();

    MetricName name = new MetricName("group", "histogram", "test", "scope");

    Histogram histogram = metricsRegistry.newHistogram(name, false);
    histogram.update(10);

    when(clock.instant()).thenReturn(reference.plusSeconds(60));
    List<Metric> metrics = collector.collect();
    assertEquals(3, metrics.size()); // three metrics -> summary, time/delta, total/delta
    Metric deltaMetric = metrics.stream().filter(metric -> metric.getMetricDescriptor().getName().endsWith("/time/delta")).findFirst().get();

    assertEquals(10d, deltaMetric.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-6);
    assertEquals(61L, deltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(1L, deltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());
  }


  @Test
  public void secondDeltaDouble() {
    Clock clock = mock(Clock.class);
    Instant reference = Instant.ofEpochMilli(1000L);
    when(clock.instant()).thenReturn(reference);
    YammerMetricsCollector collector = collectorBuilder
        .setClock(clock)
        .build();

    MetricName name = new MetricName("group", "histogram", "test", "scope");

    Histogram histogram = metricsRegistry.newHistogram(name, false);
    histogram.update(10);
    when(clock.instant()).thenReturn(reference.plusSeconds(60));
    collector.collect();

    // Update it again by 5 and advance time by another 60 seconds.
    histogram.update(5);
    when(clock.instant()).thenReturn(reference.plusSeconds(120));


    List<Metric> metrics = collector.collect();
    assertEquals(3, metrics.size()); // three metrics -> summary, time/delta, total/delta


    Metric countDeltaMetric = metrics.stream().filter(metric -> metric.getMetricDescriptor().getName().endsWith("/total/delta")).findFirst().get();

    assertEquals(1, countDeltaMetric.getTimeseries(0).getPoints(0).getInt64Value());
    assertEquals(121L, countDeltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(61L, countDeltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());

    Metric timeDeltaMetric = metrics.stream().filter(metric -> metric.getMetricDescriptor().getName().endsWith("/time/delta")).findFirst().get();

    assertEquals(5d, timeDeltaMetric.getTimeseries(0).getPoints(0).getDoubleValue(), 1e-6);
    assertEquals(121L, timeDeltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(61L, timeDeltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());
  }

  @Test
  public void testDeltaDifferentLabels() {
    Clock clock = mock(Clock.class);
    Instant reference = Instant.ofEpochMilli(1000L);
    when(clock.instant()).thenReturn(reference);
    YammerMetricsCollector collector = collectorBuilder
        .setClock(clock)
        .build();

    MetricName name1 = new MetricName("group1", "counter", "test", "scope", "group1:type=counter,name=test,foo=bar");
    MetricName name2 = new MetricName("group2", "counter", "test", "scope", "group2:type=counter,name=test,baz=boo");

    Counter counter1 = metricsRegistry.newCounter(name1);
    counter1.inc(32L);

    Counter counter2 = metricsRegistry.newCounter(name2);
    counter2.inc(48L);

    when(clock.instant()).thenReturn(reference.plusSeconds(60));
    List<Metric> metrics = collector.collect();
    assertEquals(4, metrics.size());
    Map<MetricKey, List<Metric>> deltaMetrics = metrics.stream().filter(
        metric -> metric.getMetricDescriptor().getName().equals("test-domain/counter/test/delta"))
        .collect(Collectors.groupingBy(
            (Metric m) -> new MetricKey(m.getMetricDescriptor().getName(),
                toMap(m.getMetricDescriptor(), m.getTimeseries(0)))));

    Map<String, String> metric1Labels = new HashMap<>();
    metric1Labels.putAll(labels);
    metric1Labels.put("foo", "bar");
    Metric deltaMetric = deltaMetrics
        .get(new MetricKey("test-domain/counter/test/delta", metric1Labels)).get(0);

    assertEquals(32L, deltaMetric.getTimeseries(0).getPoints(0).getInt64Value());
    assertEquals(61L, deltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(1L, deltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());


    Map<String, String> metric2Labels = new HashMap<>();
    metric2Labels.putAll(labels);
    metric2Labels.put("baz", "boo");
    deltaMetric = deltaMetrics
        .get(new MetricKey("test-domain/counter/test/delta", metric2Labels)).get(0);


    assertEquals(48L, deltaMetric.getTimeseries(0).getPoints(0).getInt64Value());
    assertEquals(61L, deltaMetric.getTimeseries(0).getPoints(0).getTimestamp().getSeconds());
    assertEquals(1L, deltaMetric.getTimeseries(0).getStartTimestamp().getSeconds());
  }

  @Test
  public void testCollectFilter() {
    MetricName name2 = new MetricName("group2", "gauge", "testDoNotInclude", "scope");

    metricsRegistry.newGauge(metricName,
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            return 100;
          }
        });

    metricsRegistry.newGauge(name2,
        new Gauge<Integer>() {
          @Override
          public Integer value() {
            return 999;
          }
        });


    YammerMetricsCollector collector = collectorBuilder
        .build();
    List<Metric> result = collector.collect();

    assertEquals("Should get exactly 2 metrics", 2, result.size());

    collector = collectorBuilder
        .setMetricFilter(key -> !key.getName().contains("test_do_not_include"))
        .build();
    result = collector.collect();
    assertEquals("Should get exactly 1 metric", 1, result.size());

    // Name, Type, value, labels
    Metric metric = result.get(0);

    assertEquals("Name should match", "test-domain/type1/name1", metric.getMetricDescriptor().getName());
  }
}