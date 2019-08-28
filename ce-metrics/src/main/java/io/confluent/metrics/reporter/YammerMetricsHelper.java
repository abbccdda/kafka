// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.metrics.reporter;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import io.confluent.metrics.record.ConfluentMetric.YammerGauge;
import io.confluent.metrics.record.ConfluentMetric.YammerHistogram;
import io.confluent.metrics.record.ConfluentMetric.YammerMeter;
import io.confluent.metrics.record.ConfluentMetric.YammerMetricName;
import io.confluent.metrics.record.ConfluentMetric.YammerTimer;

public class YammerMetricsHelper {

  private static final Logger log = LoggerFactory.getLogger(ConfluentMetricsReporter.class);

  private final YammerMetricName.Builder metricNameBuilder = YammerMetricName.newBuilder();
  private final YammerGauge.Builder gaugeBuilder = YammerGauge.newBuilder();
  private final YammerMeter.Builder meterBuilder = YammerMeter.newBuilder();
  private final YammerHistogram.Builder histogramBuilder = YammerHistogram.newBuilder();
  private final YammerTimer.Builder timerBuilder = YammerTimer.newBuilder();

  private final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

  private long delta(
      MetricName metric,
      long count
  ) {
    final String name = metric.getMBeanName();
    counters.putIfAbsent(name, new AtomicLong(count));
    return count - counters.get(name).getAndSet(count);
  }

  public static class YammerMetricsResult {

    public final List<YammerGauge> gauges;
    public final List<YammerMeter> meters;
    public final List<YammerHistogram> histograms;
    public final List<YammerTimer> timers;

    public YammerMetricsResult(
        List<YammerGauge> gauges,
        List<YammerMeter> meters,
        List<YammerHistogram> histograms,
        List<YammerTimer> timers
    ) {
      this.gauges = gauges;
      this.meters = meters;
      this.histograms = histograms;
      this.timers = timers;
    }
  }

  public YammerMetricsResult collectYammerMetrics(
      MetricsRegistry metricsRegistry,
      Pattern pattern
  ) {
    Set<Map.Entry<MetricName, Metric>> metrics = metricsRegistry.allMetrics().entrySet();
    List<YammerGauge> yammerGauges = new ArrayList<>();
    List<YammerMeter> yammerMeters = new ArrayList<>();
    List<YammerHistogram> yammerHistograms = new ArrayList<>();
    List<YammerTimer> yammerTimers = new ArrayList<>();

    for (Map.Entry<MetricName, Metric> entry : metrics) {
      MetricName metricName = entry.getKey();
      if (pattern != null && !pattern.matcher(metricName.getMBeanName()).matches()) {
        continue;
      }
      try {
        metricNameBuilder.clear();
        metricNameBuilder.setGroup(Utils.notNullOrEmpty(metricName.getGroup()));
        metricNameBuilder.setName(Utils.notNullOrEmpty(metricName.getName()));
        metricNameBuilder.setType(Utils.notNullOrEmpty(metricName.getType()));
        metricNameBuilder.setScope(Utils.notNullOrEmpty(metricName.getScope()));
        metricNameBuilder.setMBeanName(Utils.notNullOrEmpty(metricName.getMBeanName()));

        Metric metric = entry.getValue();
        if (metric instanceof Gauge) {
          yammerGauges.add(handleGauge((Gauge) metric));
        } else if (metric instanceof Meter) {
          yammerMeters.add(handleMeter(metricName, (Meter) metric));
        } else if (metric instanceof Histogram) {
          yammerHistograms.add(handleHistogram(metricName, (Histogram) metric));
        } else if (metric instanceof Timer) {
          yammerTimers.add(handleTimer(metricName, (Timer) metric));
        }
        // Kafka current doesn't use other types of Yammer metrics.
      } catch (Exception e) {
        // Due to KAFKA-4286, we could get NullPointerException when
        // reading a Yammer metric. It's better
        // to continue so that we can collect other metrics.
        log.warn("Unexpected error in processing Yammer metric {}", metricName, e);
      }
    }

    return new YammerMetricsResult(yammerGauges, yammerMeters, yammerHistograms, yammerTimers);
  }

  private YammerTimer handleTimer(MetricName metricName, Timer metric) {
    Timer timer = metric;
    timerBuilder.clear();
    timerBuilder.setMetricName(metricNameBuilder.build());
    final long count = timer.count();
    timerBuilder.setCount(count);
    timerBuilder.setDeltaCount(delta(metricName, count));
    timerBuilder.setMax(timer.max());
    timerBuilder.setMin(timer.min());
    timerBuilder.setMean(timer.mean());
    timerBuilder.setStdDev(timer.stdDev());
    timerBuilder.setSum(timer.sum());
    Snapshot snapshot = timer.getSnapshot();
    timerBuilder.setMedian(snapshot.getMedian());
    timerBuilder.setPercentile75Th(snapshot.get75thPercentile());
    timerBuilder.setPercentile95Th(snapshot.get95thPercentile());
    timerBuilder.setPercentile98Th(snapshot.get98thPercentile());
    timerBuilder.setPercentile99Th(snapshot.get99thPercentile());
    timerBuilder.setPercentile999Th(snapshot.get999thPercentile());
    timerBuilder.setSize(snapshot.size());
    timerBuilder.setOneMinuteRate(timer.oneMinuteRate());
    timerBuilder.setFiveMinuteRate(timer.fiveMinuteRate());
    timerBuilder.setFifteenMinuteRate(timer.fifteenMinuteRate());
    timerBuilder.setMeanRate(timer.meanRate());
    return timerBuilder.build();
  }

  private YammerHistogram handleHistogram(MetricName metricName, Histogram metric) {
    Histogram histogram = metric;
    histogramBuilder.clear();
    histogramBuilder.setMetricName(metricNameBuilder.build());
    final long count = histogram.count();
    histogramBuilder.setCount(count);
    histogramBuilder.setDeltaCount(delta(metricName, count));
    histogramBuilder.setMax(histogram.max());
    histogramBuilder.setMin(histogram.min());
    histogramBuilder.setMean(histogram.mean());
    histogramBuilder.setStdDev(histogram.stdDev());
    histogramBuilder.setSum(histogram.sum());
    Snapshot snapshot = histogram.getSnapshot();
    histogramBuilder.setMedian(snapshot.getMedian());
    histogramBuilder.setPercentile75Th(snapshot.get75thPercentile());
    histogramBuilder.setPercentile95Th(snapshot.get95thPercentile());
    histogramBuilder.setPercentile98Th(snapshot.get98thPercentile());
    histogramBuilder.setPercentile99Th(snapshot.get99thPercentile());
    histogramBuilder.setPercentile999Th(snapshot.get999thPercentile());
    histogramBuilder.setSize(snapshot.size());
    return histogramBuilder.build();
  }

  private YammerMeter handleMeter(MetricName metricName, Meter metric) {
    Meter meter = metric;
    meterBuilder.clear();
    meterBuilder.setMetricName(metricNameBuilder.build());
    final long count = meter.count();
    meterBuilder.setCount(count);
    meterBuilder.setDeltaCount(delta(metricName, count));
    meterBuilder.setOneMinuteRate(meter.oneMinuteRate());
    meterBuilder.setFiveMinuteRate(meter.fiveMinuteRate());
    meterBuilder.setFifteenMinuteRate(meter.fifteenMinuteRate());
    meterBuilder.setMeanRate(meter.meanRate());
    return meterBuilder.build();
  }

  private YammerGauge handleGauge(Gauge metric) {
    Gauge gauge = metric;
    gaugeBuilder.clear();
    gaugeBuilder.setMetricName(metricNameBuilder.build());
    Object value = gauge.value();
    gaugeBuilder.setValue(value == null ? Utils.EMPTY_STRING : value.toString());
    if (value instanceof Integer || value instanceof Long) {
      gaugeBuilder.setLongValue(((Number) value).longValue());
    } else if (value instanceof Float || value instanceof Double) {
      gaugeBuilder.setDoubleValue(((Number) value).doubleValue());
    }
    return gaugeBuilder.build();
  }
}
