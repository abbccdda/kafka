package io.confluent.telemetry.collector;

import com.google.common.base.Strings;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int64Value;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import com.yammer.metrics.core.Timer;
import io.confluent.metrics.YammerMetricsUtils;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.collector.CPUMetricsCollector.Builder;
import io.confluent.telemetry.collector.LastValueTracker.InstantAndValue;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.SummaryValue;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Yammer -> Opencensus is based on : https://github.com/census-instrumentation/opencensus-java/blob/master/contrib/dropwizard/src/main/java/io/opencensus/contrib/dropwizard/DropWizardMetrics.java
public class YammerMetricsCollector implements MetricsCollector {
    private static final Logger log = LoggerFactory.getLogger(YammerMetricsCollector.class);

    public static final String YAMMER_METRICS = "yammer";
    static final String DEFAULT_UNIT = "1";
    static final String NS_UNIT = "ns";

    private String domain;
    private MetricsRegistry metricsRegistry;
    private Predicate<MetricKey> metricFilter;
    private Context context;
    private final Clock clock;
    private final LastValueTracker<Long> longDeltas;
    private final LastValueTracker<Double> doubleDeltas;

    private final Map<MetricKey, Instant> metricAdded = new ConcurrentHashMap<>();

    public YammerMetricsCollector(String domain, MetricsRegistry metricsRegistry, Predicate<MetricKey> metricFilter, Context context, LastValueTracker<Long> longDeltas, LastValueTracker<Double> doubleDeltas, Clock clock) {
        this.domain = domain;
        this.metricsRegistry = metricsRegistry;
        this.metricFilter = metricFilter;
        this.context = context;
        this.clock = clock;
        this.longDeltas = longDeltas;
        this.doubleDeltas = doubleDeltas;

        this.setupMetricListener();
    }

    private void setupMetricListener() {
        metricsRegistry.addListener(new MetricsRegistryListener() {
            @Override
            public void onMetricAdded(MetricName name, com.yammer.metrics.core.Metric metric) {
                metricAdded.put(toMetricKey(name), Instant.now(clock));
            }

            @Override
            public void onMetricRemoved(MetricName metricName) {
                MetricKey key = toMetricKey(metricName);
                longDeltas.remove(key);
                doubleDeltas.remove(key);
                metricAdded.remove(key);
            }
        });
    }

    // package private for testing.
    MetricKey toMetricKey(com.yammer.metrics.core.MetricName metricName) {
        String name = MetricsUtils.fullMetricName(this.domain, metricName.getType(), metricName.getName());
        String mbeanName = Strings.nullToEmpty(metricName.getMBeanName());
        Map<String, String> labels = new HashMap<>();
        if (context.isDebugEnabled()) {
            labels.put(ORIGINAL, Strings.nullToEmpty(metricName.getGroup() + ":" + metricName.getType() + ":" + metricName.getName()));
            labels.put(LIBRARY, YAMMER_METRICS);
        }
        labels.putAll(MetricsUtils.cleanLabelNames(filterTags(YammerMetricsUtils.extractTags(mbeanName))));

        return new MetricKey(name, labels);
    }

    @Override
    public List<Metric> collect() {
        List<Metric> out = new ArrayList<>();
        Set<Map.Entry<MetricName, com.yammer.metrics.core.Metric>> metrics = metricsRegistry.allMetrics().entrySet();

        for (Map.Entry<com.yammer.metrics.core.MetricName, com.yammer.metrics.core.Metric> entry : metrics) {

            com.yammer.metrics.core.MetricName metricName = entry.getKey();
            com.yammer.metrics.core.Metric metric = entry.getValue();
            MetricKey metricKey = toMetricKey(entry.getKey());
            String name = metricKey.getName();
            Map<String, String> labels = metricKey.getLabels();

            Instant metricAddedInstant = instantAdded(metricKey);

            try {
                log.trace("Processing {}", metricName);

                if (!metricFilter.test(metricKey)) {
                    continue;
                }

                if (metric instanceof Gauge) {
                    collectGauge(name, labels, (Gauge) metric).map(out::add);
                } else if (metric instanceof Counter) {
                    out.add(collectCounter(name, labels, (Counter) metric));
                    out.add(collectDelta(name, labels, ((Counter) metric).count(), metricAddedInstant));
                } else if (metric instanceof Meter) {
                    // Only collect the counters and append "/total" to the end.
                    String meterName = name + "/total";
                    out.add(collectMeter(meterName, labels, (Meter) metric));
                    out.add(collectDelta(meterName, labels, ((Meter) metric).count(), metricAddedInstant));
                } else if (metric instanceof Timer) {
                    out.add(collectTimer(name, labels, (Timer) metric));
                    // Results in a name like /time/delta
                    collectDelta(name + "/time", labels, ((Timer) metric).sum(), metricAddedInstant).map(out::add);
                    // Results in a name like /total/delta.
                    out.add(collectDelta(name + "/total", labels, ((Timer) metric).count(), metricAddedInstant));
                } else if (metric instanceof Histogram) {
                    out.add(collectHistogram(name, labels, (Histogram) metric));
                    // Results in a name like /time/delta
                    collectDelta(name + "/time", labels, ((Histogram) metric).sum(), metricAddedInstant).map(out::add);
                    // Results in a name like /total/delta.
                    out.add(collectDelta(name + "/total", labels, ((Histogram) metric).count(), metricAddedInstant));

                } else {
                    log.debug("Unexpected metric type for {}", metricName);
                }
            } catch (Exception e) {
                log.warn("Unexpected error in processing Yammer metric {}", metricName, e);
            }
        }

        return out;
    }

    private Instant instantAdded(MetricKey metricKey) {
        // lookup when the metric was added to use it as the interval start. That should always
        // exist, but if it doesn't (e.g. if there's a race) then we use now.
        return metricAdded.getOrDefault(metricKey, Instant.now(clock));
    }

    private Map<String, String> filterTags(Map<String, String> tags) {
        tags.remove("name");
        tags.remove("type");
        return tags;
    }

    @Override
    public String toString() {
        return this.getClass().getCanonicalName();
    }

    private Optional<Metric> collectGauge(String metricName, Map<String, String> labels, com.yammer.metrics.core.Gauge gauge) {

        // Figure out which gauge instance and call the right method to get value
        Object value = gauge.value();
        Point.Builder point = Point.newBuilder().setTimestamp(MetricsUtils.now(clock));

        if (value instanceof Integer || value instanceof Long) {
            point.setInt64Value(((Number) value).longValue());
            return Optional.of(context
                .metricWithSinglePointTimeseries(metricName, MetricDescriptor.Type.GAUGE_INT64, labels, point.build()));

        } else if (value instanceof Float || value instanceof Double) {
            point.setDoubleValue(((Number) value).doubleValue());
            return Optional.of(context
                .metricWithSinglePointTimeseries(metricName, MetricDescriptor.Type.GAUGE_DOUBLE, labels, point.build()));

        } else if (value instanceof Boolean) {
            point.setInt64Value(((Boolean) value) ? 1 : 0);
            return Optional.of(context
                .metricWithSinglePointTimeseries(metricName, MetricDescriptor.Type.GAUGE_INT64, labels, point.build()));

        } else {
            // Ignoring Gauge (gauge.getKey()) with unhandled type.
            log.debug("Ignoring {} value = {}", metricName, value);
            return Optional.empty();
        }

    }

    private Metric collectCounter(String metricName, Map<String, String> labels, Counter counter) {
        Point point = Point.newBuilder()
                .setTimestamp(MetricsUtils.now(clock))
                .setInt64Value(counter.count())
                .build();
        return context.metricWithSinglePointTimeseries(metricName, MetricDescriptor.Type.CUMULATIVE_INT64, labels, point);
    }


    private Metric collectDelta(String metricName, Map<String, String> labels, long value, Instant metricAdded) {
        MetricKey key = new MetricKey(metricName, labels);

        Optional<InstantAndValue<Long>> lastValue = longDeltas.getAndSet(key, Instant.now(clock), value);
        Instant start = metricAdded;
        Long delta = value;
        if (lastValue.isPresent()) {
            start = lastValue.get().getIntervalStart();
            delta = value - lastValue.get().getValue();
        }
        Point point = Point.newBuilder()
            .setTimestamp(MetricsUtils.now(clock))
            .setInt64Value(delta)
            .build();

        return context
            .metricWithSinglePointTimeseries(metricName + "/delta", Type.GAUGE_INT64, labels, point, MetricsUtils
                .toTimestamp(start));
    }

    private Optional<Metric> collectDelta(String metricName, Map<String, String> labels, double value, Instant metricAdded) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return Optional.empty();
        }
        MetricKey key = new MetricKey(metricName, labels);

        Optional<InstantAndValue<Double>> lastValue = doubleDeltas.getAndSet(key, Instant.now(clock), value);
        Instant start = metricAdded;
        Double delta = value;
        if (lastValue.isPresent()) {
            start = lastValue.get().getIntervalStart();
            delta = value - lastValue.get().getValue();
        }

        Point point = Point.newBuilder()
            .setTimestamp(MetricsUtils.now(clock))
            .setDoubleValue(delta)
            .build();

        return Optional.of(context
            .metricWithSinglePointTimeseries(metricName + "/delta", Type.GAUGE_DOUBLE, labels, point, MetricsUtils
                .toTimestamp(start)));
    }


    private Metric collectMeter(String metricName, Map<String, String> labels, Meter meter) {
        Point point = Point.newBuilder()
                .setTimestamp(MetricsUtils.now(clock))
                .setInt64Value(meter.count())
                .build();
        return context.metricWithSinglePointTimeseries(metricName, MetricDescriptor.Type.CUMULATIVE_INT64, labels, point);
    }

    private Metric collectHistogram(String metricName, Map<String, String> labels, Histogram histogram) {
        return collectSnapshotAndCount(
                metricName, labels, DEFAULT_UNIT, histogram.getSnapshot(), histogram.count());
    }

    private Metric collectTimer(String metricName, Map<String, String> labels, Timer timer) {
        return collectSnapshotAndCount(
                metricName, labels, NS_UNIT, timer.getSnapshot(), timer.count());
    }

    private Metric collectSnapshotAndCount(
            String metricName,
            Map<String, String> labels,
            String unit,
            com.yammer.metrics.stats.Snapshot yammerSnapshot,
            long count) {
        SummaryValue.Snapshot snapshot = SummaryValue.Snapshot.newBuilder()
                .setSum(DoubleValue.newBuilder().setValue(yammerSnapshot.size() * yammerSnapshot.getMedian()).build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                        .setPercentile(50.0)
                        .setValue(yammerSnapshot.getMedian())
                        .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                        .setPercentile(75.0)
                        .setValue(yammerSnapshot.get75thPercentile())
                        .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                        .setPercentile(95.0)
                        .setValue(yammerSnapshot.get95thPercentile())
                        .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                        .setPercentile(98.0)
                        .setValue(yammerSnapshot.get98thPercentile())
                        .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                        .setPercentile(99.0)
                        .setValue(yammerSnapshot.get99thPercentile())
                        .build())
                .addPercentileValues(SummaryValue.Snapshot.ValueAtPercentile.newBuilder()
                        .setPercentile(99.9)
                        .setValue(yammerSnapshot.get999thPercentile())
                        .build())
                .build();


        SummaryValue summaryValue = SummaryValue.newBuilder()
                .setCount(Int64Value.newBuilder().setValue(count).build())
                .setSnapshot(snapshot)
                .build();

        Point point = Point.newBuilder()
                .setTimestamp(MetricsUtils.now(clock))
                .setSummaryValue(summaryValue)
                .build();

        return context
            .metricWithSinglePointTimeseries(metricName, MetricDescriptor.Type.SUMMARY, labels, point);
    }

    /**
     * Create a new Builder using values from the {@link ConfluentTelemetryConfig}.
     */
    public static Builder newBuilder(ConfluentTelemetryConfig config) {
        return newBuilder()
            .setMetricFilter(config.getMetricFilter());
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String domain;
        private MetricsRegistry metricsRegistry;
        private Predicate<MetricKey> metricFilter = s -> true;
        private Context context;
        private Clock clock = Clock.systemUTC();
        private LastValueTracker<Long> longDeltas = new LastValueTracker<>();
        private LastValueTracker<Double> doubleDeltas = new LastValueTracker<>();

        private Builder() {
        }


        public Builder setDomain(String domain) {
            this.domain = domain;
            return this;
        }

        public Builder setMetricsRegistry(MetricsRegistry metricsRegistry) {
            this.metricsRegistry = metricsRegistry;
            return this;
        }

        public Builder setMetricFilter(Predicate<MetricKey> metricFilter) {
            this.metricFilter = metricFilter;
            return this;
        }

        public Builder setContext(Context context) {
            this.context = context;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = Objects.requireNonNull(clock);
            return this;
        }

        public YammerMetricsCollector build() {
            Objects.requireNonNull(this.context);
            Objects.requireNonNull(this.domain);
            Objects.requireNonNull(this.metricsRegistry);

            return new YammerMetricsCollector(this.domain, this.metricsRegistry, this.metricFilter, this.context, this.longDeltas, this.doubleDeltas, this.clock);
        }

        public Builder setLongDeltas(LastValueTracker<Long> longDeltas) {
            this.longDeltas = longDeltas;
            return this;
        }

        public Builder setDoubleDeltas(LastValueTracker<Double> doubleDeltas) {
            this.doubleDeltas = doubleDeltas;
            return this;
        }

    }
}
