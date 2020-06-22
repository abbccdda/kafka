package io.confluent.telemetry.collector;

import com.google.common.base.Strings;
import com.google.protobuf.Timestamp;

import io.opencensus.proto.metrics.v1.Metric;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.collector.LastValueTracker.InstantAndValue;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;

public class KafkaMetricsCollector implements MetricsCollector {
    public static final String KAFKA_METRICS_LIB = "kafka";
    private static final Logger log = LoggerFactory.getLogger(KafkaMetricsCollector.class);
    private final StateLedger ledger;
    private volatile Predicate<MetricKey> metricWhitelistFilter;
    private final Context context;

    private final Clock clock;


    private static final Field METRIC_VALUE_PROVIDER_FIELD;

    static {
        try {
            METRIC_VALUE_PROVIDER_FIELD = KafkaMetric.class.getDeclaredField("metricValueProvider");
            METRIC_VALUE_PROVIDER_FIELD.setAccessible(true);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    public KafkaMetricsCollector(Predicate<MetricKey> metricWhitelistFilter, Context context, StateLedger ledger, Clock clock) {
        this.metricWhitelistFilter = metricWhitelistFilter;
        this.context = context;
        this.clock = clock;
        this.ledger = ledger;
    }

    @Override
    public void reconfigureWhitelist(Predicate<MetricKey> whitelistPredicate) {
        this.metricWhitelistFilter = whitelistPredicate;
    }

    @Override
    public void collect(Exporter exporter) {
        for (Map.Entry<MetricName, KafkaMetric> entry : ledger.getMetrics()) {
            MetricName originalMetricName = entry.getKey();
            KafkaMetric metric = entry.getValue();
            MetricKey metricKey = toMetricKey(originalMetricName);
            String name = metricKey.getName();
            Map<String, String> labels = metricKey.getLabels();

            /** All metrics implement the MetricValueProvider interface. They are divided into 2 base types:
             * 1. Gauge and
             * 2. Measurable
             *
             * Gauges can have any value but we only collect metrics with number values
             * (TODO: How do we want to deal with string values)
             * KafkaExporter -> Opencensus
             * Gauge (with number values) -> GAUGE_DOUBLE / GAUGE_INT64
             * Gauge (with string) -> UNDEFINED dropped
             *
             * Measurables are divided into simple types with single values (Avg, Count, Min, Max, Rate, SimpleRate, Sum, Total)
             * and compound types (Frequencies, Meter and Percentiles).
             *
             * We can safely assume that a Count always increases in steady state. It should be a bug if a Count metric
             * decreases. So, a count is converted to a CUMULATIVE_DOUBLE.
             *
             * Should total and sum be treated as a monotonically increasing counter ?
             * The javadocs for Total metric type say "An un-windowed cumulative total maintained over all time.".
             * The standalone Total metrics in the KafkaExporter codebase seem to be cumulative metrics that will always increase.
             * The Total metric underlying Meter type is mostly a Total of a Count metric.
             * We can assume that a Total metric always increases (but it is not guaranteed as the sample values might be both
             * negative or positive).
             * For now, Total is converted to CUMULATIVE_DOUBLE unless we find a valid counter-example.
             *
             * The Sum as it is a sample sum which is not a cumulative metric. It is converted to GAUGE_DOUBLE.
             *
             * The compound metrics are virtual metrics. They are composed of simple types or anonymous measurable types
             * which are reported. A compound metric is never reported as-is.
             *
             * A Meter metric is always created with and reported as 2 KafkaExporter metrics: a rate and a
             * count. For eg: org.apache.kafka.common.network.Selector has Meter metric for "connection-close" but it
             * has to be created with a "connection-close-rate" metric of type rate and a "connection-close-total"
             * metric of type total. So, we will never get a KafkaExporter metric with type Meter.
             *
             * Frequencies is created with a array of Frequency objects. When a Frequencies metric is registered, each
             * member Frequency object is converted into an anonymous Measurable and registered. So, a Frequencies metric
             * is reported with a set of measurables with name = Frequency.name(). As there is no way to figure out the
             * compound type, each component measurables is converted to a GAUGE_DOUBLE.
             *
             * Percentiles work the same way as Frequencies. The only difference is that it is composed of Percentile
             * types instead. So, we should treat the component measurable as GAUGE_DOUBLE.
             *
             * Some metrics are defined as either anonymous inner classes or lambdas implementing the Measurable
             * interface. As we do not have any information on how to treat them, we should fallback to treating
             * them as GAUGE_DOUBLE.
             *
             * KafkaExporter -> Opencensus mapping for measurables
             * Avg / Rate / Min / Max / Total / Sum -> GAUGE_DOUBLE
             * Count -> CUMULATIVE_DOUBLE
             * Meter has 2 elements :
             *  Total -> CUMULATIVE_DOUBLE
             *  Rate -> GAUGE_DOUBLE
             * Frequencies -> each component is GAUGE_DOUBLE
             * Percentiles -> each component is GAUGE_DOUBLE
             **/

            if (isMeasurable(metric)) {
                Measurable measurable = metric.measurable();
                double value = (Double) entry.getValue().metricValue();

                if (measurable instanceof WindowedCount || measurable instanceof CumulativeSum) {
                    collectMetric(name, labels, Type.CUMULATIVE_DOUBLE, value, exporter::emit);
                    collectDelta(originalMetricName, name, labels, value, exporter::emit);
                } else {
                    collectMetric(name, labels, Type.GAUGE_DOUBLE, value, exporter::emit);
                }
            } else {
                // It is non-measurable Gauge metric.
                // Collect the metric only if its value is a number.
                if (entry.getValue().metricValue() instanceof Number) {
                    Number value = (Number) entry.getValue().metricValue();
                    if (value instanceof Integer || value instanceof Long) {
                        // map integer types to GAUGE_INT64
                        collectMetric(name, labels, Type.GAUGE_INT64, value.longValue(), exporter::emit);
                    } else {
                        // map any other number type to GAUGE_DOUBLE
                        collectMetric(name, labels, Type.GAUGE_DOUBLE, value.doubleValue(), exporter::emit);
                    }
                } else {
                    // skip non-measurable metrics
                    log.debug("Skipping non-measurable gauge metric {}", originalMetricName.name());
                }
            }
        }
    }

    private void collectDelta(MetricName originalMetricName, String metricName, Map<String, String> labels,
                                          Double value, BiConsumer<MetricKey, Metric> emit) {
        String deltaName = metricName + "/delta";
        MetricKey metricKey = new MetricKey(deltaName, labels);
        if (!metricWhitelistFilter.test(metricKey)) {
            return;
        }

        // calculate a getAndSet, and add to out if non-empty
        InstantAndValue<Double> instantAndValue = ledger.delta(originalMetricName, Instant.now(clock), value);

        Point point = Point.newBuilder()
            .setTimestamp(MetricsUtils.now(clock))
            .setDoubleValue(instantAndValue.getValue())
            .build();
        Timestamp startTimestamp = MetricsUtils
            .toTimestamp(instantAndValue.getIntervalStart());
        emit.accept(
            metricKey,
            context.metricWithSinglePointTimeseries(deltaName, Type.CUMULATIVE_DOUBLE, labels, point, startTimestamp)
        );
    }

    private void collectMetric(String metricName, Map<String, String> labels, MetricDescriptor.Type type,
                                           double value, BiConsumer<MetricKey, Metric> emit) {
        MetricKey metricKey = new MetricKey(metricName, labels);
        if (!metricWhitelistFilter.test(metricKey)) {
            return;
        }

        emit.accept(
            metricKey,
            context.metricWithSinglePointTimeseries(metricName, type, labels,
                Point.newBuilder()
                    .setTimestamp(MetricsUtils.now(clock))
                    .setDoubleValue(value).build())
        );
    }

    private void collectMetric(String metricName, Map<String, String> labels, MetricDescriptor.Type type,
                                           long value, BiConsumer<MetricKey, Metric> emit) {
        MetricKey metricKey = new MetricKey(metricName, labels);
        if (!metricWhitelistFilter.test(metricKey)) {
            return;
        }

        emit.accept(
            metricKey,
            context.metricWithSinglePointTimeseries(metricName, type, labels,
                Point.newBuilder()
                    .setTimestamp(MetricsUtils.now(clock))
                    .setInt64Value(value).build())
        );
    }

    @Override
    public String toString() {
        return this.getClass().getCanonicalName();
    }

    private static boolean isMeasurable(KafkaMetric metric) {
        // KafkaMetric does not expose the internal MetricValueProvider and throws an IllegalStateException exception
        // if .measurable() is called for a Gauge.
        // There are 2 ways to find the type of internal MetricValueProvider for a KafkaMetric - use reflection or
        // get the information based on whether or not a IllegalStateException exception is thrown.
        // We use reflection so that we can avoid the cost of generating the stack trace when it's
        // not a measurable.
        try {
            Object provider = METRIC_VALUE_PROVIDER_FIELD.get(metric);
            return provider instanceof Measurable;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    // Package private for testing.
    MetricKey toMetricKey(MetricName metricName) {
        String group = Strings.nullToEmpty(metricName.group());
        String rawName = Strings.nullToEmpty(metricName.name());
        String name = MetricsUtils.fullMetricName(this.context.getDomain(), group, rawName);

        Map<String, String> labels = new HashMap<>();
        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LABEL_ORIGINAL,
                Strings.nullToEmpty(metricName.group()) + ":" + Strings
                    .nullToEmpty(metricName.name()));
            labels.put(MetricsCollector.LABEL_LIBRARY, KAFKA_METRICS_LIB);
        }
        labels.putAll(MetricsUtils.cleanLabelNames(metricName.tags()));

        return new MetricKey(name, labels);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Predicate<MetricKey> metricWhitelistFilter = s -> true;
        private Context context;
        private Clock clock = Clock.systemUTC();
        private StateLedger ledger = new StateLedger();

        private Builder() {
        }

        public Builder setMetricWhitelistFilter(Predicate<MetricKey> metricWhitelistFilter) {
            this.metricWhitelistFilter = Objects.requireNonNull(metricWhitelistFilter);
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

        public Builder setLedger(StateLedger ledger) {
            this.ledger = ledger;
            return this;
        }

        public KafkaMetricsCollector build() {
            Objects.requireNonNull(this.context.getDomain());

            return new KafkaMetricsCollector(this.metricWhitelistFilter, this.context, this.ledger, this.clock);
        }
    }

    /**
     * Keeps track of the state of metrics, e.g. when they were added, what their getAndSet value is,
     * and clearing them out when they're removed.
     *
     * <p>Note that this class doesn't have a context object, so it can't use the real
     * MetricKey (with contex.labels()). The StateLedger is created earlier in the process so
     * that it can handle the MetricsReporter methods (init/metricChange,metricRemoval).</p>
     */
    public static final class StateLedger implements MetricsReporter {

        private final Map<MetricName, KafkaMetric> metricMap = new ConcurrentHashMap<>();
        private final LastValueTracker<Double> doubleDeltas;
        private final Map<MetricName, Instant> metricAdded = new ConcurrentHashMap<>();
        private final Clock clock;

        public StateLedger() {
            this(new LastValueTracker<>(), Clock.systemUTC());
        }

        public StateLedger(LastValueTracker<Double> doubleDeltas, Clock clock) {
            this.doubleDeltas = doubleDeltas;
            this.clock = clock;
        }

        private Instant instantAdded(MetricName metricName) {
            // lookup when the metric was added to use it as the interval start. That should always
            // exist, but if it doesn't (e.g. if there's a race) then we use now.
            return metricAdded.getOrDefault(metricName, Instant.now(clock));
        }

        // package private for testing.
        MetricKey toKey(MetricName metricName) {
            return new MetricKey(metricName.toString(), Collections.emptyMap());
        }

        public void init(List<KafkaMetric> metrics) {
            log.debug("initializing Kafka metrics collector");
            for (KafkaMetric m : metrics) {
                metricMap.put(m.metricName(), m);
            }
        }

        public void metricChange(KafkaMetric metric) {
            metricMap.put(metric.metricName(), metric);
            metricAdded.put(metric.metricName(), Instant.now(clock));
        }

        public void metricRemoval(KafkaMetric metric) {
            log.debug("removing kafka metric : {}", metric.metricName());
            metricMap.remove(metric.metricName());
            doubleDeltas.remove(toKey(metric.metricName()));
            metricAdded.remove(metric.metricName());
        }

        public Iterable<? extends Entry<MetricName, KafkaMetric>> getMetrics() {
            return metricMap.entrySet();
        }

        public InstantAndValue<Double> delta(MetricName metricName, Instant now, Double value) {
            Optional<InstantAndValue<Double>> lastValue = doubleDeltas.getAndSet(toKey(metricName), now, value);

            return lastValue
                .map(last -> new InstantAndValue<>(last.getIntervalStart(), value - last.getValue()))
                .orElse(new InstantAndValue<>(instantAdded(metricName), value));
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }
}
