package io.confluent.telemetry;

import com.google.common.base.Verify;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.exporter.AbstractExporter;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;

public class MetricsCollectorTask implements MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollectorTask.class);

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private final Context context;
    private final Supplier<Collection<Exporter>> exportersSupplier;
    private final Collection<MetricsCollector> collectors;
    private final long collectIntervalMs;
    private volatile Predicate<MetricKey> metricsPredicate;

    public MetricsCollectorTask(
        Context ctx, Supplier<Collection<Exporter>> exportersSupplier, Collection<MetricsCollector> collectors,
        long collectIntervalMs, Predicate<MetricKey> metricsPredicate
    ) {
        Verify.verify(collectIntervalMs > 0, "collection interval cannot be less than 1");

        this.exportersSupplier = Objects.requireNonNull(exportersSupplier);

        this.collectors = Objects.requireNonNull(collectors);
        this.context = Objects.requireNonNull(ctx);
        this.collectIntervalMs = collectIntervalMs;
        this.metricsPredicate = metricsPredicate;

        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setThreadFactory(runnable -> {
            Thread thread = new Thread(runnable, "confluent-telemetry-metrics-collector-task-scheduler");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", t.getName(), e));
            return thread;
        });
    }

    public void start() {
        log.info("Starting Confluent telemetry reporter with an interval of {} ms for resource = (type = {})",
            this.collectIntervalMs,
            context.getResource().getType());
        log.debug("Telemetry reporter resource labels: {}", context.getResource().getLabelsMap());
        schedule();
    }

    private void schedule() {
        executor.scheduleAtFixedRate(
                this::collectAndExport,
                collectIntervalMs,
                collectIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    private void collectAndExport() {
        collectors.forEach(this::collectAndExport);
    }

    private void collectAndExport(MetricsCollector collector) {
        try (CompositeExporter compositeExporter = new CompositeExporter(this.exportersSupplier)) {
            collector.collect(compositeExporter);
            long metricCount = compositeExporter.getCountAndReset();
            log.trace("Collected {} metrics from {}", metricCount, collector);
            emitMetricsCollectedMetric(collector, metricCount, compositeExporter::emit);
        } catch (Throwable t) {
            log.error("Error while collecting metrics for collector = {})",
                collector,
                t);
        }
    }

    public void close() {
        executor.shutdown();
    }

    /**
     * Builds a Metric for the total number of metrics that have been collected (including the
     * additionalMetrics) for the collector.
     */
    private void emitMetricsCollectedMetric(MetricsCollector collector, long metricCount,
                                            BiConsumer<MetricKey, Metric> emit) {
        String metricName = "io.confluent.telemetry/metrics_collector_task/metrics_collected_total/delta";
        String collectorName = collector.getClass().getSimpleName();
        Map<String, String> labels = new HashMap<>();
        labels.put(MetricsCollector.LABEL_COLLECTOR, collectorName);
        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LABEL_LIBRARY, MetricsCollector.LIBRARY_NONE);
        }
        MetricKey metricKey = new MetricKey(metricName, labels);
        if (this.metricsPredicate.test(metricKey)) {
            emit.accept(
                metricKey,
                context.metricWithSinglePointTimeseries(
                    metricName,
                    Type.CUMULATIVE_INT64,
                    labels,
                    Point.newBuilder().setTimestamp(MetricsUtils.now()).setInt64Value(metricCount).build()
                )
            );
        }
    }

    @Override
    public void collect(Exporter exporter) {
        // noop
    }

    @Override
    public void reconfigurePredicate(Predicate<MetricKey> metricsPredicate) {
        this.metricsPredicate = metricsPredicate;
    }

    private static class CompositeExporter extends AbstractExporter {

        private final Supplier<Collection<Exporter>> exportersSupplier;
        private final AtomicLong collectedMetricsCount = new AtomicLong();

        public CompositeExporter(Supplier<Collection<Exporter>> exportersSupplier) {
            this.exportersSupplier = exportersSupplier;
        }

        public long getCountAndReset() {
            return collectedMetricsCount.getAndSet(0);
        }

        @Override
        public void reconfigurePredicate(Predicate<MetricKey> metricsPredicate) {
            // we never want to update the metrics predicate for this exporter
        }

        @Override
        public void doEmit(MetricKey metricKey, Metric metric) {
            boolean collected = false;
            for (Exporter e : exportersSupplier.get()) {
                if (e.emit(metricKey, metric)) {
                    collected = true;
                }
            }
            if (collected) {
                collectedMetricsCount.incrementAndGet();
            }
        }

        @Override
        public void close() {
            // exporters are closed in KafkaServerMetricsReporter.close()
        }
    }
}
