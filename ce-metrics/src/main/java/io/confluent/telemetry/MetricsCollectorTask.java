package io.confluent.telemetry;

import com.google.common.base.Verify;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsCollectorTask {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollectorTask.class);

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private final Context context;
    private final Set<Exporter> exporters;
    private final Collection<MetricsCollector> collectors;
    private final long collectIntervalMs;
    private final ConcurrentMap<MetricsCollector, AtomicLong> metricsCollected = new ConcurrentHashMap<>();
    private final ConcurrentMap<MetricsCollector, AtomicLong> metricsSent = new ConcurrentHashMap<>();

    public MetricsCollectorTask(Context ctx, Set<Exporter> exporters, Collection<MetricsCollector> collectors, long collectIntervalMs) {
        Verify.verify(collectIntervalMs > 0, "collection interval cannot be less than 1");

        this.exporters = Objects.requireNonNull(exporters);
        Verify.verify(!exporters.isEmpty(), "At least one exporter must be enabled");

        this.collectors = Objects.requireNonNull(collectors);
        this.context = Objects.requireNonNull(ctx);
        this.collectIntervalMs = collectIntervalMs;

        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setThreadFactory(runnable -> {
            Thread thread = new Thread(runnable, "confluent-telemetry-metrics-collector-task-scheduler");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", t.getName(), e));
            return thread;
        });
    }

    /**
     * Return a total count for the number of metrics collected, including the count of new
     * "additionalMetrics" that were collected by the collector.
     */
    private long metricsCollectedAddAndGet(MetricsCollector collector, long additionalMetrics) {
        // FIXME This is incorrect (https://confluentinc.atlassian.net/browse/METRICS-577)
        AtomicLong currCount = metricsCollected
            .getOrDefault(collector, new AtomicLong());

        return currCount.addAndGet(additionalMetrics);
    }

    /**
     * Builds a Metric for the total number of metrics that have been collected (including the
     * additionalMetrics) for the collector.
     */
    private Metric buildMetricsCollectedMetric(MetricsCollector collector, long additionalMetrics) {
        String collectorName = collector.getClass().getSimpleName();
        Map<String, String> labels = new HashMap<>();
        labels.put(MetricsCollector.LABEL_COLLECTOR, collectorName);
        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LABEL_LIBRARY, MetricsCollector.LIBRARY_NONE);
        }
        return context.metricWithSinglePointTimeseries(
            "io.confluent.telemetry/metrics_collector_task/metrics_collected_total",
            Type.CUMULATIVE_INT64,
            labels,
            Point.newBuilder().setTimestamp(MetricsUtils.now()).setInt64Value(
                metricsCollectedAddAndGet(collector, additionalMetrics)).build()
        );
    }

    /**
     * Get total number of metrics sent by the collector of the given name.
     */
    private long metricsSent(MetricsCollector collector) {
        // FIXME This is incorrect (https://confluentinc.atlassian.net/browse/METRICS-576)
        return metricsSent.getOrDefault(collector, new AtomicLong()).get();
    }

    private Metric buildMetricsSentMetric(MetricsCollector collector) {
        String collectorName = collector.getClass().getSimpleName();
        Map<String, String> labels = new HashMap<>();
        labels.put("collector", collectorName);
        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LABEL_LIBRARY, MetricsCollector.LIBRARY_NONE);
        }
        return context.metricWithSinglePointTimeseries(
            "io.confluent.telemetry/metrics_collector_task/metrics_sent_total",
            Type.CUMULATIVE_INT64,
            labels,
            Point.newBuilder().setTimestamp(MetricsUtils.now()).setInt64Value(metricsSent(collector)).build()
        );
    }

    private void updateMetricsSent(MetricsCollector collector, int sentCount) {
        AtomicLong currCount = metricsSent
            .getOrDefault(collector.getClass().getSimpleName(), new AtomicLong());

        currCount.addAndGet(sentCount);
    }

    public void start() {
        log.info("Starting Confluent telemetry reporter with an interval of {} ms", this.collectIntervalMs);
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
        try {
            Collection<Metric> metrics = collector.collect();
            log.trace("Collected {} metrics from {}", metrics.size(), collector);
            metrics.add(buildMetricsCollectedMetric(collector, metrics.size()));
            metrics.add(buildMetricsSentMetric(collector));

            for (Exporter exporter : exporters) {
                try {
                    exporter.export(metrics);
                } catch (Throwable t) {
                    log.error("Error exporting metrics via {}", exporter, t);
                }
            }
        } catch (Throwable t) {
            log.error("Error while collecting metrics for {}", collector, t);
        }
    }

    public void close() {
        executor.shutdown();
    }
}
