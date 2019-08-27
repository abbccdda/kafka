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
    private final Exporter exporter;
    private final Collection<MetricsCollector> collectors;
    private final long collectIntervalMs;
    private final ConcurrentMap<MetricsCollector, AtomicLong> metricsCollected = new ConcurrentHashMap<>();
    private final ConcurrentMap<MetricsCollector, AtomicLong> metricsSent = new ConcurrentHashMap<>();

    public MetricsCollectorTask(Context ctx, Exporter client, Collection<MetricsCollector> collectors, long collectIntervalMs) {
        Verify.verify(collectIntervalMs > 0, "collection interval cannot be less than 1");

        this.exporter = Objects.requireNonNull(client);
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
        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LIBRARY, MetricsCollector.NO_LIBRARY);
            labels.put(MetricsCollector.ORIGINAL, "none");
        }
        labels.put("collector", collectorName);
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
        return metricsSent.getOrDefault(collector, new AtomicLong()).get();
    }

    private Metric buildMetricsSentMetric(MetricsCollector collector) {
        String collectorName = collector.getClass().getSimpleName();
        Map<String, String> labels = new HashMap<>();
        labels.put("collector", collectorName);
        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LIBRARY, MetricsCollector.NO_LIBRARY);
            labels.put(MetricsCollector.ORIGINAL, "none");
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
                this::report,
                collectIntervalMs,
                collectIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    private void report() {
        try {
            for (MetricsCollector collector : collectors) {
                try {
                    Collection<Metric> m = collector.collect();
                    log.trace("Collected {} metrics from {}", m.size(), collector.toString());
                    m.add(buildMetricsCollectedMetric(collector, m.size()));
                    m.add(buildMetricsSentMetric(collector));

                    try {
                        exporter.write(m);
                        log.trace("Sent {} metrics", m.size());
                        updateMetricsSent(collector, m.size());
                    } catch (Exception e) {
                        log.error("Error while sending metrics for {}", collector, e);
                    }

                } catch (Exception e) {
                    log.error("Error while reporting metrics for {}", collector, e);
                }
            }
        } catch (Throwable t) {
            log.error("Error while collecting metrics", t);
        }
    }

    public void close() {
        executor.shutdown();
    }
}
