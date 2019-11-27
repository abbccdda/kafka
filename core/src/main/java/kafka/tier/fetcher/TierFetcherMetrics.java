package kafka.tier.fetcher;

import kafka.tier.fetcher.offsetcache.FetchOffsetCache;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.Gauge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

class TierFetcherMetrics {
    private final Metrics metrics;
    private final String metricGroupName = "TierFetcher";
    private final String bytesFetchedPrefix = "BytesFetched";
    private final String fetchExceptionPrefix = "FetchException";
    private final String fetchCancellationPrefix = "FetchCancellation";
    private final String fetchOffsetForTimestampExceptionPrefix = "FetchOffsetForTimestampException";
    private final Sensor bytesFetched;
    private final Sensor fetchException;
    private final Sensor fetchCancelled;
    private final Sensor fetchOffsetForTimestampException;

    private final String queueSizeName = "QueueSize";
    private final String offsetCacheHitName = "OffsetCacheHitRatio";
    private final String offsetCacheSizeName = "OffsetCacheSize";

    private final List<Sensor> sensors = new ArrayList<>();
    final MetricName queueSizeMetricName;
    final MetricName bytesFetchedRateMetricName;
    final MetricName bytesFetchedTotalMetricName;
    final MetricName offsetCacheHitMetricName;
    final MetricName offsetCacheSizeMetricName;
    final MetricName fetchExceptionRateMetricName;
    final MetricName fetchExceptionTotalMetricName;
    final MetricName fetchCancellationRateMetricName;
    final MetricName fetchCancellationTotalMetricName;
    final MetricName fetchOffsetForTimestampExceptionRateMetricName;
    final MetricName fetchOffsetForTimestampExceptionTotalMetricName;

    TierFetcherMetrics(Metrics metrics, ThreadPoolExecutor executor, FetchOffsetCache cache) {
        this.metrics = metrics;
        this.queueSizeMetricName = metrics.metricName(queueSizeName,
                metricGroupName, "The number of elements in the TierFetcher executor "
                        + "queue.",
                Collections.emptyMap());
        this.offsetCacheHitMetricName = metrics.metricName(offsetCacheHitName,
                metricGroupName, "TierFetcher offset cache hit ratio",
                Collections.emptyMap());
        this.offsetCacheSizeMetricName = metrics.metricName(offsetCacheSizeName,
                metricGroupName, "Number of entries in the TierFetcher offset cache",
                Collections.emptyMap());

        this.bytesFetched = sensor(bytesFetchedPrefix);
        this.bytesFetchedRateMetricName = metrics.metricName(bytesFetchedPrefix +
                "Rate", metricGroupName, "The number of bytes fetched per second from tiered "
                + "storage", Collections.emptyMap());
        this.bytesFetchedTotalMetricName = metrics.metricName(bytesFetchedPrefix +
                "Total", metricGroupName, "The total number of bytes fetched from tiered "
                + "storage", Collections.emptyMap());
        final Meter bytesFetchedMeter = new Meter(bytesFetchedRateMetricName,
                bytesFetchedTotalMetricName);

        this.fetchException = sensor(fetchExceptionPrefix);
        this.fetchExceptionRateMetricName = metrics.metricName(fetchExceptionPrefix +
                "Rate", metricGroupName, "The number exceptional fetch requests per second from tiered "
                + "storage", Collections.emptyMap());
        this.fetchExceptionTotalMetricName = metrics.metricName(fetchExceptionPrefix +
                "Total", metricGroupName, "The total number of exceptional fetch requests from tiered "
                + "storage", Collections.emptyMap());
        final Meter fetchObjectStoreExceptionMeter = new Meter(fetchExceptionRateMetricName,
                fetchExceptionTotalMetricName);

        this.fetchCancelled = sensor(fetchCancellationPrefix);
        this.fetchCancellationRateMetricName = metrics.metricName(fetchCancellationPrefix +
                "Rate", metricGroupName, "The number of cancelled fetch requests per second from tiered "
                + "storage", Collections.emptyMap());
        this.fetchCancellationTotalMetricName = metrics.metricName(fetchCancellationPrefix +
                "Total", metricGroupName, "The total number of cancelled fetch requests from tiered "
                + "storage", Collections.emptyMap());
        final Meter fetchCancelledMeter = new Meter(fetchCancellationRateMetricName,
                fetchCancellationTotalMetricName);

        this.fetchOffsetForTimestampException = sensor(fetchOffsetForTimestampExceptionPrefix);
        this.fetchOffsetForTimestampExceptionRateMetricName = metrics.metricName(fetchOffsetForTimestampExceptionPrefix +
                "Rate", metricGroupName, "The number of exceptional fetch offset for timestamps requests per second from tiered "
                + "storage", Collections.emptyMap());
        this.fetchOffsetForTimestampExceptionTotalMetricName = metrics.metricName(fetchOffsetForTimestampExceptionPrefix +
                "Total", metricGroupName, "The total number of exceptional fetch offset for timestamps requests from tiered "
                + "storage", Collections.emptyMap());
        final Meter fetchTierTimestampAndOffsetExceptionMeter = new Meter(fetchOffsetForTimestampExceptionRateMetricName,
                fetchOffsetForTimestampExceptionTotalMetricName);

        final Gauge<Integer> queueSizeGauge = new Gauge<Integer>() {
            @Override
            public Integer value(MetricConfig config, long now) {
                return executor.getQueue().size();
            }
        };
        metrics.addMetric(queueSizeMetricName, queueSizeGauge);

        final Gauge<Long> offsetCacheSize = new Gauge<Long>() {
            @Override
            public Long value(MetricConfig config, long now) {
                return cache.size();
            }
        };
        metrics.addMetric(offsetCacheSizeMetricName, offsetCacheSize);

        final Gauge<Double> offsetCacheHitGauge = new Gauge<Double>() {
            @Override
            public Double value(MetricConfig config, long now) {
                return cache.hitRatio();
            }
        };
        metrics.addMetric(offsetCacheHitMetricName, offsetCacheHitGauge);

        this.bytesFetched.add(bytesFetchedMeter);
        this.fetchException.add(fetchObjectStoreExceptionMeter);
        this.fetchCancelled.add(fetchCancelledMeter);
        this.fetchOffsetForTimestampException.add(fetchTierTimestampAndOffsetExceptionMeter);
    }

    private Sensor sensor(String name, Sensor... parents) {
        Sensor sensor = metrics.sensor(name, parents);
        sensors.add(sensor);
        return sensor;
    }

    public void close() {
        for (Sensor sensor : sensors)
            metrics.removeSensor(sensor.name());

        metrics.removeMetric(queueSizeMetricName);
    }

    public Sensor bytesFetched() {
        return this.bytesFetched;
    }

    public Sensor fetchException() {
        return this.fetchException;
    }

    public Sensor fetchCancelled() {
        return this.fetchCancelled;
    }

    public Sensor fetchOffsetForTimestampException() {
        return this.fetchOffsetForTimestampException;
    }
}
