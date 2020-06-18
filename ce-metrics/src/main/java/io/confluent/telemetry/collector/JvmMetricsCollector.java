// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.telemetry.collector;

import com.google.common.collect.ImmutableMap;
import com.sun.management.UnixOperatingSystemMXBean;
import java.lang.management.MemoryUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.Point;

public class JvmMetricsCollector implements MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(JvmMetricsCollector.class);

    private final Context context;
    private volatile Predicate<MetricKey> metricWhitelistFilter;
    private final Map<String, String> labels;

    public JvmMetricsCollector(Context ctx, Predicate<MetricKey> metricWhitelistFilter) {
        this.context = ctx;
        this.metricWhitelistFilter = metricWhitelistFilter;
        this.labels = createLabels(context);
    }

    @Override
    public void reconfigureWhitelist(Predicate<MetricKey> whitelistPredicate) {
        this.metricWhitelistFilter = whitelistPredicate;
    }

    @Override
    public void collect(Exporter exporter) {
        collectSystemMetrics(exporter);
        // Special casing CPU_USAGE metric (name & group) for SBK depends on it.
        collectCpuUsageMetric(exporter);
        collectMemoryMetrics(exporter);
    }

    private void collectSystemMetrics(Exporter exporter) {
        emitMetrics(exporter, getOSMetrics(), "jvm/os");
    }

    private Map<String, Number> getOSMetrics() {
        final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        final ImmutableMap.Builder<String, Number> builder = ImmutableMap.builder();

        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            final com.sun.management.OperatingSystemMXBean osMXBean =
                (com.sun.management.OperatingSystemMXBean) osBean;
            builder
                .put("FreePhysicalMemorySize", osMXBean.getFreePhysicalMemorySize())
                .put("TotalPhysicalMemorySize", osMXBean.getTotalPhysicalMemorySize())
                .put("ProcessCpuTime", osMXBean.getProcessCpuTime())
                .put("SystemCpuLoad", osMXBean.getSystemCpuLoad());

            if (osMXBean instanceof UnixOperatingSystemMXBean) {
                final UnixOperatingSystemMXBean unixOsMXBean = (UnixOperatingSystemMXBean) osMXBean;
                builder
                    .put("OpenFileDescriptorCount", unixOsMXBean.getOpenFileDescriptorCount())
                    .put("MaxFileDescriptorCount", unixOsMXBean.getMaxFileDescriptorCount());
            }
        }

        builder.put("SystemLoadAverage", osBean.getSystemLoadAverage());
        return builder.build();
    }

    private void collectCpuUsageMetric(Exporter exporter) {
        final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            final com.sun.management.OperatingSystemMXBean osMXBean =
                (com.sun.management.OperatingSystemMXBean) osBean;
            emitMetrics(exporter, ImmutableMap.of("cpu_usage", osMXBean.getProcessCpuLoad()), "cpu");
        }
    }

    private void collectMemoryMetrics(Exporter exporter) {
        emitMemoryMetrics(
            exporter, ManagementFactory.getMemoryMXBean().getHeapMemoryUsage(), "jvm/mem/heap");
        emitMemoryMetrics(
            exporter, ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage(), "jvm/mem/nonheap");
    }

    private void emitMemoryMetrics(
        Exporter exporter, MemoryUsage heapMemoryUsage, String metricGroup) {
        emitMetrics(
            exporter,
            ImmutableMap.of(
                "Committed", heapMemoryUsage.getCommitted(),
                "Used", heapMemoryUsage.getUsed()
            ),
            metricGroup);
    }

    private void emitMetrics(Exporter exporter, Map<String, Number> metrics, String metricGroup) {
        for (Map.Entry<String, Number> attribute: metrics.entrySet()) {
            final String metricKey = attribute.getKey();
            final Number metricValue = attribute.getValue();
            final String name =
                MetricsUtils.fullMetricName(context.getDomain(), metricGroup, metricKey);

            if (metricWhitelistFilter.test(new MetricKey(name, labels))) {
                exporter.emit(
                    context.metricWithSinglePointTimeseries(
                        name,
                        getType(metricValue),
                        labels,
                        getPoint(metricValue)
                    )
                );
            }
        }
    }

    private Map<String, String> createLabels(Context context) {
        final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        final Map<String, String> labels = new HashMap<>();

        labels.put("os.name", osBean.getName());
        labels.put("os.version", osBean.getVersion());
        labels.put("os.arch", osBean.getArch());
        labels.put("os.processors", String.valueOf(osBean.getAvailableProcessors()));

        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LABEL_LIBRARY, MetricsCollector.LIBRARY_NONE);
        }

        return labels;
    }

    private MetricDescriptor.Type getType(Number value) {
        if (value instanceof Integer || value instanceof Long) {
            return MetricDescriptor.Type.GAUGE_INT64;
        } else {
            return MetricDescriptor.Type.GAUGE_DOUBLE;
        }
    }

    private Point getPoint(Number value) {
        final Point.Builder pointBuilder =
          Point.newBuilder().setTimestamp(MetricsUtils.now());

        if (value instanceof Integer || value instanceof Long) {
            pointBuilder.setInt64Value(value.longValue());
        } else {
            pointBuilder.setDoubleValue(value.doubleValue());
        }

        return pointBuilder.build();
    }

    @Override
    public String toString() {
        return JvmMetricsCollector.class.getCanonicalName();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Context context;
        private Predicate<MetricKey> metricWhitelistFilter = s -> true;

        private Builder() {
        }

        public Builder setContext(Context context) {
            this.context = context;
            return this;
        }
        public Builder setMetricWhitelistFilter(Predicate<MetricKey> metricWhitelistFilter) {
            this.metricWhitelistFilter = metricWhitelistFilter;
            return this;
        }

        public JvmMetricsCollector build() {
            Objects.requireNonNull(this.context);
            return new JvmMetricsCollector(context, metricWhitelistFilter);
        }
    }
}
