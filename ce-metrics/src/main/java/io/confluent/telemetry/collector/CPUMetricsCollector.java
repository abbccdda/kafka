// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.telemetry.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.Point;

public class CPUMetricsCollector implements MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(CPUMetricsCollector.class);

    private final Context context;
    private volatile Predicate<MetricKey> metricWhitelistFilter;
    private final Optional<com.sun.management.OperatingSystemMXBean> osBean;

    public CPUMetricsCollector(Context ctx,
        Predicate<MetricKey> metricWhitelistFilter) {
        this.context = ctx;
        this.metricWhitelistFilter = metricWhitelistFilter;

        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            this.osBean = Optional.of((com.sun.management.OperatingSystemMXBean) osBean);
        } else {
            log.warn("CPU metric is not available on this operating system");
            this.osBean = Optional.empty();
        }
    }

    @Override
    public void reconfigureWhitelist(Predicate<MetricKey> whitelistPredicate) {
        this.metricWhitelistFilter = whitelistPredicate;
    }

    @Override
    public void collect(Exporter exporter) {
        if (!osBean.isPresent()) {
            return;
        }

        String name = MetricsUtils.fullMetricName(this.context.getDomain(), "cpu", "cpu_usage");

        Map<String, String> labels = new HashMap<>();
        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LABEL_LIBRARY, MetricsCollector.LIBRARY_NONE);
        }

        if (!metricWhitelistFilter.test(new MetricKey(name, labels))) {
            return;
        }

        double cpuUtil = osBean.get().getProcessCpuLoad();

        exporter.emit(context.metricWithSinglePointTimeseries(
                name,
                MetricDescriptor.Type.GAUGE_DOUBLE,
                labels,
                Point.newBuilder()
                        .setTimestamp(MetricsUtils.now())
                        .setDoubleValue(cpuUtil).build()));
    }

    @Override
    public String toString() {
        return CPUMetricsCollector.class.getCanonicalName();
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

        public CPUMetricsCollector build() {
            Objects.requireNonNull(this.context);
            Objects.requireNonNull(this.context.getDomain());
            return new CPUMetricsCollector(context, metricWhitelistFilter);
        }
    }
}
