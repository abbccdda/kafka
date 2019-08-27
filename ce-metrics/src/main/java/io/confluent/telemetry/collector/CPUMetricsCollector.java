// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.telemetry.collector;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.collector.KafkaMetricsCollector.Builder;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.Point;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CPUMetricsCollector implements MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(CPUMetricsCollector.class);

    private final String domain;
    private final Context context;
    private final Predicate<MetricKey> metricFilter;
    private final Optional<com.sun.management.OperatingSystemMXBean> osBean;

    public CPUMetricsCollector(Context ctx, String domain,
        Predicate<MetricKey> metricFilter) {
        this.context = ctx;
        this.domain = domain;
        this.metricFilter = metricFilter;

        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            this.osBean = Optional.of((com.sun.management.OperatingSystemMXBean) osBean);
        } else {
            log.warn("CPU metric is not available on this operating system");
            this.osBean = Optional.empty();
        }
    }

    /**
     * Create a new Builder using values from the {@link ConfluentTelemetryConfig}.
     */
    public static Builder newBuilder(ConfluentTelemetryConfig config) {
        return newBuilder()
            .setMetricFilter(config.getMetricFilter());
    }

    @Override
    public List<Metric> collect() {
        List<Metric> out = new ArrayList<>();

        if (!osBean.isPresent()) {
            return out;
        }

        String name = MetricsUtils.fullMetricName(this.domain, "cpu", "cpu_usage");

        Map<String, String> labels = new HashMap<>();
        if (context.isDebugEnabled()) {
            labels.put(MetricsCollector.LIBRARY, NO_LIBRARY);
            labels.put(ORIGINAL, "none");
        }

        if (!metricFilter.test(new MetricKey(name, labels))) {
            return out;
        }

        double cpuUtil = osBean.get().getProcessCpuLoad();

        out.add(context.metricWithSinglePointTimeseries(
                name,
                MetricDescriptor.Type.GAUGE_DOUBLE,
                labels,
                Point.newBuilder()
                        .setTimestamp(MetricsUtils.now())
                        .setDoubleValue(cpuUtil).build()));

        return out;
    }

    @Override
    public String toString() {
        return CPUMetricsCollector.class.getCanonicalName();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String domain;
        private Context context;
        private Predicate<MetricKey> metricFilter = s -> true;

        private Builder() {
        }

        public Builder setDomain(String domain) {
            this.domain = domain;
            return this;
        }

        public Builder setContext(Context context) {
            this.context = context;
            return this;
        }
        public Builder setMetricFilter(Predicate<MetricKey> metricFilter) {
            this.metricFilter = metricFilter;
            return this;
        }

        public CPUMetricsCollector build() {
            Objects.requireNonNull(this.context);
            Objects.requireNonNull(this.domain);
            return new CPUMetricsCollector(context, domain, metricFilter);
        }
    }
}
