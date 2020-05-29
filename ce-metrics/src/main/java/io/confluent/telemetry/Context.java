package io.confluent.telemetry;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Timestamp;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.resource.v1.Resource;
import java.util.Map;

/**
 * Context for metrics collectors.
 *
 * Encapsulates metadata such as the OpenCensus {@link Resource} and
 * includes utility methods for constructing {@link Metric}s that automatically
 * attach the <code>Resource</code> to the <code>Metric</code>.
 */
public class Context {

    private final Resource resource;

    private final boolean debugEnabled;

    private final String domain;

    /**
     * For backwards compatibility with downstream consumers (i.e. druid) the resource labels
     * may be duplicated as individual timeseries metric labels.
     * TODO remove this once downstream consumers have cutover to new format
     */
    private final boolean duplicateResourceLabelsOnTimeseries;

    @VisibleForTesting
    public Context(String domain) {
        this(Resource.getDefaultInstance(), domain);
    }

    public Context(Resource resource, String domain) {
        this(resource, domain, false, false);
    }

    public Context(Resource resource, String domain, boolean debugEnabled) {
        this(resource, domain, debugEnabled, false);
    }

    public Context(Resource resource, String domain, boolean debugEnabled, boolean duplicateResourceLabelsOnTimeseries) {
        this.resource = resource;
        this.debugEnabled = debugEnabled;
        this.duplicateResourceLabelsOnTimeseries = duplicateResourceLabelsOnTimeseries;
        this.domain = domain;
    }

    /**
     * Get the {@link Resource} in this Context.
     * The <code>Resource</code> represents the entity for which telemetry is being collected.
     *
     * <p>
     * The <code>Resource</code> <b>must</b> be set on every <code>Metric</code> collected and reported.
     * This can be enforced by using the {@link #newMetricBuilder()} method to construct <code>Metrics</code>.
     */
    public Resource getResource() {
        return resource;
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public String getDomain() {
        return this.domain;
    }
    /**
     * Build a {@link Metric} associated with this context's {@link Resource} having a single
     * timeseries containing a single point.
     *
     * @see MetricBuilderFacade#addSinglePointTimeseries(Point)
     */
    public Metric metricWithSinglePointTimeseries(
        String name,
        MetricDescriptor.Type type,
        Map<String, String> metricLabels,
        Point point
    ) {
        return metricWithSinglePointTimeseries(name, type, metricLabels, point, null);
    }

    /**
     * Build a {@link Metric} associated with this context's {@link Resource} having a single
     * timeseries containing a single point.
     *
     * @see MetricBuilderFacade#addSinglePointTimeseries(Point, Timestamp)
     */
    public Metric metricWithSinglePointTimeseries(
        String name,
        MetricDescriptor.Type type,
        Map<String, String> metricLabels,
        Point point,
        Timestamp startTimestamp
    ) {
        return newMetricBuilder()
            .withName(name)
            .withType(type)
            .withLabels(metricLabels)
            .addSinglePointTimeseries(point, startTimestamp)
            .build();
    }

    /**
     * Create a new {@link MetricBuilderFacade} pre-populated with this Context's {@link Resource}
     */
    public MetricBuilderFacade newMetricBuilder() {
        MetricBuilderFacade builder = new MetricBuilderFacade().withResource(resource);
        if (duplicateResourceLabelsOnTimeseries) {
            builder.withLabels(resource.getLabelsMap());
        }
        return builder;
    }

}
