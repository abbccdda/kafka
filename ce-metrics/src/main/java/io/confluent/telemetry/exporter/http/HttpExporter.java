package io.confluent.telemetry.exporter.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClient;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClientBatchResult;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClientStats;
import io.confluent.observability.telemetry.client.TelemetryHttpClient;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsRequest;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsResponse;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.MetricsCollectorProvider;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpExporter implements Exporter, MetricsCollectorProvider {

    private static final Logger log = LoggerFactory.getLogger(HttpExporter.class);
    public static final String GROUP = "http_exporter";
    private static final Double SECONDS_PER_MILLISECOND = 1e-3;
    private static final Function<Collection<Metric>, TelemetryReceiverSubmitMetricsRequest> REQUEST_CONVERTER =
        metrics -> TelemetryReceiverSubmitMetricsRequest.newBuilder().addAllMetrics(metrics)
            .build();
    private static final Function<ByteBuffer, TelemetryReceiverSubmitMetricsResponse> RESPONSE_DESERIALIZER = bytes -> {
        try {
            return TelemetryReceiverSubmitMetricsResponse.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    };
    private final BufferingAsyncTelemetryHttpClient<Metric, TelemetryReceiverSubmitMetricsRequest, TelemetryReceiverSubmitMetricsResponse> bufferingClient;

    public HttpExporter(HttpExporterConfig config) {

        this(config.getBufferingAsyncClientBuilder()
            .setClient(
                config.getClientBuilder()
                    .setResponseDeserializer(RESPONSE_DESERIALIZER)
                    .setEndpoint(TelemetryHttpClient.V1_METRICS_ENDPOINT)
                    .build()
            )
            .setCreateRequestFn(REQUEST_CONVERTER)
            .build());
    }

    public HttpExporter(
        BufferingAsyncTelemetryHttpClient<Metric, TelemetryReceiverSubmitMetricsRequest, TelemetryReceiverSubmitMetricsResponse> bufferingClient) {
        this.bufferingClient = bufferingClient;
        // subscribe to batch results.
        this.bufferingClient.getBatchResults().doOnNext(this::trackMetricResponses);
    }

    private void trackMetricResponses(
        BufferingAsyncTelemetryHttpClientBatchResult<Metric, TelemetryReceiverSubmitMetricsResponse> batchResult) {
        if (!batchResult.isSuccess()) {
            log.error("Confluent Telemetry Metrics Failure", batchResult.getThrowable());
        }
    }

    @VisibleForTesting
    BufferingAsyncTelemetryHttpClientStats stats() {
        return this.bufferingClient.stats();
    }

    @Override
    public void export(Collection<Metric> metrics) throws RuntimeException {
        this.bufferingClient.submit(metrics);
    }

    @Override
    public void close() {
        this.bufferingClient.close();
    }

    @Override
    public MetricsCollector collector(
        ConfluentTelemetryConfig config, Context context, String domain) {
        Predicate<MetricKey> metricFilter = config.getMetricFilter();
        return new MetricsCollector() {
            @Override
            public Collection<Metric> collect() {
                List<Metric> out = new ArrayList<>();

                BufferingAsyncTelemetryHttpClientStats stats = bufferingClient.stats();
                Timestamp now = MetricsUtils.now();

                Map<String, String> labels = new HashMap<>();
                if (context.isDebugEnabled()) {
                    labels.put(
                        io.confluent.telemetry.collector.MetricsCollector.LABEL_LIBRARY,
                        io.confluent.telemetry.collector.MetricsCollector.LIBRARY_NONE);
                }

                // Three metrics for total batches: dropped, success, failed
                String batchName = MetricsUtils.fullMetricName(domain, GROUP, "batches_total");

                Map<String, Long> statusToBatchValue = ImmutableMap.of(
                    "dropped", stats.getTotalDroppedBatches(),
                    "success", stats.getTotalSuccessfulBatches(),
                    "failed", stats.getTotalFailedBatches());

                statusToBatchValue.forEach((status, value) -> {
                    Map<String, String> statusLabels = ImmutableMap.<String, String>builder()
                        .putAll(labels).put("status", status).build();
                    if (!metricFilter.test(new MetricKey(batchName, statusLabels))) {
                        return;
                    }
                    out.add(
                        context.metricWithSinglePointTimeseries(
                            batchName,
                            Type.CUMULATIVE_INT64,
                            statusLabels,
                            Point.newBuilder()
                                .setTimestamp(now)
                                .setInt64Value(value).build())
                    );
                });

                // Two metrics for total items: success, failed. We don't have a "dropped items" metric.
                String itemName = MetricsUtils.fullMetricName(domain, GROUP, "items_total");
                Map<String, Long> statusToItemValue = ImmutableMap.of(
                    "success", stats.getTotalSuccessfulItems(),
                    "failed", stats.getTotalFailedItems());
                statusToItemValue.forEach((status, value) -> {
                    Map<String, String> statusLabels = ImmutableMap.<String, String>builder()
                        .putAll(labels).put("status", status).build();
                    if (!metricFilter.test(new MetricKey(itemName, statusLabels))) {
                        return;
                    }
                    out.add(
                        context.metricWithSinglePointTimeseries(
                            itemName,
                            Type.CUMULATIVE_INT64,
                            statusLabels,
                            Point.newBuilder()
                                .setTimestamp(now)
                                .setInt64Value(value).build())
                    );
                });

                // Timing metric. This is converted to seconds and sent as a Cumulative Double
                String timingName = MetricsUtils
                    .fullMetricName(domain, GROUP, "send_time_seconds");
                if (metricFilter.test(new MetricKey(timingName, labels))) {
                    out.add(
                        context.metricWithSinglePointTimeseries(
                            timingName,
                            Type.CUMULATIVE_DOUBLE,
                            labels,
                            Point.newBuilder()
                                .setTimestamp(now)
                                .setDoubleValue(
                                    stats.getTotalSendTimeMs() * SECONDS_PER_MILLISECOND)
                                .build())
                    );
                }
                return out;
            }
        };
    }
}
