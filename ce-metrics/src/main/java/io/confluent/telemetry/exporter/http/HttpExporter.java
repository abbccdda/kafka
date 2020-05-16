package io.confluent.telemetry.exporter.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import io.confluent.telemetry.client.BufferingAsyncTelemetryHttpClient;
import io.confluent.telemetry.client.BufferingAsyncTelemetryHttpClientBatchResult;
import io.confluent.telemetry.client.BufferingAsyncTelemetryHttpClientStats;
import io.confluent.telemetry.client.TelemetryHttpClient;
import io.opencensus.proto.agent.metrics.v1.ExportMetricsServiceRequest;
import io.opencensus.proto.agent.metrics.v1.ExportMetricsServiceResponse;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.MetricKey;
import io.confluent.telemetry.MetricsUtils;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.collector.MetricsCollectorProvider;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor.Type;
import io.opencensus.proto.metrics.v1.Point;

public class HttpExporter implements Exporter, MetricsCollectorProvider {

    private static final Logger log = LoggerFactory.getLogger(HttpExporter.class);
    public static final String GROUP = "http_exporter";
    private static final Double SECONDS_PER_MILLISECOND = 1e-3;
    private static final Function<Collection<Metric>, ExportMetricsServiceRequest> REQUEST_CONVERTER =
        metrics -> ExportMetricsServiceRequest.newBuilder().addAllMetrics(metrics)
            .build();
    private static final Function<ByteBuffer, ExportMetricsServiceResponse> RESPONSE_DESERIALIZER = bytes -> {
        try {
            return ExportMetricsServiceResponse.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    };
    private final BufferingAsyncTelemetryHttpClient<Metric, ExportMetricsServiceRequest, ExportMetricsServiceResponse> bufferingClient;
    public volatile boolean canEmitMetrics = false;

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
        this.canEmitMetrics = config.canEmitMetrics();
    }

    @VisibleForTesting
    HttpExporter(
        BufferingAsyncTelemetryHttpClient<Metric, ExportMetricsServiceRequest, ExportMetricsServiceResponse> bufferingClient) {
        this.bufferingClient = bufferingClient;
        // subscribe to batch results.
        this.bufferingClient.getBatchResults().doOnNext(this::trackMetricResponses);
    }

    @VisibleForTesting
    void setCanEmitMetrics(boolean canEmitMetrics) {
        this.canEmitMetrics = canEmitMetrics;
    }

    private void trackMetricResponses(
        BufferingAsyncTelemetryHttpClientBatchResult<Metric, ExportMetricsServiceResponse> batchResult) {
        if (!batchResult.isSuccess()) {
            log.error("Confluent Telemetry Metrics Failure", batchResult.getThrowable());
        }
    }

    @VisibleForTesting
    BufferingAsyncTelemetryHttpClientStats stats() {
        return this.bufferingClient.stats();
    }

    @Override
    public void emit(Metric metric) throws RuntimeException {
        if (!canEmitMetrics) {
            return;
        }
        this.bufferingClient.submit(Collections.singleton(metric));
    }

    @Override
    public void close() {
        this.bufferingClient.close();
    }

    @Override
    public MetricsCollector collector(
        Predicate<MetricKey> whitelistPredicate, Context context, String domain) {
        return new MetricsCollector() {

            private volatile Predicate<MetricKey> metricWhitelistFilter = whitelistPredicate;

            @Override
            public void collect(Exporter exporter) {
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
                    if (!metricWhitelistFilter.test(new MetricKey(batchName, statusLabels))) {
                        return;
                    }
                    exporter.emit(
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
                    if (!metricWhitelistFilter.test(new MetricKey(itemName, statusLabels))) {
                        return;
                    }
                    exporter.emit(
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
                if (metricWhitelistFilter.test(new MetricKey(timingName, labels))) {
                    exporter.emit(
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
            }

            @Override
            public void reconfigureWhitelist(Predicate<MetricKey> whitelistPredicate) {
                this.metricWhitelistFilter = whitelistPredicate;
            }
        };
    }

    public void reconfigure(HttpExporterConfig config) {
        String apiKey = config.getString(HttpExporterConfig.API_KEY);
        String apiSecretKey = config.getString(HttpExporterConfig.API_SECRET);
        if (config.canEmitMetrics()) {
            canEmitMetrics = true;
        } else {
            canEmitMetrics = false;
        }
        this.bufferingClient.updateCredentials(apiKey, apiSecretKey);
    }
}
