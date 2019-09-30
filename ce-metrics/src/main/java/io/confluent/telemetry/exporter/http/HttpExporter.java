package io.confluent.telemetry.exporter.http;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClient;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClientBatchResult;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClientStats;
import io.confluent.observability.telemetry.client.TelemetryHttpClient;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsRequest;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsResponse;
import io.confluent.telemetry.exporter.Exporter;
import io.opencensus.proto.metrics.v1.Metric;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpExporter implements Exporter {

    private static final Logger log = LoggerFactory.getLogger(HttpExporter.class);
    private static final Function<Collection<Metric>, TelemetryReceiverSubmitMetricsRequest> REQUEST_CONVERTER =
        metrics -> TelemetryReceiverSubmitMetricsRequest.newBuilder().addAllMetrics(metrics).build();
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

    public HttpExporter(BufferingAsyncTelemetryHttpClient<Metric, TelemetryReceiverSubmitMetricsRequest, TelemetryReceiverSubmitMetricsResponse> bufferingClient) {
        this.bufferingClient = bufferingClient;
        // subscribe to batch results.
        this.bufferingClient.getBatchResults().doOnNext(this::trackMetricResponses);
    }

    private void trackMetricResponses(BufferingAsyncTelemetryHttpClientBatchResult<Metric, TelemetryReceiverSubmitMetricsResponse> batchResult) {
        if (!batchResult.isSuccess()) {
            log.error("Confluent Telemetry Metrics Failure", batchResult.getThrowable());
        }
    }

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

}
