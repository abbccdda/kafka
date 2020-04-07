package io.confluent.telemetry.exporter.http;

import com.google.common.collect.ImmutableMap;

import io.confluent.shaded.io.reactivex.Observable;
import org.assertj.core.data.Offset;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.telemetry.ResourceBuilderFacade;
import io.confluent.telemetry.TelemetryResourceType;
import io.confluent.telemetry.client.BufferingAsyncTelemetryHttpClient;
import io.confluent.telemetry.client.BufferingAsyncTelemetryHttpClientStats;
import io.confluent.telemetry.v1.TelemetryReceiverSubmitMetricsRequest;
import io.confluent.telemetry.v1.TelemetryReceiverSubmitMetricsResponse;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.telemetry.collector.MetricsCollector;
import io.confluent.telemetry.exporter.TestExporter;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.resource.v1.Resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpExporterTest {

    @Mock
    BufferingAsyncTelemetryHttpClient<Metric, TelemetryReceiverSubmitMetricsRequest, TelemetryReceiverSubmitMetricsResponse> bufferingClient;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Test
    public void testBuildFromConfig() {
        Map<String, String> minimalConfig = ImmutableMap.of(
            HttpExporterConfig.API_KEY, "apikey",
            HttpExporterConfig.API_SECRET_KEY, "apisecretkey");

        HttpExporter exporter = new HttpExporter(new HttpExporterConfig(minimalConfig));

        BufferingAsyncTelemetryHttpClientStats stats = exporter.stats();

        assertThat(stats.getTotalItems()).isEqualTo(0);
    }

    @Test
    public void testEmit() {

        when(bufferingClient.getBatchResults()).thenReturn(Observable.empty());
        HttpExporter exporter = new HttpExporter(bufferingClient);

        Metric metric = Metric.newBuilder().setMetricDescriptor(
            MetricDescriptor.newBuilder().setName("test").build()).build();
        exporter.emit(metric);

        verify(bufferingClient).submit(Collections.singleton(metric));
    }

    @Test
    public void testCollector() {
        Map<String, String> minimalConfig = ImmutableMap.of(
            HttpExporterConfig.API_KEY, "apikey",
            HttpExporterConfig.API_SECRET_KEY, "apisecretkey",
            ConfluentTelemetryConfig.WHITELIST_CONFIG, ".*");

        HttpExporter exporter = new HttpExporter(new HttpExporterConfig(minimalConfig));

        Resource resource = new ResourceBuilderFacade(TelemetryResourceType.KAFKA)
            .withLabel("resource_label", "123")
            .withVersion("mockVersion")
            .withId("mockId")
            .build();

        Context context = new Context(resource, false, false);
        MetricsCollector collector = exporter
            .collector(key -> true, context, "io.confluent");

        TestExporter testExporter = new TestExporter();
        collector.collect(testExporter);
        List<Metric> result = testExporter.emittedMetrics();
        assertThat(result.size()).isEqualTo(6);

        Optional<Metric> submissionTimeMetric = result.stream()
            .filter(metric -> metric.getMetricDescriptor().getName().endsWith("send_time_seconds"))
            .findAny();
        assertThat(submissionTimeMetric.isPresent()).isTrue();
        assertThat(submissionTimeMetric.get().getTimeseriesList().get(0).getPointsList().get(0)
            .getDoubleValue()).isCloseTo(0.0,
            Offset.offset(1e9));
    }

}
