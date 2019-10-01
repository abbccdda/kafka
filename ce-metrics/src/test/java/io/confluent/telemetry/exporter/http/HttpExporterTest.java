package io.confluent.telemetry.exporter.http;

import com.google.common.collect.ImmutableMap;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClient;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClientStats;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsRequest;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsResponse;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.Context;
import io.confluent.observability.telemetry.ResourceBuilderFacade;
import io.confluent.observability.telemetry.TelemetryResourceType;
import io.confluent.telemetry.collector.MetricsCollector;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.MetricDescriptor;
import io.opencensus.proto.resource.v1.Resource;
import io.reactivex.Observable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.data.Offset;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

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
    public void testExport() {

        when(bufferingClient.getBatchResults()).thenReturn(Observable.empty());
        HttpExporter exporter = new HttpExporter(bufferingClient);

        List<Metric> metrics = Arrays
            .asList(Metric.newBuilder().setMetricDescriptor(
                MetricDescriptor.newBuilder().setName("test").build()).build());
        exporter.export(metrics);

        verify(bufferingClient).submit(metrics);
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
            .collector(new ConfluentTelemetryConfig(minimalConfig), context, "io.confluent");

        Collection<Metric> metrics = collector.collect();
        assertThat(metrics.size()).isEqualTo(6);

        Optional<Metric> submissionTimeMetric = metrics.stream()
            .filter(metric -> metric.getMetricDescriptor().getName().endsWith("send_time_seconds"))
            .findAny();
        assertThat(submissionTimeMetric.isPresent()).isTrue();
        assertThat(submissionTimeMetric.get().getTimeseriesList().get(0).getPointsList().get(0)
            .getDoubleValue()).isCloseTo(0.0,
            Offset.offset(1e9));
    }

}
