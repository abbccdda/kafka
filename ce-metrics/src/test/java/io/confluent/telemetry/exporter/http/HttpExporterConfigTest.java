package io.confluent.telemetry.exporter.http;

import com.google.common.collect.ImmutableMap;
import io.confluent.observability.telemetry.client.BufferingAsyncTelemetryHttpClient.Builder;
import io.confluent.observability.telemetry.client.CompressionAlgorithm;
import io.confluent.observability.telemetry.client.TelemetryHttpClient;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsRequest;
import io.confluent.observability.telemetry.v1.TelemetryReceiverSubmitMetricsResponse;
import io.opencensus.proto.metrics.v1.Metric;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class HttpExporterConfigTest {

    Map<String, String> minimalConfig = ImmutableMap.of();

    @Test
    public void testGetUriValid() {
        String url = "https://api.telemetry.confluent.cloud";
        HttpExporterConfig config = new HttpExporterConfig(
            ImmutableMap.<String, String>builder()
                .putAll(minimalConfig)
                .put(HttpExporterConfig.CLIENT_BASE_URL, url).build()
        );

        assertThat(config.getString(HttpExporterConfig.CLIENT_BASE_URL)).isEqualTo(url);
    }

    @Test
    public void testGetUriNull() {
        HttpExporterConfig config = new HttpExporterConfig(minimalConfig);

        assertThat(config.getString(HttpExporterConfig.CLIENT_BASE_URL)).isNull();
    }

    @Test
    public void testGetUriInvalid() {
        String url = "https://";

        assertThatThrownBy(() -> new HttpExporterConfig(
            ImmutableMap.<String, String>builder()
                .putAll(minimalConfig)
                .put(HttpExporterConfig.CLIENT_BASE_URL, url).build()
        )).isInstanceOf(
            ConfigException.class).hasMessageStartingWith("Invalid URI for property");
    }

    @Test
    public void testGetCompressionAlgorithmValid() {
        String compression = "gzip";
        HttpExporterConfig config = new HttpExporterConfig(
            ImmutableMap.<String, String>builder()
                .putAll(minimalConfig)
                .put(HttpExporterConfig.CLIENT_COMPRESSION, compression).build()
        );

        assertThat(config.getCompressionAlgorithm(HttpExporterConfig.CLIENT_COMPRESSION)).isEqualTo(
            CompressionAlgorithm.GZIP);
    }

    @Test
    public void testGetCompressionAlgorithmNull() {
        HttpExporterConfig config = new HttpExporterConfig(minimalConfig);
        assertThat(config.getCompressionAlgorithm(HttpExporterConfig.CLIENT_COMPRESSION)).isNull();
    }

    @Test
    public void testGetCompressionAlgorithmInvalid() {
        String compression = "unknown";
        HttpExporterConfig config = new HttpExporterConfig(
            ImmutableMap.<String, String>builder()
                .putAll(minimalConfig)
                .put(HttpExporterConfig.CLIENT_COMPRESSION, compression).build()
        );

        assertThat(config.getCompressionAlgorithm(HttpExporterConfig.CLIENT_COMPRESSION)).isNull();
    }

    @Test
    public void testGetClientBuilderDefaults() {
        HttpExporterConfig config = new HttpExporterConfig(minimalConfig);
        TelemetryHttpClient.Builder<TelemetryReceiverSubmitMetricsResponse> builder = config
            .getClientBuilder();
        assertThat(builder).isNotNull();
    }

    @Test
    public void testGetBufferingBuilderDefaults() {
        HttpExporterConfig config = new HttpExporterConfig(minimalConfig);
        Builder<Metric, TelemetryReceiverSubmitMetricsRequest, TelemetryReceiverSubmitMetricsResponse> builder = config
            .getBufferingAsyncClientBuilder();
        assertThat(builder).isNotNull();
    }
}
