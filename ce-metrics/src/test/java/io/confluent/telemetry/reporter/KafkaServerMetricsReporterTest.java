package io.confluent.telemetry.reporter;

import com.google.common.collect.ImmutableMap;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.http.HttpExporter;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import java.util.Map;
import java.util.regex.PatternSyntaxException;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNull;

public class KafkaServerMetricsReporterTest {

    KafkaServerMetricsReporter reporter;

    @Before
    public void setUp() {
        reporter = new KafkaServerMetricsReporter();
    }

    @After
    public void tearDown() {
        reporter.close();
    }

    @Test
    public void testOnUpdateInvalidRegex() {
        Map<String, String> configs = ImmutableMap.of(
            ConfluentTelemetryConfig.EXPORTER_KAFKA_ENABLED_CONFIG, "false",
            ConfluentTelemetryConfig.WHITELIST_CONFIG, "(.",
            KafkaConfig.BrokerIdProp(), "1");
        reporter.configure(configs);
        assertThatThrownBy(() -> reporter.onUpdate(new ClusterResource("clusterid")))
            .isInstanceOf(PatternSyntaxException.class);
    }

    @Test
    public void testInitConfigsInvalidIntervalConfig() {
        Map<String, String> configs = ImmutableMap
            .of(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "not-a-number");

        assertThatThrownBy(() -> reporter.configure(configs)).isInstanceOf(ConfigException.class);
    }

    @Test
    public void testInitConfigsNoExporters() {
        Map<String, String> configs = ImmutableMap.of(
            ConfluentTelemetryConfig.EXPORTER_KAFKA_ENABLED_CONFIG, "false"
        );
        reporter.configure(configs);

        assertThat(reporter.getExporters()).isEmpty();
    }

    @Test
    public void testInitNonBrokers() {
        Map<String, String> configs = ImmutableMap.of(
            ConfluentTelemetryConfig.EXPORTER_KAFKA_ENABLED_CONFIG, "true",
            KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        reporter.configure(configs);
        reporter.onUpdate(new ClusterResource(null));
        assertNull(reporter.getCollectors());
    }

    @Test
    public void initKafkaExporterFails() {
        Map<String, String> configs = ImmutableMap.of();
        assertThatThrownBy(() -> reporter.configure(configs)).isInstanceOf(ConfigException.class);
    }

    @Test
    public void initKafkaExporterSuccess() {
        Map<String, String> configs = ImmutableMap.of(
            ConfluentTelemetryConfig.EXPORTER_KAFKA_ENABLED_CONFIG, "true",
            KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        reporter.configure(configs);

        assertThat(reporter.getExporters()).hasAtLeastOneElementOfType(KafkaExporter.class);
    }

    @Test
    public void initHttpExporterFails() {
        Map<String, String> configs = ImmutableMap.of(
            ConfluentTelemetryConfig.EXPORTER_KAFKA_ENABLED_CONFIG, "false",
            ConfluentTelemetryConfig.EXPORTER_HTTP_ENABLED_CONFIG, "true",
            HttpExporterConfig.BUFFER_MAX_BATCH_SIZE, "not-a-number");
        assertThatThrownBy(() -> reporter.configure(configs)).isInstanceOf(ConfigException.class);
    }

    @Test
    public void initHttpExporterSuccess() {
        Map<String, String> configs = ImmutableMap.of(
            ConfluentTelemetryConfig.EXPORTER_KAFKA_ENABLED_CONFIG, "false",
            ConfluentTelemetryConfig.EXPORTER_HTTP_ENABLED_CONFIG, "true");
        reporter.configure(configs);
        assertThat(reporter.getExporters()).hasAtLeastOneElementOfType(HttpExporter.class);
    }

    @Test
    public void testOnUpdateWithHttpExporterRegistersCollector() {
        Map<String, String> configs = ImmutableMap.of(
            KafkaConfig.BrokerIdProp(), "1",
            KafkaConfig.LogDirsProp(), System.getProperty("java.io.tmpdir"),
            ConfluentTelemetryConfig.EXPORTER_KAFKA_ENABLED_CONFIG, "false",
            ConfluentTelemetryConfig.EXPORTER_HTTP_ENABLED_CONFIG, "true");
        reporter.configure(configs);
        reporter.onUpdate(new ClusterResource("clusterid"));
        assertThat(reporter.getCollectors())
            .filteredOn(c -> c.getClass().getEnclosingClass() != null)
            .extracting(c -> c.getClass().getEnclosingClass().toString())
            .contains(HttpExporter.class
                .toString()); // NOTE convert to string because compiler isn't happy about Class<?>
    }
}
