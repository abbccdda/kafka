package io.confluent.telemetry.reporter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.http.HttpExporter;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.server.KafkaConfig;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.config.ConfigException;

import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KafkaServerMetricsReporterTest {

    KafkaServerMetricsReporter reporter;
    Map<String, Object> configs;

    @Before
    public void setUp() {
        reporter = new KafkaServerMetricsReporter();
        configs = new HashMap<>();
        configs.put(KafkaConfig.BrokerIdProp(), "1");
        configs.put(KafkaConfig.LogDirsProp(), System.getProperty("java.io.tmpdir"));
    }

    @After
    public void tearDown() {
        reporter.close();
    }

    @Test
    public void testOnUpdateInvalidRegex() {
        configs.put(ConfluentTelemetryConfig.WHITELIST_CONFIG, "(.");
        configs.put(KafkaConfig.BrokerIdProp(), "1");
        assertThatThrownBy(() -> reporter.configure(configs))
            .isInstanceOf(ConfigException.class);
    }

    @Test
    public void testInitConfigsInvalidIntervalConfig() {
        configs.put(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "not-a-number");
        assertThatThrownBy(() -> reporter.configure(configs)).isInstanceOf(ConfigException.class);
    }

    @Test
    public void testInitConfigsNoExporters() {
        disableDefaultExporters();
        reporter.configure(configs);
        reporter.onUpdate(new ClusterResource("clusterid"));
        assertThat(reporter.getExporters()).hasSize(0);
    }

    @Test
    public void testInitNonBrokers() {
        disableDefaultExporters();
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
            ExporterConfig.ExporterType.kafka.name()
        );
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("name") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG,
            "127.0.0.1:9092"
        );
        reporter.configure(configs);
        reporter.onUpdate(new ClusterResource(null));
        assertThat(reporter.getCollectors()).isEmpty();
    }

    @Test
    public void initKafkaExporterFails() {
        disableDefaultExporters();
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.TYPE_CONFIG,
            ExporterConfig.ExporterType.kafka.name()
        );
        assertThatThrownBy(() -> reporter.configure(configs)).isInstanceOf(ConfigException.class);
    }

    @Test
    public void initKafkaExporterSuccess() {
        disableDefaultExporters();
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
            ExporterConfig.ExporterType.kafka.name()
        );
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("name") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG,
            "127.0.0.1:9092"
        );
        reporter.configure(configs);
        reporter.onUpdate(new ClusterResource("clusterid"));
        assertThat(reporter.getExporters())
            .hasEntrySatisfying("name", new Condition<>(c -> c instanceof KafkaExporter, "is KafkaExporter"));
    }

    @Test
    public void initHttpExporterFails() {
        disableDefaultExporters();
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
            ExporterConfig.ExporterType.http.name()
        );
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("name") + HttpExporterConfig.BUFFER_MAX_BATCH_SIZE,
            "not-a-number"
        );
        assertThatThrownBy(() -> reporter.configure(configs)).isInstanceOf(ConfigException.class);
    }

    @Test
    public void initHttpExporterSuccess() {
        disableDefaultExporters();
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
            ExporterConfig.ExporterType.http.name()
        );
        reporter.configure(configs);
        reporter.onUpdate(new ClusterResource("clusterid"));
        assertThat(reporter.getExporters())
            .hasEntrySatisfying("name", new Condition<>(c -> c instanceof HttpExporter, "is HttpExporter"));
    }

    @Test
    public void testOnUpdateWithHttpExporterRegistersCollector() {
        disableDefaultExporters();
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
            ExporterConfig.ExporterType.http.name()
        );
        reporter.configure(configs);
        reporter.onUpdate(new ClusterResource("clusterid"));
        assertThat(reporter.getCollectors())
            .filteredOn(c -> c.getClass().getEnclosingClass() != null)
            .extracting(c -> c.getClass().getEnclosingClass().toString())
            .contains(HttpExporter.class
                .toString()); // NOTE convert to string because compiler isn't happy about Class<?>
    }

    @Test
    public void testReconfigurables() {
        configs.putAll(
            ImmutableMap.of(
                // kafka exporter
                ConfluentTelemetryConfig.exporterPrefixForName("kafka") + ExporterConfig.TYPE_CONFIG,
                ExporterConfig.ExporterType.kafka.name(),
                ConfluentTelemetryConfig.exporterPrefixForName("kafka") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092",

                // http exporter
                ConfluentTelemetryConfig.exporterPrefixForName("http") + ExporterConfig.TYPE_CONFIG,
                ExporterConfig.ExporterType.http.name()
            )
        );

        Set<String> httpExporters = ImmutableSet.of("http", ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME);
        Set<String> allExporters =
            new ImmutableSet.Builder<String>()
                .addAll(httpExporters)
                .add("kafka")
                .add(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
                .build();

        reporter.configure(configs);

        Set<String> expectedConfigs = new HashSet<>();
        expectedConfigs.addAll(ConfluentTelemetryConfig.RECONFIGURABLES);

        // add named http exporter configs
        for (String name : httpExporters) {
            for (String configName : HttpExporterConfig.RECONFIGURABLE_CONFIGS) {
                expectedConfigs.add(
                    ConfluentTelemetryConfig.exporterPrefixForName(name) + configName
                );
            }
        }

        // add named exporter configs
        for (String name : allExporters) {
            for (String configName : ExporterConfig.RECONFIGURABLES) {
                expectedConfigs.add(
                    ConfluentTelemetryConfig.exporterPrefixForName(name) + configName
                );
            }
        }

        assertThat(reporter.reconfigurableConfigs())
            .containsAll(expectedConfigs)
            .hasSize(expectedConfigs.size());
    }

    private void disableDefaultExporters() {
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
                + ExporterConfig.ENABLED_CONFIG,
                "false"
        );
        configs.put(
            ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
                + ExporterConfig.ENABLED_CONFIG,
            "false"
        );
    }
}
