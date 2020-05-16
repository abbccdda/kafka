package io.confluent.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;

import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.http.HttpExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;

import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import org.assertj.core.api.Condition;
import org.junit.Test;

public class ConfluentTelemetryConfigTest {

  private final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder();

  @Test
  public void metricFilterDefaults() {
    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());

    Predicate<MetricKey> filter = config.buildMetricWhitelistFilter();

    assertTrue(filter.test(new MetricKey("foobar/bytes_in_per_sec/total", Collections.emptyMap())));
    assertFalse(filter.test(new MetricKey("foobar/bytes_in_per_asdfsec/total",
        Collections.emptyMap())));
  }

  @Test
  public void metricFilterOverride() {
    builder.put(ConfluentTelemetryConfig.WHITELIST_CONFIG, ".*only_match_me.*");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());

    Predicate<MetricKey> filter = config.buildMetricWhitelistFilter();

    assertFalse(filter.test(new MetricKey("foobar/bytes_in_per_sec/total", Collections.emptyMap())));
    assertTrue(filter.test(new MetricKey("foobar/only_match_me/total",
        Collections.emptyMap())));
  }

  @Test
  public void metricFilterOverrideEmpty() {
    builder.put(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());

    Predicate<MetricKey> filter = config.buildMetricWhitelistFilter();

    assertEquals(ConfluentTelemetryConfig.ALWAYS_TRUE, filter);
  }

  @Test
  public void metricFilterTestCompleteStringMatch() {
    builder.put(ConfluentTelemetryConfig.WHITELIST_CONFIG, ".*match_complete_string");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());

    Predicate<MetricKey> filter = config.buildMetricWhitelistFilter();

    // Below metric shall not be included as its not matching the given regex. Hence use `Matches`
    // method of `Pattern` class instead of `Find` method. Always check for complete string match
    // instead of sub string match.
    assertFalse(filter.test(new MetricKey("foobar/match_complete_string/total",
        Collections.emptyMap())));
    assertTrue(filter.test(new MetricKey("foobar/match_complete_string",
        Collections.emptyMap())));
  }

  @Test
  public void testDefaultPublishPeriodIsUsed() {
    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());
    assertEquals(ConfluentTelemetryConfig.DEFAULT_COLLECT_INTERVAL,
            config.getLong(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG));
  }

  @Test
  public void testNamedExporterNoConfig() {
    builder
      // disable the default exporters
      .put(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false")
      .put(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");
    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());
    assertThat(config.enabledExporters()).hasSize(0);
  }

  @Test
  public void testNamedExporterKafkaConfig() {
    String exporterPrefix = ConfluentTelemetryConfig.exporterPrefixForName("test");
    builder
        .put(exporterPrefix + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name())
        .put(exporterPrefix + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234")
        .put(exporterPrefix + KafkaExporterConfig.TOPIC_NAME_CONFIG, "topicName")
        .put(exporterPrefix + KafkaExporterConfig.TOPIC_CREATE_CONFIG, false)
        .put(exporterPrefix + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, 2)
        .put(exporterPrefix + KafkaExporterConfig.TOPIC_PARTITIONS_CONFIG, 4)
        .put(exporterPrefix + KafkaExporterConfig.TOPIC_RETENTION_MS_CONFIG, 1000);

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());
    Map<String, ExporterConfig> exporterConfigs = config.enabledExporters();
    assertThat(exporterConfigs)
      .hasEntrySatisfying("test", new Condition<>(c -> c instanceof KafkaExporterConfig, "is KafkaExporterConfig"));

    KafkaExporterConfig actual = (KafkaExporterConfig) exporterConfigs.get("test");
    KafkaExporterConfig expected =
        new KafkaExporterConfig(
          config.originalsWithPrefix(exporterPrefix)
        );
    assertEquals(expected, actual);
  }

  @Test
  public void testNamedExporterHttpConfig() {
    String exporterPrefix = ConfluentTelemetryConfig.exporterPrefixForName("test");
    builder
        .put(exporterPrefix + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.http.name())
        .put(exporterPrefix + HttpExporterConfig.CLIENT_BASE_URL, "https://api.telemetry.confluent.cloud")
        .put(exporterPrefix + HttpExporterConfig.CLIENT_COMPRESSION, "gzip");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());
    Map<String, ExporterConfig> exporterConfigs = config.enabledExporters();
    assertThat(exporterConfigs)
      .hasEntrySatisfying("test", new Condition<>(c -> c instanceof HttpExporterConfig, "is HttpExporterConfig"));

    HttpExporterConfig actual = (HttpExporterConfig) exporterConfigs.get("test");
    HttpExporterConfig expected =
        new HttpExporterConfig(
          config.originalsWithPrefix(exporterPrefix)
        );
    assertEquals(expected, actual);
  }

  @Test
  public void testDefaultExporters() {
    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());

    Map<String, ExporterConfig> exporterConfigs = config.enabledExporters();
    assertThat(exporterConfigs)
      .hasEntrySatisfying(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME, new Condition<>(c -> c instanceof KafkaExporterConfig, "is KafkaExporterConfig"));
    assertEquals(new KafkaExporterConfig(ConfluentTelemetryConfig.EXPORTER_LOCAL_DEFAULTS), (KafkaExporterConfig) exporterConfigs.get(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME));

    assertThat(exporterConfigs)
        .hasEntrySatisfying(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME, new Condition<>(c -> c instanceof HttpExporterConfig, "is HttpExporterConfig"));
    assertEquals(new HttpExporterConfig(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_DEFAULTS), (HttpExporterConfig) exporterConfigs.get(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME));
  }
}
