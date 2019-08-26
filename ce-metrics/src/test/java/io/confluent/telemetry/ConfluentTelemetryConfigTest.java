package io.confluent.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import java.util.Collections;
import java.util.function.Predicate;
import org.junit.Test;

public class ConfluentTelemetryConfigTest {

  private final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
      .put(KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");

  @Test
  public void metricFilterDefaults() {
    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());

    Predicate<MetricKey> filter = config.getMetricFilter();

    assertTrue(filter.test(new MetricKey("foobar/bytes_in_per_sec/total", Collections.emptyMap())));
    assertFalse(filter.test(new MetricKey("foobar/bytes_in_per_asdfsec/total",
        Collections.emptyMap())));
  }

  @Test
  public void metricFilterOverride() {
    builder.put(ConfluentTelemetryConfig.WHITELIST_CONFIG, ".*only_match_me.*");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());

    Predicate<MetricKey> filter = config.getMetricFilter();

    assertFalse(filter.test(new MetricKey("foobar/bytes_in_per_sec/total", Collections.emptyMap())));
    assertTrue(filter.test(new MetricKey("foobar/only_match_me/total",
        Collections.emptyMap())));
  }

  @Test
  public void metricFilterOverrideEmpty() {
    builder.put(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());

    Predicate<MetricKey> filter = config.getMetricFilter();

    assertEquals(ConfluentTelemetryConfig.ALWAYS_TRUE, filter);
  }

  @Test
  public void testDeprecatedProperties() {
    builder
        .put("confluent.telemetry.metrics.reporter.whitelist", "mockWhitelist")
        .put("confluent.telemetry.metrics.reporter.publish.ms", 60000)
        .put("confluent.telemetry.metrics.reporter.labels.foo", "fooValue")
        .put("confluent.telemetry.metrics.reporter.labels.bar", "barValue");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(builder.build());
    assertEquals(
        config.getString(ConfluentTelemetryConfig.WHITELIST_CONFIG),
        "mockWhitelist"
    );
    assertEquals(
        config.getLong(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG).longValue(),
        60000
    );
    assertEquals(
        config.getString(ConfluentTelemetryConfig.WHITELIST_CONFIG),
        "mockWhitelist"
    );

    assertThat(config.getLabels()).contains(
        entry("foo", "fooValue"),
        entry("bar", "barValue")
    );
  }

}