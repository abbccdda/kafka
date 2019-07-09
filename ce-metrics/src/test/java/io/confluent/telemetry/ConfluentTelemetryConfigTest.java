package io.confluent.telemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.Test;

public class ConfluentTelemetryConfigTest {

  @Test
  public void metricFilterDefaults() {
    Map<String, Object> overrides = new HashMap<>();
    overrides.put(ConfluentTelemetryConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(overrides);

    Predicate<MetricKey> filter = config.metricFilter();

    assertTrue(filter.test(new MetricKey("foobar/bytes_in_per_sec/total", Collections.emptyMap())));
    assertFalse(filter.test(new MetricKey("foobar/bytes_in_per_asdfsec/total",
        Collections.emptyMap())));
  }

  @Test
  public void metricFilterOverride() {
    Map<String, Object> overrides = new HashMap<>();
    overrides.put(ConfluentTelemetryConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    overrides.put(ConfluentTelemetryConfig.WHITELIST_CONFIG, ".*only_match_me.*");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(overrides);

    Predicate<MetricKey> filter = config.metricFilter();

    assertFalse(filter.test(new MetricKey("foobar/bytes_in_per_sec/total", Collections.emptyMap())));
    assertTrue(filter.test(new MetricKey("foobar/only_match_me/total",
        Collections.emptyMap())));
  }

  @Test
  public void metricFilterOverrideEmpty() {
    Map<String, Object> overrides = new HashMap<>();
    overrides.put(ConfluentTelemetryConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    overrides.put(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");

    ConfluentTelemetryConfig config = new ConfluentTelemetryConfig(overrides);

    Predicate<MetricKey> filter = config.metricFilter();

    assertEquals(ConfluentTelemetryConfig.ALWAYS_TRUE, filter);
  }
}