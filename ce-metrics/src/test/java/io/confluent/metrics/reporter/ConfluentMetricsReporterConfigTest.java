package io.confluent.metrics.reporter;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Predicate;

import io.confluent.monitoring.common.MonitoringProducerDefaults;

import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfluentMetricsReporterConfigTest {

  @Test
  public void testProducerPropertiesHasDefaults() {
    Map<String, Object> config = new HashMap<>();
    config.put(ConfluentMetricsReporterConfig.METRICS_REPORTER_PREFIX
               + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");

    final Properties producerProperties =
        ConfluentMetricsReporterConfig.getProducerProperties(config);

    Assert.assertTrue(producerProperties.containsKey(KEY_SERIALIZER_CLASS_CONFIG));
    Assert.assertTrue(producerProperties.containsKey(VALUE_SERIALIZER_CLASS_CONFIG));
    Assert.assertTrue(producerProperties.containsKey(CLIENT_ID_CONFIG));
    Assert.assertTrue(
        producerProperties.keySet().containsAll(
            MonitoringProducerDefaults.PRODUCER_CONFIG_DEFAULTS.keySet()
        )
    );

    for (Entry<String, Object> entry :
        MonitoringProducerDefaults.PRODUCER_CONFIG_DEFAULTS.entrySet()) {
      Assert.assertEquals(entry.getValue(), producerProperties.get(entry.getKey()));
    }
  }

  @Test
  public void testProducerPropertiesDefaultsCanBeOverridden() {
    final String overriddenProperty = ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
    Map<String, Object> config = new HashMap<>();
    config.put(ConfluentMetricsReporterConfig.METRICS_REPORTER_PREFIX
               + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    config.put(ConfluentMetricsReporterConfig.METRICS_REPORTER_PREFIX
               + overriddenProperty, "1234567890"
    );

    final Properties producerProperties =
        ConfluentMetricsReporterConfig.getProducerProperties(config);

    Assert.assertTrue(
        MonitoringProducerDefaults.PRODUCER_CONFIG_DEFAULTS.containsKey(overriddenProperty)
    );
    Assert.assertEquals("1234567890", producerProperties.get(overriddenProperty));
  }

  @Test
  public void testClientPropertiesPassThrough() {
    Map<String, Object> config = new HashMap<>();
    config.put(ConfluentMetricsReporterConfig.METRICS_REPORTER_PREFIX
               + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    config.put(ConfluentMetricsReporterConfig.METRICS_REPORTER_PREFIX
               + CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, "987");

    final Properties clientProperties = ConfluentMetricsReporterConfig.getClientProperties(config);

    // test pass-through of known configs
    Assert.assertTrue(clientProperties.containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    Assert.assertEquals(
        "localhost:1234",
        clientProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
    );

    // test pass-through of unknown configs
    Assert.assertTrue(clientProperties.containsKey(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG));
    Assert.assertEquals("987", clientProperties.get(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG));
  }

  @Test
  public void testMetricFilter() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConfluentMetricsReporterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConfluentMetricsReporterConfig.METRICS_INCLUDE_CONFIG, ".*only_match_me.*");

    ConfluentMetricsReporter reporter = new ConfluentMetricsReporter();
    reporter.configure(props);

    Predicate<String> filter = reporter.pattern.asPredicate();

    assertFalse(filter.test("foobar/bytes_in_per_sec/total"));
    assertTrue(filter.test("foobar/only_match_me/total"));
  }

  @Test
  public void testMetricFilterBackwardsCompatibility() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConfluentMetricsReporterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConfluentMetricsReporterConfig.METRICS_INCLUDE_CONFIG_ALIAS, ".*only_match_me.*");

    ConfluentMetricsReporter reporter = new ConfluentMetricsReporter();
    reporter.configure(props);

    Predicate<String> filter = reporter.pattern.asPredicate();

    assertFalse(filter.test("foobar/bytes_in_per_sec/total"));
    assertTrue(filter.test("foobar/only_match_me/total"));
  }
}
