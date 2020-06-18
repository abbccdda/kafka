package io.confluent.telemetry.integration;

import com.google.common.collect.ImmutableMap;
import io.confluent.metrics.reporter.integration.MetricReporterClusterTestHarness;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.reporter.TelemetryReporter;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static io.confluent.telemetry.integration.TestUtils.createAdminClient;
import static io.confluent.telemetry.integration.TestUtils.createNewConsumer;

public class KafkaServerDynamicReporterTest extends MetricReporterClusterTestHarness {

  private static long collectionIntervalMs = 2000;

  private static String telemetryReporterClass = TelemetryReporter.class.getName();
  private static String oldTelemetryReporterClass = "io.confluent.telemetry.reporter.KafkaServerMetricsReporter";

  private KafkaConsumer<byte[], byte[]> consumer;
  private AdminClient adminClient;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    consumer = createNewConsumer(brokerList);
    consumer.subscribe(Collections.singletonList(KafkaExporterConfig.DEFAULT_TOPIC_NAME));
    adminClient = createAdminClient(brokerList);
  }

  @After
  public void tearDown() throws Exception {
    adminClient.close();
    consumer.close();
    super.tearDown();
  }

  @Override
  protected void injectMetricReporterProperties(Properties props, String brokerList) {
    props.put(KafkaConfig.MetricReporterClassesProp(), "");
    props.put(ConfluentConfigs.AUTO_ENABLE_TELEMETRY_REPORTER_CONFIG, "true");

    // disable the default exporters
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
        + ExporterConfig.ENABLED_CONFIG, "false");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
        + ExporterConfig.ENABLED_CONFIG, "false");

    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.ENABLED_CONFIG, "true");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");

    props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, String.valueOf(collectionIntervalMs));
    props.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
    props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "region", "test");
    props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "pkc", "pkc-bar");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + "region", "test");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + "pkc", "pkc-bar");
    props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

    // force flush every message so that we can generate some Yammer timer metrics
    props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
  }

  public void waitForMetrics() throws InterruptedException {
    waitForCondition(() -> checkForMetrics(collectionIntervalMs),
        collectionIntervalMs * 2,
        collectionIntervalMs, () -> "Waiting for metrics to start flowing.");
  }

  public void waitForNoMetrics() throws InterruptedException {
    waitForCondition(() -> !checkForMetrics(collectionIntervalMs),
        collectionIntervalMs * 2,
        collectionIntervalMs, () -> "Waiting for metrics to stop flowing.");
  }

  @Test
  public void testMetricsReporterDynamicConfig() throws Exception {
    // enable (with auto addition)
    waitForMetrics();

    // disable the reporter (by disabling auto addition)
    updateBrokerConfigs(ImmutableMap.of(
        ConfluentConfigs.AUTO_ENABLE_TELEMETRY_REPORTER_CONFIG, "false"));
    waitForNoMetrics();

    // explicitly enable the old reporter class (no auto addition)
    updateBrokerConfigs(ImmutableMap.of(
        KafkaConfig.MetricReporterClassesProp(), oldTelemetryReporterClass,
        ConfluentConfigs.AUTO_ENABLE_TELEMETRY_REPORTER_CONFIG, "false"
    ));
    waitForMetrics();

    // disable reporter (no auto addition)
    updateBrokerConfigs(ImmutableMap.of(
        KafkaConfig.MetricReporterClassesProp(), "",
        ConfluentConfigs.AUTO_ENABLE_TELEMETRY_REPORTER_CONFIG, "false"));
    waitForNoMetrics();

    // explicitly enable the new reporter class (no auto addition)
    updateBrokerConfigs(ImmutableMap.of(
        KafkaConfig.MetricReporterClassesProp(), telemetryReporterClass,
        ConfluentConfigs.AUTO_ENABLE_TELEMETRY_REPORTER_CONFIG, "false"));
    waitForMetrics();

    // disable reporter (no auto addition)
    updateBrokerConfigs(ImmutableMap.of(
        KafkaConfig.MetricReporterClassesProp(), "",
        ConfluentConfigs.AUTO_ENABLE_TELEMETRY_REPORTER_CONFIG, "false"));
    waitForNoMetrics();

    // re-enable (with auto addition)
    updateBrokerConfigs(ImmutableMap.of(
        KafkaConfig.MetricReporterClassesProp(), "",
        ConfluentConfigs.AUTO_ENABLE_TELEMETRY_REPORTER_CONFIG, "true"
    ));
    waitForMetrics();

    updateBrokerConfigs(ImmutableMap.of(
        KafkaConfig.MetricReporterClassesProp(), "",
        ConfluentConfigs.AUTO_ENABLE_TELEMETRY_REPORTER_CONFIG, "false"));
    waitForNoMetrics();
  }

  public void updateBrokerConfigs(Map<String, String> config) throws ExecutionException, InterruptedException {
    ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "");
    adminClient.incrementalAlterConfigs(Collections.singletonMap(broker,
        config.entrySet().stream()
            .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
            .collect(Collectors.toList())
    )).all().get();
  }

  private boolean checkForMetrics(long timeout) {
    consumer.seekToEnd(Collections.emptyList());
    long startMs = System.currentTimeMillis();
    while (System.currentTimeMillis() - startMs < timeout) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      if (!records.isEmpty()) {
        return true;
      }
    }
    return false;
  }
}
