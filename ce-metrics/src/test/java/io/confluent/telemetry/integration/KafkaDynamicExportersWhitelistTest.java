package io.confluent.telemetry.integration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.metrics.reporter.integration.MetricReporterClusterTestHarness;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static io.confluent.telemetry.integration.TestUtils.createAdminClient;
import static io.confluent.telemetry.integration.TestUtils.createNewConsumer;

@Category(IntegrationTest.class)
public class KafkaDynamicExportersWhitelistTest extends MetricReporterClusterTestHarness {

  private static long collectionIntervalMs = 2000;

  private KafkaConsumer<byte[], byte[]> consumer;
  private AdminClient adminClient;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    consumer = createNewConsumer(brokerList);
    adminClient = createAdminClient(brokerList);
  }

  @After
  public void tearDown() throws Exception {
    adminClient.close();
    consumer.close();
    super.tearDown();
  }

  public static void buildTopicExporter(String topicAndName, Properties props, String brokerList) {
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(topicAndName) + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(topicAndName) + ExporterConfig.ENABLED_CONFIG, "true");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(topicAndName) + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(topicAndName) + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(topicAndName) + KafkaExporterConfig.TOPIC_NAME_CONFIG, topicAndName);
  }

  @Override
  protected void injectMetricReporterProperties(Properties props, String brokerList) {
    // disable the default exporters
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
        + ExporterConfig.ENABLED_CONFIG, "false");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
        + ExporterConfig.ENABLED_CONFIG, "false");

    // exporter A -> topic 'alpha'
    buildTopicExporter("alpha", props, brokerList);

    // exporter A -> topic 'alpha'
    buildTopicExporter("beta", props, brokerList);

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

  public void waitForMetrics(String topic) throws InterruptedException {
    waitForCondition(() -> checkForMetrics(topic, collectionIntervalMs),
        collectionIntervalMs * 2,
        collectionIntervalMs, () -> "Waiting for metrics to start flowing in topic " + topic);
  }

  public void waitForNoMetrics(String topic) throws InterruptedException {
    waitForCondition(() -> checkForNoMetrics(topic, collectionIntervalMs),
        collectionIntervalMs * 2,
        collectionIntervalMs, () -> "Waiting for metrics to stop flowing in topic " + topic);
  }

  @Test
  public void testMetricsReporterDynamicConfig() throws Exception {
    String alphaWhitelistConfig = ConfluentTelemetryConfig.exporterPrefixForName("alpha") + ExporterConfig.WHITELIST_CONFIG;
    String betaWhitelistConfig = ConfluentTelemetryConfig.exporterPrefixForName("beta") + ExporterConfig.WHITELIST_CONFIG;

    // enable with default whitelists
    waitForMetrics("alpha");
    waitForMetrics("beta");

    // modify alpha whitelist (to match nothing)
    setBrokerConfigs(ImmutableMap.of(
        alphaWhitelistConfig, ".*nothing_to_match_here.*"));
    waitForNoMetrics("alpha");
    waitForMetrics("beta");

    // modify both (to match nothing)
    setBrokerConfigs(ImmutableMap.of(
        alphaWhitelistConfig, ".*nothing_to_match_here.*",
        betaWhitelistConfig, ".*nothing_to_match_here.*"));
    waitForNoMetrics("alpha");
    waitForNoMetrics("beta");

    // remove alpha override
    deleteBrokerConfigs(ImmutableSet.of(
        alphaWhitelistConfig
    ));
    waitForMetrics("alpha");
    waitForNoMetrics("beta");

    // remove both overrides
    deleteBrokerConfigs(ImmutableSet.of(
        betaWhitelistConfig
    ));
    waitForMetrics("alpha");
    waitForMetrics("beta");

    // apply top-level whitelist
    setBrokerConfigs(ImmutableMap.of(
        ConfluentTelemetryConfig.WHITELIST_CONFIG, ".*nothing_to_match_here.*"
    ));
    waitForNoMetrics("alpha");
    waitForNoMetrics("beta");

    // apply exporter level override on alpha
    setBrokerConfigs(ImmutableMap.of(
        ConfluentTelemetryConfig.WHITELIST_CONFIG, ".*nothing_to_match_here.*",
        alphaWhitelistConfig, ".*"
    ));
    waitForMetrics("alpha");
    waitForNoMetrics("beta");

    // apply exporter level override to both
    setBrokerConfigs(ImmutableMap.of(
        ConfluentTelemetryConfig.WHITELIST_CONFIG, ".*nothing_to_match_here.*",
        alphaWhitelistConfig, ".*",
        betaWhitelistConfig, ".*"
    ));
    waitForMetrics("alpha");
    waitForMetrics("beta");

    // remove exporter-level overrides
    deleteBrokerConfigs(ImmutableSet.of(
        alphaWhitelistConfig, betaWhitelistConfig
    ));
    waitForNoMetrics("alpha");
    waitForNoMetrics("beta");

    // remove all overrides
    deleteBrokerConfigs(ImmutableSet.of(
        ConfluentTelemetryConfig.WHITELIST_CONFIG,
        alphaWhitelistConfig, betaWhitelistConfig
    ));
    waitForMetrics("alpha");
    waitForMetrics("beta");
  }

  public void deleteBrokerConfigs(Set<String> configKeys) throws ExecutionException, InterruptedException {
    ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "");
    adminClient.incrementalAlterConfigs(Collections.singletonMap(broker,
        configKeys.stream()
            .map(c -> new AlterConfigOp(new ConfigEntry(c, null), AlterConfigOp.OpType.DELETE))
            .collect(Collectors.toList())
    )).all().get();
  }

  public void setBrokerConfigs(Map<String, String> config) throws ExecutionException, InterruptedException {
    ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "");
    adminClient.incrementalAlterConfigs(Collections.singletonMap(broker,
        config.entrySet().stream()
            .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
            .collect(Collectors.toList())
    )).all().get();
  }

  private boolean checkForMetrics(String topic, long timeout) {
    consumer.unsubscribe();
    consumer.subscribe(Collections.singletonList(topic));
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

  private boolean checkForNoMetrics(String topic, long timeout) {
    consumer.unsubscribe();
    consumer.subscribe(Collections.singletonList(topic));
    consumer.seekToEnd(Collections.emptyList());
    long startMs = System.currentTimeMillis();
    while (System.currentTimeMillis() - startMs < timeout) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      if (records.isEmpty()) {
        return true;
      }
    }
    return false;
  }
}
