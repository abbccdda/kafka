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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.confluent.telemetry.integration.TestUtils.createAdminClient;
import static io.confluent.telemetry.integration.TestUtils.createNewConsumer;
import static io.confluent.telemetry.integration.TestUtils.newRecordsCheck;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class KafkaDynamicExportersMetricsPredicateTest extends MetricReporterClusterTestHarness {

  private static final long COLLECTION_INTERVAL_MS = 2000;

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

    // exporter B -> topic 'beta'
    buildTopicExporter("beta", props, brokerList);

    props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, String.valueOf(COLLECTION_INTERVAL_MS));
    props.setProperty(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, "");
    props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "region", "test");
    props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "pkc", "pkc-bar");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + "region", "test");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + "pkc", "pkc-bar");
    props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

    // force flush every message so that we can generate some Yammer timer metrics
    props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
  }

  private void waitForMetrics(String topic) {
    assertTrue("Waiting for metrics to start flowing in topic " + topic,
        newRecordsCheck(consumer, topic, COLLECTION_INTERVAL_MS * 2, false));
  }

  private void waitForNoMetrics(String topic) {
    assertTrue("Waiting for metrics to stop flowing in topic " + topic,
        newRecordsCheck(consumer, topic, COLLECTION_INTERVAL_MS * 2, true));
  }

  @Test
  public void testMetricsReporterDynamicConfig() throws Exception {
    String alphaMetricsIncludeeConfig = ConfluentTelemetryConfig.exporterPrefixForName("alpha") + ExporterConfig.METRICS_INCLUDE_CONFIG;
    String betaMetricsIncludeConfig = ConfluentTelemetryConfig.exporterPrefixForName("beta") + ExporterConfig.METRICS_INCLUDE_CONFIG;

    // enable with default metrics list
    waitForMetrics("alpha");
    waitForMetrics("beta");

    // modify alpha metrics list (to match nothing)
    setBrokerConfigs(ImmutableMap.of(
        alphaMetricsIncludeeConfig, ".*nothing_to_match_here.*"));
    waitForNoMetrics("alpha");
    waitForMetrics("beta");

    // modify both (to match nothing)
    setBrokerConfigs(ImmutableMap.of(
        alphaMetricsIncludeeConfig, ".*nothing_to_match_here.*",
        betaMetricsIncludeConfig, ".*nothing_to_match_here.*"));
    waitForNoMetrics("alpha");
    waitForNoMetrics("beta");

    // remove alpha override
    deleteBrokerConfigs(ImmutableSet.of(
        alphaMetricsIncludeeConfig
    ));
    waitForMetrics("alpha");
    waitForNoMetrics("beta");

    // remove both overrides
    deleteBrokerConfigs(ImmutableSet.of(
        betaMetricsIncludeConfig
    ));
    waitForMetrics("alpha");
    waitForMetrics("beta");

    // apply top-level metrics list
    setBrokerConfigs(ImmutableMap.of(
        ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, ".*nothing_to_match_here.*"
    ));
    waitForNoMetrics("alpha");
    waitForNoMetrics("beta");

    // apply exporter level override on alpha
    setBrokerConfigs(ImmutableMap.of(
        ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, ".*nothing_to_match_here.*",
        alphaMetricsIncludeeConfig, ".*"
    ));
    waitForMetrics("alpha");
    waitForNoMetrics("beta");

    // apply exporter level override to both
    setBrokerConfigs(ImmutableMap.of(
        ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, ".*nothing_to_match_here.*",
        alphaMetricsIncludeeConfig, ".*",
        betaMetricsIncludeConfig, ".*"
    ));
    waitForMetrics("alpha");
    waitForMetrics("beta");

    // remove exporter-level overrides
    deleteBrokerConfigs(ImmutableSet.of(
        alphaMetricsIncludeeConfig, betaMetricsIncludeConfig
    ));
    waitForNoMetrics("alpha");
    waitForNoMetrics("beta");

    // remove all overrides
    deleteBrokerConfigs(ImmutableSet.of(
        ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG,
        alphaMetricsIncludeeConfig, betaMetricsIncludeConfig
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
}
