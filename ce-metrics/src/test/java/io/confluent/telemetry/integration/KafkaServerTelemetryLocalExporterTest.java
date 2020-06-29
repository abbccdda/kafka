package io.confluent.telemetry.integration;

import io.confluent.metrics.reporter.integration.MetricReporterClusterTestHarness;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static io.confluent.telemetry.integration.TestUtils.createNewConsumer;
import static io.confluent.telemetry.integration.TestUtils.newRecordsCheck;
import static org.junit.Assert.assertTrue;

public class KafkaServerTelemetryLocalExporterTest extends MetricReporterClusterTestHarness {

  private static final long COLLECTION_INTERVAL_MS = 2000;

  private KafkaConsumer<byte[], byte[]> consumer;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    consumer = createNewConsumer(brokerList);
    consumer.subscribe(Collections.singletonList(KafkaExporterConfig.DEFAULT_TOPIC_NAME));
  }

  @After
  public void tearDown() throws Exception {
    consumer.close();
    super.tearDown();
  }

  @Override
  protected void injectMetricReporterProperties(Properties props, String brokerList) {
    // disable http exporter
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
        + ExporterConfig.ENABLED_CONFIG, "false");

    // we need to explicitly lower the replicas
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
        + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");

    // generic telemetry configs
    props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, String.valueOf(COLLECTION_INTERVAL_MS));
    props.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
    props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "region", "test");
    props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "pkc", "pkc-bar");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + "region", "test");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + "pkc", "pkc-bar");
    props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

    // force flush every message so that we can generate some Yammer timer metrics
    props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
  }

  @Test
  public void testTelemetryLocalExporter() throws Exception {
    waitForMetrics();
  }

  private void waitForMetrics() {
    String topic = KafkaExporterConfig.DEFAULT_TOPIC_NAME;
    assertTrue("Waiting for metrics to start flowing in topic " + topic,
        newRecordsCheck(consumer, topic, COLLECTION_INTERVAL_MS * 2, false));
  }
}
