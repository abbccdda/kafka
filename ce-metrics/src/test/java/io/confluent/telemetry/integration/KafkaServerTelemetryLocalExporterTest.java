package io.confluent.telemetry.integration;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import io.confluent.metrics.reporter.integration.MetricReporterClusterTestHarness;
import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import kafka.server.KafkaConfig;

import static io.confluent.telemetry.integration.TestUtils.createAdminClient;
import static io.confluent.telemetry.integration.TestUtils.createNewConsumer;
import static io.confluent.telemetry.integration.TestUtils.newRecordsCheck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class KafkaServerTelemetryLocalExporterTest extends MetricReporterClusterTestHarness {

  private static final long COLLECTION_INTERVAL_MS = 2000;

  @Parameters
  public static Iterable<Object[]> testParameters() {
    return Arrays.asList(new Object[][]{
        // test using balancer config if set
        {Collections.singletonMap(ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_CONFIG, "1"), 1},
        // test using exporter config if both exporter and balancer is set
        {ImmutableMap.of(ConfluentConfigs.BALANCER_TOPICS_REPLICATION_FACTOR_CONFIG, "1",
                         ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME) +
                         KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "2"), 2},
        });
  }

  @Parameter(0)
  public Map<String, String> additionalProperties;

  @Parameter(1)
  public int expectedReplicationFactor;


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
    consumer.close();
    super.tearDown();
  }

  @Override
  protected void injectProperties(Properties props) {
    props.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), "false");
    props.setProperty(KafkaConfig.NumPartitionsProp(), "1");
    additionalProperties.forEach(props::put);
  }

  @Override
  protected void injectMetricReporterProperties(Properties props, String brokerList) {
    // disable http exporter
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
        + ExporterConfig.ENABLED_CONFIG, "false");

    // generic telemetry configs
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

  @Test
  public void testTelemetryLocalExporter() throws Exception {
    waitForMetrics();
    checkForTopicSettings();
  }

  private void waitForMetrics() {
    String topic = KafkaExporterConfig.DEFAULT_TOPIC_NAME;
    assertTrue("Waiting for metrics to start flowing in topic " + topic,
        newRecordsCheck(consumer, topic, COLLECTION_INTERVAL_MS * 2, false));
  }

  private void checkForTopicSettings() throws Exception {
    String topic = KafkaExporterConfig.DEFAULT_TOPIC_NAME;
    TopicDescription describeTopicsResult =
        adminClient.describeTopics(Collections.singletonList(topic))
          .all()
          .get()
          .get(topic);

    assertNotNull(describeTopicsResult);
    assertEquals("Expecting default number of partitions", KafkaExporterConfig.DEFAULT_TOPIC_PARTITIONS,
                 describeTopicsResult.partitions().size());
    for (TopicPartitionInfo partition : describeTopicsResult.partitions()) {
      assertEquals("Expecting " + expectedReplicationFactor + " replicas", expectedReplicationFactor,
                   partition.replicas().size());
    }
  }
}
