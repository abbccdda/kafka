package io.confluent.telemetry.integration;

import static io.confluent.telemetry.collector.KafkaMetricsCollector.KAFKA_METRICS_LIB;
import static io.confluent.telemetry.collector.MetricsCollector.LABEL_LIBRARY;
import static io.confluent.telemetry.collector.MetricsCollector.LIBRARY_NONE;
import static io.confluent.telemetry.collector.YammerMetricsCollector.YAMMER_METRICS;
import static io.confluent.telemetry.integration.TestUtils.getLabelValueFromFirstTimeSeries;
import static io.confluent.telemetry.integration.TestUtils.labelExists;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporter;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.provider.KafkaServerProvider;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.resource.v1.Resource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import junit.framework.TestCase;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.junit.Test;

public class KafkaServerTest extends TelemetryClusterTestHarness {

  private Map<String, String> resourceLabels;

  protected void injectMetricReporterProperties(Properties props, String brokerList) {
    props.setProperty(KafkaConfig.MetricReporterClassesProp(),
        "io.confluent.telemetry.reporter.TelemetryReporter");
    // disable the default exporters
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");

    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.ENABLED_CONFIG, "true");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");

    props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
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
  public void testMetricsReporter() {
    long startMs = System.currentTimeMillis();

    List<String> brokerIds = servers.stream().map(s -> "" + s.config().brokerId())
        .collect(Collectors.toList());
    Set<String> brokerRacks = servers.stream().map(s -> "" + s.config().rack().get())
        .collect(Collectors.toSet());
    List<String> libs = Arrays.asList(KAFKA_METRICS_LIB, LIBRARY_NONE, YAMMER_METRICS);

    boolean kafkaMetricsPresent = false, yammerMetricsPresent = false, cpuVolumeMetricsPresent = false;

    while (System.currentTimeMillis() - startMs < 20000) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], byte[]> record : records) {

        // ensure version header is present
        Header versionHeader = record.headers().lastHeader(KafkaExporter.VERSION_HEADER_KEY);
        assertNotNull(versionHeader);
        assertEquals(0, ByteBuffer.wrap(versionHeader.value()).order(ByteOrder.LITTLE_ENDIAN).getInt());

        // Verify that the message de-serializes successfully
        Metric m = null;
        try {
          m = this.serde.deserializer()
              .deserialize(record.topic(), record.headers(), record.value());
        } catch (SerializationException e) {
          fail("failed to deserialize message " + e.getMessage());
        }

        // Verify labels

        // Check the resource labels are present
        Resource resource = m.getResource();
        TestCase.assertEquals("kafka", resource.getType());

        resourceLabels = resource.getLabelsMap();
        assertEquals(
            servers.get(0).clusterId(),
            resourceLabels
                .get("kafka." + KafkaServerProvider.LABEL_CLUSTER_ID)
        );

        assertTrue(
            brokerRacks.contains(
                resourceLabels.get("kafka." +
                    KafkaServerProvider.LABEL_BROKER_RACK)
            )
        );

        assertTrue(
            brokerIds.contains(
                resourceLabels.get("kafka." +
                    KafkaServerProvider.LABEL_BROKER_ID)
            )
        );

        // Check that the labels from the config are present.
        assertEquals(resourceLabels.get("kafka.region"), "test");
        assertEquals(resourceLabels.get("kafka.pkc"), "pkc-bar");

        // Check that we get all kinds of metrics
        if (labelExists(m, LABEL_LIBRARY)) {

          String lib = getLabelValueFromFirstTimeSeries(m, LABEL_LIBRARY);
          assertTrue(libs.contains(lib));

          switch (lib) {
            case KAFKA_METRICS_LIB:
              kafkaMetricsPresent = true;
              break;
            case YAMMER_METRICS:
              yammerMetricsPresent = true;
              break;
            case LIBRARY_NONE:
              cpuVolumeMetricsPresent = true;
              break;
          }
        }
      }
    }

    assertTrue(kafkaMetricsPresent);
    assertTrue(yammerMetricsPresent);
    assertTrue(cpuVolumeMetricsPresent);
  }

}
