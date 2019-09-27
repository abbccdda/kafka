package io.confluent.telemetry;

import io.confluent.metrics.reporter.integration.MetricReporterClusterTestHarness;
import io.confluent.observability.telemetry.TelemetryResourceType;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.reporter.KafkaServerMetricsReporter;
import io.confluent.telemetry.serde.OpencensusMetricsProto;
import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.resource.v1.Resource;
import java.util.Map;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.confluent.telemetry.collector.KafkaMetricsCollector.KAFKA_METRICS_LIB;
import static io.confluent.telemetry.collector.MetricsCollector.LABEL_LIBRARY;
import static io.confluent.telemetry.collector.MetricsCollector.LIBRARY_NONE;
import static io.confluent.telemetry.collector.YammerMetricsCollector.YAMMER_METRICS;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaServerMetricsReporterTest extends MetricReporterClusterTestHarness {

    private KafkaConsumer<byte[], byte[]> consumer;
    private Serde<Metric> serde = new OpencensusMetricsProto();
    private Map<String, String> resourceLabels;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        consumer = createNewConsumer();
        consumer.subscribe(Collections.singleton(KafkaExporterConfig.DEFAULT_TOPIC_NAME));
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
        super.tearDown();
    }

    protected void injectMetricReporterProperties(Properties props, String brokerList) {
        props.setProperty(KafkaConfig.MetricReporterClassesProp(), "io.confluent.telemetry.reporter.KafkaServerMetricsReporter");
        props.setProperty(KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.setProperty(KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");
        props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
        props.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
        props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "region", "test");
        props.setProperty(ConfluentTelemetryConfig.PREFIX_LABELS + "pkc", "pkc-bar");
        props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");

        // force flush every message so that we can generate some Yammer timer metrics
        props.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), "1");
    }

    @Test
    public void testMetricsReporter() {
        long startMs = System.currentTimeMillis();

        List<String> brokerIds = servers.stream().map(s -> "" + s.config().brokerId()).collect(Collectors.toList());
        List<String> libs = Arrays.asList(KAFKA_METRICS_LIB, LIBRARY_NONE, YAMMER_METRICS);

        boolean kafkaMetricsPresent = false, yammerMetricsPresent = false, cpuVolumeMetricsPresent = false;

        while (System.currentTimeMillis() - startMs < 20000) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<byte[], byte[]> record : records) {

                // Verify that the message de-serializes successfully
                Metric m = null;
                try {
                    m = this.serde.deserializer().deserialize(record.topic(), record.headers(), record.value());
                } catch (SerializationException e) {
                    fail("failed to deserialize message " + e.getMessage());
                }

                // Verify labels

                // Check the resource labels are present
                Resource resource = m.getResource();
                assertEquals(TelemetryResourceType.KAFKA.toCanonicalString(), resource.getType());

                resourceLabels = resource.getLabelsMap();
                assertEquals(
                    servers.get(0).clusterId(),
                    resourceLabels.get(TelemetryResourceType.KAFKA.prefixLabel(KafkaServerMetricsReporter.LABEL_CLUSTER_ID))
                );

                // Legacy label alias - remove after https://confluentinc.atlassian.net/browse/METRICS-516
                assertEquals(
                    servers.get(0).clusterId(),
                    resourceLabels.get("cluster_id")
                );

                assertTrue(
                    brokerIds.contains(
                        resourceLabels.get(TelemetryResourceType.KAFKA.prefixLabel(KafkaServerMetricsReporter.LABEL_BROKER_ID))
                    )
                );

                // Check that the labels from the config are present.
                assertEquals(resourceLabels.get("region"), "test");
                assertEquals(resourceLabels.get("pkc"), "pkc-bar");

                // Check that we get all kinds of metrics
                assertTrue(labelExists(m, LABEL_LIBRARY));

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

        assertTrue(kafkaMetricsPresent);
        assertTrue(yammerMetricsPresent);
        assertTrue(cpuVolumeMetricsPresent);
    }


    private boolean labelExists(Metric m, String key) {
        return m.getMetricDescriptor().getLabelKeysList().contains(LabelKey.newBuilder().setKey(key).build());
    }

    private String getLabelValueFromFirstTimeSeries(Metric m, String key) {
        int index = m.getMetricDescriptor().getLabelKeysList().indexOf(LabelKey.newBuilder().setKey(key).build());
        return m.getTimeseries(0).getLabelValues(index).getValue();
    }

    private KafkaConsumer<byte[], byte[]> createNewConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "telemetry-metric-reporter-consumer");
        // The metric topic may not be there initially. So, we need to refresh metadata more frequently to pick it up once created.
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "400");
        return new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

}
