package io.confluent.telemetry.integration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.telemetry.ConfluentTelemetryConfig;
import io.confluent.telemetry.exporter.ExporterConfig;
import io.confluent.telemetry.exporter.kafka.KafkaExporterConfig;
import io.confluent.telemetry.provider.Utils;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.resource.v1.Resource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import junit.framework.TestCase;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.collection.JavaConverters;

@Category({IntegrationTest.class})
public class KafkaClientTest extends TelemetryClusterTestHarness {

  private static final String TOPIC = "testtopic";
  private Producer<String, String> testProducer;
  private KafkaConsumer<String, String> testConsumer;

  @Before
  public void setUp() throws Exception {
    enableMetricsReporterOnBroker = false;
    super.setUp();
    kafka.utils.TestUtils.createTopic(this.zkClient, TOPIC, 2, 1,
            JavaConverters.collectionAsScalaIterableConverter(servers).asScala().toSeq(), new Properties());
    testProducer = createProducer();
    int numRecords = 500;
    produceTestData(testProducer, TOPIC, numRecords);
    testConsumer = createConsumer();
    consumeTestData(testConsumer, TOPIC, numRecords);
  }

  @After
  public void tearDown() throws Exception {
    testProducer.close();
    testConsumer.close();
    super.tearDown();
  }

  @Test
  public void testMetricsReporter() {
    long startMs = System.currentTimeMillis();
    boolean clientMetricsPresent = false;
    while (System.currentTimeMillis() - startMs < 20000) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], byte[]> record : records) {

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
        TestCase.assertEquals("client", resource.getType());

        Map<String, String> resourceLabels = resource.getLabelsMap();

        // Check that the labels from the config are present.
        TestCase
            .assertEquals(resourceLabels.get("client.region"),
                "test");
        TestCase.assertEquals(resourceLabels.get("client.pkc"),
            "pkc-bar");
        clientMetricsPresent = true;
      }
    }
    assertTrue(clientMetricsPresent);
  }

  private Producer<String, String> createProducer() {
    Properties properties = new Properties();
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
    properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
    injectClientMetricReporterProperties(properties, brokerList);
    return new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
  }

  private KafkaConsumer<String, String> createConsumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    injectClientMetricReporterProperties(properties, brokerList);
    return new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
  }

  private void injectClientMetricReporterProperties(Properties props, String brokerList) {
    props.setProperty(KafkaConfig.MetricReporterClassesProp(),
        "io.confluent.telemetry.reporter.TelemetryReporter");
    // disable the default exporters
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME)
            + ExporterConfig.ENABLED_CONFIG, "false");
    props.setProperty(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "500");
    props.setProperty(ConfluentTelemetryConfig.WHITELIST_CONFIG, "");
    props.setProperty(ConfluentTelemetryConfig.DEBUG_ENABLED, "true");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + Utils.RESOURCE_LABEL_CLUSTER_ID, "foobar");

    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.TYPE_CONFIG, ExporterConfig.ExporterType.kafka.name());
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.ENABLED_CONFIG, "true");
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ConfluentTelemetryConfig.exporterPrefixForName("test") + KafkaExporterConfig.TOPIC_REPLICAS_CONFIG, "1");

    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_PREFIX + "region", "test");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_PREFIX + "pkc", "pkc-bar");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_TYPE, "client");
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_VERSION, AppInfoParser.getVersion());
    props.setProperty(CommonClientConfigs.METRICS_CONTEXT_PREFIX + ConfluentConfigs.RESOURCE_LABEL_COMMIT_ID, AppInfoParser.getCommitId());
  }

  private void produceTestData(Producer<String, String> producer, String topic, int numRecords)
      throws ExecutionException, InterruptedException {
    List<Future<RecordMetadata>> futures = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      futures.add(
          producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i))));
    }
    for (Future<RecordMetadata> future : futures) {
      future.get();
    }
  }

  private void consumeTestData(KafkaConsumer<String, String> consumer, String topic, int numRecords)
      throws InterruptedException {

    consumer.subscribe(Collections.singleton(topic));
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    TestUtils.waitForCondition(() -> {
      for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
        records.add(record);
      }
      return records.size() == numRecords;
    }, () -> "Expected to consume " + numRecords + ", but consumed " + records.size());
  }
}
