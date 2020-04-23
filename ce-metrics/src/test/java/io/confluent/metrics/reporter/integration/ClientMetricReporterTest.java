// (Copyright) [2016 - 2016] Confluent, Inc.
package io.confluent.metrics.reporter.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import io.confluent.metrics.record.ConfluentMetric;
import scala.collection.JavaConverters;

import static io.confluent.metrics.record.ConfluentMetric.MetricType.CONSUMER;
import static io.confluent.metrics.record.ConfluentMetric.MetricType.PRODUCER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientMetricReporterTest extends MetricsReporterTest {

    private Producer<String, String> testProducer;
    private static final String TOPIC = "testtopic";
    private KafkaConsumer<String, String> testConsumer;

    @SuppressWarnings("deprecation")
    @Before
    public void setUp() throws Exception {
        enableMetricsReporterOnBroker = false;
        super.setUp();
        kafka.utils.TestUtils.createTopic(this.zkClient, TOPIC, 2, 1,
            JavaConverters.asScalaBuffer(servers), new Properties());
        testProducer = createProducer();
        int numRecords = 50;
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
    @Override
    public void testMetricsReporter() {
        long startMs = System.currentTimeMillis();
        int producerMetricCount = 0;
        int consumerMetricCount = 0;

        int kafkaMeasurables = 0;
        int yammerMetrics = 0;
        while (System.currentTimeMillis() - startMs < 20000) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                ConfluentMetric.MetricsMessage metricsMessage = serdes.deserialize(record.value());
                if (metricsMessage.getMetricType() == CONSUMER) {
                    assertEquals("groupId should be set", "test-group", metricsMessage.getGroupId());
                    assertEquals("clientId should be set", "test-consumer", metricsMessage.getClientId());
                    consumerMetricCount++;
                } else if (metricsMessage.getMetricType() == PRODUCER) {
                    assertEquals("clientId should be set", "test-producer", metricsMessage.getClientId());
                    producerMetricCount++;
                } else {
                    fail("only clients should produce metrics");
                }

                assertEquals("brokerId should be -1", -1, metricsMessage.getBrokerId());
                assertTrue("clusterId should be set", !metricsMessage.getClusterId().isEmpty());
                kafkaMeasurables += metricsMessage.getKafkaMeasurableCount();
                yammerMetrics += metricsMessage.getYammerGaugeCount();
                yammerMetrics += metricsMessage.getYammerMeterCount();
                yammerMetrics += metricsMessage.getYammerHistogramCount();
                yammerMetrics += metricsMessage.getYammerTimerCount();
            }
        }
        assertTrue("Clients should have a KafkaMeasurable metric", kafkaMeasurables > 0);
        assertEquals("Clients should not have a Yammer metric", 0, yammerMetrics);
        assertTrue("should receive metrics from consumer", consumerMetricCount > 0);
        assertTrue("should receive metrics from producer", producerMetricCount > 0);
    }

    private Producer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer");
        injectMetricReporterProperties(properties, brokerList);
        return new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        injectMetricReporterProperties(properties, brokerList);
        return new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
    }

    private void produceTestData(Producer<String, String> producer, String topic, int numRecords)
        throws ExecutionException, InterruptedException {
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (int i = 0; i < numRecords; i++)
            futures.add(producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i))));
        for (Future<RecordMetadata> future : futures)
            future.get();
    }

    private void consumeTestData(KafkaConsumer<String, String> consumer, String topic, int numRecords)
        throws InterruptedException {

        consumer.subscribe(Collections.singleton(topic));
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200)))
                records.add(record);
            return records.size() == numRecords;
        }, () -> "Expected to consume " + numRecords + ", but consumed " + records.size());
    }
}
