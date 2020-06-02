// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.audit.telemetry.exporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.kafka.test.cluster.EmbeddedKafkaCluster;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.telemetry.events.Event;
import io.confluent.telemetry.events.EventLogger;
import io.confluent.telemetry.events.EventLoggerConfig;
import io.confluent.telemetry.events.cloudevents.extensions.RouteExtension;
import io.confluent.telemetry.events.serde.Deserializer;
import io.confluent.telemetry.events.test.SomeMessage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class NonBlockingKafkaExporterTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(8);

  private KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
  private SomeMessage entry = SomeMessage.newBuilder()
      .setPrincipal(alice.toString())
      .build();

  private Event.Builder<SomeMessage> event = Event.<SomeMessage>newBuilder()
      .setType("event_type")
      .setSource("crn://authority/kafka=source")
      .setSubject("crn://authority/kafka=subject")
      .setData(entry);

  private EmbeddedKafkaCluster kafkaCluster;

  @Before
  public void setUp() throws Exception {
    kafkaCluster = new EmbeddedKafkaCluster();
    kafkaCluster.startZooKeeper();
    Properties overrides = new Properties();
    overrides.put("auto.create.topics.enable", false);
    kafkaCluster.startBrokers(1, overrides);
  }

  @After
  public void tearDown() throws Exception {
    kafkaCluster.shutdown();
  }

  @Test
  public void testBinaryEncoding() throws Exception {
    testLoopback(EventLoggerConfig.CLOUD_EVENT_BINARY_ENCODING);
  }

  @Test
  public void testStructuredEncoding() throws Exception {
    testLoopback(EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING);
  }


  private void testLoopback(String encoding) throws Exception {
    String topic = NonBlockingKafkaExporterConfig.DEFAULT_TOPIC;

    EventLogger<SomeMessage> logger = new EventLogger<>();
    HashMap<String, Object> config = new HashMap<>();
    config
        .put(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG, NonBlockingKafkaExporter.class.getCanonicalName());
    config.put(NonBlockingKafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
    config.put(NonBlockingKafkaExporterConfig.TOPIC_REPLICAS_CONFIG, 1);
    config.put(EventLoggerConfig.CLOUD_EVENT_ENCODING_CONFIG, encoding);
    logger.configure(config);

    // expected
    CloudEvent<AttributesImpl, SomeMessage> expected = event
        .setDataContentType(Protobuf.contentType(encoding))
        .build();

    logger.log(expected);

    // act
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties())) {

      consumer.subscribe(Collections.singletonList(topic));

      ConsumerRecords<byte[], byte[]> records = consumer.poll(TIMEOUT);
      Deserializer<SomeMessage> dser = Protobuf.deserializer(
          encoding, SomeMessage.class, SomeMessage.parser());
      // actual
      ConsumerRecord<byte[], byte[]> r = records.iterator().next();
      CloudEvent<AttributesImpl, SomeMessage> actual = dser.deserialize(r);
      assertEquals(expected.getAttributes().getId(), actual.getAttributes().getId());
      assertEquals(expected.getAttributes().getSpecversion(),
          actual.getAttributes().getSpecversion());
      assertEquals(expected.getAttributes().getSource(), actual.getAttributes().getSource());
      assertEquals(expected.getAttributes().getType(), actual.getAttributes().getType());
      assertEquals(expected.getAttributes().getDatacontenttype(),
          actual.getAttributes().getDatacontenttype());
      assertEquals(expected.getAttributes().getSubject(), actual.getAttributes().getSubject());
      assertEquals(entry, actual.getData().get());
    }

    logger.close();
  }

  @Test
  public void testRoutingWithStructuredEncoding() throws Exception {
    testRouting(EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING);
  }

  @Test
  public void testRoutingWithBinaryEncoding() throws Exception {
    testRouting(EventLoggerConfig.CLOUD_EVENT_BINARY_ENCODING);
  }

  private void testRouting(String encoding) throws Exception {
    int totalRecords = 10;
    List<TopicSpec> t = new ArrayList<>();
    IntStream.range(0, totalRecords)
        .forEach(i -> t.add(TopicSpec.builder().setName("t" + i).build()));
    TopicSpec.Topics topics = new TopicSpec.Topics();
    topics.setTopics(t);

    ObjectMapper o = new ObjectMapper();
    String tStr = o.writeValueAsString(topics);

    EventLogger<SomeMessage> logger = new EventLogger<>();
    HashMap<String, Object> config = new HashMap<>();
    config
        .put(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG, NonBlockingKafkaExporter.class.getCanonicalName());
    config.put(NonBlockingKafkaExporterConfig.TOPIC_CONFIG, tStr);
    config.put(NonBlockingKafkaExporterConfig.TOPIC_REPLICAS_CONFIG, 1);
    config.put(NonBlockingKafkaExporterConfig.TOPIC_PARTITIONS_CONFIG, 2);
    config.put(NonBlockingKafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
    config.put(EventLoggerConfig.CLOUD_EVENT_ENCODING_CONFIG, encoding);
    logger.configure(config);

    IntStream.range(0, totalRecords).forEach(i -> {
      logger.log(Event.<SomeMessage>newBuilder()
          .setType("event_type")
          .setSource("crn://authority/kafka=source")
          .setSubject("crn://authority/kafka=subject")
          .setData(entry)
          .setId("t" + i)
          .setRoute("t" + i)
          .setDataContentType(Protobuf.contentType(encoding))
          .build());
    });

    // act

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties())) {

      consumer.subscribe(
          IntStream.range(0, totalRecords).mapToObj(e -> "t" + e).collect(Collectors.toList()));

      long startMs = System.currentTimeMillis();
      long timeoutMs = 30_000;
      int recordsSeen = 0;
      while (System.currentTimeMillis() - startMs < timeoutMs) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(TIMEOUT);
        for (ConsumerRecord<byte[], byte[]> record : records) {
          Deserializer<SomeMessage> dser = Protobuf.deserializer(
              encoding, SomeMessage.class, SomeMessage.parser());
          CloudEvent<AttributesImpl, SomeMessage> event = dser.deserialize(record);

          assertTrue(
              event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY));
          RouteExtension re = (RouteExtension) event.getExtensions()
              .get(RouteExtension.Format.IN_MEMORY_KEY);
          assertFalse(re.getRoute().isEmpty());

          assertEquals(record.topic(), re.getRoute());
          recordsSeen++;
        }

        if (recordsSeen == totalRecords) {
          break;
        }
      }
      assertEquals(recordsSeen, totalRecords);
    }
    logger.close();
  }

  @Test
  public void testTopicNotReadyAndReconfigure() throws Exception {
    List<TopicSpec> t = new ArrayList<>();
    IntStream.range(0, 2).forEach(i -> t.add(TopicSpec.builder().setName("t" + i).build()));
    TopicSpec.Topics topics = new TopicSpec.Topics();
    topics.setTopics(t);

    ObjectMapper o = new ObjectMapper();
    String tStr = o.writeValueAsString(topics);

    EventLogger<SomeMessage> logger = new EventLogger<>();
    HashMap<String, Object> config = new HashMap<>();
    config
        .put(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG, NonBlockingKafkaExporter.class.getCanonicalName());
    config.put(NonBlockingKafkaExporterConfig.TOPIC_REPLICAS_CONFIG, 1);
    config.put(NonBlockingKafkaExporterConfig.TOPIC_PARTITIONS_CONFIG, 2);
    config.put(NonBlockingKafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());

    logger.configure(config);

    Event.Builder<SomeMessage> eventBuilder = Event.<SomeMessage>newBuilder()
        .setData(entry)
        .setSource("foo")
        .setSubject("bar")
        .setType("baz")
        .setDataContentType(Protobuf.APPLICATION_JSON)
        .setData(SomeMessage.getDefaultInstance());

    // Check if the default route is ready
    assertTrue(logger.ready(eventBuilder.build()));

    eventBuilder.setRoute("t1");
    assertFalse(logger.ready(eventBuilder.build()));

    config.put(NonBlockingKafkaExporterConfig.TOPIC_CONFIG, tStr);
    logger.reconfigure(config);

    assertTrue(logger.ready(eventBuilder.build()));

    logger.close();
  }

  private Properties consumerProperties() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.id");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

}
