/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit.integration;

import static io.confluent.security.audit.telemetry.exporter.NonBlockingKafkaExporterConfig.KAFKA_EXPORTER_PREFIX;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.audit.AuthenticationInfo;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.audit.telemetry.exporter.NonBlockingKafkaExporter;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.telemetry.events.Event;
import io.confluent.telemetry.events.EventLogger;
import io.confluent.telemetry.events.EventLoggerConfig;
import io.confluent.telemetry.events.serde.Deserializer;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
@Category(IntegrationTest.class)
public class NonBlockingKafkaExporterTest {

  private static final Logger log = LoggerFactory.getLogger(NonBlockingKafkaExporterTest.class);


  private static final String BROKER_USER = "kafka";

  private static final KafkaPrincipal EVENT_LOG_WRITER =
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogWriter");

  private static final KafkaPrincipal EVENT_LOG_READER =
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");

  private EventLogClusters.Config eventLogClustersConfig;
  private EventLogClusters eventLogClusters;

  private Set<KafkaConsumer<byte[], byte[]>> consumers = new HashSet<>();
  private EventLogger logger;

  @Before
  public void setUp() {
    eventLogClustersConfig = new EventLogClusters.Config()
        .users(BROKER_USER, EVENT_LOG_WRITER.getName(), EVENT_LOG_READER.getName());
  }

  @After
  public void tearDown() throws Exception {
    try {
      for (KafkaConsumer<byte[], byte[]> consumer : consumers) {
        consumer.close();
      }
      if (logger != null) {
        logger.close();
      }
      if (eventLogClusters != null) {
        eventLogClusters.shutdown();
      }
    } finally {
      SecurityTestUtils.clearSecurityConfigs();
      KafkaTestUtils.verifyThreadCleanup();
    }
  }

  private static Map<String, Object> asMap(Headers kafkaHeaders) {
    return StreamSupport.stream(kafkaHeaders.spliterator(), Boolean.FALSE)
        .map(header -> new AbstractMap.SimpleEntry<String, Object>(header.key(), header.value()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }


  private CloudEvent<AttributesImpl, AuditLogEntry> firstReceivedEvent(
      KafkaConsumer<byte[], byte[]> consumer,
      long timeoutMs) {
    long startMs = System.currentTimeMillis();

    while (System.currentTimeMillis() - startMs < timeoutMs) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], byte[]> record : records) {
        Deserializer<AuditLogEntry> dser = Protobuf.deserializer(
            EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING, AuditLogEntry.class,
            AuditLogEntry.parser());

        return dser.deserialize(record);
      }
    }
    return null;
  }

  private KafkaConsumer<byte[], byte[]> consumer(String consumerGroup) {
    return consumer(consumerGroup, AuditLogRouterJsonConfig.DEFAULT_TOPIC);
  }

  private KafkaConsumer<byte[], byte[]> consumer(String consumerGroup, String topic) {
    Properties consumerProperties = eventLogClusters
        .consumerProps(EVENT_LOG_READER.getName(), consumerGroup);

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);

    consumer.subscribe(Collections.singleton(topic));
    consumers.add(consumer);
    return consumer;
  }

  private Map<String, String> loggerConfig(String user, Map<String, String> configOverrides) {
    HashMap<String, String> config = new HashMap<>();
    Properties producerProperties = eventLogClusters
        .producerProps(user);
    for (String key : producerProperties.stringPropertyNames()) {
      config.put(KAFKA_EXPORTER_PREFIX + key, producerProperties.getProperty(key));
    }
    config.put(AuditLogConfig.EVENT_EXPORTER_CLASS_CONFIG,
        NonBlockingKafkaExporter.class.getCanonicalName());
    config.put(AuditLogConfig.TOPIC_REPLICAS_CONFIG, "1");

    config.put(AUDIT_EVENT_ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig(
        eventLogClusters.kafkaCluster.bootstrapServers(),
        AuditLogRouterJsonConfig.DEFAULT_TOPIC,
        AuditLogRouterJsonConfig.DEFAULT_TOPIC));
    config.putAll(configOverrides);
    AuditLogConfig.toEventLoggerConfig(config).entrySet().stream()
        .forEach(e -> config.put(e.getKey(), String.valueOf(e.getValue())));

    return config;
  }

  private EventLogger logger(String name, String user, Map<String, String> configOverrides) {
    EventLogger logger = new EventLogger();
    Map<String, String> cfg = loggerConfig(user, configOverrides);
    logger.configure(cfg);

    return logger;
  }

  private EventLogger logger(String name, Map<String, String> configOverrides) {
    return logger(name, eventLogClusters.logWriterUser(), configOverrides);
  }

  private EventLogger logger(String name) {
    return logger(name, Collections.emptyMap());
  }

  private CloudEvent sampleEvent() {
    KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
    AuditLogEntry entry = AuditLogEntry.newBuilder()
        .setAuthenticationInfo(AuthenticationInfo.newBuilder()
            .setPrincipal(alice.toString())
            .build())
        .build();

    return Event.newBuilder()
        .setType("event_type")
        .setSource("crn://authority/kafka=source")
        .setSubject("crn://authority/kafka=subject")
        .setData(entry)
        .setRoute("confluent-audit-log-events")
        .setDataContentType(Protobuf.APPLICATION_JSON)
        .build();
  }


  @Test
  public void testEventLoggedOnNewTopic() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    KafkaConsumer<byte[], byte[]> consumer = consumer("event-log");
    logger = logger("testEventLoggedOnNewTopic");

    CloudEvent<AttributesImpl, AuditLogEntry> sentEvent = sampleEvent();
    TestUtils.waitForCondition(() -> logger.ready(sentEvent), "logger is not ready");

    logger.log(sentEvent);

    CloudEvent<AttributesImpl, AuditLogEntry> receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent.getAttributes().getId(), receivedEvent.getAttributes().getId());
  }

  @Test
  public void testEventLoggedWithoutPermissions() throws Throwable {
    // Try to log with the credentials of an existing user, who doesn't have the right permissions
    String eventLogPrincipal = "User:not_write";

    eventLogClusters = new EventLogClusters(eventLogClustersConfig);
    eventLogClusters.createLogReaderUser(eventLogPrincipal);

    // create this so the eventLogReader doesn't fail
    eventLogClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.DEFAULT_TOPIC, 2, 1);

    KafkaConsumer<byte[], byte[]> consumer = consumer("event-log");
    logger = logger("testEventLoggedWithoutPermissions", eventLogPrincipal, Collections.emptyMap());

    CloudEvent<AttributesImpl, AuditLogEntry> sentEvent = sampleEvent();
    TestUtils.waitForCondition(() -> logger.ready(sentEvent), "logger is not ready");
    logger.log(sentEvent);

    CloudEvent<AttributesImpl, AuditLogEntry> receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNull(receivedEvent);
  }


  @Test
  public void testEventLoggedOnExistingTopic() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    eventLogClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.DEFAULT_TOPIC, 2, 1);

    KafkaConsumer<byte[], byte[]> consumer = consumer("event-log");
    logger = logger("testEventLoggedOnExistingTopic");

    CloudEvent<AttributesImpl, AuditLogEntry> sentEvent = sampleEvent();
    TestUtils.waitForCondition(() -> logger.ready(sentEvent), "logger is not ready");
    logger.log(sentEvent);

    CloudEvent<AttributesImpl, AuditLogEntry> receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent.getAttributes().getId(), receivedEvent.getAttributes().getId());
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testReconfigureFailure() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    KafkaConsumer<byte[], byte[]> consumer1 = consumer("event-log-1");
    logger = logger("testReconfigureFailure");

    CloudEvent<AttributesImpl, AuditLogEntry> sentEvent1 = sampleEvent();
    TestUtils.waitForCondition(() -> logger.ready(sentEvent1), "logger is not ready");
    logger.log(sentEvent1);

    CloudEvent<AttributesImpl, AuditLogEntry> receivedEvent1 = firstReceivedEvent(consumer1, 10000);
    assertNotNull(receivedEvent1);
    assertEquals(sentEvent1.getAttributes().getId(), receivedEvent1.getAttributes().getId());

    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG, "{}")
    );

    // Make sure that the previous configuration is still used if a reconfigure() failure happens
    assertThrows(ConfigException.class,
        () -> logger.reconfigure(loggerConfig(eventLogClusters.logWriterUser(), config)));

    CloudEvent<AttributesImpl, AuditLogEntry> sentEvent2 = sampleEvent();
    assertNotEquals(sentEvent1.getAttributes().getId(),
        sentEvent2.getAttributes().getId()); // should have different IDs and timestamps
    logger.log(sentEvent2);

    CloudEvent<AttributesImpl, AuditLogEntry> receivedEvent2 = firstReceivedEvent(consumer1, 10000);
    assertNotNull(receivedEvent2);
    assertEquals(sentEvent2.getAttributes().getId(), receivedEvent2.getAttributes().getId());
  }

  @Test
  public void testMultiBroker() throws Throwable {
    eventLogClustersConfig.setNumBrokers(3);
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);
    assertEquals(3, eventLogClusters.kafkaCluster.bootstrapServers().split(",").length);

    KafkaConsumer<byte[], byte[]> consumer = consumer("event-log");
    logger = logger("testMultiBroker");

    AuditLogRouterJsonConfig jsonConfig = AuditLogRouterJsonConfig.load(
        AuditLogRouterJsonConfigUtils.defaultConfig(
            eventLogClusters.kafkaCluster.bootstrapServers(),
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC));
    // test roundtrip from "a,b,c" to ["a", "b", "c"]
    assertEquals(eventLogClusters.kafkaCluster.bootstrapServers(),
        jsonConfig.bootstrapServers());

    CloudEvent<AttributesImpl, AuditLogEntry> sentEvent = sampleEvent();
    TestUtils.waitForCondition(() -> logger.ready(sentEvent), "logger is not ready");
    logger.log(sentEvent);

    CloudEvent<AttributesImpl, AuditLogEntry> receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent.getAttributes().getId(), receivedEvent.getAttributes().getId());
  }

  @Test
  public void testDontCreateTopicsFailure() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    // This is no longer a failure. We now retry later
    //assertThrows(ConfigException.class, () ->
    logger("testDontCreateTopicsFailure",
        Utils.mkMap(Utils.mkEntry(AuditLogConfig.TOPIC_CREATE_CONFIG, "false")));
  }

  @Test
  public void testDontCreateTopicsAlreadyCreated() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    eventLogClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.DEFAULT_TOPIC, 2, 1);

    logger = logger("testDontCreateTopicsAlreadyCreated",
        Utils.mkMap(Utils.mkEntry(AuditLogConfig.TOPIC_CREATE_CONFIG, "false")));
  }

  @Test
  public void testDontCreateTopicsNoTopics() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(AuditLogConfig.TOPIC_CREATE_CONFIG, "false"),
        Utils.mkEntry(AUDIT_EVENT_ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfig(
                eventLogClusters.kafkaCluster.bootstrapServers(),
                "",
                ""))
    );
    logger = logger("testDontCreateTopicsNoTopics", config);
  }

  @Ignore  // Takes to long. KIP-526 will make TOPIC_EXPIRY_MS configurable
  @Test
  public void testMetadataExpiration() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);
    Map<String, String> config = Utils.mkMap(
        // make sure the metadata expires
        Utils.mkEntry(CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
            String.valueOf(ProducerMetadata.TOPIC_EXPIRY_MS / 2))
    );

    logger = logger("testMetadataExpiration", config);

    CloudEvent event = sampleEvent();

    assertTrue(logger.ready(event));
    Thread.sleep(ProducerMetadata.TOPIC_EXPIRY_MS * 3 / 2);
    // metadata should still be present
    assertTrue(logger.ready(event));
  }

  @Test
  public void testMetadataRefresh() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);
    Map<String, String> config = Utils.mkMap(
        // make sure the metadata expires
        Utils.mkEntry(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, String.valueOf(2))
    );

    logger = logger("testMetadataExpiration", config);
    NonBlockingKafkaExporter exporter = (NonBlockingKafkaExporter) logger.eventExporter();

    Instant lastRefresh = exporter.lastMetadataRefresh();

    TestUtils.waitForCondition(() -> lastRefresh.isBefore(exporter.lastMetadataRefresh()),
        10_000, "Metadata Refreshed");

    // refresh should have happened
    assertTrue(lastRefresh.isBefore(exporter.lastMetadataRefresh()));
  }
}
