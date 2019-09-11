// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.audit.integration;

import static io.confluent.security.audit.EventLogConfig.EVENT_LOGGER_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuthenticationInfo;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.EventLogger;
import io.confluent.security.audit.appender.KafkaEventAppender;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class KafkaEventAppenderTest {

  private static final String BROKER_USER = "kafka";

  private static final KafkaPrincipal EVENT_LOG_WRITER =
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE,
          EventLogConfig.DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG);

  private static final KafkaPrincipal EVENT_LOG_READER =
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");

  private EventLogClusters.Config eventLogConfig;
  private EventLogClusters eventLogClusters;
  private String clusterId;

  private KafkaConsumer<byte[], CloudEvent> consumer;
  private EventLogger logger;

  @Before
  public void setUp() throws Throwable {
    List<String> otherUsers = Arrays.asList(
        EVENT_LOG_READER.getName(),
        EVENT_LOG_WRITER.getName()
    );
    eventLogConfig = new EventLogClusters.Config()
        .users(BROKER_USER, EventLogConfig.DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG, "eventLogReader");
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (consumer != null) {
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

  private CloudEvent firstReceivedEvent(KafkaConsumer<byte[], CloudEvent> consumer,
      long timeoutMs) {
    long startMs = System.currentTimeMillis();

    while (System.currentTimeMillis() - startMs < timeoutMs) {
      ConsumerRecords<byte[], CloudEvent> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], CloudEvent> record : records) {
        return record.value();
      }
    }
    return null;
  }

  private KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup) {
    Properties consumerProperties = eventLogClusters
        .consumerProps(EVENT_LOG_READER.getName(), consumerGroup);

    KafkaConsumer<byte[], CloudEvent> consumer = new KafkaConsumer<>(consumerProperties);

    consumer.subscribe(Collections.singleton(EventLogConfig.DEFAULT_TOPIC_CONFIG));
    return consumer;

  }

  private EventLogger logger(String name, Map<String, String> configOverrides) {
    HashMap<String, String> config = new HashMap<>();
    Properties producerProperties = eventLogClusters.producerProps(EVENT_LOG_WRITER.getName());
    for (String key : producerProperties.stringPropertyNames()) {
      config.put(EVENT_LOGGER_PREFIX + key, producerProperties.getProperty(key));
    }
    config.put(EventLogConfig.EVENT_LOGGER_CLASS_CONFIG,
        KafkaEventAppender.class.getCanonicalName());
    config
        .put(EventLogConfig.BOOTSTRAP_SERVERS_CONFIG,
            eventLogClusters.kafkaCluster.bootstrapServers());
    config.put(EventLogConfig.TOPIC_REPLICAS_CONFIG, "1");
    config.putAll(configOverrides);
    return EventLogger.logger(name, config);
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
    return CloudEventUtils
        .wrap("event_type", "crn://authority/kafka=source", "crn://authority/kafka=subject", entry);
  }

  @Test
  public void testEventLoggedOnNewTopic() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogConfig);
    initializeEventLogClusters();

    consumer = consumer("event-log");
    logger = logger("testEventLoggedOnNewTopic");

    CloudEvent sentEvent = sampleEvent();
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent, receivedEvent);
  }

  @Test
  public void testEventLoggedOnNewTopicWithoutPermissions() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogConfig);
    initializeEventLogClusters();

    // topic should exist for the reader, but is not the topic we're writing to
    eventLogClusters.kafkaCluster.createTopic(EventLogConfig.DEFAULT_TOPIC_CONFIG, 2, 1);

    consumer = consumer("event-log");
    logger = logger("testEventLoggedOnNewTopic",
        Utils.mkMap(Utils.mkEntry(EventLogConfig.TOPIC_CONFIG, "some_topic")));

    CloudEvent sentEvent = sampleEvent();
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNull(receivedEvent);
  }


  @Test
  public void testEventLoggedOnExistingTopic() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogConfig);
    initializeEventLogClusters();

    eventLogClusters.kafkaCluster.createTopic(EventLogConfig.DEFAULT_TOPIC_CONFIG, 2, 1);

    consumer = consumer("event-log");
    logger = logger("testEventLogged");

    CloudEvent sentEvent = sampleEvent();
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent, receivedEvent);
  }


  private void initializeEventLogClusters() throws Exception {
    this.clusterId = eventLogClusters.kafkaClusterId();
  }

}
