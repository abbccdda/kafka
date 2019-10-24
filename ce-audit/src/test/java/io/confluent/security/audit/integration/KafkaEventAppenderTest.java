// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.audit.integration;

import static io.confluent.security.audit.EventLogConfig.EVENT_LOGGER_PREFIX;
import static io.confluent.security.audit.EventLogConfig.ROUTER_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.audit.AuthenticationInfo;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.EventLogger;
import io.confluent.security.audit.appender.KafkaEventAppender;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;
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

  private static final KafkaPrincipal EVENT_LOG_READER =
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");

  private EventLogClusters.Config eventLogClustersConfig;
  private EventLogClusters eventLogClusters;

  private Set<KafkaConsumer<byte[], CloudEvent>> consumers = new HashSet<>();
  private EventLogger logger;

  @Before
  public void setUp() {
    eventLogClustersConfig = new EventLogClusters.Config()
        .users(BROKER_USER, EventLogConfig.DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG, "eventLogReader");
  }

  @After
  public void tearDown() throws Exception {
    try {
      for (KafkaConsumer<byte[], CloudEvent> consumer : consumers) {
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
    return consumer(consumerGroup, AuditLogRouterJsonConfig.DEFAULT_TOPIC);
  }

  private KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup, String topic) {
    Properties consumerProperties = eventLogClusters
        .consumerProps(EVENT_LOG_READER.getName(), consumerGroup);

    KafkaConsumer<byte[], CloudEvent> consumer = new KafkaConsumer<>(consumerProperties);

    consumer.subscribe(Collections.singleton(topic));
    consumers.add(consumer);
    return consumer;
  }

  private Map<String, String> loggerConfig(String user, Map<String, String> configOverrides) {
    HashMap<String, String> config = new HashMap<>();
    Properties producerProperties = eventLogClusters
        .producerProps(user);
    for (String key : producerProperties.stringPropertyNames()) {
      config.put(EVENT_LOGGER_PREFIX + key, producerProperties.getProperty(key));
    }
    config.put(EventLogConfig.EVENT_APPENDER_CLASS_CONFIG,
        KafkaEventAppender.class.getCanonicalName());
    config.put(EventLogConfig.TOPIC_REPLICAS_CONFIG, "1");
    config.put(EventLogConfig.ROUTER_CONFIG, AuditLogRouterJsonConfigUtils.defaultConfig(
        eventLogClusters.kafkaCluster.bootstrapServers(),
        AuditLogRouterJsonConfig.DEFAULT_TOPIC,
        AuditLogRouterJsonConfig.DEFAULT_TOPIC));
    config.putAll(configOverrides);
    return config;
  }

  private EventLogger logger(String name, String user, Map<String, String> configOverrides) {
    return EventLogger.logger(name, loggerConfig(user, configOverrides));
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
    return CloudEventUtils
        .wrap("event_type", "crn://authority/kafka=source", "crn://authority/kafka=subject", entry);
  }

  @Test
  public void testEventLoggedOnNewTopic() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    KafkaConsumer<byte[], CloudEvent> consumer = consumer("event-log");
    logger = logger("testEventLoggedOnNewTopic");

    CloudEvent sentEvent = sampleEvent();
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent, receivedEvent);
  }

  @Test
  public void testEventLoggedWithoutPermissions() throws Throwable {
    // Try to log with the credentials of an existing user, who doesn't have the right permissions
    String eventLogPrincipal = "User:not_write";

    eventLogClusters = new EventLogClusters(eventLogClustersConfig);
    eventLogClusters.createLogReaderUser(eventLogPrincipal);

    // create this so the eventLogReader doesn't fail
    eventLogClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.DEFAULT_TOPIC, 2, 1);

    KafkaConsumer<byte[], CloudEvent> consumer = consumer("event-log");
    logger = logger("testEventLoggedWithoutPermissions", eventLogPrincipal, Collections.emptyMap());

    CloudEvent sentEvent = sampleEvent();
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNull(receivedEvent);
  }


  @Test
  public void testEventLoggedOnExistingTopic() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    eventLogClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.DEFAULT_TOPIC, 2, 1);

    KafkaConsumer<byte[], CloudEvent> consumer = consumer("event-log");
    logger = logger("testEventLoggedOnExistingTopic");

    CloudEvent sentEvent = sampleEvent();
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent, receivedEvent);
  }


  @Test
  public void testSuppressAuditLoggerPrincipalEvents() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    eventLogClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.DEFAULT_TOPIC, 2, 1);

    AuditLogEntry entry = AuditLogEntry.newBuilder()
        .setAuthenticationInfo(AuthenticationInfo.newBuilder()
            .setPrincipal(EventLogConfig.DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG)
            .build())
        .build();
    CloudEvent sentEvent = CloudEventUtils
        .wrap("event_type", "crn://authority/kafka=source", "crn://authority/kafka=subject", entry);

    KafkaConsumer<byte[], CloudEvent> consumer = consumer("event-log");
    logger = logger("testSuppressAuditLoggerPrincipalEvents");

    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNull(receivedEvent);
  }


  @Test
  public void testSuppressAuditLoggerNondefaultPrincipalEvents() throws Throwable {
    String eventLogPrincipal = "User:my_event_log_principal";

    eventLogClustersConfig = eventLogClustersConfig
        .users(BROKER_USER, eventLogPrincipal, "eventLogReader")
        .setAuditLogPrincipal(eventLogPrincipal);

    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    eventLogClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.DEFAULT_TOPIC, 2, 1);

    /* The Default principal now appears in logs... */
    AuditLogEntry entry = AuditLogEntry.newBuilder()
        .setAuthenticationInfo(AuthenticationInfo.newBuilder()
            .setPrincipal(EventLogConfig.DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG)
            .build())
        .build();
    CloudEvent sentEvent = CloudEventUtils
        .wrap("event_type", "crn://authority/kafka=source", "crn://authority/kafka=subject", entry);

    KafkaConsumer<byte[], CloudEvent> consumer = consumer("event-log");
    logger = logger("testSuppressAuditLoggerNondefaultPrincipalEvents",
        Utils.mkMap(Utils.mkEntry(EventLogConfig.EVENT_LOG_PRINCIPAL_CONFIG, eventLogPrincipal)));

    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent, receivedEvent);

    /* But the custom principal does not */
    AuditLogEntry entry2 = AuditLogEntry.newBuilder()
        .setAuthenticationInfo(AuthenticationInfo.newBuilder()
            .setPrincipal(eventLogPrincipal)
            .build())
        .build();
    CloudEvent sentEven2 = CloudEventUtils
        .wrap("event_type", "crn://authority/kafka=source", "crn://authority/kafka=subject",
            entry2);

    CloudEvent receivedEvent2 = firstReceivedEvent(consumer, 10000);
    assertNull(receivedEvent2);
  }

  @Test
  public void testReconfigure() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    KafkaConsumer<byte[], CloudEvent> consumer1 = consumer("event-log-1");
    logger = logger("testReconfigure");

    CloudEvent sentEvent1 = sampleEvent();
    logger.log(sentEvent1);

    CloudEvent receivedEvent1 = firstReceivedEvent(consumer1, 30000);
    assertNotNull(receivedEvent1);
    assertEquals(sentEvent1, receivedEvent1);

    String newAllowedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "__allowed_new";
    String newDeniedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "__denied_new";

    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfig(
                eventLogClusters.kafkaCluster.bootstrapServers(),
                newAllowedTopic,
                newDeniedTopic))
    );
    logger.reconfigure(loggerConfig(eventLogClusters.logWriterUser(), config));

    CloudEvent sentEvent2 = sampleEvent();
    assertNotEquals(sentEvent1, sentEvent2); // should have different IDs and timestamps
    logger.log(sentEvent2);

    KafkaConsumer<byte[], CloudEvent> consumer2 = consumer("event-log-2", newDeniedTopic);
    CloudEvent receivedEvent2 = firstReceivedEvent(consumer2, 10000);
    assertNotNull(receivedEvent2);
    assertEquals(sentEvent2, receivedEvent2);

    CloudEvent receivedEvent3 = firstReceivedEvent(consumer1, 10000);
    assertNull(receivedEvent3);
  }

  @Test
  public void testReconfigureFailure() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    KafkaConsumer<byte[], CloudEvent> consumer1 = consumer("event-log-1");
    logger = logger("testReconfigureFailure");

    CloudEvent sentEvent1 = sampleEvent();
    logger.log(sentEvent1);

    CloudEvent receivedEvent1 = firstReceivedEvent(consumer1, 10000);
    assertNotNull(receivedEvent1);
    assertEquals(sentEvent1, receivedEvent1);

    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(ROUTER_CONFIG, "{}")
    );
    assertThrows(ConfigException.class, () ->
        logger.reconfigure(loggerConfig(eventLogClusters.logWriterUser(), config)));

    CloudEvent sentEvent2 = sampleEvent();
    assertNotEquals(sentEvent1, sentEvent2); // should have different IDs and timestamps
    logger.log(sentEvent2);

    CloudEvent receivedEvent2 = firstReceivedEvent(consumer1, 10000);
    assertNotNull(receivedEvent2);
    assertEquals(sentEvent2, receivedEvent2);
  }

  @Test
  public void testReconfigureEmptyTopics() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    KafkaConsumer<byte[], CloudEvent> consumer1 = consumer("event-log-1");
    logger = logger("testReconfigureEmptyTopics");

    CloudEvent sentEvent1 = sampleEvent();
    logger.log(sentEvent1);

    CloudEvent receivedEvent1 = firstReceivedEvent(consumer1, 10000);
    assertNotNull(receivedEvent1);
    assertEquals(sentEvent1, receivedEvent1);

    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfig(
                eventLogClusters.kafkaCluster.bootstrapServers(),
                "",
                ""))
    );
    logger.reconfigure(loggerConfig(eventLogClusters.logWriterUser(), config));

    CloudEvent sentEvent2 = sampleEvent();
    assertNotEquals(sentEvent1, sentEvent2); // should have different IDs and timestamps
    logger.log(sentEvent2);

    CloudEvent receivedEvent2 = firstReceivedEvent(consumer1, 10000);
    assertNull(receivedEvent2);
  }

  @Test
  public void testMultiBroker() throws Throwable {
    eventLogClustersConfig.setNumBrokers(3);
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);
    assertEquals(3, eventLogClusters.kafkaCluster.bootstrapServers().split(",").length);

    KafkaConsumer<byte[], CloudEvent> consumer = consumer("event-log");
    logger = logger("testMultiBroker");

    AuditLogRouterJsonConfig jsonConfig = AuditLogRouterJsonConfig.load(
        AuditLogRouterJsonConfigUtils.defaultConfig(
            eventLogClusters.kafkaCluster.bootstrapServers(),
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC));
    // test roundtrip from "a,b,c" to ["a", "b", "c"]
    assertEquals(eventLogClusters.kafkaCluster.bootstrapServers(),
        jsonConfig.bootstrapServers());

    CloudEvent sentEvent = sampleEvent();
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent, receivedEvent);
  }

  @Test
  public void testDontCreateTopicsFailure() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    // This is no longer a failure. We now retry later
    //assertThrows(ConfigException.class, () ->
    logger("testDontCreateTopicsFailure",
        Utils.mkMap(Utils.mkEntry(EventLogConfig.TOPIC_CREATE_CONFIG, "false")));
  }

  @Test
  public void testDontCreateTopicsAlreadyCreated() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    eventLogClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.DEFAULT_TOPIC, 2, 1);

    logger = logger("testDontCreateTopicsAlreadyCreated",
        Utils.mkMap(Utils.mkEntry(EventLogConfig.TOPIC_CREATE_CONFIG, "false")));
  }

  @Test
  public void testDontCreateTopicsNoTopics() throws Throwable {
    eventLogClusters = new EventLogClusters(eventLogClustersConfig);

    Map<String, String> config = Utils.mkMap(
        Utils.mkEntry(EventLogConfig.TOPIC_CREATE_CONFIG, "false"),
        Utils.mkEntry(ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfig(
                eventLogClusters.kafkaCluster.bootstrapServers(),
                "",
                ""))
    );
    logger = logger("testDontCreateTopicsNoTopics", config);
  }
}
