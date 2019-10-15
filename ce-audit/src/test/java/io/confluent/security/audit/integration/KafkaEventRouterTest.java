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
import io.confluent.security.audit.AuthorizationInfo;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.EventLogger;
import io.confluent.security.audit.appender.KafkaEventAppender;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
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
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class KafkaEventRouterTest {

  private static final String BROKER_USER = "kafka";

  private static final KafkaPrincipal EVENT_LOG_WRITER =
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE,
          EventLogConfig.DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG);

  private static final KafkaPrincipal EVENT_LOG_READER =
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "eventLogReader");

  private EventLogClusters.Config eventLogConfig;
  private EventLogClusters eventLogClusters;

  private KafkaConsumer<byte[], CloudEvent> consumer;
  private EventLogger logger;
  private String json = "{\"destinations\":{\"topics\":{\"_confluent-audit-log_success\":{\"retention_ms\":2592000000},\"_confluent-audit-log_failure\":{\"retention_ms\":2592000000},\"_confluent-audit-log_ksql\":{\"retention_ms\":2592000000},\"_confluent-audit-log_connect_success\":{\"retention_ms\":2592000000},\"_confluent-audit-log_connect_failure\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_produce_allowed\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_produce_denied\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_consume_allowed\":{\"retention_ms\":15552000000},\"_confluent-audit-log_clicks_consume_denied\":{\"retention_ms\":15552000000},\"_confluent-audit-log_accounting\":{\"retention_ms\":15552000000},\"_confluent-audit-log_cluster\":{\"retention_ms\":15552000000}}},\"default_topics\":{\"allowed\":\"_confluent-audit-log_success\",\"denied\":\"_confluent-audit-log_failure\"},\"excluded_principals\":[\"User:Alice\",\"User:service_account_id\"],\"routes\":{\"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/ksql=ksql1\":{\"authorize\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_ksql\"}},\"crn://mds1.example.com/kafka=vBmKJkYpSNW+cRw0z4BrBQ/connect=*\":{\"authorize\":{\"allowed\":\"_confluent-audit-log_connect_success\",\"denied\":\"_confluent-audit-log_connect_failure\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=clicks\":{\"produce\":{\"allowed\":\"_confluent-audit-log_clicks_produce_allowed\",\"denied\":\"_confluent-audit-log_clicks_produce_denied\"},\"consume\":{\"allowed\":\"_confluent-audit-log_clicks_consume_allowed\",\"denied\":\"_confluent-audit-log_clicks_consume_denied\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=accounting-*\":{\"produce\":{\"allowed\":null,\"denied\":\"_confluent-audit-log_accounting\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=*\":{\"produce\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"consume\":{\"denied\":\"_confluent-audit-log_cluster\"}},\"crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA\":{\"interbroker\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"other\":{\"denied\":\"_confluent-audit-log_cluster\"}},\"crn://mds1.example.com/kafka=*\":{\"interbroker\":{\"allowed\":\"\",\"denied\":\"_confluent-audit-log_cluster\"},\"other\":{\"denied\":\"_confluent-audit-log_cluster\"}}},\"metadata\":{\"resource_version\":\"f109371d0a856a40a2a96cca98f90ec2\",\"updated_at\":\"2019-08-21T18:31:47+00:00\"}}";

  @Before
  public void setUp() throws Throwable {
    List<String> otherUsers = Arrays.asList(
        EVENT_LOG_READER.getName(),
        EVENT_LOG_WRITER.getName()
    );
    eventLogConfig = new EventLogClusters.Config()
        .users(BROKER_USER, EventLogConfig.DEFAULT_EVENT_LOG_PRINCIPAL_CONFIG, "eventLogReader");
    eventLogClusters = new EventLogClusters(eventLogConfig);
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

  private KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup, String topic) {
    Properties consumerProperties = eventLogClusters
        .consumerProps(EVENT_LOG_READER.getName(), consumerGroup);

    KafkaConsumer<byte[], CloudEvent> consumer = new KafkaConsumer<>(consumerProperties);

    consumer.subscribe(Collections.singleton(topic));
    return consumer;

  }

  private EventLogger logger(String name, Map<String, String> configOverrides) {
    HashMap<String, String> config = new HashMap<>();
    Properties producerProperties = eventLogClusters.producerProps(EVENT_LOG_WRITER.getName());
    for (String key : producerProperties.stringPropertyNames()) {
      config.put(EVENT_LOGGER_PREFIX + key, producerProperties.getProperty(key));
    }
    config.put(EventLogConfig.EVENT_APPENDER_CLASS_CONFIG,
        KafkaEventAppender.class.getCanonicalName());
    config.put(EventLogConfig.TOPIC_REPLICAS_CONFIG, "1");
    config.put(EventLogConfig.ROUTER_CONFIG, json);
    config.putAll(configOverrides);
    return EventLogger.logger(name, config);
  }

  private EventLogger logger(String name) {
    return logger(name, Collections.emptyMap());
  }

  private CloudEvent sampleEvent(String subject, String method, String principal, boolean granted) {
    return CloudEventUtils
        .wrap("io.confluent.security.authorization",
            "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
            subject,
            AuditLogEntry.newBuilder()
                .setMethodName(method)
                .setAuthenticationInfo(AuthenticationInfo.newBuilder()
                    .setPrincipal(principal)
                    .build())
                .setAuthorizationInfo(AuthorizationInfo.newBuilder()
                    .setGranted(granted))
                .build());
  }


  @Test
  public void testEventLoggedOnDefaultTopic() throws Throwable {
    // This is the default topic configured in the config, not EventLogConfig.DEFAULT_TOPIC_CONFIG
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_success";
    eventLogClusters.kafkaCluster.createTopic(expectedTopic, 2, 1);

    consumer = consumer("event-log", expectedTopic);
    logger = logger("testEventLoggedOnDefaultTopic");

    CloudEvent sentEvent = sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW-cRw0z4BrBQ",
        "CreateTopics", "User:Bob", true);
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent, receivedEvent);
  }

  @Test
  public void testEventLoggedOnRoutedTopic() throws Throwable {
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_cluster";
    eventLogClusters.kafkaCluster.createTopic(expectedTopic, 2, 1);

    consumer = consumer("event-log", expectedTopic);
    logger = logger("testEventLoggedOnRoutedTopic");

    CloudEvent sentEvent = sampleEvent("crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA",
        "CreateTopics", "User:Bob", false);
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNotNull(receivedEvent);
    assertEquals(sentEvent, receivedEvent);
  }

  @Test
  public void testEventSuppressedTopic() throws Throwable {
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_success";
    eventLogClusters.kafkaCluster.createTopic(expectedTopic, 2, 1);

    consumer = consumer("event-log", expectedTopic);
    logger = logger("testEventSuppressedTopic");

    CloudEvent sentEvent = sampleEvent(
        "crn://mds1.example.com/kafka=63REM3VWREiYtMuVxZeplA/topic=payroll",
        "kafka.Produce", "User:Bob", true);
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNull(receivedEvent);
  }

  @Test
  public void testEventExcludedUser() throws Throwable {
    String expectedTopic = AuditLogRouterJsonConfig.TOPIC_PREFIX + "_success";
    eventLogClusters.kafkaCluster.createTopic(expectedTopic, 2, 1);

    consumer = consumer("event-log", expectedTopic);
    logger = logger("testEventExcludedUser");

    CloudEvent sentEvent = sampleEvent("crn://mds1.example.com/kafka=vBmKJkYpSNW-cRw0z4BrBQ",
        "CreateTopics", "User:Alice", true);
    logger.log(sentEvent);

    CloudEvent receivedEvent = firstReceivedEvent(consumer, 10000);
    assertNull(receivedEvent);
  }


}
