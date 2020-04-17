/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit.provider;

import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_PREFIX;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_NAME_CONFIG;

import io.confluent.events.EventLoggerConfig;
import io.confluent.events.exporter.kafka.KafkaExporter;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.audit.integration.EventLogClusters;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.test.utils.RbacClusters;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This tests the case where audit log messages are sent to a dedicated audit logging cluster using
 * a defined audit logging principal
 */
@Category(IntegrationTest.class)
public class OtherClusterTest extends ClusterTestCommon {

  private EventLogClusters.Config eventLogConfig;
  private EventLogClusters eventLogClusters;

  @Before
  public void setUp() throws Throwable {

    eventLogConfig = new EventLogClusters.Config()
        .users(BROKER_USER, LOG_WRITER_USER, LOG_READER_USER);

    eventLogClusters = new EventLogClusters(eventLogConfig);
    rbacConfig = new RbacClusters.Config()
        .users(BROKER_USER, otherUsers)
        .withLdapGroups()
        .overrideBrokerConfig(CRN_AUTHORITY_NAME_CONFIG, AUTHORITY_NAME);

    Properties props = eventLogClusters.producerProps(LOG_WRITER_USER);
    for (String key : props.stringPropertyNames()) {
      rbacConfig.overrideBrokerConfig(AUDIT_PREFIX + EventLoggerConfig.KAFKA_EXPORTER_PREFIX + key,
          props.getProperty(key));
    }
    rbacConfig.overrideBrokerConfig(AUDIT_PREFIX + EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG,
        KafkaExporter.class.getName());

    rbacConfig.overrideBrokerConfig(AUDIT_EVENT_ROUTER_CONFIG,
        AuditLogRouterJsonConfigUtils.defaultConfigProduceConsumeInterbroker(
            props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
            AUTHORITY_NAME,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            Collections.emptyList()))
        .overrideBrokerConfig(AUDIT_PREFIX + EventLoggerConfig.TOPIC_REPLICAS_CONFIG, "1")
        .overrideBrokerConfig("auto.create.topics.enable", "false");

    rbacClusters = new RbacClusters(rbacConfig);
  }

  @After
  public void tearDown() {
    try {
      closeConsumers();
      if (rbacClusters != null) {
        rbacClusters.shutdown();
      }
      if (eventLogClusters != null) {
        eventLogClusters.shutdown();
      }
    } finally {
      SecurityTestUtils.clearSecurityConfigs();
      KafkaTestUtils.verifyThreadCleanup();
    }
  }

  KafkaConsumer<byte[], byte[]> consumer(String consumerGroup, String topic) {
    Properties consumerProperties = eventLogClusters
        .consumerProps(LOG_READER_USER, consumerGroup);

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
    consumers.add(consumer);

    consumer.subscribe(Collections.singleton(topic));
    return consumer;
  }

  @Test
  public void testProduceConsume() throws Throwable {

    initializeClusters();
    TestUtils.waitForCondition(() -> auditLoggerReady(), "auditLoggerReady");
    KafkaConsumer<byte[], byte[]> consumer = consumer("event-log");

    produceConsume(consumer);
  }
}
