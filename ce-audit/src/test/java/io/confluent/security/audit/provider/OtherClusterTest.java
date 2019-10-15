package io.confluent.security.audit.provider;

import io.confluent.crn.CrnAuthorityConfig;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.EventLogConfig;
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
        .overrideBrokerConfig(CrnAuthorityConfig.AUTHORITY_NAME_PROP, AUTHORITY_NAME);

    Properties props = eventLogClusters.producerProps(LOG_WRITER_USER);
    for (String key : props.stringPropertyNames()) {
      rbacConfig.overrideBrokerConfig(EventLogConfig.EVENT_LOGGER_PREFIX + key,
          props.getProperty(key));
    }
    rbacConfig.overrideBrokerConfig(
        EventLogConfig.ROUTER_CONFIG,
        AuditLogRouterJsonConfigUtils.defaultConfigProduceConsumeInterbroker(
            props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
            AUTHORITY_NAME,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            Collections.emptyList()));
    rbacConfig.overrideBrokerConfig(EventLogConfig.TOPIC_REPLICAS_CONFIG, "1");

    rbacClusters = new RbacClusters(rbacConfig);
  }

  @After
  public void tearDown() {
    try {
      if (consumer != null) {
        consumer.close();
      }
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

  KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup, String topic) {
    Properties consumerProperties = eventLogClusters
        .consumerProps(LOG_READER_USER, consumerGroup);

    consumer = new KafkaConsumer<>(consumerProperties);

    consumer.subscribe(Collections.singleton(topic));
    return consumer;
  }

  @Test
  public void testProduceConsume() throws Throwable {

    initializeClusters();
    TestUtils.waitForCondition(() -> auditLoggerReady(), "auditLoggerReady");
    consumer("event-log");

    produceConsume();
  }
}
