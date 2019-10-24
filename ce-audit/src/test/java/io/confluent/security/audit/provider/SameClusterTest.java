package io.confluent.security.audit.provider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.crn.ConfluentResourceName;
import io.confluent.crn.CrnAuthorityConfig;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.audit.serde.CloudEventProtoSerde;
import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.test.utils.RbacClusters;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This tests the case where the bootstrap servers are not set up for audit logging and the log
 * messages are sent to the same cluster using the interbroker user.
 */
@Category(IntegrationTest.class)
public class SameClusterTest extends ClusterTestCommon {

  static final String APP3_TOPIC = "app3-topic";

  @Before
  public void setUp() {
    rbacConfig = new RbacClusters.Config()
        .users(BROKER_USER, otherUsers)
        // simplify debugging to only have audit log topics on one of the clusters
        .overrideMetadataBrokerConfig(EventLogConfig.EVENT_LOGGER_ENABLED_CONFIG, "false")
        .withLdapGroups();
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
    } finally {
      SecurityTestUtils.clearSecurityConfigs();
      KafkaTestUtils.verifyThreadCleanup();
    }
  }

  void initializeClusters() throws Exception {
    super.initializeClusters();

    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, LOG_READER_USER, "DeveloperRead", clusterId,
        Utils.mkSet(
            new io.confluent.security.authorizer.ResourcePattern("Topic",
                AuditLogRouterJsonConfig.TOPIC_PREFIX, PatternType.PREFIXED),
            new io.confluent.security.authorizer.ResourcePattern("Group", "*",
                PatternType.LITERAL)));

    // give us something to wait on
    rbacClusters.kafkaCluster.createTopic(AuditLogRouterJsonConfig.TOPIC_PREFIX + "_dummy", 1, 1);

    rbacClusters
        .waitUntilAccessAllowed(LOG_READER_USER, AuditLogRouterJsonConfig.TOPIC_PREFIX + "_dummy");
  }

  KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup, String topic) {
    Properties consumerProperties = KafkaTestUtils
        .consumerProps(rbacClusters.kafkaCluster.bootstrapServers(),
            rbacClusters.kafkaSecurityProtocol,
            rbacClusters.kafkaSaslMechanism,
            rbacClusters.users.get(LOG_READER_USER).jaasConfig,
            consumerGroup);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        CloudEventProtoSerde.class.getName());

    consumer = new KafkaConsumer<>(consumerProperties);

    consumer.subscribe(Collections.singleton(topic));
    return consumer;
  }

  @Test
  public void testUnconfigured() throws Throwable {
    // Don't configure anything about audit logs. They should be on by default,
    // they should send the logs to the local cluster. They should include management
    // messages only
    rbacClusters = new RbacClusters(rbacConfig);

    initializeClusters();
    TestUtils.waitForCondition(() -> auditLoggerReady(), "auditLoggerReady");

    consumer("event-log");

    rbacClusters.clientBuilder(RESOURCE_OWNER1).buildAdminClient()
        .createTopics(Collections.singleton(new NewTopic(APP3_TOPIC, 1, (short) 1)));

    String app3TopicCrn = ConfluentResourceName.newBuilder()
        .addElement("kafka", rbacClusters.kafkaClusterId())
        .addElement("topic", APP3_TOPIC)
        .build()
        .toString();

    assertTrue(eventsMatched(consumer, 30000, Collections.singletonList(
        e -> match(e, "User:" + RESOURCE_OWNER1, app3TopicCrn, "kafka.CreateTopics",
            AuthorizeResult.ALLOWED, PolicyType.ALLOW_ROLE)
    )));

    // Consume message should *not* be received
    rbacClusters.produceConsume(RESOURCE_OWNER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    assertFalse(eventsMatched(consumer, 10000, Collections.singletonList(
        e -> "kafka.CreateTopics".equals(e.getMethodName())
    )));
  }

  @Test
  public void testProduceConsume() throws Throwable {

    rbacConfig
        .overrideBrokerConfig(
            EventLogConfig.ROUTER_CONFIG,
            AuditLogRouterJsonConfigUtils.defaultConfigProduceConsumeInterbroker(
                null,
                AUTHORITY_NAME,
                AuditLogRouterJsonConfig.DEFAULT_TOPIC,
                AuditLogRouterJsonConfig.DEFAULT_TOPIC,
                Collections.singletonList(
                    new KafkaPrincipal(KafkaPrincipal.USER_TYPE, LOG_READER_USER))))
        .overrideBrokerConfig(CrnAuthorityConfig.AUTHORITY_NAME_PROP, AUTHORITY_NAME);

    rbacClusters = new RbacClusters(rbacConfig);

    initializeClusters();
    TestUtils.waitForCondition(() -> auditLoggerReady(), "auditLoggerReady");

    consumer("event-log");

    produceConsume();
  }
}
