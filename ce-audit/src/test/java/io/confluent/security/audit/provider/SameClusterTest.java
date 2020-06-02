/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.security.audit.provider;

import static io.confluent.security.audit.telemetry.exporter.NonBlockingKafkaExporterConfig.KAFKA_EXPORTER_PREFIX;
import static io.confluent.security.audit.router.AuditLogRouterJsonConfig.DEFAULT_TOPIC;
import static io.confluent.security.audit.router.AuditLogRouterJsonConfig.TOPIC_PREFIX;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_PREFIX;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.CRN_AUTHORITY_NAME_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.crn.ConfluentResourceName;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogRouterJsonConfigUtils;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.test.utils.RbacClusters;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
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

  Set<AdminClient> adminClients = new HashSet<>();

  // app1-topic and app2-topic are used in produceConsume
  static final String APP3_TOPIC = "app3-topic";
  static final String APP4_TOPIC = "app4-topic";
  static final String APP5_TOPIC = "app5-topic";
  static final String APP6_TOPIC = "app6-topic";

  // simple router config that sends audit messages to a non-default topic
  static final String OTHER_TOPIC_ROUTER_CONFIG =
      "{\n"
          + "    \"destinations\": {\n"
          + "        \"topics\": {\n"
          + "            \"confluent-audit-log-events\": {\n"
          + "                \"retention_ms\": 7776000000\n"
          + "            },\n"
          + "            \"confluent-audit-log-events-other\": {\n"
          + "                \"retention_ms\": 7776000000\n"
          + "            }\n"
          + "        }\n"
          + "    },\n"
          + "    \"default_topics\": {\n"
          + "        \"allowed\": \"confluent-audit-log-events-other\",\n"
          + "        \"denied\": \"confluent-audit-log-events-other\"\n"
          + "    }\n"
          + "}";

  // router config that enables produce logging for a single topic
  static final String APP3_ROUTER_CONFIG =
      "{\n"
          + "    \"routes\": {\n"
          + "        \"crn:///kafka=*/topic=app3-topic\": {\n"
          + "            \"produce\": {\n"
          + "                \"allowed\": \"confluent-audit-log-events-app3\",\n"
          + "                \"denied\": \"confluent-audit-log-events-app3\"\n"
          + "            }\n"
          + "        }\n"
          + "    },\n"
          + "    \"destinations\": {\n"
          + "        \"topics\": {\n"
          + "            \"confluent-audit-log-events\": {\n"
          + "                \"retention_ms\": 7776000000\n"
          + "            },\n"
          + "            \"confluent-audit-log-events-app3\": {\n"
          + "                \"retention_ms\": 7776000000\n"
          + "            }\n"
          + "        }\n"
          + "    },\n"
          + "    \"default_topics\": {\n"
          + "        \"allowed\": \"confluent-audit-log-events\",\n"
          + "        \"denied\": \"confluent-audit-log-events\"\n"
          + "    }\n"
          + "}";

  @Before
  public void setUp() {
    rbacConfig = new RbacClusters.Config()
        .users(BROKER_USER, otherUsers)
        // simplify debugging to only have audit log topics on one of the clusters
        .overrideMetadataBrokerConfig(ConfluentConfigs.AUDIT_LOGGER_ENABLE_CONFIG, "false")
        // Because the broker is already shut down by the test by the time the audit log producer's
        // close gets called, the flush in that close waits until the delivery timeout. Make that
        // not take 120 seconds...
        .overrideBrokerConfig(
            AUDIT_PREFIX + KAFKA_EXPORTER_PREFIX + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
            "9000")
        .overrideBrokerConfig(
            AUDIT_PREFIX + KAFKA_EXPORTER_PREFIX + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
            "10000")
        .withLdapGroups();
  }

  @After
  public void tearDown() {
    try {
      closeConsumers();
      for (AdminClient adminClient : adminClients) {
        adminClient.close();
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
                TOPIC_PREFIX, PatternType.PREFIXED),
            new io.confluent.security.authorizer.ResourcePattern("Group", "*",
                PatternType.LITERAL)));

    // give us something to wait on
    rbacClusters.kafkaCluster.createTopic(TOPIC_PREFIX + "_dummy", 1, 1);

    rbacClusters
        .waitUntilAccessAllowed(LOG_READER_USER, TOPIC_PREFIX + "_dummy");
  }

  KafkaConsumer<byte[], byte[]> consumer(String consumerGroup, String topic) {
    Properties consumerProperties = KafkaTestUtils
        .consumerProps(rbacClusters.kafkaCluster.bootstrapServers(),
            rbacClusters.kafkaSecurityProtocol,
            rbacClusters.kafkaSaslMechanism,
            rbacClusters.users.get(LOG_READER_USER).jaasConfig,
            consumerGroup);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties);
    consumers.add(consumer);

    consumer.subscribe(Collections.singleton(topic));
    return consumer;
  }

  private void checkCreateTopicAuditMessageReceived(String topicToCreate, String auditTopic)
      throws CrnSyntaxException {
    KafkaConsumer<byte[], byte[]> consumer = consumer("event-log-" + topicToCreate, auditTopic);

    AdminClient resourceOwnerAdminClient = rbacClusters.clientBuilder(RESOURCE_OWNER1)
        .buildAdminClient();
    adminClients.add(resourceOwnerAdminClient);
    resourceOwnerAdminClient
        .createTopics(Collections.singleton(new NewTopic(topicToCreate, 1, (short) 1)));

    String resourceName = ConfluentResourceName.newBuilder()
        .addElement("kafka", rbacClusters.kafkaClusterId())
        .addElement("topic", topicToCreate)
        .build()
        .toString();

    assertTrue(eventsMatchUnordered(consumer, 30000,
        e -> match(e, "User:" + RESOURCE_OWNER1, resourceName, "kafka.CreateTopics",
            AuthorizeResult.ALLOWED, AuthorizePolicy.PolicyType.ALLOW_ROLE)
    ));
  }

  @Test
  public void testDefaultConfigurationAndReconfigure() throws Throwable {
    // This test is larger than we would normally like, so that we reduce the number
    // of times we set up and tear down the kafka cluster. It tests:
    //
    // - Are the reconfigurable settings exposed to an AdminClient's describeConfigs
    // - Can we dynamically reconfigure from the default to something else
    // - Does the reconfiguration stick around if we reconfigure something unrelated
    // - Can we delete the reconfigured value and get back the default configuration
    // - Does the deletion affect the unrelated configuration

    // Don't configure anything about audit logs. They should be on by default,
    // they should send the logs to the local cluster. They should include management
    // messages only
    rbacClusters = new RbacClusters(rbacConfig);

    initializeClusters();
    TestUtils.waitForCondition(() -> auditLoggerReady(), "auditLoggerReady");

    // Check initial configuration
    ConfigResource key = new ConfigResource(Type.BROKER, "0");
    AdminClient brokerAdminClient = rbacClusters.clientBuilder(BROKER_USER).buildAdminClient();
    adminClients.add(brokerAdminClient);
    Map<ConfigResource, Config> configs = brokerAdminClient
        .describeConfigs(Collections.singleton(key)).all().get();

    // Make sure these configurations appear in the list
    assertEquals(1, configs.size());
    Config config = configs.get(key);
    assertEquals(ConfluentConfigs.CRN_AUTHORITY_NAME_DEFAULT,
        config.get(CRN_AUTHORITY_NAME_CONFIG).value());
    assertEquals(ConfluentConfigs.AUDIT_EVENT_ROUTER_DEFAULT,
        config.get(ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG).value());
    assertEquals(ConfluentConfigs.AUDIT_LOGGER_ENABLE_DEFAULT,
        config.get(ConfluentConfigs.AUDIT_LOGGER_ENABLE_CONFIG).value());

    // Check that Audit Logging works
    checkCreateTopicAuditMessageReceived(APP3_TOPIC, DEFAULT_TOPIC);

    // Check dynamic reconfiguration
    AuditLogRouterJsonConfig newConfig = AuditLogRouterJsonConfig.load(OTHER_TOPIC_ROUTER_CONFIG);
    String newConfigJson = newConfig.toJsonString();

    // Make sure this is reconfigurable as expected
    ConfigResource cluster = new ConfigResource(Type.BROKER, "");
    AlterConfigsResult result = brokerAdminClient.incrementalAlterConfigs(
        Utils.mkMap(Utils.mkEntry(cluster,
            Collections.singleton(
                new AlterConfigOp(new ConfigEntry(ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG,
                    newConfigJson), OpType.SET)))));
    result.values().get(cluster).get();

    // Wait for config change to be applied to broker since this is async
    TestUtils.waitForCondition(
        () -> {
          ConfigEntry routerConfig = singleConfig(brokerAdminClient, cluster,
              ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG);
          return routerConfig != null && routerConfig.value().equals(newConfigJson);
        },
        ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG + " not updated");

    ConfigEntry configEntry = singleConfig(brokerAdminClient, cluster,
        ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG);
    assertEquals(newConfigJson, configEntry.value());

    // Check that Audit Logging works on the expected topic
    checkCreateTopicAuditMessageReceived(APP4_TOPIC, "confluent-audit-log-events-other");

    // Make sure that an unrelated reconfiguration doesn't break our reconfiguration
    AlterConfigsResult result2 = brokerAdminClient.incrementalAlterConfigs(
        Utils.mkMap(Utils.mkEntry(cluster,
            Collections.singleton(
                new AlterConfigOp(new ConfigEntry("background.threads",
                    "20"), OpType.SET)))));
    result2.values().get(cluster).get();

    TestUtils.waitForCondition(
        () -> singleConfig(brokerAdminClient, cluster, "background.threads") != null,
        "background.threads not updated");

    configEntry = singleConfig(brokerAdminClient, cluster, "background.threads");
    assertEquals("20", configEntry.value());
    configEntry = singleConfig(brokerAdminClient, cluster,
        ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG);
    assertEquals(newConfigJson, configEntry.value());

    // Check that Audit Logging works on the expected topic
    checkCreateTopicAuditMessageReceived(APP5_TOPIC, "confluent-audit-log-events-other");

    // Make sure that we can delete the reconfigured value
    AlterConfigsResult result3 = brokerAdminClient.incrementalAlterConfigs(
        Utils.mkMap(Utils.mkEntry(cluster,
            Collections.singleton(
                new AlterConfigOp(
                    new ConfigEntry(ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG, "not used"),
                    OpType.DELETE)))));
    result.values().get(cluster).get();

    // Make sure that that delete didn't do something strange to the unrelated configuration
    configEntry = singleConfig(brokerAdminClient, cluster, "background.threads");
    assertEquals("20", configEntry.value());

    TestUtils.waitForCondition(
        () -> singleConfig(brokerAdminClient, cluster, ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG)
            == null,
        ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG + " not updated");

    // This should reset us to the default config, which still allows audit logging
    checkCreateTopicAuditMessageReceived(APP6_TOPIC, DEFAULT_TOPIC);
  }

  private ConfigEntry singleConfig(AdminClient adminClient, ConfigResource cluster, String config)
      throws Exception {
    Map<ConfigResource, Config> describedConfigs = adminClient
        .describeConfigs(Collections.singleton(cluster)).all().get();
    Config clusterConfig = describedConfigs.get(cluster);
    assertNotNull("Cluster config is null", clusterConfig);
    return clusterConfig.get(config);
  }

  @Test
  public void testProduceConsume() throws Throwable {

    rbacConfig.overrideBrokerConfig(ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG,
        AuditLogRouterJsonConfigUtils.defaultConfigProduceConsumeInterbroker(
            null,
            AUTHORITY_NAME,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            Collections.singletonList(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, LOG_READER_USER))))
        .overrideBrokerConfig(CRN_AUTHORITY_NAME_CONFIG, AUTHORITY_NAME);

    rbacClusters = new RbacClusters(rbacConfig);

    initializeClusters();
    TestUtils.waitForCondition(() -> auditLoggerReady(), "auditLoggerReady");

    KafkaConsumer<byte[], byte[]> consumer = consumer("event-log");

    produceConsume(consumer);
  }


  @Test
  public void testInterbroker() throws Throwable {

    rbacConfig.overrideBrokerConfig(ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG,
        AuditLogRouterJsonConfigUtils.defaultConfigProduceConsumeInterbroker(
            null,
            AUTHORITY_NAME,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            Collections.singletonList(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, LOG_READER_USER))))
        .overrideBrokerConfig(CRN_AUTHORITY_NAME_CONFIG, AUTHORITY_NAME)
        .withKafkaServers(3);

    rbacClusters = new RbacClusters(rbacConfig);

    initializeClusters();
    TestUtils.waitForCondition(() -> auditLoggerReady(), "auditLoggerReady");

    KafkaConsumer<byte[], byte[]> consumer = consumer("event-log");
    consumers.add(consumer);

    rbacClusters.clientBuilder(BROKER_USER).buildAdminClient()
        .createTopics(Collections.singleton(new NewTopic(APP3_TOPIC, 1, (short) 3)));

    String app3TopicCrn = ConfluentResourceName.newBuilder()
        .setAuthority(AUTHORITY_NAME)
        .addElement("kafka", rbacClusters.kafkaClusterId())
        .addElement("topic", APP3_TOPIC)
        .build()
        .toString();

    String clusterCrn = ConfluentResourceName.newBuilder()
        .setAuthority(AUTHORITY_NAME)
        .addElement("kafka", rbacClusters.kafkaClusterId())
        .build()
        .toString();

    assertTrue(eventsMatchUnordered(consumer, 30000,
        e -> match(e, "User:" + BROKER_USER, app3TopicCrn, "kafka.CreateTopics",
            AuthorizeResult.ALLOWED, PolicyType.SUPER_USER),
        e -> match(e, "User:" + BROKER_USER, clusterCrn, "kafka.LeaderAndIsr",
            AuthorizeResult.ALLOWED, PolicyType.SUPER_USER),
        e -> match(e, "User:" + BROKER_USER, clusterCrn, "kafka.FetchFollower",
            AuthorizeResult.ALLOWED, PolicyType.SUPER_USER),
        e -> match(e, "User:" + BROKER_USER, clusterCrn, "kafka.UpdateMetadata",
            AuthorizeResult.ALLOWED, PolicyType.SUPER_USER)
    ));
  }
}
