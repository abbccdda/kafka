package io.confluent.security.audit.provider;

import static org.junit.Assert.assertNotNull;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.crn.ConfluentResourceName;
import io.confluent.crn.CrnAuthorityConfig;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.integration.EventLogClusters;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.test.utils.RbacClusters;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public class ConfluentAuditLogProviderRbacTest {

  private static final String BROKER_USER = "kafka";
  private static final String DEVELOPER1 = "app1-developer";
  private static final String RESOURCE_OWNER1 = "resourceOwner1";
  private static final String DEVELOPER_GROUP = "app-developers";

  private static final String LOG_WRITER_USER = "audit-log-writer";
  private static final String LOG_READER_USER = "audit-log-reader";

  private static final String APP1_TOPIC = "app1-topic";
  private static final String APP1_CONSUMER_GROUP = "app1-consumer-group";
  private static final String APP2_TOPIC = "app2-topic";
  private static final String APP2_CONSUMER_GROUP = "app2-consumer-group";

  private static final String AUTHORITY_NAME = "mds.example.com";

  private RbacClusters.Config rbacConfig;
  private RbacClusters rbacClusters;
  private String clusterId;

  private EventLogClusters.Config eventLogConfig;
  private EventLogClusters eventLogClusters;

  private Queue<AuditLogEntry> entries = new ArrayDeque<>();
  private KafkaConsumer<byte[], CloudEvent> consumer;

  private static final Logger log = LoggerFactory
      .getLogger(ConfluentAuditLogProviderRbacTest.class);

  @Before
  public void setUp() throws Throwable {

    List<String> otherUsers = Arrays.asList(
        DEVELOPER1,
        RESOURCE_OWNER1,
        LOG_READER_USER,
        LOG_WRITER_USER
    );

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
        AuditLogRouterJsonConfig.defaultConfig(
            props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
            AuditLogRouterJsonConfig.DEFAULT_TOPIC,
            AuditLogRouterJsonConfig.DEFAULT_TOPIC));
    rbacConfig.overrideBrokerConfig(EventLogConfig.TOPIC_REPLICAS_CONFIG, "1");

    rbacClusters = new RbacClusters(rbacConfig);
  }

  @After
  public void tearDown() throws Exception {
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

  private void initializeRbacClusters() throws Exception {
    this.clusterId = rbacClusters.kafkaClusterId();
    rbacClusters.kafkaCluster.createTopic(APP1_TOPIC, 2, 1);
    rbacClusters.kafkaCluster.createTopic(APP2_TOPIC, 2, 1);

    initializeRoles();
  }

  private void initializeRoles() throws Exception {
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER1, "ResourceOwner", clusterId,
        Utils.mkSet(
            new io.confluent.security.authorizer.ResourcePattern("Topic", "*", PatternType.LITERAL),
            new io.confluent.security.authorizer.ResourcePattern("Group", "*",
                PatternType.LITERAL)));

    rbacClusters
        .assignRole(AccessRule.GROUP_PRINCIPAL_TYPE, DEVELOPER_GROUP, "DeveloperRead", clusterId,
            Utils.mkSet(new io.confluent.security.authorizer.ResourcePattern("Topic", "app2",
                    PatternType.PREFIXED),
                new io.confluent.security.authorizer.ResourcePattern("Group", "app2",
                    PatternType.PREFIXED)));

    rbacClusters.updateUserGroup(DEVELOPER1, DEVELOPER_GROUP);
    rbacClusters.waitUntilAccessAllowed(RESOURCE_OWNER1, APP1_TOPIC);
    rbacClusters.waitUntilAccessAllowed(DEVELOPER1, APP2_TOPIC);
  }

  private boolean match(AuditLogEntry entry,
      String userName,
      String resourceName,
      String operation,
      AuthorizeResult result,
      PolicyType policyType) {
    boolean success = true;
    if (!userName.equals(entry.getAuthenticationInfo().getPrincipal())) {
      log.debug("{} != {}", userName, entry.getAuthenticationInfo().getPrincipal());
      success = false;
    }
    if (!userName.equals(entry.getAuthenticationInfo().getPrincipal())) {
      log.debug("{} != {}", userName, entry.getAuthenticationInfo().getPrincipal());
      success = false;
    }
    if (!resourceName.equals(entry.getResourceName())) {
      log.debug("{} != {}", resourceName, entry.getResourceName());
      success = false;
    }
    if (!operation.equals(entry.getMethodName())) {
      log.debug("{} != {}", operation, entry.getMethodName());
      success = false;
    }
    if ((result == AuthorizeResult.ALLOWED) != entry.getAuthorizationInfo().getGranted()) {
      log.debug("{} != {}", result, entry.getAuthorizationInfo().getGranted());
      success = false;
    }
    return success;
  }

  private CloudEvent firstMatchingEvent(KafkaConsumer<byte[], CloudEvent> consumer,
      long timeoutMs,
      String userName,
      String resourceName,
      String operation,
      AuthorizeResult result,
      PolicyType policyType) {
    long startMs = System.currentTimeMillis();

    while (System.currentTimeMillis() - startMs < timeoutMs) {
      ConsumerRecords<byte[], CloudEvent> records = consumer.poll(Duration.ofMillis(200));
      for (ConsumerRecord<byte[], CloudEvent> record : records) {
        try {
          AuditLogEntry entry = record.value().getData().unpack(AuditLogEntry.class);
          if (match(entry, userName, resourceName, operation, result, policyType)) {
            log.debug("CloudEvent matched: " + CloudEventUtils.toJsonString(record.value()));
            return record.value();
          } else {
            log.debug("CloudEvent didn't match: " + CloudEventUtils.toJsonString(record.value()));
          }
        } catch (InvalidProtocolBufferException e) {
          log.error("Invalid CloudEvent", e);
        }
      }
    }
    return null;
  }

  private KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup) {
    return consumer(consumerGroup, AuditLogRouterJsonConfig.DEFAULT_TOPIC);
  }

  private KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup, String topic) {
    Properties consumerProperties = eventLogClusters
        .consumerProps(LOG_READER_USER, consumerGroup);

    consumer = new KafkaConsumer<>(consumerProperties);

    consumer.subscribe(Collections.singleton(topic));
    return consumer;
  }

  private void addAcls(String principalType,
      String principalName,
      String topic,
      String consumerGroup,
      PatternType patternType) throws Exception {
    ClientBuilder clientBuilder = rbacClusters.clientBuilder(BROKER_USER);
    KafkaPrincipal principal = new KafkaPrincipal(principalType, principalName);
    KafkaTestUtils.addProducerAcls(clientBuilder, principal, topic, patternType);
    KafkaTestUtils.addConsumerAcls(clientBuilder, principal, topic, consumerGroup, patternType);
  }

  @Test
  public void testAuditLogKafka() throws Throwable {

    initializeRbacClusters();
    consumer("event-log");

    String clusterCrn = ConfluentResourceName.newBuilder()
        .setAuthority(AUTHORITY_NAME)
        .addElement("kafka", rbacClusters.kafkaClusterId())
        .build()
        .toString();

    // Verify inter-broker logs
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + BROKER_USER, clusterCrn, "kafka.ClusterAction",
        AuthorizeResult.ALLOWED, PolicyType.SUPER_USER));

    String app1TopicCrn = ConfluentResourceName.newBuilder()
        .setAuthority(AUTHORITY_NAME)
        .addElement("kafka", rbacClusters.kafkaClusterId())
        .addElement("topic", APP1_TOPIC)
        .build()
        .toString();

    // Verify RBAC authorization logs
    rbacClusters.produceConsume(RESOURCE_OWNER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + RESOURCE_OWNER1, app1TopicCrn, "kafka.Write",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ROLE));

    // Verify deny logs
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + DEVELOPER1, app1TopicCrn, "kafka.Describe",
        AuthorizeResult.DENIED, PolicyType.DENY_ON_NO_RULE));

    // Verify ZK-based ACL logs
    addAcls(KafkaPrincipal.USER_TYPE, DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP,
        PatternType.LITERAL);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + DEVELOPER1, app1TopicCrn, "kafka.Write",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ACL));

    // Verify centralized ACL logs
    KafkaPrincipal dev1Principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, DEVELOPER1);
    String app3Group = "app3-consumer-group";

    String app3GroupCrn = ConfluentResourceName.newBuilder()
        .setAuthority(AUTHORITY_NAME)
        .addElement("kafka", rbacClusters.kafkaClusterId())
        .addElement("group", app3Group)
        .build()
        .toString();

    rbacClusters.createCentralizedAcl(dev1Principal, "Read", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Topic", APP2_TOPIC,
            PatternType.LITERAL), PermissionType.ALLOW);
    rbacClusters.createCentralizedAcl(dev1Principal, "Read", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Group", app3Group,
            PatternType.LITERAL), PermissionType.ALLOW);
    rbacClusters.createCentralizedAcl(dev1Principal, "Write", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Topic", APP2_TOPIC,
            PatternType.LITERAL), PermissionType.ALLOW);
    rbacClusters.produceConsume(DEVELOPER1, APP2_TOPIC, app3Group, true);
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + DEVELOPER1, app3GroupCrn, "kafka.Read",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ACL));

  }
}
