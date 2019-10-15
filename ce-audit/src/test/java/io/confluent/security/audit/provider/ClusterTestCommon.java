package io.confluent.security.audit.provider;

import static org.junit.Assert.assertNotNull;

import io.confluent.crn.ConfluentResourceName;
import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.test.utils.RbacClusters;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Shared code for Cluster Tests */
abstract class ClusterTestCommon {

  private static final Logger log = LoggerFactory.getLogger(ClusterTestCommon.class);

  static final String BROKER_USER = "kafka";
  static final String DEVELOPER1 = "app1-developer";
  static final String RESOURCE_OWNER1 = "resourceOwner1";
  static final String DEVELOPER_GROUP = "app-developers";
  static final String LOG_WRITER_USER = "audit-log-writer";
  static final String LOG_READER_USER = "audit-log-reader";
  static final String APP1_TOPIC = "app1-topic";
  static final String APP1_CONSUMER_GROUP = "app1-consumer-group";
  static final String APP2_TOPIC = "app2-topic";
  static final String AUTHORITY_NAME = "mds.example.com";

  static List<String> otherUsers = Arrays.asList(
      DEVELOPER1,
      RESOURCE_OWNER1,
      LOG_READER_USER,
      LOG_WRITER_USER
  );

  RbacClusters.Config rbacConfig;
  RbacClusters rbacClusters;
  String clusterId;
  KafkaConsumer<byte[], CloudEvent> consumer;


  static boolean match(AuditLogEntry entry,
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

  static CloudEvent firstMatchingEvent(KafkaConsumer<byte[], CloudEvent> consumer,
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
        } catch (IOException e) {
          log.error("Invalid CloudEvent", e);
        }
      }
    }
    return null;
  }

  void addAcls(String principalType,
      String principalName,
      String topic,
      String consumerGroup,
      PatternType patternType) throws Exception {
    ClientBuilder clientBuilder = rbacClusters.clientBuilder(BROKER_USER);
    KafkaPrincipal principal = new KafkaPrincipal(principalType, principalName);
    KafkaTestUtils.addProducerAcls(clientBuilder, principal, topic, patternType);
    KafkaTestUtils.addConsumerAcls(clientBuilder, principal, topic, consumerGroup, patternType);
  }

  boolean auditLoggerReady() {
    try {
      if (rbacClusters.kafkaCluster.brokers().isEmpty()) {
        return false;
      }
      for (KafkaServer broker : rbacClusters.kafkaCluster.brokers()) {
        ConfluentServerAuthorizer authorizer =
            (ConfluentServerAuthorizer) broker.authorizer().get();
        ConfluentAuditLogProvider provider =
            (ConfluentAuditLogProvider) authorizer.auditLogProvider();
        if (!provider.localFileLoggerReady() || !provider.kafkaLoggerReady()) {
          return false;
        }
      }
      return true;
    } catch (ClassCastException e) {
      return false;
    }
  }


  void initializeClusters() throws Exception {
    this.clusterId = rbacClusters.kafkaClusterId();
    rbacClusters.kafkaCluster.createTopic(APP1_TOPIC, 2, 1);
    rbacClusters.kafkaCluster.createTopic(APP2_TOPIC, 2, 1);

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

  abstract KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup, String topic);

  KafkaConsumer<byte[], CloudEvent> consumer(String consumerGroup) {
    return consumer(consumerGroup, AuditLogRouterJsonConfig.DEFAULT_TOPIC);
  }

  void produceConsume() throws Throwable {

    String app1TopicCrn = ConfluentResourceName.newBuilder()
        .setAuthority(AUTHORITY_NAME)
        .addElement("kafka", rbacClusters.kafkaClusterId())
        .addElement("topic", APP1_TOPIC)
        .build()
        .toString();

    // Verify RBAC authorization logs
    rbacClusters.produceConsume(RESOURCE_OWNER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + RESOURCE_OWNER1, app1TopicCrn, "kafka.OffsetFetch",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ROLE));
    rbacClusters.produceConsume(RESOURCE_OWNER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + RESOURCE_OWNER1, app1TopicCrn, "kafka.Produce",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ROLE));

    // Verify deny logs
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + DEVELOPER1, app1TopicCrn, "kafka.Metadata",
        AuthorizeResult.DENIED, PolicyType.DENY_ON_NO_RULE));

    // Verify ZK-based ACL logs
    addAcls(KafkaPrincipal.USER_TYPE, DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP,
        PatternType.LITERAL);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    assertNotNull(firstMatchingEvent(consumer, 10000,
        "User:" + DEVELOPER1, app1TopicCrn, "kafka.Produce",
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
        "User:" + DEVELOPER1, app3GroupCrn, "kafka.OffsetFetch",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ACL));
  }
}
