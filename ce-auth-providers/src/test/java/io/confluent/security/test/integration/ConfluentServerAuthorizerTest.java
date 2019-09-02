// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.test.integration;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.test.utils.LdapTestUtils;
import io.confluent.security.test.utils.RbacClusters;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ConfluentServerAuthorizerTest {

  private static final String BROKER_USER = "kafka";
  private static final String DEVELOPER1 = "app1-developer";
  private static final String RESOURCE_OWNER1 = "resourceOwner1";
  private static final String DEVELOPER_GROUP = "app-developers";

  private static final String APP1_TOPIC = "app1-topic";
  private static final String APP1_CONSUMER_GROUP = "app1-consumer-group";
  private static final String APP2_TOPIC = "app2-topic";
  private static final String APP2_CONSUMER_GROUP = "app2-consumer-group";

  private RbacClusters.Config rbacConfig;
  private RbacClusters rbacClusters;
  private String clusterId;

  @Before
  public void setUp() throws Throwable {

    List<String> otherUsers = Arrays.asList(
        DEVELOPER1,
        RESOURCE_OWNER1
    );
    rbacConfig = new RbacClusters.Config()
        .users(BROKER_USER, otherUsers);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (rbacClusters != null)
        rbacClusters.shutdown();
    } finally {
      SecurityTestUtils.clearSecurityConfigs();
      KafkaTestUtils.verifyThreadCleanup();
    }
  }

  @Test
  public void testRbacWithAcls() throws Throwable {
    rbacConfig = rbacConfig.withLdapGroups();
    rbacClusters = new RbacClusters(rbacConfig);
    initializeRbacClusters();

    // Access granted using role for user
    rbacClusters.produceConsume(RESOURCE_OWNER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);

    // Access granted using role for group
    rbacClusters.produceConsume(DEVELOPER1, APP2_TOPIC, APP2_CONSUMER_GROUP, true);

    // Access granted using literal ACL for user
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    addAcls(KafkaPrincipal.USER_TYPE, DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP,
        PatternType.LITERAL);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);

    String auditTopic = "__audit_topic";
    rbacClusters.kafkaCluster.createTopic(auditTopic, 1, 1);

    ClientBuilder clientBuilder = rbacClusters.clientBuilder(DEVELOPER1);
    KafkaPrincipal auditors = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Auditors");
    KafkaPrincipal developer1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, DEVELOPER1);
    try (AdminClient adminClient = clientBuilder.buildAdminClient()) {
      waitForAccess(adminClient, auditTopic, false);

      // Access granted using prefixed ACL for group
      addTopicAcls(auditors, "__audit", PatternType.PREFIXED, AclPermissionType.ALLOW);
      waitForAccess(adminClient, auditTopic, false);

      rbacClusters.updateUserGroup(DEVELOPER1, auditors.getName());
      waitForAccess(adminClient, auditTopic, true);

      // Access denied using literal ACL for user
      addTopicAcls(developer1, auditTopic, PatternType.LITERAL, AclPermissionType.DENY);
      waitForAccess(adminClient, auditTopic, false);

      // Access allowed by role, but denied by ACL
      addTopicAcls(developer1, "app2", PatternType.PREFIXED, AclPermissionType.DENY);
      waitForAccess(adminClient, APP2_TOPIC, false);
    }

    SecurityTestUtils.verifyAuthorizerLicense(rbacClusters.kafkaCluster, LicenseStatus.FREE_TIER);
    RbacTestUtils.verifyMetadataStoreMetrics();
    RbacTestUtils.verifyMetric("failure-start-seconds-ago", "LdapGroupManager", 0, 0);
    RbacTestUtils.verifyMetric("record-send-rate", "KafkaAuthStore", 10, 100000);
    RbacTestUtils.verifyMetric("record-error-rate", "KafkaAuthStore", 0, 0);
  }

  @Test
  public void testLdapServerFailure() throws Throwable {
    rbacConfig = rbacConfig.withLdapGroups()
        .overrideMetadataBrokerConfig(LdapConfig.REFRESH_INTERVAL_MS_PROP, "10")
        .overrideMetadataBrokerConfig(LdapConfig.RETRY_TIMEOUT_MS_PROP, "1000");
    rbacClusters = new RbacClusters(rbacConfig);
    initializeRbacClusters();
    assertNotNull(rbacClusters.miniKdcWithLdapService);
    rbacClusters.miniKdcWithLdapService.stopLdap();
    rbacClusters.waitUntilAccessDenied(DEVELOPER1, APP2_TOPIC);
    RbacTestUtils.verifyMetric("failure-start-seconds-ago", "LdapGroupManager", 1, 20);
    LdapTestUtils.restartLdapServer(rbacClusters.miniKdcWithLdapService);
    rbacClusters.waitUntilAccessAllowed(DEVELOPER1, APP2_TOPIC);
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
    rbacClusters
        .assignRole(AccessRule.GROUP_PRINCIPAL_TYPE, DEVELOPER_GROUP, "DeveloperWrite", clusterId,
            Utils.mkSet(new io.confluent.security.authorizer.ResourcePattern("Topic", "app2",
                PatternType.PREFIXED)));

    rbacClusters.updateUserGroup(DEVELOPER1, DEVELOPER_GROUP);
    rbacClusters.waitUntilAccessAllowed(RESOURCE_OWNER1, APP1_TOPIC);
    rbacClusters.waitUntilAccessAllowed(DEVELOPER1, APP2_TOPIC);
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

  private void addTopicAcls(KafkaPrincipal principal,
      String topic,
      PatternType patternType,
      AclPermissionType permissionType) throws Exception {
    ClientBuilder clientBuilder = rbacClusters.clientBuilder(BROKER_USER);
    try (AdminClient adminClient = clientBuilder.buildAdminClient()) {
      AclBinding topicAcl = new AclBinding(
          new ResourcePattern(ResourceType.TOPIC, topic, patternType),
          new AccessControlEntry(principal.toString(),
              "*", AclOperation.DESCRIBE, permissionType));
      adminClient.createAcls(Collections.singleton(topicAcl)).all().get();
    }
  }

  private boolean canAccess(AdminClient adminClient, String topic) {
    try {
      adminClient.describeTopics(Collections.singleton(topic)).all().get();
      return true;
    } catch (Exception e) {
      assertTrue("Unexpected exception " + e, e.getCause() instanceof AuthorizationException);
      return false;
    }
  }

  private void waitForAccess(AdminClient adminClient, String topic, boolean authorized)
      throws Exception {
    TestUtils.waitForCondition(() -> canAccess(adminClient, topic) == authorized,
        "Access control not applied");
  }

  @Test
  public void testRbacWithCentralizedAcls() throws Throwable {
    rbacConfig = rbacConfig.withLdapGroups();
    rbacClusters = new RbacClusters(rbacConfig);
    initializeRbacClusters();
    KafkaPrincipal dev1Principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, DEVELOPER1);

    rbacClusters.createCentralizedAcl(dev1Principal, "Read", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL), PermissionType.ALLOW);
    rbacClusters.createCentralizedAcl(dev1Principal, "Read", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL), PermissionType.ALLOW);
    rbacClusters.createCentralizedAcl(dev1Principal, "Write", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL), PermissionType.ALLOW);

    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);

    String auditTopic = "__audit_topic";
    rbacClusters.kafkaCluster.createTopic(auditTopic, 1, 1);

    ClientBuilder clientBuilder = rbacClusters.clientBuilder(DEVELOPER1);
    KafkaPrincipal auditors = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Auditors");
    KafkaPrincipal developer1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, DEVELOPER1);
    try (AdminClient adminClient = clientBuilder.buildAdminClient()) {
      waitForAccess(adminClient, auditTopic, false);

      // Access granted using prefixed ACL for group
      addTopicAcls(auditors, "__audit", PatternType.PREFIXED, AclPermissionType.ALLOW);
      KafkaPrincipal grpPrincipal = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "Auditors");

      rbacClusters.createCentralizedAcl(grpPrincipal,
          "Describe",
          clusterId,
          new io.confluent.security.authorizer.ResourcePattern("Topic", "__audit", PatternType.PREFIXED),
          PermissionType.ALLOW);
      waitForAccess(adminClient, auditTopic, false);

      rbacClusters.updateUserGroup(DEVELOPER1, auditors.getName());
      waitForAccess(adminClient, auditTopic, true);

      // Access denied using literal ACL for user
      rbacClusters.createCentralizedAcl(developer1,
          "Describe",
          clusterId,
          new io.confluent.security.authorizer.ResourcePattern("Topic", auditTopic, PatternType.LITERAL),
          PermissionType.DENY);
      waitForAccess(adminClient, auditTopic, false);

      // Access allowed by role, but denied by ACL
      rbacClusters.createCentralizedAcl(developer1,
          "Describe",
          clusterId,
          new io.confluent.security.authorizer.ResourcePattern("Topic", "app2", PatternType.PREFIXED),
          PermissionType.DENY);
      waitForAccess(adminClient, APP2_TOPIC, false);
    }
  }
}

