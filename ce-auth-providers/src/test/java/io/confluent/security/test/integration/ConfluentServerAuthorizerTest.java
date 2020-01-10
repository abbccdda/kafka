// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.test.integration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import io.confluent.security.auth.provider.audit.MockAuditLogProvider;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.authorizer.AclAccessRule;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizePolicy.NoMatchingRule;
import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.AuthorizePolicy.SuperUser;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.authorizer.provider.AuthorizationLogData;
import io.confluent.security.rbac.RbacAccessRule;
import io.confluent.security.test.utils.LdapTestUtils;
import io.confluent.security.test.utils.RbacClusters;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
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

    SecurityTestUtils.verifyConfluentLicense(rbacClusters.kafkaCluster, LicenseStatus.FREE_TIER);
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

  @Test
  public void testCentralizedAclsOnly() throws Throwable {
    rbacConfig = rbacConfig
        .overrideBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT")
        .overrideMetadataBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "CONFLUENT");
    verifyAcls(false);
  }

  @Test
  public void testCentralizedAclsMigrationDisabled() throws Throwable {
    rbacConfig = rbacConfig
        .overrideBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ZK_ACL,CONFLUENT")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.MIGRATE_ACLS_FROM_ZK_PROP, "false")
        .overrideMetadataBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ZK_ACL,CONFLUENT");
    verifyAcls(true);
  }

  @Test
  public void testCentralizedAclsMigrationEnabled() throws Throwable {
    rbacConfig = rbacConfig
        .overrideBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ZK_ACL,CONFLUENT")
        .overrideBrokerConfig(ConfluentAuthorizerConfig.MIGRATE_ACLS_FROM_ZK_PROP, "true")
        .overrideMetadataBrokerConfig(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, "ZK_ACL,CONFLUENT");
    verifyAcls(true);
  }

  private void verifyAcls(boolean zkAclsEnabled) throws Throwable {

    if (zkAclsEnabled) {
      String brokerPrincipal = KafkaPrincipal.USER_TYPE + ':' + BROKER_USER;
      AclBinding clusterAcl = new AclBinding(
          new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
          new AccessControlEntry(brokerPrincipal, "*", AclOperation.ALL, AclPermissionType.ALLOW));
      AclBinding readAcl = new AclBinding(
          new ResourcePattern(ResourceType.TOPIC, "*", PatternType.LITERAL),
          new AccessControlEntry(brokerPrincipal, "*", AclOperation.READ, AclPermissionType.ALLOW));
      AclBinding metadataTopicAcl = new AclBinding(
          new ResourcePattern(ResourceType.TOPIC, "_confluent", PatternType.PREFIXED),
          new AccessControlEntry(brokerPrincipal, "*", AclOperation.ALL, AclPermissionType.ALLOW));
      AclBinding metadataGroupAcl = new AclBinding(
          new ResourcePattern(ResourceType.GROUP, "_confluent", PatternType.PREFIXED),
          new AccessControlEntry(brokerPrincipal, "*", AclOperation.ALL, AclPermissionType.ALLOW));

      rbacConfig = rbacConfig
          .overrideBrokerConfig(ConfluentAuthorizerConfig.BROKER_USERS_PROP, "")
          .overrideMetadataBrokerConfig(ConfluentAuthorizerConfig.BROKER_USERS_PROP, "")
          .withMetadataBrokerAcls(Arrays.asList(clusterAcl, readAcl, metadataTopicAcl, metadataGroupAcl))
          .withBrokerAcls(Arrays.asList(clusterAcl, readAcl));
    }
    rbacClusters = new RbacClusters(rbacConfig);

    this.clusterId = rbacClusters.kafkaClusterId();
    rbacClusters.kafkaCluster.createTopic(APP1_TOPIC, 2, 1);

    rbacClusters.waitUntilAccessDenied(DEVELOPER1, APP1_TOPIC);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    addAcls(KafkaPrincipal.USER_TYPE, DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, PatternType.LITERAL);
    rbacClusters.waitUntilAccessAllowed(DEVELOPER1, APP1_TOPIC);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);

    KafkaPrincipal developer1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, DEVELOPER1);
    removeTopicAcls(developer1, APP1_TOPIC, PatternType.LITERAL, AclPermissionType.ALLOW);
    rbacClusters.waitUntilAccessDenied(DEVELOPER1, APP1_TOPIC);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
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

  private void removeTopicAcls(KafkaPrincipal principal,
      String topic,
      PatternType patternType,
      AclPermissionType permissionType) throws Exception {
    ClientBuilder clientBuilder = rbacClusters.clientBuilder(BROKER_USER);
    try (AdminClient adminClient = clientBuilder.buildAdminClient()) {
      AclBindingFilter filter =
          new AclBindingFilter(
              new ResourcePattern(ResourceType.TOPIC, topic, patternType).toFilter(),
              new AccessControlEntryFilter(principal.toString(),
                  "*", AclOperation.ANY, permissionType));
      adminClient.deleteAcls(Collections.singleton(filter)).all().get();
    }
  }

  private boolean canAccess(Admin adminClient, String topic) {
    try {
      adminClient.describeTopics(Collections.singleton(topic)).all().get();
      return true;
    } catch (Exception e) {
      assertTrue("Unexpected exception " + e, e.getCause() instanceof AuthorizationException);
      return false;
    }
  }

  private void waitForAccess(Admin adminClient, String topic, boolean authorized)
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

  @Test
  public void testAuditLog() throws Throwable {
    rbacConfig = rbacConfig.withLdapGroups()
        .overrideBrokerConfig(MockAuditLogProvider.AUDIT_LOG_SIZE_CONFIG, "10000");
    rbacClusters = new RbacClusters(rbacConfig);
    initializeRbacClusters();

    // Verify inter-broker logs
    verifyAuditLogEntry(BROKER_USER, "kafka-cluster", "ClusterAction",
        AuthorizeResult.ALLOWED, PolicyType.SUPER_USER);

    // Verify RBAC authorization logs
    rbacClusters.produceConsume(RESOURCE_OWNER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    verifyAuditLogEntry(RESOURCE_OWNER1, APP1_TOPIC, "Write",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ROLE);

    // Verify deny logs
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    verifyAuditLogEntry(DEVELOPER1, APP1_TOPIC, "Describe",
        AuthorizeResult.DENIED, PolicyType.DENY_ON_NO_RULE);

    // Verify ZK-based ACL logs
    addAcls(KafkaPrincipal.USER_TYPE, DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, PatternType.LITERAL);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    verifyAuditLogEntry(DEVELOPER1, APP1_TOPIC, "Write",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ACL);

    // Verify centralized ACL logs
    KafkaPrincipal dev1Principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, DEVELOPER1);
    String app3Group = "app3-consumer-group";
    rbacClusters.createCentralizedAcl(dev1Principal, "Read", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Topic", APP2_TOPIC, PatternType.LITERAL), PermissionType.ALLOW);
    rbacClusters.createCentralizedAcl(dev1Principal, "Read", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Group", app3Group, PatternType.LITERAL), PermissionType.ALLOW);
    rbacClusters.createCentralizedAcl(dev1Principal, "Write", clusterId,
        new io.confluent.security.authorizer.ResourcePattern("Topic", APP2_TOPIC, PatternType.LITERAL), PermissionType.ALLOW);
    rbacClusters.produceConsume(DEVELOPER1, APP2_TOPIC, app3Group, true);
    verifyAuditLogEntry(DEVELOPER1, app3Group, "Read",
        AuthorizeResult.ALLOWED, PolicyType.ALLOW_ACL);

    ClientBuilder clientBuilder = rbacClusters.clientBuilder(BROKER_USER);
    try (AdminClient adminClient = clientBuilder.buildAdminClient()) {
      ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER,
          String.valueOf(rbacClusters.kafkaCluster.brokers().get(0).config().brokerId()));

      // Verify provider reconfiguration
      ConfigEntry entry = new ConfigEntry(MockAuditLogProvider.AUDIT_LOG_SIZE_CONFIG, "20000");
      Collection<AlterConfigOp> configs = Collections.singleton(new AlterConfigOp(entry, OpType.SET));
      adminClient.incrementalAlterConfigs(Collections.singletonMap(broker, configs)).all().get();
      TestUtils.waitForCondition(() -> MockAuditLogProvider.logSize == 20000, "Custom config not updated");

      // Verify that invalid reconfiguration fails
      ConfigEntry invalidEntry = new ConfigEntry(MockAuditLogProvider.AUDIT_LOG_SIZE_CONFIG, "invalid");
      Collection<AlterConfigOp> invalidConfigs = Collections.singleton(new AlterConfigOp(invalidEntry, OpType.SET));
      ExecutionException e = assertThrows(ExecutionException.class, () ->
          adminClient.incrementalAlterConfigs(Collections.singletonMap(broker, invalidConfigs)).all().get());
      assertTrue("Unexpected exception " + e.getCause(), e.getCause() instanceof InvalidRequestException);
    }
  }

  private void verifyAuditLogEntry(String userName,
      String resourceName,
      String operation,
      AuthorizeResult result,
      PolicyType policyType) {
    Queue<AuthorizationLogData> entries = MockAuditLogProvider.AUDIT_LOG;
    assertFalse("No audit log entries", entries.isEmpty());
    boolean hasEntry = entries.stream().anyMatch(e -> e.requestContext.principal().getName().equals(userName) &&
        e.action.resourceName().equals(resourceName) &&
        e.action.operation().name().equals(operation) &&
        e.authorizeResult == result &&
        e.authorizePolicy.policyType() == policyType);
    assertTrue("Entry not found for " + policyType + ", logs=" + entries, hasEntry);
    entries.forEach(this::verifyAuditLogEntry);
  }

  private void verifyAuditLogEntry(AuthorizationLogData data) {
    AuthorizePolicy policy = data.authorizePolicy;
    switch (policy.policyType()) {
      case SUPER_USER:
      case SUPER_GROUP:
        assertTrue("Unexpected policy " + policy, policy instanceof SuperUser);
        assertNotNull(((SuperUser) policy).principal());
        break;
      case NO_MATCHING_RULE:
      case ALLOW_ON_NO_RULE:
      case DENY_ON_NO_RULE:
        assertTrue("Unexpected policy " + policy, policy instanceof NoMatchingRule);
        break;
      case ALLOW_ACL:
      case DENY_ACL:
        assertTrue("Unexpected policy " + policy, policy instanceof AclAccessRule);
        break;
      case ALLOW_ROLE:
        assertTrue("Unexpected policy " + policy, policy instanceof RbacAccessRule);
        break;
      default:
          fail("Unexpected authorize policy in audit log entry: " + policy);
    }
  }
}

