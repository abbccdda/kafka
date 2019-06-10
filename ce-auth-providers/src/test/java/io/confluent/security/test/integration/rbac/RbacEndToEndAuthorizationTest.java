// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.test.integration.rbac;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.kafka.test.utils.KafkaTestUtils.ClientBuilder;
import io.confluent.kafka.test.utils.SecurityTestUtils;
import io.confluent.license.License;
import io.confluent.license.test.utils.LicenseTestUtils;
import io.confluent.license.validator.ConfluentLicenseValidator.LicenseStatus;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.test.utils.RbacClusters;
import io.confluent.security.test.utils.RbacClusters.Config;
import io.confluent.security.test.utils.RbacTestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import kafka.log.LogConfig$;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({License.class})
@PowerMockIgnore({"javax.*", "sun.*"})
public class RbacEndToEndAuthorizationTest {

  private static final String BROKER_USER = "kafka";
  private static final String SYSTEM_ADMIN = "root";
  private static final String DEVELOPER1 = "app1-developer";
  private static final String DEVELOPER2 = "app2-developer";
  private static final String RESOURCE_OWNER = "resourceOwner1";
  private static final String OPERATOR = "operator1";
  private static final String CLUSTER_ADMIN = "clusterAdmin1";
  private static final String DEVELOPER_GROUP = "app-developers";

  private static final String APP1_TOPIC = "app1-topic";
  private static final String APP1_CONSUMER_GROUP = "app1-consumer-group";
  private static final String APP2_TOPIC = "app2-topic";
  private static final String APP2_CONSUMER_GROUP = "app2-consumer-group";

  private Config config;
  private RbacClusters rbacClusters;
  private String clusterId;

  @Before
  public void setUp() throws Throwable {
    List<String> otherUsers = Arrays.asList(
        SYSTEM_ADMIN,
        DEVELOPER1,
        DEVELOPER2,
        RESOURCE_OWNER,
        OPERATOR,
        CLUSTER_ADMIN
    );
    config = new Config()
        .users(BROKER_USER, otherUsers);
    LicenseTestUtils.injectPublicKey();
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
  public void testProduceConsumeWithRbac() throws Throwable {
    setupRbacClusters(1);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(DEVELOPER2, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    rbacClusters.produceConsume(DEVELOPER2, APP2_TOPIC, APP2_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(SYSTEM_ADMIN, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(SYSTEM_ADMIN, APP2_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(RESOURCE_OWNER, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.produceConsume(RESOURCE_OWNER, APP2_TOPIC, APP1_CONSUMER_GROUP, true);
    SecurityTestUtils.verifyAuthorizerLicense(rbacClusters.kafkaCluster, LicenseStatus.LICENSE_ACTIVE);
  }

  @Test
  public void testClusterScopedRoles() throws Throwable {
    setupRbacClusters(1);
    rbacClusters.produceConsume(OPERATOR, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    rbacClusters.produceConsume(CLUSTER_ADMIN, APP1_TOPIC, APP1_CONSUMER_GROUP, false);

    KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, OPERATOR);
    ClientBuilder clientBuilder = rbacClusters.clientBuilder(CLUSTER_ADMIN);
    try (AdminClient adminClient = clientBuilder.buildAdminClient()) {
      // Verify that ClusterAdmin can create topic using cluster-scoped role
      String topic = "sometopic";
      NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
      adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

      // Verify that ClusterAdmin can alter topic using cluster-scoped role with topic operation
      ConfigResource topicResource = new ConfigResource(Type.TOPIC, topic);
      AlterConfigOp alterOp = new AlterConfigOp(new ConfigEntry(LogConfig$.MODULE$.FlushMsProp(), "1000"), AlterConfigOp.OpType.SET);
      adminClient.incrementalAlterConfigs(Collections.singletonMap(topicResource, Collections.singletonList(alterOp))).all().get();

      // Verify that ClusterAdmin can grant ACL-based access
      AclBinding topicAcl = new AclBinding(
          new org.apache.kafka.common.resource.ResourcePattern(ResourceType.TOPIC, APP1_TOPIC, PatternType.LITERAL),
          new AccessControlEntry(principal.toString(),
              "*", AclOperation.ALL, AclPermissionType.ALLOW));
      AclBinding groupAcl = new AclBinding(
          new org.apache.kafka.common.resource.ResourcePattern(ResourceType.GROUP, APP1_CONSUMER_GROUP, PatternType.LITERAL),
          new AccessControlEntry(principal.toString(),
              "*", AclOperation.ALL, AclPermissionType.ALLOW));
      adminClient.createAcls(Arrays.asList(topicAcl, groupAcl)).all().get();
    }
    rbacClusters.produceConsume(OPERATOR, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
  }

  @Test
  public void testProduceConsumeWithGroupRoles() throws Throwable {
    setupRbacClusters(1);
    rbacClusters.updateUserGroup(DEVELOPER2, DEVELOPER_GROUP);
    rbacClusters.assignRole(AccessRule.GROUP_PRINCIPAL_TYPE, DEVELOPER_GROUP, "DeveloperRead", clusterId,
        Utils.mkSet(new ResourcePattern("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL),
            new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL)));
    rbacClusters.assignRole(AccessRule.GROUP_PRINCIPAL_TYPE, DEVELOPER_GROUP, "DeveloperWrite", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL)));
    rbacClusters.waitUntilAccessAllowed(DEVELOPER2, APP1_TOPIC);
    rbacClusters.produceConsume(DEVELOPER2, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
  }

  @Test
  public void testAuthorizationWithRolesInOtherScopes() throws Throwable {
    setupRbacClusters(1);
    createAdditionalRoles("confluent/core/anotherCluster");
    createAdditionalRoles("confluent/anotherDepartment/testCluster");
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
  }

  @Test
  public void testAuthWriterFailover() throws Throwable {
    setupRbacClusters(2);
    rbacClusters.produceConsume(DEVELOPER1, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    rbacClusters.restartMasterWriter();

    rbacClusters.produceConsume(DEVELOPER2, APP1_TOPIC, APP1_CONSUMER_GROUP, false);
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "ResourceOwner", clusterId,
        Utils.mkSet(new ResourcePattern("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL),
            new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL)));
    rbacClusters.waitUntilAccessAllowed(DEVELOPER2, APP1_TOPIC);
    rbacClusters.produceConsume(DEVELOPER2, APP1_TOPIC, APP1_CONSUMER_GROUP, true);
    RbacTestUtils.verifyMetadataStoreMetrics();
  }

  private void setupRbacClusters(int numMetadataServers) throws Exception {
    for (int i = 1; i < numMetadataServers; i++)
      config = config.addMetadataServer();
    rbacClusters = new RbacClusters(config.withLicense());

    rbacClusters.kafkaCluster.createTopic(APP1_TOPIC, 2, 1);
    rbacClusters.kafkaCluster.createTopic(APP2_TOPIC, 2, 1);
    clusterId = rbacClusters.kafkaClusterId();

    initializeRoles();
  }

  private void initializeRoles() throws Exception {
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "DeveloperRead", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL),
            new ResourcePattern("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "DeveloperWrite", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "DeveloperRead", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", "app2", PatternType.PREFIXED),
            new ResourcePattern("Group", "app2", PatternType.PREFIXED)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "DeveloperWrite", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", "app2", PatternType.PREFIXED)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER, "ResourceOwner", clusterId,
        Utils.mkSet(new ResourcePattern("Topic", "*", PatternType.LITERAL),
            new ResourcePattern("Group", "*", PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, OPERATOR, "Operator", clusterId, Collections.emptySet());
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, CLUSTER_ADMIN, "ClusterAdmin", clusterId, Collections.emptySet());
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, SYSTEM_ADMIN, "SystemAdmin", clusterId, Collections.emptySet());

    rbacClusters.waitUntilAccessAllowed(DEVELOPER1, APP1_TOPIC);
    rbacClusters.waitUntilAccessAllowed(DEVELOPER2, APP2_TOPIC);
    rbacClusters.waitUntilAccessAllowed(RESOURCE_OWNER, APP1_TOPIC);
    rbacClusters.waitUntilAccessAllowed(SYSTEM_ADMIN, APP1_TOPIC);
  }

  private void createAdditionalRoles(String cluster) throws Exception {
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER1, "DeveloperRead", cluster,
        Utils.mkSet(new ResourcePattern("Topic", APP1_TOPIC, PatternType.LITERAL),
            new ResourcePattern("Group", APP1_CONSUMER_GROUP, PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, DEVELOPER2, "DeveloperRead", cluster,
        Utils.mkSet(new ResourcePattern("Topic", "app2", PatternType.PREFIXED),
            new ResourcePattern("Group", "app2", PatternType.PREFIXED)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, RESOURCE_OWNER, "ResourceOwner", cluster,
        Utils.mkSet(new ResourcePattern("Topic", "*", PatternType.LITERAL),
            new ResourcePattern("Group", "*", PatternType.LITERAL)));
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, OPERATOR, "Operator", cluster,
        Collections.emptySet());
    rbacClusters.assignRole(KafkaPrincipal.USER_TYPE, SYSTEM_ADMIN, "SystemAdmin", cluster,
        Collections.emptySet());
  }
}

